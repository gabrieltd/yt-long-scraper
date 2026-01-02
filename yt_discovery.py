import argparse
import asyncio
import json
from pathlib import Path
from urllib.parse import quote

from playwright.async_api import async_playwright

import db
from dotenv import load_dotenv
import os
async def run(query: str, *, headless: bool, limit: int | None = None) -> list[dict]:
	async with async_playwright() as p:
		print("⌛ Scraping iniciado con query: " + query)

		browser = await p.chromium.launch(headless=headless)
		page = await browser.new_page()
		os.makedirs("debug", exist_ok=True)

		try:
			await page.goto(
				f"https://www.youtube.com/results?search_query={quote(query)}",
				wait_until="domcontentloaded",
			)
			await page.screenshot(
				path="debug/01_after_goto.png",
				full_page=True
			)
		
			# Optional UI-driven filters (Spanish YouTube UI)
			await page.get_by_role("button", name="Filtros de búsqueda").click()
			await page.get_by_role("link", name="Este mes").click()
			await page.wait_for_timeout(800)  # Wait for filter to apply
			await page.get_by_role("button", name="Filtros de búsqueda").click()
			await page.wait_for_timeout(800)  # Wait for filter to apply
			await page.get_by_role("link", name="Más de 20 minutos").click()
			await page.wait_for_timeout(800)  # Wait for filter to apply

			# Scroll to bottom until message 'No hay más resultados' or 'No more results' is found
			while True:
				# Scroll down by evaluating scroll on the ytd-app element
				await page.evaluate("document.querySelector('ytd-app').scrollIntoView({block: 'end', behavior: 'smooth'});")
				# Wait for results to load
				await asyncio.sleep(2)

				# Check for 'No more results' message
				no_more_results = await page.locator(
					"xpath=//yt-formatted-string[contains(text(), 'No hay más resultados') or contains(text(), 'No more results')]"
				).count()
				if no_more_results > 0:
					break

			await page.wait_for_selector("ytd-video-renderer")

			results: list[dict] = await page.evaluate(
				"""
								(query) => {
									return Array.from(document.querySelectorAll('ytd-video-renderer')).map(video => {
										const videoLink = video.querySelector('a#video-title')?.href;
										const videoId = videoLink
											? new URL(videoLink).searchParams.get('v')
											: null;

										const channelAnchors = [
										...video.querySelectorAll(
											'a#channel-thumbnail[href], ytd-channel-name a[href], a[href^="/@"], a[href^="/channel/"], a[href^="/c/"]'
										)
										];

										// deduplicar
										const channels = Array.from(
										new Map(
											channelAnchors.map(a => [
											a.getAttribute('href'),
											{
												name: a.textContent?.trim() || null,
												url: 'https://www.youtube.com' + a.getAttribute('href')
											}
											])
										).values()
										);

										const duration = video
											.querySelector('ytd-thumbnail-overlay-time-status-renderer badge-shape div')
											?.textContent.trim() || null;

										const meta = video.querySelectorAll(
											'#metadata-line span.inline-metadata-item'
										);

										const viewsText = meta[0]?.textContent.trim() || null;
										const publishedText = meta[1]?.textContent.trim() || null;

										const videoType =
											duration && duration.includes(':')
												? 'video'
												: 'short';

										return {
										  query,
										  video_id: videoId,
										  channels,              // <-- CAMBIO CLAVE
										  duration,
										  published_text: publishedText,
										  views_text: viewsText,
										  video_type: videoType,
										  is_multi_creator: channels.length > 1
										};
									});
								}
				""",
				query,
			)

			if limit is not None:
				return results[: max(0, limit)]
			return results
		except: 
			await page.screenshot(
				path="debug/02_no_filters_button.png",
				full_page=True
			)
			return []
		finally:
			html = await page.content()
			with open("debug/03_html.html", "w", encoding="utf-8") as f:
				f.write(html)
			await browser.close()


def parse_args() -> argparse.Namespace:
		parser = argparse.ArgumentParser(description="Scrape YouTube search results via Playwright")
		parser.add_argument("--query", "-q", default="documental", help="YouTube search query")
		parser.add_argument(
				"--limit",
				"-n",
				type=int,
				default=None,
				help="Max number of results to output (default: all captured)",
		)

		headless_group = parser.add_mutually_exclusive_group()
		headless_group.add_argument(
				"--headless",
				action="store_true",
				default=True,
				help="Run browser in headless mode (default)",
		)
		headless_group.add_argument(
				"--headed",
				action="store_true",
				default=False,
				help="Run browser with UI (not headless)",
		)

		parser.add_argument(
				"--out",
				"-o",
				type=Path,
				default=None,
				help="Write JSON output to a file instead of stdout",
		)
		return parser.parse_args()


def main() -> None:
		args = parse_args()
		headless = False if args.headed else True

		async def _main_async() -> None:
			# DB lifecycle is intentionally handled via db.py (no SQL here).
			load_dotenv()
			await db.init_db()
			search_run_id = await db.create_search_run(args.query, mode="exploration")
			try:
				results = await run(args.query, headless=headless, limit=args.limit)
				print(f"✅ Scraping completado. {len(results)} resultados encontrados.")
				inserted, ignored = await db.insert_videos_raw(search_run_id, results)
				print(f"💾 Insertados en DB: {inserted}. Ignorados por repetidos: {ignored}.")
				payload = json.dumps(results, ensure_ascii=False, indent=2)

				if args.out:
					args.out.parent.mkdir(parents=True, exist_ok=True)
					args.out.write_text(payload, encoding="utf-8")
					print(f"📃 Resultados escritos en el archivo: {args.out}")	
			finally:
				await db.finish_search_run(search_run_id)
				await db.close_db()

		asyncio.run(_main_async())


if __name__ == "__main__":
		main()
	
