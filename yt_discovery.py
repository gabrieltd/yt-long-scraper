import argparse
import asyncio
import json
import sys
from pathlib import Path
from urllib.parse import quote

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

import db
from dotenv import load_dotenv


BLOCKED_RESOURCE_TYPES = {"font", "image", "media"}
SCROLL_WAIT_TIMEOUT_MS = 2_000


async def _result_state(page, no_more_message: str) -> dict[str, int | bool]:
	return await page.evaluate(
		"""
		(message) => ({
			count: document.querySelectorAll('ytd-video-renderer').length,
			noMore: Array.from(document.querySelectorAll('yt-formatted-string'))
				.some(node => node.textContent?.includes(message))
		})
		""",
		no_more_message,
	)


async def _wait_for_result_change(
	page,
	*,
	previous_count: int,
	no_more_message: str,
) -> dict[str, int | bool]:
	"""Wait only as long as YouTube needs to append the next result batch."""
	try:
		await page.wait_for_function(
			"""
			([count, message]) =>
				document.querySelectorAll('ytd-video-renderer').length > count ||
				Array.from(document.querySelectorAll('yt-formatted-string'))
					.some(node => node.textContent?.includes(message))
			""",
			arg=[previous_count, no_more_message],
			timeout=SCROLL_WAIT_TIMEOUT_MS,
			polling=100,
		)
	except PlaywrightTimeoutError:
		pass
	return await _result_state(page, no_more_message)


async def _scroll_until_complete(
	page,
	*,
	limit: int | None,
	no_more_message: str,
	max_scroll_seconds: int = 300,
	max_stale_scrolls: int = 5,
) -> int:
	"""Load search results until the requested boundary or YouTube exhaustion."""
	started_at = asyncio.get_running_loop().time()
	stale_scrolls = 0
	state = await _result_state(page, no_more_message)
	previous_count = int(state["count"])

	while True:
		current_count = int(state["count"])
		if limit is not None and current_count >= max(0, limit):
			return current_count
		if state["noMore"]:
			return current_count

		await page.evaluate(
			"window.scrollTo(0, document.documentElement.scrollHeight);"
		)
		state = await _wait_for_result_change(
			page,
			previous_count=previous_count,
			no_more_message=no_more_message,
		)
		current_count = int(state["count"])
		elapsed = asyncio.get_running_loop().time() - started_at
		if elapsed >= max_scroll_seconds:
			print(
				f"⚠️ Scroll timeout after {int(elapsed)}s — "
				"collecting results found so far."
			)
			return current_count

		if current_count <= previous_count:
			stale_scrolls += 1
			if stale_scrolls >= max_stale_scrolls:
				print(
					f"⚠️ No new results after {max_stale_scrolls} scrolls "
					f"({current_count} videos) — stopping scroll."
				)
				return current_count
		else:
			stale_scrolls = 0
		previous_count = current_count


async def _save_debug_artifacts(
	page,
	*,
	screenshot_name: str | None = None,
	include_html: bool = True,
) -> None:
	"""Best-effort diagnostics that never turn a scrape result into a failure."""
	try:
		debug_dir = Path("debug")
		debug_dir.mkdir(parents=True, exist_ok=True)
		if screenshot_name:
			await page.screenshot(path=str(debug_dir / screenshot_name), full_page=True)
		if include_html:
			html = await page.content()
			(debug_dir / "03_html.html").write_text(html, encoding="utf-8")
	except Exception as exc:
		print(f"⚠️ Could not write debug artifacts: {exc}")

# Language configuration for bilingual support
LANG_CONFIG = {
    "en-US": {
        "locale": "en-US",
        "timezone": "America/New_York",
        "accept_language": "en-US,en;q=0.9",
        "ui": {
            "search_filters": "Search filters",
            "no_more_results": "No more results",
        },
        "filters": {
            "upload_date": {
                "last_hour": "Last hour",
                "today": "Today",
                "this_week": "This week",
                "this_month": "This month",
                "this_year": "This year"
            },
            "duration": {
                "under_4": "Under 3 minutes",
                "4_20": "3 - 20 minutes",
                "over_20": "Over 20 minutes"
            },
            "features": {
                "live": "Live",
                "4k": "4K",
                "hd": "HD",
                "subtitles": "Subtitles/CC",
                "creative_commons": "Creative Commons",
                "360": "360°",
                "vr180": "VR180",
                "3d": "3D",
                "hdr": "HDR",
                "location": "Location",
                "purchased": "Purchased"
            },
            "sort_by": {
                "relevance": "Relevance",
                "upload_date": "Upload date",
                "view_count": "View count",
                "rating": "Rating"
            }
        },
        "messages": {
            "scraping_started": "⌛ Scraping started with query: ",
            "scraping_completed": "✅ Scraping completed. {} results found.",
            "db_inserted": "💾 Inserted in DB: {}. Ignored as duplicates: {}.",
            "results_written": "📃 Results written to file: {}"
        }
    },
    "es-MX": {
        "locale": "es-MX",
        "timezone": "America/Mexico_City",
        "accept_language": "es-MX,es;q=0.9",
        "ui": {
            "search_filters": "Filtros de búsqueda",
            "no_more_results": "No hay más resultados",
        },
        "filters": {
            "upload_date": {
                "last_hour": "Última hora",
                "today": "Hoy",
                "this_week": "Esta semana",
                "this_month": "Este mes",
                "this_year": "Este año"
            },
            "duration": {
                "under_4": "Menos de 3 minutos",
                "4_20": "De 3 a 20 minutos",
                "over_20": "Más de 20 minutos"
            },
            "features": {
                "live": "En directo",
                "4k": "4K",
                "hd": "HD",
                "subtitles": "Subtítulos",
                "creative_commons": "Creative Commons",
                "360": "360°",
                "vr180": "VR180",
                "3d": "3D",
                "hdr": "HDR",
                "location": "Ubicación",
                "purchased": "Comprado"
            },
            "sort_by": {
                "relevance": "Relevancia",
                "upload_date": "Fecha de subida",
                "view_count": "Recuento de visualizaciones",
                "rating": "Calificación"
            }
        },
        "messages": {
            "scraping_started": "⌛ Scraping iniciado con query: ",
            "scraping_completed": "✅ Scraping completado. {} resultados encontrados.",
            "db_inserted": "💾 Insertados en DB: {}. Ignorados por repetidos: {}.",
            "results_written": "📃 Resultados escritos en el archivo: {}"
        }
    }
}
async def run(
    query: str,
    *,
    headless: bool,
    limit: int | None = None,
    lang: str = "es-MX",
    upload_date: str | None = None,
    duration: str | None = None,
    features: list[str] | None = None,
    sort_by: str | None = None,
    debug_artifacts: bool = False,
) -> list[dict]:
	# Force UTF-8 output to handle emojis on Windows CI
	sys.stdout.reconfigure(encoding='utf-8')
	
	# Get language configuration
	config = LANG_CONFIG[lang]
	
	async with async_playwright() as p:
		print(config["messages"]["scraping_started"] + query)

		browser = await p.chromium.launch(headless=headless)

		context = await browser.new_context(
			locale=config["locale"],
			timezone_id=config["timezone"],
			viewport={"width": 1920, "height": 1080},
			user_agent=(
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
				"AppleWebKit/537.36 (KHTML, like Gecko) "
				"Chrome/120.0.0.0 Safari/537.36"
			),
			is_mobile=False,
			has_touch=False,
			extra_http_headers={
				"Accept-Language": config["accept_language"]
			},
		)

		async def _route_handler(route) -> None:
			if route.request.resource_type in BLOCKED_RESOURCE_TYPES:
				await route.abort()
			else:
				await route.continue_()

		await context.route("**/*", _route_handler)

		page = await context.new_page()

		try:
			await page.goto(
				f"https://www.youtube.com/results?search_query={quote(query)}",
				wait_until="domcontentloaded",
			)
			if debug_artifacts:
				await _save_debug_artifacts(
					page,
					screenshot_name="01_after_goto.png",
					include_html=False,
				)
		
			# Apply UI-driven filters based on user arguments
			filters_button = config["ui"]["search_filters"]
			# Apply upload date filter if specified
			if upload_date:
				filter_text = config["filters"]["upload_date"].get(upload_date)
				if filter_text:
					await page.get_by_role("button", name=filters_button).click()
					await page.get_by_role("link", name=filter_text).click()
					await page.wait_for_timeout(800)
			
			# Apply duration filter if specified
			if duration:
				filter_text = config["filters"]["duration"].get(duration)
				if filter_text:
					await page.get_by_role("button", name=filters_button).click()
					await page.get_by_role("link", name=filter_text).click()
					await page.wait_for_timeout(800)
			
			# Apply features filters if specified
			if features:
				for feature in features:
					filter_text = config["filters"]["features"].get(feature)
					if filter_text:
						await page.get_by_role("button", name=filters_button).click()
						await page.get_by_role("link", name=filter_text).click()
						await page.wait_for_timeout(800)
			
			# Apply sort by filter if specified
			if sort_by:
				filter_text = config["filters"]["sort_by"].get(sort_by)
				if filter_text:
					await page.get_by_role("button", name=filters_button).click()
					await page.get_by_role("link", name=filter_text).click()
					await page.wait_for_timeout(800)

			await page.wait_for_selector("ytd-video-renderer")

			await _scroll_until_complete(
				page,
				limit=limit,
				no_more_message=config["ui"]["no_more_results"],
			)

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
		except Exception as exc:
			print(f"⚠️ YouTube discovery failed for {query!r}: {exc}")
			await _save_debug_artifacts(
				page,
				screenshot_name="02_no_filters_button.png",
			)
			return []
		finally:
			if debug_artifacts:
				await _save_debug_artifacts(page)
			await browser.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
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
		parser.add_argument(
				"--debug-artifacts",
				action="store_true",
				help="Write screenshots and page HTML under debug/",
		)
		parser.add_argument(
				"--skip-schema",
				action="store_false",
				dest="ensure_schema",
				help="Skip idempotent schema setup when a parent runner already applied it",
		)
		
		# Language selection
		lang_group = parser.add_mutually_exclusive_group()
		lang_group.add_argument(
				"--EN",
				action="store_const",
				const="en-US",
				dest="lang",
				help="Use English (en-US) interface",
		)
		lang_group.add_argument(
				"--ES",
				action="store_const",
				const="es-MX",
				dest="lang",
				help="Use Spanish (es-MX) interface (default)",
		)
		parser.set_defaults(lang="es-MX", ensure_schema=True)
		
		# YouTube search filters
		parser.add_argument(
				"--upload-date",
				choices=["last_hour", "today", "this_week", "this_month", "this_year"],
				default=None,
				help="Filter by upload date",
		)
		parser.add_argument(
				"--duration",
				choices=["under_4", "4_20", "over_20"],
				default=None,
				help="Filter by video duration",
		)
		parser.add_argument(
				"--features",
				nargs="+",
				choices=["live", "4k", "hd", "subtitles", "creative_commons", "360", "vr180", "3d", "hdr", "location", "purchased"],
				default=None,
				help="Filter by video features (can specify multiple)",
		)
		parser.add_argument(
				"--sort-by",
				choices=["relevance", "upload_date", "view_count", "rating"],
				default=None,
				help="Sort results by specific criteria",
		)
		
		return parser.parse_args(argv)


def main() -> None:
		args = parse_args()
		headless = False if args.headed else True
		config = LANG_CONFIG[args.lang]

		async def _main_async() -> None:
			# DB lifecycle is intentionally handled via db.py (no SQL here).
			load_dotenv()
			# Pass language to init_db for table naming (convert locale to simple lang code)
			language = "en" if args.lang == "en-US" else "es"
			search_run_id = None
			try:
				await db.init_db(
					language=language,
					ensure_schema=args.ensure_schema,
				)
				search_run_id = await db.create_search_run(args.query, mode="exploration")
				
				results = await run(
					args.query,
					headless=headless,
					limit=args.limit,
					lang=args.lang,
					upload_date=args.upload_date,
					duration=args.duration,
					features=args.features,
					sort_by=args.sort_by,
					debug_artifacts=args.debug_artifacts,
				)
				print(config["messages"]["scraping_completed"].format(len(results)))
				inserted, ignored = await db.insert_videos_raw(search_run_id, results)
				print(config["messages"]["db_inserted"].format(inserted, ignored))
				payload = json.dumps(results, ensure_ascii=False, indent=2)

				if args.out:
					args.out.parent.mkdir(parents=True, exist_ok=True)
					args.out.write_text(payload, encoding="utf-8")
					print(config["messages"]["results_written"].format(args.out))
			except Exception as e:
				print(f"⚠️ Error during execution: {e}")
				raise
			finally:
				# Safely close DB even if there were errors
				try:
					if search_run_id:
						await db.finish_search_run(search_run_id)
				except Exception as e:
					print(f"⚠️ Error finishing search run: {e}")
				try:
					await db.close_db()
				except Exception as e:
					print(f"⚠️ Error closing database: {e}")

		asyncio.run(_main_async())


if __name__ == "__main__":
		main()
	
