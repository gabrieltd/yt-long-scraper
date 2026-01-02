import argparse
import asyncio
import logging

from dotenv import load_dotenv

from db import init_db, close_db, claim_channels_for_analysis, insert_channel_analysis_bulk
from yt_channel_analysis import analyze_and_persist_channel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


async def worker(worker_id: int, *, batch_size: int, force_single_insert: bool):
	log.info(f"[worker-{worker_id}] started")
	buffer: list[dict] = []

	while True:
		rows = await claim_channels_for_analysis(limit=batch_size)
		if not rows:
			log.info(f"[worker-{worker_id}] no more channels, exiting")
			break

		log.info(f"[worker-{worker_id}] claimed {len(rows)} channels")

		for row in rows:
			await analyze_and_persist_channel(
				row,
				force_single_insert=force_single_insert,
				buffer=buffer,
				batch_size=batch_size,
			)

	# flush leftovers
	if buffer:
		await insert_channel_analysis_bulk(buffer)
		buffer.clear()


async def main_async(args):
	load_dotenv()
	await init_db()

	try:
		await asyncio.gather(
			*[
				worker(
					i + 1,
					batch_size=args.batch_size,
					force_single_insert=args.force_single_insert,
				)
				for i in range(args.workers)
			]
		)
	finally:
		await close_db()


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("--workers", type=int, default=6)
	parser.add_argument("--batch-size", type=int, default=20)
	parser.add_argument("--force-single-insert", action="store_true")
	args = parser.parse_args()

	asyncio.run(main_async(args))


if __name__ == "__main__":
	main()
