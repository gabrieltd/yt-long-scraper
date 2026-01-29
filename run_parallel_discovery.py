import sys
import asyncio
import argparse
import subprocess
from pathlib import Path
from colorama import Fore, Style, init
from dotenv import load_dotenv

# Initialize colorama
init(autoreset=True)

import db  # Assuming db.py exists and handles async DB ops

async def get_already_run_queries(language: str = "es") -> set[str]:
    """Retrieves already executed queries from the database."""
    try:
        await db.init_db(language=language)
        executed = await db.get_executed_queries()
        await db.close_db()
        return executed
    except Exception as e:
        print(f"{Fore.YELLOW}⚠️ Warning: Could not read query history ({e}). All queries will be considered new.{Style.RESET_ALL}")
        return set()

async def worker(
    instance_id: int,
    queries: list[str],
    lang: str = "es-MX",
    upload_date: str | None = None,
    duration: str | None = None,
    features: list[str] | None = None,
    sort_by: str | None = None
):
    """
    Worker function that processes a list of queries sequentially.
    """
    total = len(queries)
    print(f"{Fore.CYAN}[Instance {instance_id}] Started processing {total} queries.{Style.RESET_ALL}")

    for i, query in enumerate(queries, 1):
        print(f"{Fore.BLUE}[Instance {instance_id}] ({i}/{total}) Running: '{query}'{Style.RESET_ALL}")
        
        try:
            # Construct command
            # Using sys.executable to ensure we use the same Python environment
            cmd = [sys.executable, "yt_discovery.py", "--query", query, "--headless"]
            
            # Add language flag
            if lang == "en-US":
                cmd.append("--EN")
            else:
                cmd.append("--ES")
            
            # Add filter arguments if specified
            if upload_date:
                cmd.extend(["--upload-date", upload_date])
            if duration:
                cmd.extend(["--duration", duration])
            if features:
                cmd.append("--features")
                cmd.extend(features)
            if sort_by:
                cmd.extend(["--sort-by", sort_by])
            
            # Execute subprocess asynchronously
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL, # Suppress stdout to keep console clean
                stderr=asyncio.subprocess.PIPE     # Capture stderr in case of errors
            )
            
            # Wait for finish
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                 print(f"{Fore.GREEN}[Instance {instance_id}] ✅ Finished: '{query}'{Style.RESET_ALL}")
            else:
                 error_msg = stderr.decode().strip() if stderr else "Unknown error"
                 print(f"{Fore.RED}[Instance {instance_id}] ❌ Failed: '{query}' - {error_msg}{Style.RESET_ALL}")

        except Exception as e:
            print(f"{Fore.RED}[Instance {instance_id}] 💥 Exception running '{query}': {e}{Style.RESET_ALL}")
            
        # Small delay to prevent complete system choke if tasks are very short
        await asyncio.sleep(1)

    print(f"{Fore.CYAN}[Instance {instance_id}] Completed all tasks.{Style.RESET_ALL}")

async def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Run multiple instances of YouTube discovery in parallel.")
    parser.add_argument("--instances", type=int, required=True, help="Number of parallel instances (workers) to run.")
    parser.add_argument("--batch-size", type=int, required=True, help="Number of queries assigned to each instance.")
    parser.add_argument("--queries-file", type=str, default=None, help="Path to the queries file. If not specified, auto-selects based on language.")
    parser.add_argument("--reprocess-duplicates", action="store_true", help="Reprocess queries that have already been executed.")
    
    # Language selection
    lang_group = parser.add_mutually_exclusive_group()
    lang_group.add_argument("--EN", action="store_const", const="en-US", dest="lang", help="Use English (en-US) interface")
    lang_group.add_argument("--ES", action="store_const", const="es-MX", dest="lang", help="Use Spanish (es-MX) interface (default)")
    parser.set_defaults(lang="es-MX")
    
    # YouTube search filters
    parser.add_argument("--upload-date", choices=["last_hour", "today", "this_week", "this_month", "this_year"], default=None, help="Filter by upload date")
    parser.add_argument("--duration", choices=["under_4", "4_20", "over_20"], default=None, help="Filter by video duration")
    parser.add_argument("--features", nargs="+", choices=["live", "4k", "hd", "subtitles", "creative_commons", "360", "vr180", "3d", "hdr", "location", "purchased"], default=None, help="Filter by video features")
    parser.add_argument("--sort-by", choices=["relevance", "upload_date", "view_count", "rating"], default=None, help="Sort results by specific criteria")
    
    args = parser.parse_args()
    
    # Auto-select queries file based on language if not specified
    if args.queries_file:
        queries_file = Path(args.queries_file)
    else:
        queries_file = Path("queries_en.txt" if args.lang == "en-US" else "queries.txt")

    if not queries_file.exists():
        print(f"{Fore.RED}❌ Error: Query file '{queries_file}' not found.{Style.RESET_ALL}")
        return

    # 1. Read queries
    print(f"Loading queries from {queries_file}...")
    with open(queries_file, "r", encoding="utf-8") as f:
        all_queries = [line.strip() for line in f if line.strip()]
    
    total_loaded = len(all_queries)
    print(f"Loaded {total_loaded} queries.")

    # 2. Filter executed
    pending_queries = []
    if args.reprocess_duplicates:
        print("Reprocessing all queries (ignoring history).")
        pending_queries = all_queries
    else:
        print("Checking database for executed queries...")
        # Convert locale to simple language code
        language = "en" if args.lang == "en-US" else "es"
        already_run = await get_already_run_queries(language)
        
        pending_queries = [q for q in all_queries if q not in already_run]
        total_pending = len(pending_queries)
        print(f"Pending queries: {total_pending} (Filtered {total_loaded - total_pending} executed)")

    if not pending_queries:
        print(f"{Fore.GREEN}No pending queries to process!{Style.RESET_ALL}")
        return

    # 3. Distribute work
    # User requirement: "si se seleccionan 50 queries y 10 instancias... total 500 queries"
    # This implies we take (instances * batch_size) queries from the top of Pending
    
    # Convert locale to simple language code for tracking
    language = "en" if args.lang == "en-US" else "es"
    
    needed_total = args.instances * args.batch_size
    queries_to_process = pending_queries[:needed_total]
    
    actual_count = len(queries_to_process)
    print(f"Processing {actual_count} queries across {args.instances} instances (Target: {needed_total})")
    
    if actual_count < needed_total:
         print(f"{Fore.YELLOW}⚠️ Warning: Not enough pending queries to fill all batches. Some instances may have less work or be idle.{Style.RESET_ALL}")

    # Create batches
    # Strategy: Just slice sequentially as requested
    # Instance 0: 0 to batch_size
    # Instance 1: batch_size to 2*batch_size
    
    tasks = []
    
    for i in range(args.instances):
        start = i * args.batch_size
        end = start + args.batch_size
        
        batch = queries_to_process[start:end]
        
        if batch:
            tasks.append(worker(
                i + 1,
                batch,
                lang=args.lang,
                upload_date=args.upload_date,
                duration=args.duration,
                features=args.features,
                sort_by=args.sort_by
            ))
        else:
            print(f"Instance {i + 1} has no queries assigned.")

    if not tasks:
        print("No tasks created.")
        return

    # 4. Run
    print(f"{Fore.MAGENTA}🚀 Starting {len(tasks)} workers...{Style.RESET_ALL}")
    await asyncio.gather(*tasks)
    print(f"{Fore.MAGENTA}🏁 All instances finished.{Style.RESET_ALL}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{Fore.RED}🛑 Execution stopped by user.{Style.RESET_ALL}")
