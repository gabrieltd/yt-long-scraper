import sys
import time
import subprocess
import asyncio
import argparse
import math
import json
from pathlib import Path
import db
from dotenv import load_dotenv

async def get_already_run_queries() -> set[str]:
    try:
        await db.init_db()
        executed = await db.get_executed_queries()
        await db.close_db()
        return executed
    except Exception as e:
        print(f"⚠️ Warning: No se pudo leer el historial de queries ({e}). Se ejecutarán todas.")
        return set()

def main():
    load_dotenv()
    # Force UTF-8 output to handle emojis on Windows CI
    sys.stdout.reconfigure(encoding='utf-8')

    parser = argparse.ArgumentParser(description="Run YouTube discovery on queries.")
    parser.add_argument("--batch-size", type=int, help="Number of queries per batch")
    parser.add_argument("--batch-index", type=int, help="Index of the batch to run (0-based)")
    parser.add_argument("--check-batches", action="store_true", help="Return JSON list of batch indices that have pending queries")
    args = parser.parse_args()

    queries_file = Path("queries.txt")
    if not queries_file.exists():
        print(f"❌ Error: {queries_file} no encontrado.")
        return

    # 1. Leer queries del archivo
    with open(queries_file, "r", encoding="utf-8") as f:
        # Filtramos líneas vacías
        queries = [line.strip() for line in f if line.strip()]

    total_queries = len(queries)

    # Mode: Check Batches
    if args.check_batches:
        if not args.batch_size:
            print("To check batches, you must provide --batch-size.")
            sys.exit(1)

        already_run = asyncio.run(get_already_run_queries())
        batch_size = args.batch_size
        needed_batches = []
        
        batch_count = math.ceil(total_queries / batch_size)
        
        for i in range(batch_count):
            start = i * batch_size
            end = start + batch_size
            batch_queries = queries[start:min(end, total_queries)]
            
            # If ANY query in this batch is not in already_run, we need this batch
            if any(q for q in batch_queries if q not in already_run):
                needed_batches.append(i)
        
        # Print ONLY the JSON list (no other text to stdout to avoid parsing issues)
        print(json.dumps(needed_batches))
        return
    
    # Logic for batching
    if args.batch_size is not None and args.batch_index is not None:
        start_idx = args.batch_index * args.batch_size
        end_idx = start_idx + args.batch_size
        # Slice safely
        queries = queries[start_idx:end_idx]
        print(f"🔢 Batch Mode: Processing batch {args.batch_index} (Size: {args.batch_size})")
        print(f"   Range: [{start_idx} - {min(end_idx, total_queries)}) of {total_queries} total queries.")
    else:
        print(f"Processing all {total_queries} queries (No batch mode).")

    if not queries:
        print("⚠️ No queries in this batch (index might be out of range).")
        return

    # 2. Obtener queries ya ejecutadas
    print("🔎 Verificando historial de queries ejecutadas...")
    already_run = asyncio.run(get_already_run_queries())
    
    # 3. Filtrar
    pending_queries = [q for q in queries if q not in already_run]
    
    skipped_count = len(queries) - len(pending_queries)
    if skipped_count > 0:
        print(f"⏩ Saltando {skipped_count} queries que ya fueron procesadas anteriormente.")
    
    if not pending_queries:
        print("✅ No hay queries pendientes en este batch. Todo está al día.")
        return

    total = len(pending_queries)
    print(f"🚀 Iniciando procesamiento de {total} queries PENDIENTES desde {queries_file}")

    for i, query in enumerate(pending_queries, 1):
        print(f"\n==================================================")
        print(f"▶️ [{i}/{total}] Ejecutando query: '{query}'")
        print(f"==================================================")
        
        try:
            # Ejecutamos yt_discovery.py como subproceso
            # Se asume que usa las opciones por defecto (headless=True, limit=None o lo que tenga el script)
            # Puedes agregar --limit 50 si quisieras forzar un límite
            cmd = [sys.executable, "yt_discovery.py", "--query", query, "--headless"]
            
            subprocess.run(cmd, check=False) # check=False para que no se detenga si un script falla
            
            print(f"✅ Query '{query}' finalizada.")
            
            # Pequeña pausa entre ejecuciones para dar respiro
            time.sleep(2)
            
        except KeyboardInterrupt:
            print("\n🛑 Ejecución detenida por el usuario.")
            sys.exit(0)
        except Exception as e:
            print(f"⚠️ Error inesperado ejecutando '{query}': {e}")

    print("\n🎉 Todas las queries de este batch han sido procesadas.")

if __name__ == "__main__":
    main()
