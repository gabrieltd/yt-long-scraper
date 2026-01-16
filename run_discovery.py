import sys
import time
import subprocess
import asyncio
from pathlib import Path
import db

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
    queries_file = Path("queries.txt")
    if not queries_file.exists():
        print(f"❌ Error: {queries_file} no encontrado.")
        return

    # 1. Leer queries del archivo
    with open(queries_file, "r", encoding="utf-8") as f:
        # Filtramos líneas vacías
        queries = [line.strip() for line in f if line.strip()]

    # 2. Obtener queries ya ejecutadas
    print("🔎 Verificando historial de queries ejecutadas...")
    already_run = asyncio.run(get_already_run_queries())
    
    # 3. Filtrar
    pending_queries = [q for q in queries if q not in already_run]
    
    skipped_count = len(queries) - len(pending_queries)
    if skipped_count > 0:
        print(f"⏩ Saltando {skipped_count} queries que ya fueron procesadas anteriormente.")
    
    if not pending_queries:
        print("✅ No hay queries pendientes. Todo está al día.")
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

    print("\n🎉 Todas las queries han sido procesadas.")

if __name__ == "__main__":
    main()
