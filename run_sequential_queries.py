import subprocess
import time
import sys
import os

QUERIES = [
#     "historia completa de",
#     "historia olvidada de",
#     "análisis profundo de",
#     "documental histórico",
#     "auge y caída de",
#     "imperio olvidado",
#     "civilización perdida",
#     "qué pasó realmente en",
#     "por qué cayó",
#     "cronología completa de",
#     "historia real de",
#     "explicación completa de",
#     "documental largo",
#     "ensayo histórico",
#     "versión completa",
# # -----------------------------
#     # Historia general
#     # -----------------------------
#     "historia de",
#     "historia completa de",
#     "historia real de",
#     "historia olvidada de",
#     "historia desconocida de",
#     "historia detallada de",
#     "historia explicada",
#     "historia que no te contaron de",
#     "origen y caída de",
#     "auge y caída de",
#     "ascenso y caída de",
#     "cronología completa de",
#     "línea de tiempo de",
#     "cómo surgió",
#     "cómo desapareció",

#     # -----------------------------
#     # Análisis / ensayo profundo
#     # -----------------------------
#     "análisis de",
#     "análisis completo de",
#     "análisis profundo de",
#     "análisis histórico de",
#     "análisis detallado de",
#     "explicación completa de",
#     "explicación profunda de",
    "explicación definitiva de",
    "qué pasó realmente en",
    "por qué ocurrió",
    "por qué fracasó",
    "por qué cayó",
    "cómo funcionaba",

    # -----------------------------
    # Imperios y civilizaciones
    # -----------------------------
    "imperio",
    "imperio antiguo",
    "imperio olvidado",
    "imperio desaparecido",
    "grandes imperios",
    "imperios que colapsaron",
    "imperios que desaparecieron",
    "civilización antigua",
    "civilización perdida",
    "civilización desaparecida",
    "civilización que colapsó",
    "culturas antiguas",
    "culturas desaparecidas",

    # -----------------------------
    # Guerras / conflictos
    # -----------------------------
    "guerra de",
    "guerra explicada",
    "guerra completa",
    "conflicto histórico",
    "batalla de",
    "batallas decisivas",
    "campañas militares",
    "estrategia militar",
    "tácticas militares",
    "errores militares",
    "derrotas históricas",
    "guerras olvidadas",
    "guerras que cambiaron la historia",

    # -----------------------------
    # Edad media / mundo antiguo
    # -----------------------------
    "edad media explicada",
    "vida en la edad media",
    "cómo se vivía en",
    "castillos medievales",
    "reinos medievales",
    "imperios medievales",
    "sociedad medieval",
    "vida cotidiana en",
    "antigüedad clásica",
    "mundo antiguo",
    "roma antigua",
    "grecia antigua",
    "egipto antiguo",

    # -----------------------------
    # Personajes históricos
    # -----------------------------
    "biografía completa de",
    "vida y muerte de",
    "el reinado de",
    "el gobierno de",
    "el legado de",
    "el fin de",
    "la caída de",
    "gobernantes más poderosos",
    "reyes más temidos",
    "emperadores romanos",
    "líderes históricos",

    # -----------------------------
    # Ciencia / conocimiento
    # -----------------------------
    "historia de la ciencia",
    "historia de la medicina",
    "historia de la tecnología",
    "descubrimientos científicos",
    "experimentos históricos",
    "avances científicos",
    "errores científicos",
    "teorías que cambiaron el mundo",
    "científicos olvidados",

    # -----------------------------
    # Geopolítica / estados
    # -----------------------------
    "historia de un país",
    "historia política de",
    "formación de un estado",
    "colapso de un estado",
    "países que desaparecieron",
    "fronteras cambiantes",
    "conflictos territoriales",
    "imperios coloniales",
    "colonialismo explicado",

    # -----------------------------
    # Arquitectura / ciudades
    # -----------------------------
    "arquitectura antigua",
    "ciudades antiguas",
    "ciudades perdidas",
    "ciudades desaparecidas",
    "grandes construcciones antiguas",
    "obras de ingeniería antiguas",
    "cómo se construyó",
    "misterios arquitectónicos",

    # -----------------------------
    # Misterios históricos (sin fantasía)
    # -----------------------------
    "misterios históricos",
    "enigmas históricos",
    "eventos históricos inexplicables",
    "sucesos históricos extraños",
    "documentos perdidos",
    "hechos históricos ocultos",

    # -----------------------------
    # Religión / mundo antiguo (histórico)
    # -----------------------------
    "historia de las religiones",
    "religión en la antigüedad",
    "mitología explicada",
    "mitología antigua",
    "dioses antiguos",
    "creencias antiguas",
    "rituales antiguos",
    "textos antiguos explicados",

    # -----------------------------
    # Queries comodín (muy potentes)
    # -----------------------------
    "documental completo",
    "documental histórico",
    "documental largo",
    "documental narrado",
    "ensayo histórico",
    "ensayo documental",
    "versión completa",
    "versión extendida",
    "explicado a fondo",
    "explicado paso a paso",
]

def main():
    total_queries = len(QUERIES)
    script_path = os.path.join(os.path.dirname(__file__), "yt_discovery.py")
    
    print(f"🚀 Iniciando ejecución de {total_queries} queries secuencialmente...")
    
    for i, query in enumerate(QUERIES, 1):
        print(f"\n[{i}/{total_queries}] Ejecutando discovery para: '{query}'")
        
        try:
            # Call yt_discovery.py via CLI
            # equivalent to: python yt_discovery.py --query "..." --headless
            subprocess.run(
                [sys.executable, script_path, "--query", query, "--headless"],
                check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"❌ Error al ejecutar query '{query}': {e}")
        except KeyboardInterrupt:
            print("\n🛑 Ejecución interrumpida por el usuario.")
            sys.exit(0)
            
        if i < total_queries:
            print("⏳ Esperando 5 segundos...")
            time.sleep(5)

    print("\n✅ Todas las queries han sido procesadas.")

if __name__ == "__main__":
    main()
