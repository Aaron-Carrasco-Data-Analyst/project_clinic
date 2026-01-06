from src.conexion_sql import ConexionSQL
import os
import logging
from src import log_config  # inicializa logging

logger = logging.getLogger(__name__)

def extract_to_bronze(fecha_desde: str = None):
    conn = ConexionSQL("oltp")
    conn.conectar("PRUEBA_OLTP")

    queries = {
        "pacientes": "SELECT * FROM pacientes",
        "medicos": "SELECT * FROM medicos",
        "diagnosticos": "SELECT * FROM diagnosticos",
        "atenciones": "SELECT * FROM atenciones"
    }

    if fecha_desde:
        queries["atenciones"] += f" WHERE fecha >= '{fecha_desde}'"

    os.makedirs("data/bronze", exist_ok=True)

    for name, query in queries.items():
        df = conn.ejecutar_select_a_pd(query)
        output_path = f"data/bronze/{name}.parquet"
        df.to_parquet(output_path, index=False)
        logger.info("%s guardado en Bronze (%s filas)", name, len(df))

    conn.cerrar_conexion()
    logger.info("Extracción a Bronze finalizada correctamente.")

if __name__ == "__main__":
    extract_to_bronze("2025-12-01")
    #extract_to_bronze()