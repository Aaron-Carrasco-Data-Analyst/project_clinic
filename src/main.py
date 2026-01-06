import logging
from datetime import datetime, timedelta
from src.extract import extract_to_bronze
from src.transform import transform_to_silver
from src.load import load_to_dw

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    logger.info( "Iniciando ETL desde src/main.py ...")

    # Definir fecha_desde: ayer
    fecha_desde = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info("Extrayendo datos desde %s", fecha_desde)

    # Extract
    extract_to_bronze(fecha_desde)

    # Transform
    transform_to_silver()

    # Load
    load_to_dw()

    logger.info("✅ ETL finalizado con éxito")

if __name__ == "__main__":
    main()