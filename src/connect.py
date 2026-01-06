import os
import urllib.parse
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import logging

# Importa la configuración de logging
from src import log_config  # esto inicializa logging al importar

dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
load_dotenv(dotenv_path)

class ConnectSQL:
    def __init__(self, tipo: str) -> None:
        self.tipo = tipo.lower()   # "oltp" o "dw"
        self.DRIVER = "ODBC Driver 17 for SQL Server"
        self.engine: Engine | None = None
        self.logger = logging.getLogger(__name__)

    def conectar(self, bbdd: str, username: str = '', password: str = '') -> Engine:
        if self.tipo == "oltp":
            server = os.getenv("DB_SERVER_OLTP")
        elif self.tipo == "dw":
            server = os.getenv("DB_SERVER_OLAP")
        else:
            raise ValueError("Tipo de servidor no soportado")

        if username and password:
            params = urllib.parse.quote_plus(
                f"DRIVER={{{self.DRIVER}}};"
                f"SERVER={server};"
                f"DATABASE={bbdd};"
                f"UID={username};PWD={password};"
                "TrustServerCertificate=yes"
            )
        else:
            params = urllib.parse.quote_plus(
                f"DRIVER={{{self.DRIVER}}};"
                f"SERVER={server};"
                f"DATABASE={bbdd};"
                "Trusted_Connection=yes;"
                "TrustServerCertificate=yes"
            )

        self.engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={params}",
            fast_executemany=True
        )

        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        self.logger.info("Conexión establecida con %s (%s)", self.tipo, bbdd)
        return self.engine

    def execute_select_to_pd(self, query: str, chunksize: int = None) -> pd.DataFrame:
        if not self.engine:
            raise ValueError("Engine no inicializado. Llama primero a conectar().")
        df = pd.read_sql_query(sql=query, con=self.engine, chunksize=chunksize)
        self.logger.info(
            "Query ejecutado y DataFrame generado (%s filas)",
            len(df) if isinstance(df, pd.DataFrame) else "chunks"
        )
        return df

    def close_connection(self) -> None:
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self.logger.info("Conexión cerrada correctamente.")