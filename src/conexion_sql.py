import os, logging, urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

class ConexionSQL:
    def __init__(self, tipo: str) -> None:
        self.tipo = tipo.lower()   # "oltp" o "dw"
        self.DRIVER = "ODBC Driver 17 for SQL Server"
        self.engine: Engine | None = None
        self.logger = logging.getLogger(__name__)

    def conectar(self, bbdd: str, username: str = '', password: str = '') -> Engine:
        """Crea un engine SQLAlchemy con Trusted_Connection o SQL Authentication."""
        if self.tipo == "oltp":
            server = os.getenv("DB_SERVER_OLTP")
            print(server)
        elif self.tipo == "dw":
            server = os.getenv("DB_SERVER_OLAP")
        else:
            raise ValueError("Tipo de servidor no soportado")

        # 🔑 Aquí está la adaptación: usamos el mismo patrón que tu código funcional
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

        # Probar conexión
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        self.logger.info("Conexión establecida con %s (%s)", self.tipo, bbdd)
        return self.engine
    
print("Hola")