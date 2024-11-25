# infrastructure/config.py

import os
from dotenv import load_dotenv

# Cargar variables del archivo .env
load_dotenv()

class Config:
    # SQL Server Configuration
    SQL_SERVER = os.getenv('SQL_SERVER', 'localhost')
    SQL_DATABASE = os.getenv('SQL_DATABASE', 'TemporaryDB')
    # SQL_USER = os.getenv('SQL_USER', 'sa')                  # Añadir al .env
    # SQL_PASSWORD = os.getenv('SQL_PASSWORD')                # Añadir al .env
    SQL_DRIVER = os.getenv('SQL_DRIVER', 'ODBC Driver 17 for SQL Server')
    SQL_PORT = os.getenv('SQL_PORT', '1433')                # Añadir al .env

    SQL_CONNECTION_STRING = (
        f"mssql+pyodbc://@{SQL_SERVER}:{SQL_PORT}/{SQL_DATABASE}"
        f"?driver={SQL_DRIVER}&trusted_connection=yes"
    )

    # PostgreSQL Configuration
    PG_SERVER = os.getenv('PG_SERVER', 'localhost')
    PG_DATABASE = os.getenv('PG_DATABASE', 'clone_sigrid')
    PG_USER = os.getenv('PG_USER', 'postgres')
    PG_PASSWORD = os.getenv('PG_PASSWORD')  # Debe estar en el .env
    PG_PORT = os.getenv('PG_PORT', '5432')

    PG_CONNECTION_STRING = (
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_SERVER}:{PG_PORT}/{PG_DATABASE}"
    )
