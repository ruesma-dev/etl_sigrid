# domain/entities.py

from dataclasses import dataclass

@dataclass
class Database:
    name: str
    host: str
    port: int
    user: str
    password: str
    driver: str = "ODBC Driver 17 for SQL Server"  # Por defecto para SQL Server
    data_path: str = ""
    log_path: str = ""
