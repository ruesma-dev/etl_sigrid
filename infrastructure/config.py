# infrastructure/config.py
"""
Carga y validación de variables de entorno
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(ENV_PATH)


class _Env:
    """Ayudante para obtener variables con valor por defecto."""

    @staticmethod
    def get(key: str, default: Optional[str] = None) -> str:
        val = os.getenv(key, default)
        if val is None:
            raise RuntimeError(f"Falta variable de entorno: {key}")
        return val


class Config:  # pylint: disable=too-few-public-methods
    # ---------------------- SQL Server ---------------------------------- #
    SQL_SERVER: str = _Env.get("SQL_SERVER")  # p.ej. localhost\MSSQLSERVER01
    SQL_DATABASE: str = _Env.get("SQL_DATABASE", "TemporaryDB")
    SQL_DRIVER: str = _Env.get("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
    SQL_USER: str = os.getenv("SQL_USER", "").strip()
    SQL_PASSWORD: str = os.getenv("SQL_PASSWORD", "").strip()

    # Si falta user o password → autenticación integrada
    INTEGRATED_AUTH: bool = not (SQL_USER and SQL_PASSWORD)

    SQL_DATA_PATH: str = _Env.get(
        "SQL_DATA_PATH", r"C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA"
    )
    SQL_LOG_PATH: str = _Env.get(
        "SQL_LOG_PATH", r"C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA"
    )

    # ---------------------- Backups ------------------------------------- #
    LOCAL_BAK_FOLDER: str = _Env.get(
        "LOCAL_BAK_FOLDER",
        r"C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\Backup",
    )

    # ---------------------- PostgreSQL ---------------------------------- #
    PG_SERVER: str = _Env.get("PG_SERVER", "localhost")
    PG_DATABASE: str = _Env.get("PG_DATABASE", "clone_sigrid")
    PG_USER: str = _Env.get("PG_USER", "postgres")
    PG_PASSWORD: str = _Env.get("PG_PASSWORD", "postgres")
    PG_PORT: int = int(_Env.get("PG_PORT", "5432"))
