# application/sync_sql_to_postgres.py
"""
Use-case: copiar tablas completas de SQL Server → PostgreSQL
"""

from __future__ import annotations

import logging
import traceback
import urllib.parse
from typing import List

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


class SyncSQLToPostgres:
    def __init__(
        self,
        *,
        sql_server_config: dict,
        postgres_config: dict,
        tables: List[str],
    ) -> None:
        self.sql_cfg = sql_server_config
        self.pg_cfg = postgres_config
        self.tables = tables

    # -------------------- PG helpers ----------------------------------- #
    def _ensure_pg_db(self) -> None:
        with psycopg2.connect(
            dbname="postgres",
            user=self.pg_cfg["user"],
            password=self.pg_cfg["password"],
            host=self.pg_cfg["host"],
            port=self.pg_cfg["port"],
        ) as con, con.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s;",
                (self.pg_cfg["dbname"],),
            )
            if not cur.fetchone():
                cur.execute(f'CREATE DATABASE "{self.pg_cfg["dbname"]}";')
                logger.info("Creada base PG '%s'", self.pg_cfg["dbname"])

    def _pg_engine(self):
        url = (
            f"postgresql://{self.pg_cfg['user']}:{self.pg_cfg['password']}@"
            f"{self.pg_cfg['host']}:{self.pg_cfg['port']}/{self.pg_cfg['dbname']}"
        )
        return create_engine(url)

    # -------------------- SQL Server engine ---------------------------- #
    def _sql_engine(self):
        if self.sql_cfg["integrated_auth"]:
            odbc = (
                f"DRIVER={{{self.sql_cfg['driver']}}};"
                f"SERVER={self.sql_cfg['server']};"
                f"DATABASE={self.sql_cfg['database']};"
                "Trusted_Connection=yes;"
            )
        else:
            odbc = (
                f"DRIVER={{{self.sql_cfg['driver']}}};"
                f"SERVER={self.sql_cfg['server']};"
                f"DATABASE={self.sql_cfg['database']};"
                f"UID={self.sql_cfg['user']};PWD={self.sql_cfg['password']};"
            )
        params = urllib.parse.quote_plus(odbc)
        return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    # -------------------- Tabla a tabla -------------------------------- #
    def _process_table(self, sql_eng, pg_eng, table: str) -> None:
        logger.info("➡️  %s", table)
        try:
            df = pd.read_sql_table(table, sql_eng)
            if df.empty:
                logger.info("Tabla vacía, se omite.")
                return
            df.to_sql(table, pg_eng, if_exists="replace", index=False, method="multi")
            logger.info("✔  Copiada (%s filas)", len(df))
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Error en tabla %s: %s", table, exc)
            logger.debug(traceback.format_exc())

    # -------------------- Orquestador ---------------------------------- #
    def execute(self) -> None:  # noqa: D401
        self._ensure_pg_db()
        sql_eng = self._sql_engine()
        pg_eng = self._pg_engine()

        for tbl in self.tables:
            self._process_table(sql_eng, pg_eng, tbl)

        sql_eng.dispose()
        pg_eng.dispose()
        logger.info("🚀  Sincronización completa")
