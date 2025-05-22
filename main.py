# main.py
"""
Punto de entrada del micro-ETL SIGRID
Restaurar .bak  ➜  levantar BD SQL Server  ➜  copiar tablas a PostgreSQL
"""

from __future__ import annotations

import logging
import os
import sys
from typing import List

import pyodbc
from dotenv import load_dotenv

from application.restore_sql_database_use_case import RestoreSQLDatabaseUseCase
from application.sync_sql_to_postgres import SyncSQLToPostgres
from infrastructure.config import Config

# --------------------------------------------------------------------- #
# Init
# --------------------------------------------------------------------- #
load_dotenv()  # lee .env
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------- #
def get_all_sqlserver_tables(cfg: dict) -> List[str]:
    """
    Devuelve una lista con los nombres de tablas de usuario (dbo.*).
    """
    if cfg["integrated_auth"]:
        conn_str = (
            f"DRIVER={cfg['driver']};"
            f"SERVER={cfg['server']};"
            f"DATABASE={cfg['database']};"
            "Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={cfg['driver']};"
            f"SERVER={cfg['server']};"
            f"DATABASE={cfg['database']};"
            f"UID={cfg['user']};PWD={cfg['password']};"
        )

    with pyodbc.connect(conn_str) as cnx:
        cur = cnx.cursor()
        cur.execute(
            """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo';
            """
        )
        return [row.TABLE_NAME for row in cur.fetchall()]

# --------------------------------------------------------------------- #
# Main pipeline
# --------------------------------------------------------------------- #
def main() -> None:  # noqa: D401
    bak_path = os.path.join(Config.LOCAL_BAK_FOLDER, "ruesma202505210030.bak")
    if not os.path.exists(bak_path):
        logger.error("No existe el .bak en %s", bak_path)
        sys.exit(1)

    # 1️⃣ Restaurar backup en SQL Server ----------------------------------
    restorer = RestoreSQLDatabaseUseCase(
        bak_file_path=bak_path,
        database_name=Config.SQL_DATABASE,
        sql_server=Config.SQL_SERVER,
        auth=None  # None indica autenticación integrada
        if Config.INTEGRATED_AUTH
        else {"user": Config.SQL_USER, "password": Config.SQL_PASSWORD},
    )

    if not restorer.execute():
        sys.exit("❌ Restauración abortada")

    # 2️⃣ Sincronizar datos a PostgreSQL ----------------------------------
    sql_cfg = {
        "server": Config.SQL_SERVER,
        "database": Config.SQL_DATABASE,
        "driver": Config.SQL_DRIVER,
        "user": Config.SQL_USER,
        "password": Config.SQL_PASSWORD,
        "integrated_auth": Config.INTEGRATED_AUTH,
    }

    tables = get_all_sqlserver_tables(sql_cfg)
    logger.info("Tablas a sincronizar: %s", tables)

    sync_uc = SyncSQLToPostgres(
        sql_server_config=sql_cfg,
        postgres_config={
            "dbname": Config.PG_DATABASE,
            "user": Config.PG_USER,
            "password": Config.PG_PASSWORD,
            "host": Config.PG_SERVER,
            "port": Config.PG_PORT,
        },
        tables=tables,
    )
    sync_uc.execute()
    logger.info("Sincronización finalizada ✅")


if __name__ == "__main__":
    main()
