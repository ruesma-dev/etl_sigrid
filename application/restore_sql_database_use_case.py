# application/restore_sql_database_use_case.py
"""
Use-case: restaurar una base SQL Server (.bak) creando una BD temporal.

✔ Obtiene los nombres lógicos con **pyodbc** (más robusto que
  parsear la salida de sqlcmd).
✔ Luego ejecuta `sqlcmd` para el RESTORE definitivo.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

import pyodbc

logger = logging.getLogger(__name__)


class RestoreSQLDatabaseUseCase:
    """Restaura un archivo .bak como nueva base en la instancia indicada."""

    def __init__(
        self,
        *,
        bak_file_path: str | Path,
        database_name: str,
        sql_server: str,
        auth: Optional[Dict[str, str]] = None,  # None ➜ Win auth (integrada)
        base_path: Optional[str | Path] = None,  # carpeta padre para MDF/LDF
    ) -> None:
        self.bak_file = Path(bak_file_path)
        self.database_name = database_name
        self.sql_server = sql_server
        self.auth = auth
        self.base_path = Path(base_path or self.bak_file.parent)
        self.data_dir = self.base_path / "Data"
        self.log_dir = self.base_path / "Logs"

    # ------------------------------------------------------------------ #
    #  Conexiones
    # ------------------------------------------------------------------ #
    def _pyodbc_conn(self):
        """Devuelve una conexión ODBC para ejecutar FILELISTONLY."""
        if self.auth:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.sql_server};"
                "DATABASE=master;"
                f"UID={self.auth['user']};PWD={self.auth['password']};"
            )
        else:  # autenticación integrada
            conn_str = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                f"SERVER={self.sql_server};"
                "DATABASE=master;"
                "Trusted_Connection=yes;"
            )
        return pyodbc.connect(conn_str, autocommit=True)

    def _sqlcmd_base(self) -> List[str]:
        if self.auth:
            return [
                "sqlcmd",
                "-S",
                self.sql_server,
                "-U",
                self.auth["user"],
                "-P",
                self.auth["password"],
            ]
        return ["sqlcmd", "-S", self.sql_server, "-E"]

    @staticmethod
    def _run(cmd: List[str]) -> None:
        """Ejecuta un comando y lanza excepción si termina con error."""
        logger.debug("Ejecutando: %s", " ".join(cmd))
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip())

    # ------------------------------------------------------------------ #
    #  Lógica
    # ------------------------------------------------------------------ #
    def _logical_names(self) -> Dict[str, str]:
        """Consulta FILELISTONLY y devuelve logical names de MDF/LDF."""
        with self._pyodbc_conn() as con:
            cur = con.cursor()
            cur.execute("RESTORE FILELISTONLY FROM DISK = ?", (str(self.bak_file),))
            rows = cur.fetchall()
            data_name = ""
            log_name = ""
            for row in rows:
                # Columnas estándar: LogicalName, PhysicalName, Type, ...
                if row.Type == "D":
                    data_name = row.LogicalName
                elif row.Type == "L":
                    log_name = row.LogicalName
            if not (data_name and log_name):
                raise ValueError("No se detectaron nombres lógicos en FILELISTONLY")
            return {"data": data_name, "log": log_name}

    # ------------------------------------------------------------------ #
    #  Público
    # ------------------------------------------------------------------ #
    def execute(self) -> bool:  # noqa: D401
        """Restaurar; devuelve True si la operación finaliza con éxito."""
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            self.log_dir.mkdir(parents=True, exist_ok=True)

            logical = self._logical_names()
            logger.info("Logical names detectados: %s", logical)

            data_path = self.data_dir / f"{self.database_name}.mdf"
            log_path = self.log_dir / f"{self.database_name}_Log.ldf"

            restore_sql = f"""
            RESTORE DATABASE [{self.database_name}]
            FROM DISK = '{self.bak_file}'
            WITH
                MOVE '{logical['data']}' TO '{data_path}',
                MOVE '{logical['log']}'  TO '{log_path}',
                REPLACE, STATS = 10;
            """
            self._run(self._sqlcmd_base() + ["-Q", restore_sql])
            logger.info("Base '%s' restaurada correctamente ✅", self.database_name)
            return True

        except Exception as exc:  # pylint: disable=broad-except
            logger.error("❌ Restauración fallida: %s", exc)
            return False
