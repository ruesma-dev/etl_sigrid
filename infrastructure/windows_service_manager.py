# infrastructure/windows_service_manager.py
from __future__ import annotations
import logging
import subprocess
import time
from dataclasses import dataclass
from enum import Enum, auto


class ServiceStatus(Enum):
    RUNNING = auto()
    STOPPED = auto()
    START_PENDING = auto()
    UNKNOWN = auto()


@dataclass
class WindowsServiceManager:
    service_name: str  # 'MSSQLSERVER' o 'MSSQL$SQLEXPRESS'

    # --- helpers -----------------------------------------------------------

    def _run_sc(self, command: str) -> str:
        """
        Ejecuta 'sc.exe <command> <service_name>' y devuelve stdout.
        Lanza CalledProcessError si exit-code != 0.
        """
        cmd = ["sc", command, self.service_name]           # ←  FIX
        # print(cmd)
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, shell=False)
        print(result)
        return result.stdout

    # --- API público -------------------------------------------------------

    def status(self) -> ServiceStatus:
        try:
            output = self._run_sc("query")
        except subprocess.CalledProcessError as exc:
            logging.error(f"No se pudo consultar el servicio {self.service_name}: {exc.stderr or exc.stdout}")
            return ServiceStatus.UNKNOWN

        if "RUNNING" in output:
            return ServiceStatus.RUNNING
        if "STOPPED" in output:
            return ServiceStatus.STOPPED
        if "START_PENDING" in output:
            return ServiceStatus.START_PENDING
        return ServiceStatus.UNKNOWN

    def start(self, retries: int = 5, delay: int = 2) -> bool:
        """
        Inicia el servicio si no está RUNNING.
        Devuelve True cuando termina en RUNNING.
        """
        if self.status() == ServiceStatus.RUNNING:
            logging.info(f"Servicio '{self.service_name}' ya estaba iniciado.")
            return True

        logging.info(f"Iniciando servicio '{self.service_name}'…")
        try:
            self._run_sc("start")
        except subprocess.CalledProcessError as exc:
            logging.error(f"No se pudo iniciar '{self.service_name}': {exc.stderr or exc.stdout}")
            return False

        for _ in range(retries):
            if self.status() == ServiceStatus.RUNNING:
                logging.info(f"Servicio '{self.service_name}' iniciado correctamente.")
                return True
            time.sleep(delay)

        logging.error(f"Timeout al iniciar el servicio '{self.service_name}'.")
        return False
