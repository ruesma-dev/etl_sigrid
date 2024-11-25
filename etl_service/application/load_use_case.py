# etl_service/application/load_use_case.py

from domain.repositories_interfaces import PostgresRepositoryInterface
import logging

class LoadUseCase:
    def __init__(self, postgres_repo: PostgresRepositoryInterface):
        self.postgres_repo = postgres_repo

    def execute(self, data: dict):
        try:
            # No es necesario crear la base de datos aquí, ya se maneja en el repositorio
            for table_name, df in data.items():
                logging.info(f"Cargando tabla '{table_name}' en PostgreSQL.")
                self.postgres_repo.insert_table(df, table_name)
        except Exception as e:
            logging.error(f"Error durante la carga: {e}")
            raise
