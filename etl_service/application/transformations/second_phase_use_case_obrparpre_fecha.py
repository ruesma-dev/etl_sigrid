# etl_service/application/transformations/second_phase_use_case_obrparpre_fecha.py

import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

class SecondPhaseUseCaseObrparpreFecha:
    """
    Caso de uso de segunda fase para copiar 'fecha_fin' desde 'obrfas'
    a 'obrparpre' (o la tabla que definas).
    Se une por 'composite_key', presente en ambas tablas.

    Crea una nueva tabla unida con la columna 'fecha_fin'.
    """

    def __init__(self, postgres_repo):
        self.postgres_repo = postgres_repo

    def execute_copy_fecha_fin(self,
                               obrparpre_table_key,
                               obrfas_table_key,
                               new_table_key,
                               composite_key="composite_key"):
        """
        Lee la tabla 'obrparpre' y la tabla 'obrfas', las une por 'composite_key'
        y crea una nueva tabla en PostgreSQL con la columna 'fecha_fin' agregada.

        :param obrparpre_table_key: Nombre lógico (en TABLE_CONFIG) o real de la tabla obrparpre.
        :param obrfas_table_key: Nombre lógico (en TABLE_CONFIG) o real de la tabla obrfas (con fecha_fin).
        :param new_table_key: Nombre lógico/nuevo de la tabla resultante.
        :param composite_key: Columna de unión (default 'composite_key').
        """
        try:
            logging.info(f"=== [Segunda fase] Copiando fecha_fin desde '{obrfas_table_key}' a '{obrparpre_table_key}' ===")

            # 1) Obtener nombres reales de tablas
            obrparpre_name = self._get_target_table_name(obrparpre_table_key)
            obrfas_name = self._get_target_table_name(obrfas_table_key)
            new_table_name = self._get_target_table_name(new_table_key)

            logging.info(f"Mapeo: {obrparpre_table_key} => {obrparpre_name}, "
                         f"{obrfas_table_key} => {obrfas_name}, => {new_table_key} => {new_table_name}")

            # 2) Leer ambas tablas
            df_parpre = self._read_table_from_postgres(obrparpre_name)
            df_fas = self._read_table_from_postgres(obrfas_name)

            if df_parpre.empty:
                logging.warning(f"La tabla '{obrparpre_name}' está vacía. Abortando.")
                return
            if df_fas.empty:
                logging.warning(f"La tabla '{obrfas_name}' está vacía. Abortando.")
                return

            logging.info(f"Filas en '{obrparpre_name}': {len(df_parpre)}")
            logging.info(f"Filas en '{obrfas_name}': {len(df_fas)}")

            # 3) Realizar el merge por 'composite_key' (left join: conservo todas filas de obrparpre)
            merged_df = df_parpre.merge(
                df_fas[[composite_key, "fecha_fin"]],   # solo necesitamos 'fecha_fin' y la key
                on=composite_key,
                how="left"
            )

            logging.info("=== Ejemplo de merge (HEAD) ===")
            logging.info(merged_df.head(10).to_string())

            # 4) Crear la nueva tabla en PostgreSQL
            logging.info(f"Creando nueva tabla '{new_table_name}' con la columna 'fecha_fin' unida...")
            self._create_new_table(new_table_name, merged_df)
            logging.info(f"Tabla '{new_table_name}' creada con éxito.")

        except SQLAlchemyError as e:
            logging.error(f"Error SQLAlchemy copiando 'fecha_fin': {e}")
        except Exception as ex:
            logging.error(f"Error inesperado copiando 'fecha_fin': {ex}")

    # ------------------------------------------------
    # Métodos de apoyo
    # ------------------------------------------------
    def _get_target_table_name(self, table_key: str) -> str:
        from etl_service.application.transformations.table_config import TABLE_CONFIG
        return TABLE_CONFIG.get(table_key, {}).get('target_table', table_key)

    def _read_table_from_postgres(self, table_name: str) -> pd.DataFrame:
        query = f'SELECT * FROM "{table_name}"'
        try:
            with self.postgres_repo.engine.connect() as conn:
                df = pd.read_sql(query, conn)
            return df
        except SQLAlchemyError as e:
            logging.error(f"No se pudo leer la tabla '{table_name}': {e}")
            return pd.DataFrame()

    def _create_new_table(self, table_name: str, df: pd.DataFrame):
        """
        Crea o recrea una tabla en PostgreSQL con el DataFrame proporcionado.
        (Puedes usar COPY o method='multi' aquí, según tu preferencia).
        """
        from sqlalchemy import text

        backup_table = f"{table_name}_backup"

        with self.postgres_repo.engine.begin() as conn:
            # Renombrar la tabla original si existe (opcional) o drop
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))

            # Convertir NaN en None
            df = df.where(pd.notnull(df), None)

            # Insertar con 'to_sql' (method='multi', chunksize) o COPY
            df.to_sql(table_name, conn, if_exists='fail', index=False, method='multi', chunksize=10_000)
            logging.info(f"Tabla '{table_name}' recreada con {len(df)} filas.")
