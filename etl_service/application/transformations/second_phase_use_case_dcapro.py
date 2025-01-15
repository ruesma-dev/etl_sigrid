# etl_service/application/transformations/second_phase_use_case_dcapro_obrparpar.py

import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

class SecondPhaseUseCaseDcaproObrparpar:
    """
    Caso de uso: añadir 'codigo_partida' en la tabla 'dcapro' tomando 'cod' de 'obrparpar'.
    Relación: dcapro.paride == obrparpar.ide
    """

    def __init__(self, postgres_repo):
        self.postgres_repo = postgres_repo

    def execute_dcapro_obrparpar_codigo_partida(self, dcapro_key="dcapro", obrparpar_key="obrparpar"):
        """
        1. Busca en table_config el 'target_table' real de 'dcapro' y 'obrparpar'.
        2. Lee ambos DF desde Postgres.
        3. Merge en dcapro.paride == obrparpar.ide
        4. Crea 'codigo_partida' = obrparpar.cod
        5. Sobrescribe la tabla de dcapro con la nueva columna.
        """
        try:
            logging.info(f"=== [dcapro -> obrparpar] Creando 'codigo_partida' a partir de {dcapro_key} y {obrparpar_key} ===")

            dcapro_table = self._get_target_table_name(dcapro_key)      # p.ej. "DimAlbaranCompraProductos"
            obrparpar_table = self._get_target_table_name(obrparpar_key)# p.ej. "DimPartidasObra"

            logging.info(f"Mapeo: {dcapro_key} => {dcapro_table}, {obrparpar_key} => {obrparpar_table}")

            # Leer los DF
            dcapro_df = self._read_table_from_postgres(dcapro_table)
            obrparpar_df = self._read_table_from_postgres(obrparpar_table)

            if dcapro_df.empty or obrparpar_df.empty:
                logging.warning("Alguna tabla [dcapro, obrparpar] está vacía => se aborta el proceso.")
                return

            logging.info(f"Filas en {dcapro_table}={len(dcapro_df)}, {obrparpar_table}={len(obrparpar_df)}")

            # Verificar que la col 'paride' existe en dcapro y 'cod','ide' en obrparpar
            if 'paride' not in dcapro_df.columns:
                logging.warning(f"La tabla {dcapro_table} no tiene la columna 'paride'. Abortando.")
                return
            if 'ide' not in obrparpar_df.columns or 'cod' not in obrparpar_df.columns:
                logging.warning(f"La tabla {obrparpar_table} no tiene 'ide' o 'cod'. Abortando.")
                return

            obrparpar_df.rename(
                columns={'ide': 'obrparpar_ide', 'cod': 'codigo_partida'}, inplace=True
            )

            # MERGE: left join => dcapro.paride == obrparpar.ide
            merged_df = dcapro_df.merge(
                obrparpar_df[['obrparpar_ide', 'codigo_partida']],
                how='left',
                left_on='paride',
                right_on='obrparpar_ide'
            )

            # Reconstruir la tabla => sugiere borrar/crear
            self._recreate_table(dcapro_table, merged_df)
            logging.info(f"'{dcapro_table}' actualizada con la columna 'codigo_partida' (desde obrparpar.cod).")

        except SQLAlchemyError as e:
            logging.error(f"Error SQLAlchemy: {e}")
        except Exception as ex:
            logging.error(f"Error inesperado: {ex}")

    # ----------------------------------------------
    # Métodos de apoyo
    # ----------------------------------------------
    def _get_target_table_name(self, table_key: str) -> str:
        """
        Retorna el 'target_table' real definido en table_config
        (si no se encuentra, retorna el key tal cual).
        """
        from etl_service.application.transformations.table_config import TABLE_CONFIG

        if table_key in TABLE_CONFIG:
            return TABLE_CONFIG[table_key].get('target_table', table_key)
        else:
            return table_key

    def _read_table_from_postgres(self, table_name: str) -> pd.DataFrame:
        query = f'SELECT * FROM "{table_name}"'
        try:
            with self.postgres_repo.engine.connect() as conn:
                df = pd.read_sql(query, conn)
            return df
        except SQLAlchemyError as e:
            logging.error(f"No se pudo leer la tabla '{table_name}': {e}")
            return pd.DataFrame()

    def _recreate_table(self, table_name: str, df: pd.DataFrame):
        """
        Elimina la tabla original (renombrándola a backup) y crea la nueva con to_sql.
        Convertimos NaT => None en datetime antes de to_sql para evitar error psqcopg2
        con timestamps = 'NaT'.
        """
        from sqlalchemy import text

        backup_table = f"{table_name}_backup"
        with self.postgres_repo.engine.begin() as conn:
            # Renombrar la original => backup
            conn.execute(text(f'ALTER TABLE IF EXISTS "{table_name}" RENAME TO "{backup_table}"'))
            logging.info(f"Tabla '{table_name}' renombrada a '{backup_table}' (backup).")

            # Convertir NaT => None en las columnas de tipo datetime64
            for col in df.select_dtypes(include=['datetime64[ns]']):
                df[col] = df[col].where(df[col].notna(), None)

            # Crear la tabla final
            df.to_sql(table_name, conn, if_exists='fail', index=False)
            logging.info(f"Tabla '{table_name}' re-creada exitosamente con la columna nueva.")
