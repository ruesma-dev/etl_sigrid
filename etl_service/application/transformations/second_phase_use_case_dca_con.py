# etl_service/application/transformations/second_phase_use_case_dca_con.py

import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

class SecondPhaseUseCaseConObraDca:
    """
    Caso de uso de segunda fase para crear 'codigo_obra' en la tabla DCA,
    basándose en la relación CON (DimConceptosETC) → OBR (FactObra) → DCA.
    """
    def __init__(self, postgres_repo):
        self.postgres_repo = postgres_repo

    def execute_con_obra_dca_codigo_obra(self,
                                         con_key="con",
                                         obr_key="obr",
                                         dca_key="dca"):
        """
        Crea la columna 'codigo_obra' en DCA, que obtiene de con.cod,
        relacionando:
          - con: con.ide == obr.cenide
          - obr: obr.ide == dca.obride
        """
        try:
            logging.info(
                f"=== [con → obr → dca] Creando 'codigo_obra' a partir de {con_key}, {obr_key}, {dca_key} ===")

            # 1) Obtener los nombres REALES de las tablas en la DB
            con_table = self._get_target_table_name(con_key)
            obr_table = self._get_target_table_name(obr_key)
            dca_table = self._get_target_table_name(dca_key)

            logging.info(
                f"Mapeo: {con_key} => {con_table}, {obr_key} => {obr_table}, {dca_key} => {dca_table}")

            # 1) Leer tablas
            con_df = self._read_table_from_postgres(con_table)
            obr_df = self._read_table_from_postgres(obr_table)
            dca_df = self._read_table_from_postgres(dca_table)

            if con_df.empty or obr_df.empty or dca_df.empty:
                logging.warning("Alguna de las tablas [con, obr, dca] está vacía. No se hace la transformación.")
                return

            # 2) Merge(obr, con)
            if 'ide' not in con_df.columns or 'cod' not in con_df.columns:
                logging.warning(f"La tabla '{con_table}' no tiene 'ide' o 'cod'. Abortando.")
                return
            if 'cenide' not in obr_df.columns or 'ide' not in obr_df.columns:
                logging.warning(f"La tabla '{obr_table}' no tiene 'cenide' o 'ide'. Abortando.")
                return
            if 'obride' not in dca_df.columns:
                logging.warning(f"La tabla '{dca_table}' no tiene 'obride'. Abortando.")
                return

            logging.info(f"Filas en con={len(con_df)}, obr={len(obr_df)}, dca={len(dca_df)}")

            subset_con = con_df[['ide', 'cod']].copy()
            subset_con = subset_con.rename(columns={col: f"{col}_con" for col in subset_con.columns})
            merged_obr = obr_df.merge(
                subset_con,
                how='left',
                left_on='cenide',
                right_on='ide_con',
            )

            # 3) Merge(dca, merged_obr) => left_on=dca.obride, right_on=merged_obr.ide
            # Renombrar las columnas generadas
            merged_obr.rename(
                columns={'ide': 'obr_ide'}, inplace=True
            )
            final_merged = dca_df.merge(
                merged_obr[['obr_ide', 'cod_con']],  # Solo pk y 'cod'
                how='left',
                left_on='obride',
                right_on='obr_ide',
            )

            # Eliminar la columna obr_ide
            final_merged.drop(columns=['obr_ide'], inplace=True)

            # 4) Crear la columna 'codigo_obra' con el valor de 'cod_con'
            final_merged.rename(
                columns={'cod_con': 'codigo_obra'}, inplace=True
            )
            print(final_merged)

            # 5) Re-crear la tabla
            self._recreate_table(dca_table, final_merged)
            logging.info(f"Tabla '{dca_table}' actualizada con 'codigo_obra'.")

        except SQLAlchemyError as e:
            logging.error(f"Error SQLAlchemy: {e}")
        except Exception as ex:
            logging.error(f"Error inesperado: {ex}")


    # --------------------------------------------------------------------------
    # Métodos de apoyo
    # --------------------------------------------------------------------------
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
        backup_table = f"{table_name}_backup"
        with self.postgres_repo.engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE IF EXISTS "{table_name}" RENAME TO "{backup_table}"'))
            logging.info(f"Tabla '{table_name}' renombrada a '{backup_table}' (backup).")

            # NaT => None en columnas datetime
            for col in df.select_dtypes(include=['datetime64[ns]']):
                df[col] = df[col].where(df[col].notna(), None)

            df.to_sql(table_name, conn, if_exists='fail', index=False)
            logging.info(f"Tabla '{table_name}' re-creada con la nueva estructura.")
