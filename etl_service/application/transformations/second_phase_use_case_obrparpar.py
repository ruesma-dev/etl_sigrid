# etl_service/application/transformations/second_phase_use_case_obrparpar.py

import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

class SecondPhaseUseCaseConObraObrparpar:
    """
    Caso de uso de segunda fase para crear 'codigo_obra' en la tabla obrparpar,
    basándose en la relación CON (DimConceptosETC) → OBR (FactObra) → OBRPARPAR (DimPartidasObra).
    """
    def __init__(self, postgres_repo):
        self.postgres_repo = postgres_repo

    def execute_con_obra_obrparpar_codigo_obra(self,
                                               con_key="con",
                                               obr_key="obr",
                                               obrparpar_key="obrparpar"):
        """
        Crea la columna 'codigo_obra' en obrparpar, que obtiene de con.cod,
        relacionando:
          - con: con.ide == obr.cenide
          - obr: obr.ide == obrparpar.obride
        """
        try:
            logging.info(f"=== [con → obr → dca] Creando 'codigo_obra' a partir de {con_key}, {obr_key}, {obrparpar_key} ===")

            # 1) Obtener los nombres REALES de las tablas en la DB
            con_table = self._get_target_table_name(con_key)
            obr_table = self._get_target_table_name(obr_key)
            obrparpar_table = self._get_target_table_name(obrparpar_key)

            logging.info(f"Mapeo: {con_key} => {con_table}, {obr_key} => {obr_table}, {obrparpar_key} => {obrparpar_table}")

            # 1) Leer tablas
            con_df = self._read_table_from_postgres(con_table)
            obr_df = self._read_table_from_postgres(obr_table)
            obrparpar_df = self._read_table_from_postgres(obrparpar_table)

            if con_df.empty or obr_df.empty or obrparpar_df.empty:
                logging.warning("Alguna de las tablas [con, obr, obrparpar] está vacía. No se hace la transformación.")
                return

            # 2) Merge(obr, con)
            if 'ide' not in con_df.columns or 'cod' not in con_df.columns:
                logging.warning(f"La tabla '{con_table}' no tiene 'ide' o 'cod'. Abortando.")
                return
            if 'cenide' not in obr_df.columns or 'ide' not in obr_df.columns:
                logging.warning(f"La tabla '{obr_table}' no tiene 'cenide' o 'ide'. Abortando.")
                return
            if 'obride' not in obrparpar_df.columns:
                logging.warning(f"La tabla '{obrparpar_table}' no tiene 'obride'. Abortando.")
                return

            logging.info(f"Filas en con={len(con_df)}, obr={len(obr_df)}, obrparpar={len(obrparpar_df)}")

            subset_con = con_df[['ide', 'cod']].copy()
            subset_con = subset_con.rename(columns={col: f"{col}_con" for col in subset_con.columns})
            merged_obr = obr_df.merge(
                subset_con,
                how='left',
                left_on='cenide',
                right_on='ide_con',
            )

            # 3) Merge(obrparpar, merged_obr) => left_on=obrparpar.obride, right_on=merged_obr.ide
            # Renombrar las columnas generadas
            merged_obr.rename(
                columns={'ide': 'obr_ide'}, inplace=True
            )
            final_merged = obrparpar_df.merge(
                merged_obr[['obr_ide', 'cod_con']],  # Solo pk y 'cod'
                how='left',
                left_on='obride',
                right_on='obr_ide',
            )

            # Eliminar la columna ide_y
            final_merged.drop(columns=['obr_ide'], inplace=True)


            # 5) Crear la columna 'codigo_obra' con el valor de 'cod'
            final_merged.rename(
                columns={'cod_con': 'codigo_obra'}, inplace=True
            )
            print(final_merged)

            # 6) Crear la columna 'cod_obraConcat' = cod + "_" + codigo_obra
            #    Manejo de nulos para evitar TypeError al concatenar strings con NaN
            final_merged['cod_obraConcat'] = final_merged.apply(
                lambda row: (str(row['cod']) if pd.notna(row['cod']) else '')
                            + '_'
                            + (str(row['codigo_obra']) if pd.notna(row['codigo_obra']) else ''),
                axis=1
            )

            # 5) Re-crear la tabla
            self._recreate_table(obrparpar_table, final_merged)
            logging.info(f"Tabla '{obrparpar_table}' actualizada con 'codigo_obra'.")

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
