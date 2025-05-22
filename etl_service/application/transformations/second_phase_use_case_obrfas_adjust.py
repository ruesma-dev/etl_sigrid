# etl_service/application/transformations/second_phase_use_case_obrfas_adjust.py

import logging
import pandas as pd
from dateutil.relativedelta import relativedelta
from math import ceil
from sqlalchemy.exc import SQLAlchemyError


class SecondPhaseUseCaseObrfasAdjust:
    """
    Caso de uso:
      - Lee la tabla 'obrfas'.
      - Calcula 'diff_meses' redondeando hacia arriba entre fecha_fin y fecha_inicio.
      - Crea/ajusta la columna 'fecha_fin' restando ese número de meses solo si diff_meses != 0.
      - Genera una tabla final (o la misma) con las dos columnas ('diff_meses' y 'fecha_fin' ajustada).
    """

    def __init__(self, postgres_repo):
        self.postgres_repo = postgres_repo

    def execute_adjust_fechas(self, obrfas_table_key="obrfas", new_table_key="obrfas_adjusted"):
        """
        Lee 'obrfas', realiza los dos pasos solicitados y crea/actualiza 'fecha_fin' y la nueva columna 'diff_meses'.

        1) diff_meses = diferencia de meses (ceiling) entre fecha_fin y fecha_inicio.
        2) Si diff_meses != 0 => fecha_fin = fecha_fin - diff_meses meses.

        Crea nueva tabla con el resultado.
        """
        try:
            logging.info(f"=== [Segunda fase] Ajustando 'fecha_fin' en '{obrfas_table_key}' ===")

            # 1) Obtener nombres reales de tablas
            obrfas_name = self._get_target_table_name(obrfas_table_key)
            new_table_name = self._get_target_table_name(new_table_key)

            logging.info(f"Mapeo: {obrfas_table_key} => {obrfas_name}, => {new_table_key} => {new_table_name}")

            # 2) Leer la tabla obrfas
            df = self._read_table_from_postgres(obrfas_name)
            if df.empty:
                logging.warning(f"La tabla '{obrfas_name}' está vacía. Abortando.")
                return

            logging.info(f"Filas en '{obrfas_name}': {len(df)}")

            # Aseguramos que las columnas fecha_inicio y fecha_fin sean datetime
            df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce")
            df["fecha_fin"] = pd.to_datetime(df["fecha_fin"], errors="coerce")

            # 3) Calcular diff_meses redondeando hacia arriba
            #    Para cada fila, usamos una función que calcule la diferencia de meses con ceil
            def calc_diff_meses(row):
                f_ini = row["fecha_inicio"]
                f_fin = row["fecha_fin"]
                if pd.isnull(f_ini) or pd.isnull(f_fin):
                    return 0
                # Usamos relativedelta para obtener diferencia en años y meses
                rd = relativedelta(f_fin, f_ini)
                total_meses = rd.years * 12 + rd.months
                # Si hay días/horas/segundos de diferencia, consideramos que es un mes parcial => +1
                # es decir, si rd.days>0, redondeamos hacia arriba
                if rd.days > 0 or rd.hours > 0 or rd.minutes > 0 or rd.seconds > 0:
                    total_meses += 1
                return total_meses

            df["diff_meses"] = df.apply(calc_diff_meses, axis=1)

            # 4) Ajustar fecha_fin si diff_meses != 0
            #    Restamos X meses usando relativedelta
            def adjust_fecha_fin(row):
                if row["diff_meses"] <= 0 or pd.isnull(row["fecha_fin"]):
                    return row["fecha_fin"]
                # Restar 'diff_meses' a la fecha_fin original
                return row["fecha_fin"] - relativedelta(months=row["diff_meses"]-1)

            df["fecha_fin"] = df.apply(adjust_fecha_fin, axis=1)

            logging.info("=== Ejemplo de filas ajustadas (HEAD) ===")
            logging.info(df.head(20).to_string())

            # 5) Crear la nueva tabla en PostgreSQL
            self._create_new_table(new_table_name, df)
            logging.info(f"Tabla '{new_table_name}' creada con 'diff_meses' y 'fecha_fin' ajustados.")

        except SQLAlchemyError as e:
            logging.error(f"Error SQLAlchemy en ajustar fechas: {e}")
        except Exception as ex:
            logging.error(f"Error inesperado en ajustar fechas: {ex}")

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
        """
        from sqlalchemy import text

        with self.postgres_repo.engine.begin() as conn:
            # Borramos la tabla si existe
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))

            # Convertimos NaN en None
            df = df.where(pd.notnull(df), None)

            # Insertamos con 'to_sql' method='multi' (o un COPY si prefieres)
            df.to_sql(table_name, conn, if_exists='fail', index=False, method='multi', chunksize=10_000)
            logging.info(f"Tabla '{table_name}' creada con {len(df)} filas.")
