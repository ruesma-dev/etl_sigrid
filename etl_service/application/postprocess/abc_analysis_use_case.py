# etl_service/application/postprocess/abc_analysis_use_case.py

import logging
import pandas as pd
from dateutil.relativedelta import relativedelta
from math import ceil
from sqlalchemy.exc import SQLAlchemyError


class AbcAnalysisUseCase:
    """
    Caso de uso:
      - Leer DimMasterCoste, DimMasterVenta, y DimPartidasObra de PostgreSQL.
      - Hacer join (paridede -> ide).
      - Filtrar por version (defecto=1).
      - Determinar top 90% en coste y venta.
    """

    def __init__(self, postgres_repo):
        self.postgres_repo = postgres_repo

    def execute_abc_analysis(
        self,
        version_value=1,
        cost_table="DimMasterCoste",
        sale_table="DimMasterVenta",
        part_table="DimPartidasObra",
        cost_col="importe_fase",
        sale_col="importe_fase"
    ):
        """
        Lógica principal:
          - version_value: Valor de la columna 'version' por el que filtrar (default=1).
          - cost_table: Nombre de la tabla con la master coste (DimMasterCoste).
          - sale_table: Nombre de la tabla con la master venta (DimMasterVenta).
          - part_table: Nombre de la tabla con las partidas (DimPartidasObra).
          - cost_col: Columna que representa el coste (para ABC).
          - sale_col: Columna que representa la venta (para ABC).

        Este ejemplo asume que 'importe_fase' contiene el valor monetario que usaremos en ABC.
        Ajusta según la columna real de tu caso.
        """
        try:
            logging.info("=== [ABC] Leyendo tablas de Postgres... ===")
            df_cost = self._read_table(cost_table)
            df_sale = self._read_table(sale_table)
            df_parts = self._read_table(part_table)

            # Verificar que las tablas no estén vacías
            if df_cost.empty or df_sale.empty or df_parts.empty:
                logging.warning("Alguna de las tablas está vacía. Abortando ABC analysis.")
                return

            logging.info(f"Filas en {cost_table}: {len(df_cost)}, en {sale_table}: {len(df_sale)}, en {part_table}: {len(df_parts)}")

            # Filtrar por version (si las tablas tienen la columna 'version')
            if "fas" in df_cost.columns:
                df_cost = df_cost[df_cost["fas"] == version_value]
            if "fas" in df_sale.columns:
                df_sale = df_sale[df_sale["fas"] == version_value]
            if "fas" in df_parts.columns:
                df_parts = df_parts[df_parts["fas"] == version_value]

            logging.info(f"Tras filtrar por version={version_value}: coste={len(df_cost)}, venta={len(df_sale)}, parts={len(df_parts)}")

            # Joins: DimMasterCoste.paridede -> DimPartidasObra.ide
            merged_cost = df_cost.merge(
                df_parts,
                left_on="paride",
                right_on="ide",
                how="left"
            )
            print('ejecutado el join entre master coste y partidas')
            # Joins: DimMasterVenta.paridede -> DimPartidasObra.ide
            merged_sale = df_sale.merge(
                df_parts,
                left_on="paride",
                right_on="ide",
                how="left"
            )
            print('ejecutado el join entre master venta y partidas')
            logging.info(f"Filas tras join coste-partidas: {len(merged_cost)}, venta-partidas: {len(merged_sale)}")

            # ABC 90% en coste
            df_abc_cost = self._select_top_90_percent(merged_cost, cost_col)
            logging.info(f"ABC COSTE => {len(df_abc_cost)} filas (top 90%).")

            # ABC 90% en venta
            df_abc_sale = self._select_top_90_percent(merged_sale, sale_col)
            logging.info(f"ABC VENTA => {len(df_abc_sale)} filas (top 90%).")

            # Mostrar ejemplo
            logging.info("=== ABC COSTE (HEAD) ===")
            logging.info(df_abc_cost.head(10).to_string())
            logging.info("=== ABC VENTA (HEAD) ===")
            logging.info(df_abc_sale.head(10).to_string())

            # Si quisieras persistir df_abc_cost y df_abc_sale en PostgreSQL,
            # podrías hacerlo con un create_new_table(...).

        except SQLAlchemyError as e:
            logging.error(f"Error SQLAlchemy en ABC analysis: {e}")
        except Exception as ex:
            logging.error(f"Error inesperado en ABC analysis: {ex}")

    def _select_top_90_percent(self, df, value_col):
        """
        Dado un DataFrame 'df' y la columna 'value_col' con el valor
        (p.ej. coste o venta), selecciona las filas que representan
        el 90% de la suma total, ordenando descendente.
        """
        # Ordenar descendente
        df_sorted = df.sort_values(by=value_col, ascending=False).copy()
        logging.info(f"ejecutando 90 %")
        df_sorted[value_col] = pd.to_numeric(df_sorted[value_col], errors='coerce').fillna(0).astype(int)

        total_value = df_sorted[value_col].sum()
        if total_value == 0:
            # Evitamos división 0
            return df_sorted.head(0)

        logging.info(f"ejecutando 90 % fase 2")

        threshold = 0.9 * total_value

        logging.info(f"ejecutando 90 % fase 3")

        df_sorted['cumsum_value'] = df_sorted[value_col].cumsum()
        logging.info(f"ejecutando 90 % fase 4")
        df_abc = df_sorted[df_sorted['cumsum_value'] <= threshold]
        logging.info(f"ejecutando 90 % fase 5")
        return df_abc

    def _read_table(self, table_name):
        """
        Lee una tabla completa de PostgreSQL en un DataFrame.
        """
        query = f'SELECT * FROM "{table_name}"'
        try:
            with self.postgres_repo.engine.connect() as conn:
                df = pd.read_sql(query, conn)
            return df
        except SQLAlchemyError as e:
            logging.error(f"No se pudo leer la tabla '{table_name}': {e}")
            return pd.DataFrame()
