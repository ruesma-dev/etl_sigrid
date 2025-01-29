# etl_service/application/transformations/second_phase_use_case_obrparpre_planif.py

import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from calendar import monthrange
import io
import numpy as np
from calendar import monthrange


class SecondPhaseUseCaseObrparprePlanif:
    """
    Caso de uso de segunda fase para procesar la columna 'planif' en la tabla 'obrparpar'.

    - 'fasnum' se reinicia por cada fila original (no por 'obride').
    - Además de 'obride' y la columna spliteada (ahora llamada 'porcentaje'),
      queremos copiar también las columnas originales: can, pre, paride, fas, amb.
    - Añadimos:
        - 'importe' = can * pre
        - 'importe_fase' = importe * porcentaje
    - Incluimos también la columna 'porcentaje' (el valor tras hacer el split).

    Flujo:
      1) Filtrar filas donde 'planif' no sea blanco.
      2) Agregar 'original_idx' (índice de la fila original).
      3) Hacer split por '|'.
      4) Explode, para que cada valor sea una fila nueva.
      5) 'fasnum' se reinicia con cada nueva fila original.
      6) Se calcula:
         - porcentaje = valor numérico obtenido del split
         - importe = can * pre (si existen esas columnas)
         - importe_fase = importe * porcentaje
      7) Seleccionamos las columnas:
         obride, can, pre, paride, fas, amb, porcentaje, fasnum, importe, importe_fase
         (aquellas que existan realmente).
      8) Guardar los resultados en 'obrparpre_planif_exploded.csv'
         e imprimir las primeras 50 filas para revisión.
    """

    def __init__(self, postgres_repo):
        self.postgres_repo = postgres_repo
        # self.ambito = ambito

    def execute_obrparpre_planif_transform(self, obrparpre_key="obrparpre", new_table_key="obrparpre_planif", ambito=8):
        """
        Lógica de transformación para la columna 'planif' (en 'obrparpar'):
          - Filtra filas vacías en 'planif'.
          - Explota la columna 'planif' separada por '|', nombrando el valor numérico resultante como 'porcentaje'.
          - Reinicia 'fasnum' por cada fila original.
          - Calcula 'importe' = can * pre (si existen 'can' y 'pre').
          - Calcula 'importe_fase' = importe * porcentaje (si existen las tres columnas).
          - Copia las columnas:
             obride, can, pre, paride, fas, amb, porcentaje, fasnum, importe, importe_fase.
          - Guarda el resultado en CSV y muestra las primeras 50 filas.
        """
        try:
            logging.info(
                f"=== [Obrparpre Planif] Procesando columna 'planif' en '{obrparpre_key}' ==="
            )

            # 1. Obtener el nombre real de la tabla (y de la "nueva" tabla, si fuese necesario)
            obrparpar_table = self._get_target_table_name(obrparpre_key)
            new_table = self._get_target_table_name(new_table_key)

            logging.info(f"Mapeo: {obrparpre_key} => {obrparpar_table}, {new_table_key} => {new_table}")

            # 2. Leer la tabla obrparpar desde PostgreSQL
            obrparpar_df = self._read_table_from_postgres(obrparpar_table)

            if obrparpar_df.empty:
                logging.warning(f"La tabla '{obrparpar_table}' está vacía. Abortando proceso.")
                return

            logging.info(f"Filas en '{obrparpar_table}': {len(obrparpar_df)}")

            # 3. Verificar columnas requeridas
            #    Nota: Aseguramos 'obride' y 'planif';
            #    las demás (can, pre, paride, fas, amb) pueden o no estar presentes.
            required_columns = ['obride', 'planif']
            for col in required_columns:
                if col not in obrparpar_df.columns:
                    logging.warning(f"La tabla '{obrparpar_table}' no tiene la columna '{col}'. Abortando.")
                    return

            # 4. Filtrar filas donde 'planif' esté en blanco o NaN
            obrparpar_df['planif'] = obrparpar_df['planif'].astype(str).str.strip()
            # Nos quedamos solo con las filas que tienen algo en 'planif'
            obrparpar_df = obrparpar_df[obrparpar_df['planif'] != '']
            if obrparpar_df.empty:
                logging.warning("No hay filas con 'planif' válido (no vacío). Abortando.")
                return

            # 5. Crear un índice de la fila original
            obrparpar_df['original_idx'] = obrparpar_df.index

            # 6. Hacer split de 'planif' por '|'
            obrparpar_df['planif_split'] = obrparpar_df['planif'].str.split('|')
            logging.info("=== Ejemplo de planif_split (HEAD) ===")
            logging.info(obrparpar_df[['original_idx', 'obride', 'planif', 'planif_split']].head(10).to_string())

            # 7. Explode para convertir cada elemento de la lista en una fila nueva
            exploded_df = obrparpar_df.explode('planif_split').reset_index(drop=True)
            logging.info("=== Ejemplo de exploded_df (HEAD) ===")
            logging.info(exploded_df[['original_idx', 'obride', 'planif_split']].head(10).to_string())

            # 8. Limpiar y convertir planif_split a 'porcentaje'
            exploded_df['planif_split'] = exploded_df['planif_split'].fillna('').str.strip()
            exploded_df['porcentaje'] = pd.to_numeric(exploded_df['planif_split'], errors='coerce')  # a float

            # 9. Crear 'fasnum' reiniciándose por cada 'original_idx'
            exploded_df['fasnum'] = exploded_df.groupby('original_idx').cumcount() + 1

            # 10. Calcular 'importe' = can * pre (si existen las columnas)
            if 'can' in exploded_df.columns and 'pre' in exploded_df.columns:
                # Convertir a float (por si vienen como string)
                exploded_df['can'] = pd.to_numeric(exploded_df['can'], errors='coerce')
                exploded_df['pre'] = pd.to_numeric(exploded_df['pre'], errors='coerce')
                exploded_df['importe'] = exploded_df['can'] * exploded_df['pre']
            else:
                exploded_df['importe'] = None

            # 11. Calcular 'importe_fase' = importe * porcentaje (si importe y porcentaje existen)
            exploded_df['importe_fase'] = None
            # Solo creamos un valor si 'importe' y 'porcentaje' son numéricos
            if 'importe' in exploded_df.columns and 'porcentaje' in exploded_df.columns:
                exploded_df['importe_fase'] = exploded_df['importe'] * exploded_df['porcentaje']

            # 12. Seleccionar columnas finales
            # Incluimos: obride, can, pre, paride, fas, amb, porcentaje, fasnum, importe, importe_fase
            desired_cols = [
                'obride', 'can', 'pre', 'paride', 'fas', 'amb',
                'porcentaje', 'fasnum', 'importe', 'importe_fase'
            ]
            final_cols = [c for c in desired_cols if c in exploded_df.columns]
            final_df = exploded_df[final_cols].copy()

            # == Nuevo Paso: Calcular 'importe_fase_diff' para amb = 8 agrupado por 'fas' si la fila actual tiene importe_fase != 0 ==
            if 'fas' in final_df.columns and 'amb' in final_df.columns and 'importe_fase' in final_df.columns:
                logging.info(
                    "=== Calculando 'importe_fase_diff' para amb = 8 dentro de cada 'fas' cuando importe_fase != 0 ===")

                # Inicializamos la columna con None
                final_df['importe_fase_diff'] = None

                # Máscara para filas con amb = 8
                mask_amb8 = final_df['amb'] == ambito

                # Paso 1: Calculamos la diferencia con la fila anterior dentro de cada 'fas'
                #         Esto asigna NaN a la primera fila de cada grupo (fasnum=1), o si no
                #         encuentra fila anterior en el mismo 'fas'.
                final_df.loc[mask_amb8, 'importe_fase_diff'] = (
                    final_df[mask_amb8]
                    .groupby('fas')['importe_fase']
                    .diff()
                )

                # Paso 2: Si la fila actual tiene importe_fase == 0, descartamos la diferencia (None)
                #         Solo en filas donde amb=8, claro.
                final_df.loc[mask_amb8 & (final_df['importe_fase'] == 0), 'importe_fase_diff'] = None

                # Paso 3: Si fasnum == 1, entonces importe_fase_diff = importe_fase
                final_df.loc[mask_amb8 & (final_df['fasnum'] == 1), 'importe_fase_diff'] = (
                    final_df.loc[mask_amb8 & (final_df['fasnum'] == 1), 'importe_fase']
                )

                logging.info("=== Ejemplo de 'importe_fase_diff' calculado (HEAD) ===")
                logging.info(
                    final_df.loc[mask_amb8, ['fas', 'fasnum', 'importe_fase', 'importe_fase_diff']]
                    .head(20)
                    .to_string()
                )

            dim_obra_fases_table = "DimObraFases"  # Ajusta al nombre real de la tabla
            dim_obra_fases_df = self._read_table_from_postgres(dim_obra_fases_table)

            if dim_obra_fases_df.empty:
                logging.warning(f"La tabla '{dim_obra_fases_table}' está vacía o no existe; no se añade 'fecha_fin'.")
            else:
                # Verificamos que existan las columnas necesarias
                required_merge_cols = ["obride", "fasnum", "fecha_fin"]
                if all(col in dim_obra_fases_df.columns for col in required_merge_cols):
                    # Hacemos el merge (join) con final_df
                    # 'on=["obride","fasnum"]' indica que se unan por esas dos columnas
                    # 'how="left"' conserva todas las filas de final_df, añadiendo fecha_fin solo si coincide
                    final_df = final_df.merge(
                        dim_obra_fases_df[["obride", "fasnum", "fecha_fin"]],
                        on=["obride", "fasnum"],
                        how="left"
                    )
                    logging.info("Se ha unido la columna 'fecha_fin' de la tabla 'DimObraFases' a 'final_df'.")
                else:
                    logging.warning(
                        f"No se pudo unir la columna 'fecha_fin'; faltan columnas {required_merge_cols} en '{dim_obra_fases_table}'."
                    )

            # (Ejemplo) Después de construir y/o modificar final_df
            #           y ANTES de crear la tabla o guardar a CSV,
            #           insertamos este bloque:

            dim_ambito_obra_table = "DimFasesAmbito"  # Ajusta al nombre real de la tabla
            dim_ambito_obra_df = self._read_table_from_postgres(dim_ambito_obra_table)

            if dim_ambito_obra_df.empty:
                logging.warning(f"La tabla '{dim_ambito_obra_table}' está vacía o no existe; no se añade 'plafec'.")
            else:
                # Verificamos que existan las columnas necesarias para el join
                required_merge_cols = ["obride", "amb", "fas", "plafec"]
                if all(col in dim_ambito_obra_df.columns for col in required_merge_cols):
                    # Hacemos el merge (join) con final_df
                    # 'on=["obride","amb","fas"]' indica que se unan por esas tres columnas
                    # 'how="left"' conserva todas las filas de final_df, añadiendo plafec solo si coincide
                    final_df = final_df.merge(
                        dim_ambito_obra_df[["obride", "amb", "fas", "plafec"]],
                        on=["obride", "amb", "fas"],
                        how="left"
                    )
                    logging.info("Se ha unido la columna 'plafec' de la tabla 'DimAmbitoObra' a 'final_df'.")
                else:
                    logging.warning(
                        f"No se pudo unir la columna 'plafec'; faltan columnas {required_merge_cols} en '{dim_ambito_obra_table}'."
                    )

            if 'plafec' in final_df.columns and 'fas' in final_df.columns:
                logging.info("=== Ajustando la columna 'plafec' sumando (fas - 1) meses con control de fin de mes ===")

                # Llamada a la función vectorizada para ajustar 'plafec' sumando meses
                add_months_eom_vectorized(final_df, date_col='plafec', fas_col='fasnum', new_col='plafec_final')

                # Ejemplo: filtrar algunas filas para mostrar en el log
                logging.info("=== Ejemplo de la columna 'plafec_final' (HEAD) ===")
                logging.info(
                    final_df[['obride', 'fasnum', 'plafec', 'plafec_final']].head(20).to_string()
                )

            else:
                logging.warning("No se encontraron las columnas 'plafec' y/o 'fas'; no se realizó el ajuste de meses.")

            # # 13. Filtrar para obride = 2098292 e imprimir las primeras 50 filas
            # filtered_df = final_df[final_df['obride'] == 2098292]
            # logging.info("=== Ejemplo de las primeras 50 filas transformadas para obride = 2098292 ===")
            # logging.info(filtered_df.head(500).to_string())
            #
            # # 14. Guardar a CSV solo las filas filtradas
            # output_csv = "obrparpre_planif_exploded_2098292.csv"
            # filtered_df.to_csv(output_csv, index=False, encoding='utf-8')

            # output_csv = f'{new_table_key}.csv'
            # final_df.to_csv(output_csv, index=False, encoding='utf-8')
            # logging.info(f"Se ha guardado el CSV filtrado para obride = 2098292: {output_csv}")
            # logging.info("=== Comienzo de carga ===")

            self._create_new_table(new_table, final_df)
            logging.info(f"Tabla '{new_table}' creada (o recreada) con el contenido de final_df en PostgreSQL.")

            logging.info("=== Proceso completado (CSV y Tabla en PostgreSQL) ===")


        except SQLAlchemyError as e:
            logging.error(f"Error SQLAlchemy en Obrparpre Planif: {e}")
        except Exception as ex:
            logging.error(f"Error inesperado en Obrparpre Planif: {ex}")

    # ------------------------------------------------
    # Métodos de apoyo
    # ------------------------------------------------
    def _get_target_table_name(self, table_key: str) -> str:
        """
        Retorna el 'target_table' real definido en TABLE_CONFIG
        (si no se encuentra, retorna el key tal cual).
        """
        from etl_service.application.transformations.table_config import TABLE_CONFIG
        return TABLE_CONFIG.get(table_key, {}).get('target_table', table_key)

    def _read_table_from_postgres(self, table_name: str) -> pd.DataFrame:
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

    # def _create_new_table(self, table_name: str, df: pd.DataFrame):
    #     """
    #     Crea o recrea una tabla en PostgreSQL con el DataFrame proporcionado,
    #     eliminando la tabla previa si existe (en vez de renombrarla).
    #     Y usa method='multi' con chunksize para mejorar la velocidad.
    #     """
    #     with self.postgres_repo.engine.begin() as conn:
    #         # 1) Eliminar la tabla si existe
    #         conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
    #
    #         # 2) Convertir NaN en None
    #         df = df.where(pd.notnull(df), None)
    #
    #         # 3) Crear la nueva tabla usando to_sql con 'if_exists="fail"' (ya la hemos droppeado)
    #         #    method="multi" => Insert en lotes
    #         #    chunksize= (p.e. 10_000) => Ajustar según tu caso
    #         df.to_sql(
    #             table_name,
    #             conn,
    #             if_exists='fail',
    #             index=False,
    #             method='multi',
    #             chunksize=10_000
    #         )
    #         logging.info(f"Tabla '{table_name}' creada con éxito usando inserts por lotes.")

    # def _create_new_table(self, table_name: str, df: pd.DataFrame):
    #     """
    #     Crea o recrea una tabla en PostgreSQL con el DataFrame proporcionado usando COPY,
    #     lo cual suele ser mucho más rápido para grandes volúmenes de datos.
    #     """
    #     with self.postgres_repo.engine.begin() as conn:
    #         print('# 1) Borramos la tabla si existe')
    #         conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
    #
    #         print('# 2) Convertimos NaN en None')
    #         df = df.where(pd.notnull(df), None)
    #
    #         print(df['fecha_fin'])
    #
    #
    #         print('# 3) Limpiamos columnas de tipo timestamp para evitar problemas')
    #         for col in df.columns:
    #             if "fecha" in col.lower() or "timestamp" in col.lower() or pd.api.types.is_datetime64_any_dtype(
    #                     df[col]):
    #                 print(f'Procesando columna {col} para asegurar formato datetime y reemplazar NaT...')
    #                 # Convertimos a datetime (si aún no lo es)
    #                 df[col] = pd.to_datetime(df[col], errors="coerce")
    #                 # Reemplazamos NaT (valores no válidos) con None de manera vectorizada
    #                 df[col] = df[col].fillna(value=pd.NaT).astype(object).where(df[col].notna(), None)
    #                 print(f'Columna {col} procesada con éxito.')
    #
    #         print(df['fecha_fin'])
    #
    #         numeric_cols = []
    #         # Si conoces las columnas numeric, puedes listarlas:
    #         # numeric_cols = ['porcentaje','importe','importe_fase', ...]
    #
    #         # O detectar por nombre si "porcentaje" está en col, etc.
    #         for col in df.columns:
    #             if "porcentaje" in col.lower() or "importe" in col.lower():
    #                 numeric_cols.append(col)
    #
    #         for col in numeric_cols:
    #             print(f"Convirtiendo {col} a numérico")
    #             # Convertir a numérico, cadenas vacías => NaN
    #             df[col] = pd.to_numeric(df[col], errors='coerce')
    #             # Reemplazar NaN => None
    #             df[col] = df[col].where(pd.notnull(df[col]), None)
    #
    #         print('# 4) Creamos la tabla manualmente o usando to_sql con 0 fila')
    #         #    para que tenga la estructura, pero sin insertar datos.
    #         df.head(0).to_sql(table_name, conn, if_exists='fail', index=False)  # Crea estructura sin filas
    #
    #         # 5) Generar CSV en memoria
    #         csv_buffer = io.StringIO()
    #         df.to_csv(csv_buffer, index=False, header=False)
    #         csv_buffer.seek(0)
    #
    #         # 6) Realizar COPY con raw_connection
    #         raw_conn = conn.connection
    #         cursor = raw_conn.cursor()
    #
    #         copy_sql = f"""
    #             COPY "{table_name}" FROM STDIN WITH CSV NULL 'None'
    #         """
    #         cursor.copy_expert(copy_sql, csv_buffer)
    #         cursor.close()
    #         logging.info(f"Tabla '{table_name}' creada y datos insertados vía COPY con éxito.")
    def _create_new_table(self, table_name: str, df: pd.DataFrame):
        """
        Crea o recrea una tabla en PostgreSQL con el DataFrame proporcionado usando COPY,
        lo cual suele ser mucho más rápido para grandes volúmenes de datos que 'to_sql(method="multi")'.

        Pasos:
          1) Elimina la tabla si existe.
          2) Crea la tabla vacía a partir de df.head(0).
          3) Genera un CSV en memoria con separador ',' y decimal '.'.
             Reemplaza valores nulos con 'None'.
          4) Ejecuta COPY FROM STDIN WITH CSV NULL 'None' para la inserción masiva.
        """

        # 1) Conexión con BEGIN para ejecutar en transacción
        with self.postgres_repo.engine.begin() as conn:
            # LOG: Inicia proceso
            total_rows = len(df)
            logging.info(f"Iniciando creación de '{table_name}' en PostgreSQL con {total_rows} filas...")

            # Eliminar la tabla si existe
            logging.info(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))

            # Convertir NaN en None (para evitar problemas en CSV)
            df = df.where(pd.notnull(df), None)

            # 2) Crear la tabla vacía usando el esquema de df (pero sin insertar filas)
            logging.info("Creando la estructura de la tabla (sin filas)...")
            df.head(0).to_sql(table_name, conn, if_exists='fail', index=False)
            logging.info(f"Estructura de '{table_name}' creada con éxito.")

            # 3) Generar CSV en memoria
            logging.info(f"Generando CSV en memoria para {total_rows} filas...")
            csv_buffer = io.StringIO()
            # - sep=',' para separador de campos
            # - decimal='.' para punto decimal
            # - na_rep='None' para que los nulos se escriban como 'None'
            # - header=False para no incluir cabeceras
            df.to_csv(csv_buffer, index=False, header=False,
                      sep=',', decimal='.', na_rep='None')
            csv_buffer.seek(0)
            logging.info("CSV en memoria listo. Iniciando COPY...")

            # 4) Realizar COPY con raw_connection
            raw_conn = conn.connection
            cursor = raw_conn.cursor()

            copy_sql = f"""
                COPY "{table_name}" FROM STDIN WITH CSV
                NULL 'None'
                DELIMITER ',' 
            """
            cursor.copy_expert(copy_sql, csv_buffer)
            cursor.close()

            logging.info(f"Tabla '{table_name}' creada e insertada vía COPY con éxito. Finalizado.")


def add_months_eom(original_date, months_to_add):
    """
    Suma 'months_to_add' meses a 'original_date' (tipo datetime o string de fecha),
    ajustando el día al último disponible del mes si la fecha resultante no existe.

    Ejemplos:
      - 2024-08-31 + 1 mes => 2024-09-30 (no existe el 31 en septiembre).
      - 2023-01-31 + 1 mes => 2023-02-28 (o 29 en año bisiesto).
      - 2023-04-30 + 1 mes => 2023-05-30 (existe el 30).
    """
    if pd.isnull(original_date):
        return None
    if months_to_add is None or pd.isnull(months_to_add):
        return original_date  # no hacemos nada si no hay un valor en months_to_add

    # Convertimos a Timestamp (si no lo está)
    date = pd.to_datetime(original_date)

    # Si no hay meses que sumar o es cero, devolvemos la fecha tal cual
    if months_to_add == 0:
        return date

    # Nuevo año y mes
    year = date.year
    month = date.month + months_to_add
    day = date.day

    # Ajustamos año/mes en base a la suma
    # (month-1)//12 son las "vueltas" enteras de 12 meses
    year += (month - 1) // 12
    month = (month - 1) % 12 + 1

    # Días que tiene el mes resultante
    days_in_new_month = monthrange(year, month)[1]
    # Ajustamos el día al mínimo entre 'day' y 'days_in_new_month'
    new_day = min(day, days_in_new_month)

    return pd.Timestamp(year=year, month=month, day=new_day)

def add_months_eom_vectorized(df, date_col='plafec', fas_col='fas', new_col='plafec_final'):
    """
    Transformación vectorizada que:
      1) Convierte la columna date_col a datetime.
      2) Ajusta esa fecha al día 1 del mes.
      3) Suma (fas - 1) meses.
      4) Ajusta la fecha al último día del mes resultante.

    Crea la columna new_col con la fecha final.
    """

    # 1) Convertir la columna de fecha a datetime
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    # Filtrar las filas donde la fecha es NaT (no válida)
    valid_date_mask = df[date_col].notnull()

    # Inicializar la nueva columna con NaT para filas inválidas
    df[new_col] = pd.NaT

    if valid_date_mask.any():
        # Aplicamos los cálculos solo a las filas con fechas válidas
        valid_df = df[valid_date_mask].copy()

        # 2) Extraer año, mes; forzar día = 1
        valid_df['year'] = valid_df[date_col].dt.year
        valid_df['month'] = valid_df[date_col].dt.month
        valid_df['day'] = 1

        # 3) (fas - 1) => cuántos meses sumamos
        valid_df['shift_months'] = (valid_df[fas_col].fillna(1) - 1).astype(int)

        # Sumamos shift_months a la columna 'month'
        valid_df['month'] += valid_df['shift_months']

        # Ajustamos 'year' y 'month'
        valid_df['year'] += (valid_df['month'] - 1) // 12
        valid_df['month'] = ((valid_df['month'] - 1) % 12) + 1

        # Convertimos 'month' y 'year' a int explícitamente
        valid_df['month'] = valid_df['month'].astype(int)
        valid_df['year'] = valid_df['year'].astype(int)

        # 4) Ajustar el 'day' al último día del mes
        year_vals = valid_df['year'].values
        month_vals = valid_df['month'].values

        # Días base por mes (año no bisiesto)
        days_per_month = np.array([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31], dtype=int)

        def is_leap(y):
            return (y % 4 == 0) & ((y % 100 != 0) | (y % 400 == 0))

        days_in_result = days_per_month[month_vals - 1]
        feb_mask = (month_vals == 2) & is_leap(year_vals)
        days_in_result[feb_mask] = 29

        # Ajustamos el día
        valid_df['day'] = np.minimum(valid_df['day'].values, days_in_result)

        # 5) Reconstruimos la fecha final
        valid_df[new_col] = pd.to_datetime(
            dict(year=valid_df['year'], month=valid_df['month'], day=valid_df['day']),
            errors='coerce'
        )

        # Asignamos las fechas finales de las filas válidas al DataFrame original
        df.loc[valid_date_mask, new_col] = valid_df[new_col]

    # Opcional: limpiar columnas intermedias
    drop_cols = ['year', 'month', 'day', 'shift_months']
    df.drop(columns=[col for col in drop_cols if col in df.columns], inplace=True)


#TODO: solo se esta haciedno el estudio de planificacion valorada para ambito 8 (master coste)