# 04_sql_validation.py

import pytest
import logging
import pyodbc
from infrastructure.config import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@pytest.mark.database
def test_database_restoration():
    """
    Valida que la base de datos restaurada contiene los datos correctos.
    Verifica el valor del campo `cenide` en la tabla `obr` para el `ide` 496412.
    """
    query = "SELECT cenide FROM obr WHERE ide = 496412"

    try:
        # Conectar a SQL Server
        connection_string = f"DRIVER={{{Config.SQL_DRIVER}}};SERVER={Config.SQL_SERVER};DATABASE={Config.SQL_DATABASE};Trusted_Connection=yes;"
        conn = pyodbc.connect(connection_string)
        logging.info("Conexión a la base de datos establecida correctamente.")

        # Ejecutar consulta
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        assert result is not None, "No se encontraron resultados para la consulta."
        assert result[0] == 496688, f"El valor de `cenide` para `ide` 496412 es incorrecto: {result[0]} (esperado: 496688)."

        logging.info("Validación de la base de datos completada con éxito.")

    except AssertionError as ae:
        logging.error(f"Validación fallida: {ae}")
        pytest.fail(f"Prueba fallida: {ae}")

    except Exception as e:
        logging.error(f"Error al conectar o consultar la base de datos: {e}")
        pytest.fail(f"Error inesperado: {e}")

    finally:
        # Cerrar conexión solo si está definida
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")
