import pandas as pd
import os
import glob


def procesar_archivos_gastos():
    # 1. Especificar la carpeta donde están los archivos
    nombre_carpeta = "seguimiento"
    ruta_carpeta = os.path.join(os.getcwd(), nombre_carpeta)

    if not os.path.isdir(ruta_carpeta):
        print(f"Error: La carpeta '{nombre_carpeta}' no se encuentra en el directorio actual.")
        print(f"Directorio actual: {os.getcwd()}")
        return None, None

    patron_archivos = os.path.join(ruta_carpeta, "*.txt")
    archivos_txt = glob.glob(patron_archivos)

    if not archivos_txt:
        print(f"No se encontraron archivos .txt en la carpeta '{nombre_carpeta}'.")
        return None, None

    lista_dataframes = []
    print(f"Archivos .txt encontrados en '{nombre_carpeta}':")
    for archivo in archivos_txt:
        nombre_archivo_sin_extension = os.path.splitext(os.path.basename(archivo))[0]
        print(f" - Procesando: {os.path.basename(archivo)} (Codigo_Proyecto: {nombre_archivo_sin_extension})")
        try:
            df_temp = pd.read_csv(archivo, sep=',', dtype=str)
            if not df_temp.empty and list(df_temp.columns) == ['Codigo', 'Gasto', 'Categoria']:
                df_temp[
                    'Codigo_Proyecto'] = nombre_archivo_sin_extension  # Añadir el nombre del archivo como Codigo_Proyecto
                lista_dataframes.append(df_temp)
            else:
                print(
                    f"Advertencia: El archivo {os.path.basename(archivo)} no tiene el formato esperado (Codigo,Gasto,Categoria) o está vacío. Se omitirá.")
        except Exception as e:
            print(f"Error al leer el archivo {os.path.basename(archivo)}: {e}")

    if not lista_dataframes:
        print("No se pudieron leer datos válidos de ningún archivo .txt.")
        return None, None

    # 2. Concatenar todos los DataFrames
    df_concatenado = pd.concat(lista_dataframes, ignore_index=True)
    df_concatenado.fillna('', inplace=True)

    # 3. Crear Tabla_Gastos_Categorias (dimensión de Gasto-Categoria)
    # Ahora incluimos Codigo_Proyecto para asegurar que ID_Gasto_Categoria sea único
    # si el mismo Gasto-Categoria puede aparecer en diferentes proyectos.
    # Si quieres que Gasto-Categoria sea globalmente único sin importar el proyecto,
    # entonces no incluyas Codigo_Proyecto aquí. Por ahora, lo mantendremos como está
    # en tu solicitud original: ID único para Gasto-Categoria.
    df_gastos_categorias_dim = df_concatenado[['Gasto', 'Categoria']].drop_duplicates().reset_index(drop=True)
    df_gastos_categorias_dim['ID_Gasto_Categoria'] = df_gastos_categorias_dim.index + 1
    df_gastos_categorias_dim = df_gastos_categorias_dim[['ID_Gasto_Categoria', 'Gasto', 'Categoria']]

    # 4. Preparar Tabla_Codigos
    df_concatenado['ID_Gasto'] = df_concatenado.index + 1

    df_final_codigos = pd.merge(
        df_concatenado,
        df_gastos_categorias_dim,
        on=['Gasto', 'Categoria'],
        how='left'
    )

    # Seleccionar y ordenar las columnas para Tabla_Codigos, incluyendo Codigo_Proyecto
    tabla_codigos = df_final_codigos[['ID_Gasto', 'Codigo_Proyecto', 'Codigo', 'ID_Gasto_Categoria']]

    tabla_gastos_categorias = df_gastos_categorias_dim

    return tabla_codigos, tabla_gastos_categorias


# --- Ejecución del script ---
if __name__ == "__main__":
    tabla_codigos_final, tabla_gastos_categorias_final = procesar_archivos_gastos()

    if tabla_codigos_final is not None and tabla_gastos_categorias_final is not None:
        print("\n--- Tabla_Codigos ---")
        print(tabla_codigos_final.head())
        print(f"\nDimensiones de Tabla_Codigos: {tabla_codigos_final.shape}")

        print("\n--- Tabla_Gastos_Categorias ---")
        print(tabla_gastos_categorias_final.head())
        print(f"\nDimensiones de Tabla_Gastos_Categorias: {tabla_gastos_categorias_final.shape}")

        try:
            tabla_codigos_final.to_csv("Tabla_Codigos_Generada.csv", index=False, sep=';')
            tabla_gastos_categorias_final.to_csv("Tabla_Gastos_Categorias_Generada.csv", index=False, sep=';')
            print(
                "\nTablas guardadas como 'Tabla_Codigos_Generada.csv' y 'Tabla_Gastos_Categorias_Generada.csv' en el directorio del script.")
            print("Se usó ';' como separador para evitar problemas con comas en los datos.")
        except Exception as e:
            print(f"\nError al guardar las tablas en CSV: {e}")