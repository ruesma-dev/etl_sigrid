# etl/procesar_archivos_gastos.py
import os
import glob
import pandas as pd


def procesar_archivos_gastos() -> tuple[pd.DataFrame, pd.DataFrame] | tuple[None, None]:
    """
    Lee los .txt de la carpeta 'seguimiento', genera:
      1) tabla_dim_gastos  … dimensión con Gasto único
      2) tabla_hechos      … códigos + foreign-key a la dimensión
    Devuelve (tabla_hechos, tabla_dim_gastos).
    """
    carpeta = os.path.join(os.getcwd(), "seguimiento")
    patron  = os.path.join(carpeta, "*.txt")

    if not os.path.isdir(carpeta):
        print(f"Carpeta '{carpeta}' inexistente"); return None, None

    archivos = glob.glob(patron)
    if not archivos:
        print("No hay .txt que procesar"); return None, None

    # ---------- Lectura ----------
    dfs: list[pd.DataFrame] = []
    for path in archivos:
        nombre = os.path.splitext(os.path.basename(path))[0]          # Codigo_Proyecto
        try:
            df = pd.read_csv(path, sep=",", dtype=str)
        except Exception as exc:
            print(f"Error leyendo {path}: {exc}"); continue

        if list(df.columns) != ["Codigo", "Gasto", "Categoria"] or df.empty:
            print(f"Omitido {path}: formato inesperado"); continue

        df["Codigo_Proyecto"] = nombre
        dfs.append(df)

    if not dfs:
        print("Sin datos válidos"); return None, None

    df_raw = pd.concat(dfs, ignore_index=True).fillna("")

    # ---------- Dimensión Gasto único ----------
    dim_gastos = (df_raw[["Gasto", "Categoria"]]
                  .drop_duplicates(subset=["Gasto"])         # <-- Gasto ÚNICO
                  .reset_index(drop=True))
    dim_gastos["ID_Gasto"] = dim_gastos.index + 1
    dim_gastos = dim_gastos[["ID_Gasto", "Gasto", "Categoria"]]

    # ---------- Hechos / tabla códigos ----------
    df_raw["ID_Registro"] = df_raw.index + 1
    hechos = (df_raw
              .merge(dim_gastos[["ID_Gasto", "Gasto"]], on="Gasto", how="left")
              [["ID_Registro", "Codigo_Proyecto", "Codigo",
                "Categoria", "ID_Gasto"]])

    # ---------- Validación opcional ----------
    assert dim_gastos["Gasto"].is_unique, "¡La dimensión no es única en Gasto!"

    return hechos, dim_gastos


if __name__ == "__main__":
    tabla_codigos, tabla_gastos = procesar_archivos_gastos()
    if tabla_codigos is None:
        exit()

    print("\n--- Tabla_Codigos ---")
    print(tabla_codigos.head())
    print(tabla_codigos.shape)

    print("\n--- Tabla_Gastos (dimensión) ---")
    print(tabla_gastos.head())
    print(tabla_gastos.shape)

    tabla_codigos.to_csv("Tabla_Codigos_Generada.csv", sep=";", index=False)
    tabla_gastos.to_csv("Tabla_Gastos_Generada.csv",   sep=";", index=False)
    print("\nCSV generados con separador ';'")
