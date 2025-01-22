import pandas as pd

# Leer el archivo Excel ignorando las primeras 3 líneas
file_path = r'C:\\Users\\pgris\\OneDrive - Ruesma\\Documentos\\Cuadro de Mando\\LISTADO PERSONAL ASIGNADO A CENTRO DE TRABAJO.xlsx'
df = pd.read_excel(file_path, skiprows=3, engine='openpyxl')

# Encontrar la fila que contiene 'jefes de grupo' en la columna A (sin importar mayúsculas o minúsculas y que contenga ese string)
split_index = df[df.iloc[:, 0].str.contains('jefes de grupo', case=False, na=False)].index[0]

# Dividir el DataFrame en dos partes
first_part = df.iloc[:split_index]
second_part = df.iloc[split_index:]

# Eliminar filas en first_part que tengan NaN en las columnas 2 y 3 a la vez
first_part = first_part.dropna(subset=[first_part.columns[2], first_part.columns[3]], how='all')

# Lista para almacenar los dataframes resultantes del primer trozo
dataframes = []

# Variable para almacenar temporalmente las filas del dataframe actual
temp_df = []

# Bandera para indicar si estamos en el primer dataframe
first_df = True

# Iterar sobre las filas del primer trozo
for index, row in first_part.iterrows():
    # Verificar si la columna B existe y si tiene un valor no nulo
    if len(row) > 1 and pd.notna(row.iloc[1]):
        # Si temp_df no está vacío, guardamos el dataframe actual con la primera fila como encabezado
        if temp_df:
            df_temp = pd.DataFrame(temp_df)
            df_temp.columns = df_temp.iloc[0]  # Establecer la primera fila como encabezado
            df_temp = df_temp[1:]  # Eliminar la primera fila que ahora es el encabezado
            dataframes.append(df_temp)
            temp_df = []
        # Añadir la fila actual a temp_df y establecer la bandera de primer dataframe a False
        temp_df.append(row)
        first_df = False
    else:
        # Añadir la fila actual a temp_df
        temp_df.append(row)

# Añadir el último dataframe si temp_df no está vacío
if temp_df:
    df_temp = pd.DataFrame(temp_df)
    df_temp.columns = df_temp.iloc[0]  # Establecer la primera fila como encabezado
    df_temp = df_temp[1:]  # Eliminar la primera fila que ahora es el encabezado
    dataframes.append(df_temp)

# Mostrar los dataframes resultantes completos
for i, df in enumerate(dataframes):
    print(f"DataFrame {i+1}:\n{df.to_string()}\n")
