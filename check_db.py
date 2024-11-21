import os
import subprocess
import logging


def ensure_directories_exist(base_path, subdirectories):
    """
    Crea las subcarpetas necesarias si no existen.
    """
    for subdirectory in subdirectories:
        dir_path = os.path.join(base_path, subdirectory)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            print(f"Carpeta creada: {dir_path}")
        else:
            print(f"Carpeta ya existe: {dir_path}")


def delete_database(database_name):
    """
    Elimina una base de datos de SQL Server.
    """
    try:
        print(f"Eliminando la base de datos '{database_name}' si existe...")
        delete_cmd = f"DROP DATABASE {database_name}"
        cmd = [
            'sqlcmd',
            '-S', 'localhost',
            '-E',
            '-Q', delete_cmd
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
        if result.returncode == 0:
            print(f"La base de datos '{database_name}' se eliminó correctamente.")
        else:
            print(f"Error al eliminar la base de datos: {result.stderr}")
    except Exception as e:
        print(f"Error durante la eliminación de la base de datos: {e}")


def restore_database_with_temporary_name(bak_file_path, new_database_name="TemporaryDB"):
    """
    Restaura una base de datos desde un archivo .bak con un nuevo nombre, utilizando SQL Server.
    """
    base_path = r"C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\Backup"
    subdirectories = ["Data", "Logs"]
    ensure_directories_exist(base_path, subdirectories)

    # Rutas para los archivos de base de datos
    data_path = os.path.join(base_path, "Data", f"{new_database_name}.mdf")
    log_path = os.path.join(base_path, "Logs", f"{new_database_name}_Log.ldf")

    # Verificar si el archivo .bak existe
    if not os.path.exists(bak_file_path):
        print(f"Error: El archivo .bak no existe en la ruta especificada: {bak_file_path}")
        return False

    try:
        # Obtener los nombres lógicos del archivo .bak
        print("Obteniendo los nombres lógicos del archivo .bak...")
        filelist_cmd = f"RESTORE FILELISTONLY FROM DISK = '{bak_file_path}'"
        cmd = [
            'sqlcmd',
            '-S', 'localhost',
            '-E',
            '-Q', filelist_cmd
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, shell=True, check=True)
        filelist_output = result.stdout
        print("Salida de FILELISTONLY obtenida correctamente:")
        print(filelist_output)

        # Parsear nombres lógicos
        logical_names = parse_logical_names(filelist_output)
        print(f"Nombres lógicos detectados: {logical_names}")

        # Preparar comando de restauración
        restore_cmd = f"""
        RESTORE DATABASE [{new_database_name}]
        FROM DISK = '{bak_file_path}'
        WITH 
        MOVE '{logical_names["data"]}' TO '{data_path}',
        MOVE '{logical_names["log"]}' TO '{log_path}',
        REPLACE, STATS = 10;
        """
        print("Comando RESTORE generado:")
        print(restore_cmd)

        # Guardar el comando en un archivo temporal para evitar problemas de comillas
        temp_sql_file = os.path.join(base_path, "restore_command.sql")
        with open(temp_sql_file, "w") as f:
            f.write(restore_cmd)

        # Ejecutar el archivo SQL desde sqlcmd
        restore_cmd_exec = [
            'sqlcmd',
            '-S', 'localhost',
            '-E',
            '-i', temp_sql_file
        ]

        print(f"Iniciando restauración de la base de datos como '{new_database_name}'...")
        restore_result = subprocess.run(restore_cmd_exec, capture_output=True, text=True, shell=True)

        print(f"Salida del comando de restauración:")
        print(restore_result.stdout)

        if restore_result.returncode != 0:
            print(f"Error durante la restauración: {restore_result.stderr}")
            return False

        print(f"La base de datos '{new_database_name}' se restauró correctamente.")

        # Comprobar si la base de datos está levantada
        if verify_database_status(new_database_name):
            print(f"La base de datos '{new_database_name}' está ONLINE y funcional.")
            return True
        else:
            print(f"Error: La base de datos '{new_database_name}' no se encuentra en la lista de bases de datos.")
            return False

    except subprocess.CalledProcessError as e:
        print(f"Error al restaurar la base de datos: {e}")
        return False



def parse_logical_names(filelist_output):
    """
    Parsear los nombres lógicos desde la salida de RESTORE FILELISTONLY.
    """
    import re
    logical_names = {"data": None, "log": None}
    lines = filelist_output.splitlines()

    for line in lines:
        match = re.match(r"^\s*(\S+)\s+([A-Za-z]:\\.+?\.\w+)\s+([DL])", line)
        if match:
            logical_name, physical_name, file_type = match.groups()
            if file_type == "D":  # Archivo de datos
                logical_names["data"] = logical_name
            elif file_type == "L":  # Archivo de log
                logical_names["log"] = logical_name

    if not logical_names["data"] or not logical_names["log"]:
        raise ValueError("No se pudieron determinar los nombres lógicos del archivo .bak.")

    return logical_names


def verify_database_status(database_name):
    """
    Verifica si la base de datos está ONLINE en SQL Server.
    """
    try:
        print(f"Verificando si la base de datos '{database_name}' está ONLINE...")
        query_cmd = f"SELECT name, state_desc FROM sys.databases WHERE name = '{database_name}'"
        cmd = [
            'sqlcmd',
            '-S', 'localhost',
            '-E',
            '-Q', query_cmd
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, shell=True, check=True)
        if database_name in result.stdout and "ONLINE" in result.stdout:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error al verificar el estado de la base de datos: {e}")
        return False


if __name__ == "__main__":
    # Eliminar la base de datos si existe previamente
    delete_database("TemporaryDB")

    # Restaurar la base de datos
    bak_file_path = r"C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\Backup\ruesma202411070030.bak"
    new_database_name = "TemporaryDB"

    if restore_database_with_temporary_name(bak_file_path, new_database_name):
        print(f"La base de datos '{new_database_name}' fue restaurada y está funcional.")
    else:
        print("La restauración de la base de datos falló.")
