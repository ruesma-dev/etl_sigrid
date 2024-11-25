ETL Service - Proceso de Extracción, Transformación y Carga
Este documento proporciona una descripción detallada del microservicio ETL Service, que es responsable de extraer datos de una base de datos SQL Server y cargarlos en una base de datos PostgreSQL. El proceso sigue los principios de la Arquitectura Limpia (Clean Architecture) para garantizar un código modular, mantenible y escalable.

Tabla de Contenidos
Introducción
Arquitectura del Proyecto
Estructura de Directorios
Requisitos Previos
Instalación
Configuración
Ejecución del Proceso ETL
Uso como Función AWS Lambda
Registro de Logs
Manejo de Errores
Pruebas
Consideraciones de Seguridad
Contribuciones
Licencia
Introducción
El ETL Service es un microservicio diseñado para:

Extraer datos de una base de datos SQL Server.
Transformar los datos si es necesario (actualmente no implementado).
Cargar los datos en una base de datos PostgreSQL.
El servicio está diseñado para ser agnóstico y fácilmente integrable en diferentes entornos, incluyendo despliegues en la nube como AWS Lambda.

Arquitectura del Proyecto
El proyecto sigue los principios de la Arquitectura Limpia (Clean Architecture), lo que significa que:

Independencia de Frameworks: La arquitectura no depende de ningún framework o biblioteca externa.
Testabilidad: El código es fácilmente testeable en aislamiento.
Independencia de la UI: La interfaz de usuario o interacción externa no afecta la lógica de negocio.
Independencia de la Base de Datos: Los detalles de las bases de datos están abstraídos en repositorios.
Independencia de Agencias Externas: El código es independiente de librerías o servicios de terceros.
Estructura de Directorios
markdown
Copiar código
etl_service/
├── application/
│   ├── __init__.py
│   ├── extract_use_case.py
│   ├── load_use_case.py
│   ├── etl_process_use_case.py
├── domain/
│   ├── __init__.py
│   ├── entities.py
│   ├── repositories_interfaces.py
├── infrastructure/
│   ├── __init__.py
│   ├── config.py
│   ├── sql_server_repository.py
│   ├── postgres_repository.py
├── main.py
├── requirements.txt
└── README.md
application/: Contiene los casos de uso que encapsulan la lógica de negocio.
domain/: Define las entidades y las interfaces de los repositorios.
infrastructure/: Implementa los repositorios y la configuración de la base de datos.
main.py: Punto de entrada del microservicio.
Requisitos Previos
Python 3.7 o superior.
Entornos virtuales (recomendado): venv o conda.
Bases de datos:
SQL Server con acceso de lectura.
PostgreSQL con permisos para crear bases de datos y tablas.
Instalación
Clonar el repositorio:

bash
Copiar código
git clone https://github.com/tu_usuario/etl_service.git
cd etl_service
Crear un entorno virtual:

bash
Copiar código
python -m venv venv
Activar el entorno virtual:

En Windows:

bash
Copiar código
venv\Scripts\activate
En Unix o MacOS:

bash
Copiar código
source venv/bin/activate
Instalar las dependencias:

bash
Copiar código
pip install -r requirements.txt
Configuración
La configuración del microservicio se maneja a través del archivo infrastructure/config.py, que obtiene las variables de entorno necesarias.

Variables de Entorno
Puedes establecer las siguientes variables de entorno:

SQL Server:

SQL_SERVER: Dirección del servidor SQL Server.
SQL_DATABASE: Nombre de la base de datos de origen.
SQL_DRIVER: Driver ODBC para SQL Server (por defecto: ODBC Driver 17 for SQL Server).
SQL_PORT: Puerto del servidor SQL Server (por defecto: 1433).
SQL_USER: Usuario para autenticación SQL (si aplica).
SQL_PASSWORD: Contraseña para autenticación SQL (si aplica).
PostgreSQL:

PG_SERVER: Dirección del servidor PostgreSQL.
PG_DATABASE: Nombre de la base de datos de destino.
PG_PORT: Puerto del servidor PostgreSQL (por defecto: 5432).
PG_USER: Usuario de PostgreSQL.
PG_PASSWORD: Contraseña de PostgreSQL.
Configuración por Defecto
Si no se establecen las variables de entorno, se utilizarán los valores por defecto especificados en config.py.

Ejemplo de Archivo .env
Puedes crear un archivo .env en la raíz del proyecto para definir las variables de entorno:

makefile
Copiar código
SQL_SERVER=localhost
SQL_DATABASE=MiBaseDeDatosSQL
SQL_DRIVER=ODBC Driver 17 for SQL Server
SQL_PORT=1433
SQL_USER=mi_usuario_sql
SQL_PASSWORD=mi_contraseña_sql

PG_SERVER=localhost
PG_DATABASE=MiBaseDeDatosPostgres
PG_PORT=5432
PG_USER=mi_usuario_postgres
PG_PASSWORD=mi_contraseña_postgres
Luego, utiliza una herramienta como python-dotenv para cargar estas variables. Añade python-dotenv a tus dependencias si decides usar esta opción.

Ejecución del Proceso ETL
Ejecutar el Script Principal
Para ejecutar el proceso ETL, utiliza el script main.py.

Transferir Todas las Tablas
Si deseas transferir todas las tablas de la base de datos de origen:

bash
Copiar código
python main.py
Transferir Tablas Específicas
Si deseas transferir tablas específicas, proporciona los nombres de las tablas como argumentos:

bash
Copiar código
python main.py tabla1 tabla2 tabla3
Ejemplo con la Tabla 'prv'
Para transferir solo la tabla 'prv':

bash
Copiar código
python main.py prv
Notas Importantes
Tablas Vacías: El proceso omitirá las tablas que estén vacías en la base de datos de origen.
Creación de la Base de Datos: Si la base de datos de destino en PostgreSQL no existe, el proceso intentará crearla.
Uso como Función AWS Lambda
Si deseas desplegar este microservicio como una función AWS Lambda, sigue estos pasos:

Añadir un Manejador para Lambda

En main.py, añade la función lambda_handler:

python
Copiar código
def lambda_handler(event, context):
    tables_to_transfer = event.get('tables_to_transfer', [])
    main(tables_to_transfer)
Empaquetar las Dependencias

Las dependencias deben estar empaquetadas junto con el código o utilizando capas de Lambda.

Configurar Variables de Entorno

Establece las variables de entorno necesarias en la configuración de la función Lambda.

Desplegar la Función

Utiliza herramientas como Serverless Framework, AWS SAM, o Zappa para facilitar el despliegue.

Registro de Logs
El microservicio utiliza el módulo logging para registrar información sobre el proceso.

Nivel de Logging: Por defecto, está configurado en INFO.
Formato de los Logs: '%(asctime)s - %(levelname)s - %(message)s'.
Cambiar el Nivel de Logging
Puedes cambiar el nivel de logging a DEBUG para obtener información más detallada:

python
Copiar código
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
Guardar Logs en un Archivo
Para guardar los logs en un archivo, ajusta la configuración:

python
Copiar código
logging.basicConfig(filename='etl_process.log', level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
Manejo de Errores
Excepciones por Tabla: El proceso maneja excepciones individualmente por cada tabla. Si ocurre un error al procesar una tabla, se registra y el proceso continúa con la siguiente.
Registro de Errores: Todos los errores se registran en los logs con nivel ERROR.