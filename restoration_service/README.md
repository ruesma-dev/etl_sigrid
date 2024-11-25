# Restoration Service

Este microservicio se encarga de restaurar una base de datos SQL Server desde un archivo `.bak` recibido a través de RabbitMQ.

## Estructura de Carpetas

- `application/`: Contiene los casos de uso de la aplicación.
- `domain/`: Define las entidades y las interfaces de los repositorios.
- `infrastructure/`: Implementa las interfaces del dominio y gestiona la configuración.
- `main.py`: Punto de entrada del microservicio.
- `Dockerfile`: Configuración para construir la imagen Docker.
- `requirements.txt`: Dependencias de Python.

## Configuración

Crea un archivo `.env` en el directorio raíz de `restoration_service/` con el siguiente contenido:

```dotenv
SQL_SERVER=localhost
SQL_DATABASE=TemporaryDB
SQL_DRIVER=ODBC Driver 17 for SQL Server
SQL_DATA_PATH=C:\Backup\LocalBaks\Data
SQL_LOG_PATH=C:\Backup\LocalBaks\Logs
LOCAL_BAK_FOLDER=C:\Backup\LocalBaks
RABBITMQ_HOST=localhost
RABBITMQ_QUEUE=restore_queue
