# ETL Service for SQL Server to PostgreSQL

![ETL Pipeline](https://img.shields.io/badge/ETL-Pipeline-blue)
![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
  - [Clean Architecture](#clean-architecture)
  - [Project Structure](#project-structure)
- [Features](#features)
  - [Data Extraction](#data-extraction)
  - [Data Transformation](#data-transformation)
    - [Handling Date Fields](#handling-date-fields)
  - [Data Loading](#data-loading)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Running the ETL Process](#running-the-etl-process)
  - [Running Tests](#running-tests)
- [Testing](#testing)
  - [Unit Tests](#unit-tests)
  - [Integration Tests](#integration-tests)
- [Logging](#logging)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

---

## Overview

The **ETL Service** is a robust Python-based application designed to facilitate the **Extract, Transform, Load (ETL)** process from **SQL Server** to **PostgreSQL**. This service ensures data integrity, handles complex transformations, and maintains high maintainability through the implementation of design patterns like the **Factory Pattern**.

---

## Architecture

### Clean Architecture

The project adheres to the **Clean Architecture** principles, ensuring a separation of concerns and enhancing scalability and maintainability. The architecture is divided into four primary layers:

1. **Domain**: Contains the business logic and entities.
2. **Application**: Implements use cases and transformation logic.
3. **Infrastructure**: Manages external interactions like database connections.
4. **Presentation**: Handles the entry point of the application.

### Project Structure


---

## Features

### Data Extraction

- **SQL Server Integration**: Connects to SQL Server databases to extract data from specified tables.
- **Dynamic Table Handling**: Capable of handling multiple tables as per configuration.

### Data Transformation

- **Factory Pattern**: Utilizes the Factory Pattern to apply specific transformations based on table names, enhancing maintainability and scalability.
- **Date Handling**:
  - **Null Conversion**: Converts `0` values in date columns to `NULL`.
  - **Date Parsing**: Transforms integer dates in `yyyymmdd` format to Python `datetime` objects.
  - **Column Renaming**: Renames date columns to more descriptive names.

#### Handling Date Fields

The transformation process for date fields involves:

1. **Renaming Columns**:
   - `fecinipre` → `fecha_inicio_prevista`
   - `fecfinpre` → `fecha_fin_prevista`
   - `fecinirea` → `fecha_inicio_real`
   - `fecfinrea` → `fecha_fin_real`

2. **Value Conversion**:
   - **Zero to NULL**: Replaces `0` with `NULL`.
   - **Integer to Date**: Converts integers in `yyyymmdd` format to `datetime` objects.
   - **Invalid Formats**: Converts non-conforming values to `NULL`.

### Data Loading

- **PostgreSQL Integration**: Inserts transformed data into PostgreSQL tables.
- **Table Creation**: Automatically creates target tables (`FacObra`) if they do not exist.
- **Data Integrity**: Ensures that data types in PostgreSQL match the transformed data.

---

## Installation

### Prerequisites

- **Python 3.9+**: Ensure Python is installed on your system. You can download it [here](https://www.python.org/downloads/).
- **pip**: Python package installer.

### Clone the Repository

```bash
git clone https://github.com/tu-usuario/etl_service.git
cd etl_service


python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate


pip install -r requirements.txt


# SQL Server Configuration
SQL_SERVER=localhost
SQL_PORT=1433
SQL_DATABASE=TuBaseDeDatosSQL
SQL_USER=tu_usuario
SQL_PASSWORD=tu_contraseña
SQL_DRIVER=ODBC+Driver+17+for+SQL+Server

# PostgreSQL Configuration
PG_SERVER=localhost
PG_PORT=5432
PG_DATABASE=TuBaseDeDatosPG
PG_USER=tu_usuario_pg
PG_PASSWORD=tu_contraseña_pg
```
## Usage

### Running the ETL Process

```bash
Examples
Run ETL for the obr table:

bash
Copiar código
python etl_service/main.py obr
Run ETL for multiple tables:

bash
Copiar código
python etl_service/main.py obr otra_tabla
Run ETL for all tables:

Simply run without specifying table names:

bash
Copiar código
python etl_service/main.py

```
### Running Tests
The project includes comprehensive unit tests to ensure the reliability of each component.


```bash
Unit tests are located in the tests/ directory, organized by functionality.

Transformations:

test_date_transformation.py: Validates date transformations, including handling of 0 values and yyyymmdd formats.
ETL Process:

test_obr_etl.py: Ensures the ETL process for the obr table functions correctly.

test_load_facobra.py: Checks the loading of transformed data into PostgreSQL.

test_complete_obr_etl.py: Verifies the complete ETL workflow from extraction to loading.
```