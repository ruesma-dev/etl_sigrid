# etl_service/application/transformations/row_modifications_mapping.py

ROW_INSERTION_MAPPING = {
    "DimCentroCoste": [
        {"ide": 0, "cenide": 0},
        # Añade más filas según sea necesario
    ],
    # Añade más tablas y sus filas a insertar si es necesario
}

ROW_DELETION_MAPPING = {
    "DimCentroCoste": [
        {"ide": 496414},  # Ejemplo: eliminar filas donde 'ide' == 100
        # Añade más criterios de eliminación según sea necesario
    ],
    # Añade más tablas y sus criterios de eliminación si es necesario
}
