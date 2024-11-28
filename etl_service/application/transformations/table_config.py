# etl_service/application/transformations/table_config.py

TABLE_CONFIG = {
    'obr': {
        'source_table': 'obr',  # Nombre de la tabla en la base de datos de origen
        'target_table': 'FacObra',
        'primary_key': 'ide',
        'rename_columns': {
            'res': 'nombre_obra'
            # Añade más renombrados específicos para 'obr' si es necesario
        },
        'date_columns': [
            'fecinipre',
            'fecfinpre',
            'fecinirea',
            'fecfinrea',
            'feccieest',
            'fecadj',
            'fecfincie',
            'fecciepre',
            'fecapelic',
            'feclic',
            'fecofe'
        ]
    },
    'cen': {
        'source_table': 'cen',  # Nombre de la tabla en la base de datos de origen
        'target_table': 'DimCentroCoste',
        'primary_key': 'ide',
        'rename_columns': {
            # Añade renombrados específicos para 'cen' si es necesario
            # Ejemplo:
            # 'columna_original': 'columna_renombrada'
        },
        'date_columns': [
            'fecfinpre',
            'fecfinrea',
            'fecbloqueo',
            'fecfincie'
        ]
    },
    # Añade más tablas y sus configuraciones aquí si es necesario
}
