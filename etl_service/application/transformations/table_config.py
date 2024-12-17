# etl_service/application/transformations/table_config.py

TABLE_CONFIG = {
    'obr': {
        'source_table': 'obr',  # Nombre de la tabla en la base de datos de origen
        'target_table': 'FactObra',
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
        ],
        'foreign_keys': [
            {
                'column': 'cenide',
                'ref_table': 'DimCentroCoste',
                'ref_column': 'ide'
            }
            # Añade más claves foráneas según sea necesario
        ],
        'data_cleaning': {
            'handle_invalid_foreign_keys': 'add_placeholder'  # Opciones: 'delete', 'set_null', 'add_placeholder'
        }
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
        ],
        'foreign_keys': [],
        'data_cleaning': {
            'add_placeholder_row': True  # Indicamos que se debe agregar una fila placeholder
        }
    },
    # Añade más tablas y sus configuraciones aquí si es necesario
}
