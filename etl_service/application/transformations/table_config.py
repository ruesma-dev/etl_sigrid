# etl_service/application/transformations/table_config.py

# el nombre de la columna de fecha debe ser el original antes de cambiarlo

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
    'con': {
        'source_table': 'con',  # Nombre de la tabla en la base de datos de origen
        'target_table': 'DimConceptosETC',
        'primary_key': 'ide',
        'rename_columns': {
            'fec': 'fecha_alta',
            'fecbaj': 'fecha_baja',
        },
        'date_columns': [
            'fec',
            'fecbaj'
        ],
        'foreign_keys': [],  # Si es necesario, añadir aquí claves foráneas
        'data_cleaning': {},
        'combine_columns': [
            {
                'new_column_name': 'primary_ide',
                'columns_to_combine': ['tip', 'est'],
                'separator': '_'
            }
        ]
    },
    'auxobrtip': {
        'source_table': 'auxobrtip',
        'target_table': 'DimTipoObra',
        'primary_key': 'ide',
        'rename_columns': {
            'fecbaj': 'fecha_baja'
        },
        'date_columns': [
            'fecbaj'
        ],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'auxobrcla': {
        'source_table': 'auxobrcla',
        'target_table': 'DimSubtipoObra',
        'primary_key': 'ide',
        'rename_columns': {
            'fecbaj': 'fecha_baja'
        },
        'date_columns': [
            'fecbaj'
        ],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'conest': {
        'source_table': 'conest',
        'target_table': 'DimEstadoConcepto',
        'primary_key': 'ide',
        'rename_columns': {
            # No hay columnas de fecha u otras que requieran renombrado,
            # pero puedes añadir renombrados si lo deseas.
            # Ejemplo: 'res': 'descripcion_estado'
        },
        'date_columns': [],  # No hay columnas de fecha
        'foreign_keys': [
            # Referencias: psecan.estide -> conest.ide
            # Si en el futuro deseas agregar esta clave foránea,
            # puedes hacerlo aquí.
        ],
        'data_cleaning': {},
        'combine_columns': [
            {
                'new_column_name': 'primary_ide',
                'columns_to_combine': ['tip', 'est'],
                'separator': '_'
            }
        ]
    },
    'dca': {
        'source_table': 'dca',
        'target_table': 'DimAlbaranCompra',  # Puedes cambiarlo según tu convención
        'primary_key': 'ide',
        'rename_columns': {
            # Si deseas renombrar columnas, agrégalos aquí.
        },
        'date_columns': [
            'fecdoc',  # Fecha del documento
            'fecpag',  # Fecha de pago
            'fecent',  # Fecha entrega prevista
            'feclim',  # Fecha límite
            'fecrec',  # Fecha real de recepción
            'fecfac',  # Fecha facturación
            'alqfec',  # Fecha periodo alquiler
            'divcamfec'  # Fecha de cambio divisas
        ],
        'foreign_keys': [
            # Añade claves foráneas si corresponde
        ],
        'data_cleaning': {}
        # No hay secciones combine_columns para esta tabla, ni se requieren
    }
}