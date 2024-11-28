# etl_service/application/transformations/column_mappings.py

COLUMN_MAPPINGS = {
    'obr': {
        'rename': {
            'res': 'nombre_obra'
            # Puedes añadir más renombres específicos para 'obr' aquí si es necesario
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
        'rename': {
            # Si hay columnas que necesitan renombrarse en 'cen', añádelas aquí
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
    # Añade más tablas y sus mapeos aquí si es necesario
}

# Opcional: Mapeo de nombres de tablas de origen a destino
TABLE_NAME_MAPPINGS = {
    'obr': 'FacObra',
    'cen': 'centro_costes'
    # Añade más tablas aquí si es necesario
}
