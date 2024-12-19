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
    },
    'ctr': {
        'source_table': 'ctr',
        'target_table': 'DimContratoCompra',  # Puedes cambiar el nombre del target_table según tu convención
        'primary_key': 'ide',
        'rename_columns': {
            # Añade renombrados si es necesario
        },
        'date_columns': [
            'fecdoc',  # Fecha del documento
            'fecpag',  # Fecha de pago
            'fecent',  # Fecha entrega prevista
            'feclim',  # Fecha limite
            'fecfac',  # Fecha facturación
            'divcamfec',  # Fecha de cambio divisas
            'fecvig1',  # Fecha vigencia contrato desde
            'fecvig2',  # Fecha vigencia contrato hasta
            'fecrevpre'  # Revisión de precios. Fecha inicial
        ],
        'foreign_keys': [],
        'data_cleaning': {}
        # No combine_columns en este caso
    },
    'dcapro': {
            'source_table': 'dcapro',
            'target_table': 'DimAlbaranCompraProductos',
            'primary_key': 'ide',
            'rename_columns': {},
            'date_columns': [
                'fec',     # Fecha prevista
                'garfec',  # Fecha garantía
                'fecimp'   # Fecha imputación informativa
            ],
            'foreign_keys': [],
            'data_cleaning': {}
    },
    'dcaproana': {
        'source_table': 'dcaproana',
        'target_table': 'DimAlbaranCompraAnalitica',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [],  # No se detectan fechas
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dcaprodes': {
        'source_table': 'dcaprodes',
        'target_table': 'DimAlbaranCompraDestinos',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [],  # No hay columnas fecha
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dcapropar': {
        'source_table': 'dcapropar',
        'target_table': 'DimAlbaranCompraPartidas',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [],  # Sin fechas
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dcaproser': {
        'source_table': 'dcaproser',
        'target_table': 'DimAlbaranCompraProductosSeries',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fec'  # Fecha caducidad
        ],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dcarec': {
        'source_table': 'dcarec',
        'target_table': 'DimAlbaranCompraRecargos',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [],  # Sin fechas detectadas
        'foreign_keys': [],
        'data_cleaning': {}
    },
    # obrcer (Obras: Certificaciones por obra)
    # Columna fec es tipo fecha.
    'obrcer': {
        'source_table': 'obrcer',
        'target_table': 'DimCertificacionObra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fec'
        ],
        'foreign_keys': [],
        'data_cleaning': {}
    },

    # obrcos (Obras: Centros de coste)
    # Columna orifec es tipo fecha (según la doc orifec = Fecha de origen)
    'obrcos': {
        'source_table': 'obrcos',
        'target_table': 'DimCentroCosteObra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'orifec'
        ],
        'foreign_keys': [],
        'data_cleaning': {}
    },

    # obrctr (Obras: Contratos de obra)
    # Hay múltiples columnas tipo fecha:
    # fecprelic, fecrealic, fecpreadj, fecreaadj, fecprefir, fecreafir, fecprefct, fecreafct, fecpreact, fecreaact,
    # fecpreini, fecreaini, fecprefin, fecreafin, fecprosol, fecprorec, fecproliq, fecproapr, fecdefsol, fecdefrec,
    # fecdefliq, fecdefapr, fecinipla, fecinigar, fecfingar, fecdevret, fecaprtec, fecapreco, fecultsit, fecrevpre
    'obrctr': {
        'source_table': 'obrctr',
        'target_table': 'DimContratoObra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fecprelic', 'fecrealic', 'fecpreadj', 'fecreaadj',
            'fecprefir', 'fecreafir', 'fecprefct', 'fecreafct',
            'fecpreact', 'fecreaact', 'fecpreini', 'fecreaini',
            'fecprefin', 'fecreafin', 'fecprosol', 'fecprorec',
            'fecproliq', 'fecproapr', 'fecdefsol', 'fecdefrec',
            'fecdefliq', 'fecdefapr', 'fecinipla', 'fecinigar',
            'fecfingar', 'fecdevret', 'fecaprtec', 'fecapreco',
            'fecultsit', 'fecrevpre'
        ],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'obrparpar': {
        'source_table': 'obrparpar',
        'target_table': 'DimPartidasObra',
        'primary_key': 'ide',
        'rename_columns': {

        },
        'date_columns': [
            'fecini',  # Fecha inicio
            'fecfin'   # Fecha final
        ],
        'foreign_keys': [
            # Añade claves foráneas si corresponde
            # Ejemplo:
            # {
            #     'column': 'obride',
            #     'ref_table': 'FactObra',
            #     'ref_column': 'ide'
            # }
        ],
        'data_cleaning': {}
    },
    'cer': {
        'source_table': 'cer',
        'target_table': 'DimCertificacion',  # Puedes cambiar el nombre según tu convención
        'primary_key': 'ide',
        'rename_columns': {
            # Añade renombrados si es necesario
        },
        'date_columns': [
            'fecdoc',   # Fecha del documento
            'feccob',   # Fecha de cobro
            'fecent',   # Fecha entrega prevista
            'feclim',   # Fecha límite
            'fecfac',   # Fecha facturación
            'divcamfec',# Fecha de cambio divisas
            'alqfec'    # Fecha periodo alquiler
        ],
        'foreign_keys': [
            # Añade claves foráneas si corresponde
        ],
        'data_cleaning': {}
    },

    'cerpro': {
        'source_table': 'cerpro',
        'target_table': 'DimCertificacionProductos', # Cambia el nombre según tu convención
        'primary_key': 'ide',
        'rename_columns': {
            # Añade renombrados si es necesario
        },
        'date_columns': [
            'fec',    # Fecha prevista
            'garfec', # Fecha garantía
            'fecimp'  # Fecha imputación informativa
        ],
        'foreign_keys': [
            # Añade claves foráneas si corresponde
        ],
        'data_cleaning': {}
    },
    'cob': {
        'source_table': 'cob',
        'target_table': 'DimCobro',
        'primary_key': 'ide',
        'rename_columns': {
            # Añade renombrados si es necesario
        },
        'date_columns': [
            'fecven',   # Fecha de vencimiento
            'fecrea',   # Fecha de cobro real
            'fecreaemi',# Fecha de emisión real
            'divcamfec' # Fecha de cambio divisas
        ],
        'foreign_keys': [
            # Añade claves foráneas si corresponde
        ],
        'data_cleaning': {}
    },

    'dvf': {
        'source_table': 'dvf',
        'target_table': 'DimFacturaVenta',
        'primary_key': 'ide',
        'rename_columns': {
            # Añade renombrados si es necesario
        },
        'date_columns': [
            'fecdoc',   # Fecha del documento
            'feccob',   # Fecha de cobro
            'fecent',   # Fecha entrega prevista
            'feclim',   # Fecha límite
            'fecfac',   # Fecha facturación
            'divcamfec',# Fecha de cambio divisas
            'alqfec'    # Fecha periodo alquiler
        ],
        'foreign_keys': [
            # Añade claves foráneas si corresponde
        ],
        'data_cleaning': {}
    },

    'dvfpro': {
        'source_table': 'dvfpro',
        'target_table': 'DimFacturaVentaProductos',
        'primary_key': 'ide',
        'rename_columns': {
            # Añade renombrados si es necesario
        },
        'date_columns': [
            'fec',     # Fecha prevista
            'garfec',  # Fecha garantía
            'fecimp',  # Fecha imputación informativa
            'fec1',    # Fecha1 alquiler (si se considera fecha)
            'fec2',    # Fecha2 alquiler (si se considera fecha)
            'perfini', # Periodificación: Fecha inicial
            'perffin', # Periodificación: Fecha final
            'alqfec'   # Fecha periodo alquiler (si corresponde)
        ],
        'foreign_keys': [
            # Añade claves foráneas si corresponde
        ],
        'data_cleaning': {}
    },
    'pro': {
            'source_table': 'pro',
            'target_table': 'DimProducto',  # Ajusta el nombre según tu convención
            'primary_key': 'ide',
            'rename_columns': {
                # Añade renombrados específicos para 'pro' si es necesario
                # Ejemplo:
                # 'nombre_original': 'nombre_renombrado'
            },
            'date_columns': [
                'fectipdes',    # Fecha descatalogación
                'fecult',       # Fecha Última Compra
                'fecser',       # Fecha Servir
                'fecrec',       # Fecha Recibir
                'fecultact',    # Fecha última actualización
                'esigpromf1',   # i-Sigrid promocion desde
                'esigpromf2',   # i-Sigrid promocion hasta
                'esigpliqf1',   # i-Sigrid liquidación desde
                'esigpliqf2',   # i-Sigrid liquidación hasta
                'esigpnovf1',   # i-Sigrid novedad desde
                'esigpnovf2',   # i-Sigrid novedad hasta
            ],
            'foreign_keys': [
            ],
            'data_cleaning': {}
        },
    'cli': {
            'source_table': 'cli',
            'target_table': 'DimCliente',  # Ajusta el nombre según tu convención
            'primary_key': 'ide',
            'rename_columns': {
                # Añade renombrados específicos para 'cli' si es necesario
                # Ejemplo:
                # 'raz': 'razon_social',
                # 'cif': 'cif_nif'
            },
            'date_columns': [
                'creevafec',  # Fecha evaluación
                'evafec',  # Evaluación Fecha
                'fecnac',  # Fecha de nacimiento
                'dopfec1',  # Fecha inicio para documentación
                'dopfec2',  # Fecha final para documentación
                'abofec1',  # Abonado, Fecha inicio abono
                'abofec2'  # Abonado, Fecha fin abono
            ],
            'foreign_keys': [
            ],
            'data_cleaning': {}
        },
        'prv': {
            'source_table': 'prv',
            'target_table': 'DimProveedor',  # Ajusta el nombre si es necesario
            'primary_key': 'ide',
            'rename_columns': {
                # Si quieres renombrar columnas, añádelas aquí
                # 'raz': 'razon_social'
            },
            'date_columns': [
                'fecnac',  # Fecha de nacimiento
                'cerfeccad',  # Certificado: Fecha caducidad
                'dopfec1',  # Fecha inicio para documentación
                'dopfec2'   # Fecha final para documentación
            ],
            'foreign_keys': [
            ],
            'data_cleaning': {}
        },
    }