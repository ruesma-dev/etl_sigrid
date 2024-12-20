TABLE_CONFIG = {
    'obr': {
        'source_table': 'obr',
        'target_table': 'FactObra',
        'primary_key': 'ide',
        'rename_columns': {
            'res': 'nombre_obra'
        },
        'date_columns': [
            'fecinipre', 'fecfinpre', 'fecinirea', 'fecfinrea',
            'feccieest', 'fecadj', 'fecfincie', 'fecciepre',
            'fecapelic', 'feclic', 'fecofe'
        ],
        'foreign_keys': [
            {
                'column': 'cenide',
                'ref_table': 'DimCentroCoste',
                'ref_column': 'ide'
            }
        ],
        'data_cleaning': {
            'handle_invalid_foreign_keys': 'add_placeholder'
        },
        # Hacemos join con 'con' usando cenide = con.ide (según lo solicitado)
        'join_with_con': {
            'join_column': 'cenide'
        }
    },
    'cen': {
        'source_table': 'cen',
        'target_table': 'DimCentroCoste',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fecfinpre', 'fecfinrea', 'fecbloqueo', 'fecfincie'],
        'foreign_keys': [],
        'data_cleaning': {
            'add_placeholder_row': True
        }
    },
    'con': {
        'source_table': 'con',
        'target_table': 'DimConceptosETC',
        'primary_key': 'ide',
        'rename_columns': {
            'fec': 'fecha_alta',
            'fecbaj': 'fecha_baja',
        },
        'date_columns': ['fec','fecbaj'],
        'foreign_keys': [],
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
        'date_columns': ['fecbaj'],
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
        'date_columns': ['fecbaj'],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'conest': {
        'source_table': 'conest',
        'target_table': 'DimEstadoConcepto',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [],
        'foreign_keys': [],
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
        'target_table': 'DimAlbaranCompra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fecdoc', 'fecpag', 'fecent', 'feclim', 'fecrec',
            'fecfac', 'alqfec', 'divcamfec'
        ],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'ctr': {
        'source_table': 'ctr',
        'target_table': 'DimContratoCompra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fecdoc', 'fecpag', 'fecent', 'feclim', 'fecfac',
            'divcamfec', 'fecvig1', 'fecvig2', 'fecrevpre'
        ],
        'foreign_keys': [],
        'data_cleaning': {},
        # Join con con a través de ide
        'join_with_con': {
            'join_column': 'ide'
        }
    },
    'dcapro': {
        'source_table': 'dcapro',
        'target_table': 'DimAlbaranCompraProductos',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fec','garfec','fecimp'],
        'foreign_keys': [],
        'data_cleaning': {},
        # Join con con a través de docide
        'join_with_con': {
            'join_column': 'docide'
        }
    },
    'dcaproana': {
        'source_table': 'dcaproana',
        'target_table': 'DimAlbaranCompraAnalitica',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dcaprodes': {
        'source_table': 'dcaprodes',
        'target_table': 'DimAlbaranCompraDestinos',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dcapropar': {
        'source_table': 'dcapropar',
        'target_table': 'DimAlbaranCompraPartidas',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dcaproser': {
        'source_table': 'dcaproser',
        'target_table': 'DimAlbaranCompraProductosSeries',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fec'],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dcarec': {
        'source_table': 'dcarec',
        'target_table': 'DimAlbaranCompraRecargos',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'obrcer': {
        'source_table': 'obrcer',
        'target_table': 'DimCertificacionObra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fec'],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'obrcos': {
        'source_table': 'obrcos',
        'target_table': 'DimCentroCosteObra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['orifec'],
        'foreign_keys': [],
        'data_cleaning': {}
    },
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
        'rename_columns': {},
        'date_columns': ['fecini','fecfin'],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'cer': {
        'source_table': 'cer',
        'target_table': 'DimCertificacion',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fecdoc', 'feccob', 'fecent', 'feclim', 'fecfac',
            'divcamfec', 'alqfec'
        ],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'cerpro': {
        'source_table': 'cerpro',
        'target_table': 'DimCertificacionProductos',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fec', 'garfec', 'fecimp'],
        'foreign_keys': [],
        'data_cleaning': {},
        # Join con con a través de docide
        'join_with_con': {
            'join_column': 'docide'
        }
    },
    'cob': {
        'source_table': 'cob',
        'target_table': 'DimCobro',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fecven', 'fecrea', 'fecreaemi', 'divcamfec'],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dvf': {
        'source_table': 'dvf',
        'target_table': 'DimFacturaVenta',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fecdoc', 'feccob', 'fecent', 'feclim', 'fecfac',
            'divcamfec', 'alqfec'
        ],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dvfpro': {
        'source_table': 'dvfpro',
        'target_table': 'DimFacturaVentaProductos',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fec', 'garfec', 'fecimp', 'fec1', 'fec2',
            'perfini', 'perffin', 'alqfec'
        ],
        'foreign_keys': [],
        'data_cleaning': {},
        # Join con con a través de docide
        'join_with_con': {
            'join_column': 'docide'
        }
    },
    'pro': {
        'source_table': 'pro',
        'target_table': 'DimProducto',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fectipdes', 'fecult', 'fecser', 'fecrec', 'fecultact',
            'esigpromf1','esigpromf2','esigpliqf1','esigpliqf2',
            'esigpnovf1','esigpnovf2'
        ],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'cli': {
        'source_table': 'cli',
        'target_table': 'DimCliente',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'creevafec','evafec','fecnac','dopfec1','dopfec2','abofec1','abofec2'
        ],
        'foreign_keys': [],
        'data_cleaning': {},
        # Join con con a través de ide
        'join_with_con': {
            'join_column': 'ide'
        }
    },
    'prv': {
        'source_table': 'prv',
        'target_table': 'DimProveedor',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fecnac','cerfeccad','dopfec1','dopfec2'],
        'foreign_keys': [],
        'data_cleaning': {},
        # Join con con a través de ide
        'join_with_con': {
            'join_column': 'ide'
        }
    },
    'dcf': {
        'source_table': 'dcf',
        'target_table': 'DimFacturaCompra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fecdoc', 'fecpag', 'fecent', 'feclim', 'fecrec',
            'fecfac', 'alqfec', 'divcamfec'
        ],
        'foreign_keys': [],
        'data_cleaning': {},
        # join con con a traves de docide
    },
    'dcfpro': {
        'source_table': 'dcfpro',
        'target_table': 'DimFacturaCompraProductos',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fec','garfec','fecimp'],
        'foreign_keys': [],
        'data_cleaning': {},
        # join con con a traves de docide
        'join_with_con': {
            'join_column': 'docide'
        }
    }
}
