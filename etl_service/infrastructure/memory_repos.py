# etl_service/infrastructure/memory_repos.py

class CtrMemoryRepo:
    def __init__(self, ctr_df):
        self.ctr_df = ctr_df

    def get_by_ide(self, entide):
        # Filtra el DataFrame por ide == entide
        # Ajustar según la columna primaria que tenga ctr
        row = self.ctr_df[self.ctr_df['ide'] == entide]
        if not row.empty:
            return row.iloc[0].to_dict()
        return None


class ConMemoryRepo:
    def __init__(self, con_df):
        self.con_df = con_df

    def get_with_conditions(self, entide, emp, tip):
        # Filtra por entide, emp, tip
        # Ajustar según las columnas que tenga 'con'
        row = self.con_df[
            (self.con_df['ide'] == entide) &
            (self.con_df['emp'] == emp) &
            (self.con_df['tip'] == tip)
        ]
        if not row.empty:
            return row.iloc[0].to_dict()
        return None


class ObrMemoryRepo:
    def __init__(self, obr_df):
        self.obr_df = obr_df

    def get_by_conide(self, con_ide):
        # Este método asume que obr_df tiene una columna que relacione
        # con con_ide. Ajustar según tu modelo.
        # Por ejemplo, si 'obr' tiene un campo 'conide' que se corresponde con con_ide:
        row = self.obr_df[self.obr_df['conide'] == con_ide]
        if not row.empty:
            return row.iloc[0].to_dict()
        return None
