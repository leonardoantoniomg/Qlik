import pandas as pd

class FatoCopas:

    def __init__(self,default=None):
        if default is None:
            self.arg = []
        else:
            self.arg = default


    def set_arquivo(self, arquivo):
        self.__arquivo = arquivo

    def get_arquivo(self):
        return self.__arquivo

    def extract(self):
        self.set_arquivo(r'D:\Git\Qlik\Sense\WorldCup\DataSets\WorldCups.csv')
        arquivo = pd.read_csv(self.get_arquivo(), sep = ',')

        self.transform(arquivo)


    def transform(self, p_arquivo):
        df = p_arquivo
        df = pd.DataFrame(df)

        df["Year"] = (df["Year"]).fillna(0).astype(int)

        df["Country"] = df["Country"].str.replace('""', '')
        df["Country"] = df["Country"].str.replace('"', '')
        df["Country"] = df["Country"].str.replace('rn', '')
        df["Country"] = df["Country"].str.replace('>', '')

        df["Winner"] = df["Winner"].str.replace('""', '')
        df["Winner"] = df["Winner"].str.replace('"', '')
        df["Winner"] = df["Winner"].str.replace('rn', '')
        df["Winner"] = df["Winner"].str.replace('>', '')

        df["Runners-Up"] = df["Runners-Up"].str.replace('""', '')
        df["Runners-Up"] = df["Runners-Up"].str.replace('"', '')
        df["Runners-Up"] = df["Runners-Up"].str.replace('rn', '')
        df["Runners-Up"] = df["Runners-Up"].str.replace('>', '')

        df["Third"] = df["Third"].str.replace('""', '')
        df["Third"] = df["Third"].str.replace('"', '')
        df["Third"] = df["Third"].str.replace('rn', '')
        df["Third"] = df["Third"].str.replace('>', '')

        df["Fourth"] = df["Fourth"].str.replace('""', '')
        df["Fourth"] = df["Fourth"].str.replace('"', '')
        df["Fourth"] = df["Fourth"].str.replace('rn', '')
        df["Fourth"] = df["Fourth"].str.replace('>', '')

        DimTempos = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimTempos.csv', sep = ';')
        df_DimTempos = pd.DataFrame(DimTempos)
        df_DimTempos["Year"] = (df_DimTempos["Year"]).fillna(0).astype(int)
        df_a = pd.merge(df, df_DimTempos, how = "inner", on = "Year")
        df_a = df_a.rename(columns={'IdTempo': 'IdTempo'})

        DimPaises = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimPaises.csv', sep = ';')
        df_DimPaises = pd.DataFrame(DimPaises)
        df_b = pd.merge(df_a, df_DimPaises, how = "inner", on = "Country")
        df_b = df_b.rename(columns={'IdPais': 'IdPais'})

        Winner = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimSelecoes.csv', sep = ';')
        df_Winner = pd.DataFrame(Winner)
        df_c = pd.merge(df_b, df_Winner, how = "inner", left_on = "Winner", right_on = "Team Name")
        df_c = df_c.rename(columns={'IdSelecao': 'IdWinner'})

        RunnersUp = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimSelecoes.csv', sep = ';')
        df_RunnersUp = pd.DataFrame(RunnersUp)
        df_d = pd.merge(df_c, df_RunnersUp, how = "inner", left_on = "Runners-Up", right_on = "Team Name")
        df_d = df_d.rename(columns={'IdSelecao': 'IdRunnersUp'})

        Third = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimSelecoes.csv', sep = ';')
        df_Third = pd.DataFrame(Third)
        df_e = pd.merge(df_d, df_Third, how = "inner", left_on = "Third", right_on = "Team Name")
        df_e = df_e.rename(columns={'IdSelecao': 'IdThird'})

        Fourth = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimSelecoes.csv', sep = ';')
        df_Fourth = pd.DataFrame(Fourth)
        df_f = pd.merge(df_e, df_Fourth, how = "inner", left_on = "Fourth", right_on = "Team Name")
        df_f = df_f.rename(columns={'IdSelecao': 'IdFourth'})

        cols_to_drop = ['Year', 'Country', 'Winner', 'Runners-Up', 'Third', 'Fourth', 'Team Name_x', 'Team Name_y']
        df_final = df_f.drop(cols_to_drop, axis=1)

        self.load(df_final)

    def load(self, p_df_s):
        df_s = p_df_s

        df_s.to_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\FatoCopas.csv'
                  ,sep = ";"
                  ,encoding="utf-8"
                  ,index=True
                  ,index_label="IdFatoCopas")


if __name__ == "__main__":
    Copas = FatoCopas()
    Copas.extract()