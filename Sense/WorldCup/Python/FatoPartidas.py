import pandas as pd

class FatoPartidas:

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
        self.set_arquivo(r'D:\Git\Qlik\Sense\WorldCup\DataSets\WorldCupMatches.csv')
        arquivo = pd.read_csv(self.get_arquivo(), sep = ',')

        self.transform(arquivo)


    def transform(self, p_arquivo):
        df = p_arquivo
        df = pd.DataFrame(df)
        df["Year"] = (df["Year"]).fillna(0).astype(int)

        df["Home Team Name"] = df["Home Team Name"].str.replace('""', '')
        df["Home Team Name"] = df["Home Team Name"].str.replace('"', '')
        df["Home Team Name"] = df["Home Team Name"].str.replace('rn', '')
        df["Home Team Name"] = df["Home Team Name"].str.replace('>', '')

        df["Away Team Name"] = df["Away Team Name"].str.replace('""', '')
        df["Away Team Name"] = df["Away Team Name"].str.replace('"', '')
        df["Away Team Name"] = df["Away Team Name"].str.replace('rn', '')
        df["Away Team Name"] = df["Away Team Name"].str.replace('>', '')


        DimTempos = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimTempos.csv', sep = ';')
        df_DimTempos = pd.DataFrame(DimTempos)
        df_DimTempos["Year"] = (df_DimTempos["Year"]).fillna(0).astype(int)
        df_a = pd.merge(df, df_DimTempos, how = "inner", on = "Year")

        DimArbitros = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimArbitros.csv', sep = ';')
        df_DimArbitros = pd.DataFrame(DimArbitros)
        df_b = pd.merge(df_a, df_DimArbitros, how = "inner", on = "Referee")

        DimAssistenteUm = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimAssistentesUm.csv', sep = ';')
        df_DimAssistenteUm = pd.DataFrame(DimAssistenteUm)
        df_c = pd.merge(df_b, df_DimAssistenteUm, how = "inner", on = "Assistant 1")

        DimAssistenteDois = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimAssistentesDois.csv', sep = ';')
        df_DimAssistenteDois = pd.DataFrame(DimAssistenteDois)
        df_d = pd.merge(df_c, df_DimAssistenteDois, how = "inner", on = "Assistant 2")

        DimCidades = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimCidades.csv', sep = ';')
        df_DimCidades = pd.DataFrame(DimCidades)
        df_e = pd.merge(df_d, df_DimCidades, how = "inner", on = "City")
        df_e = df_e.rename(columns={'IdCidade': 'IdCidade'})

        DimEstadios = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimEstadios.csv', sep = ';')
        df_DimEstadios = pd.DataFrame(DimEstadios)
        df_f = pd.merge(df_e, df_DimEstadios, how = "inner", on = "Stadium")

        DimEtapas = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimEtapas.csv', sep = ';')
        df_DimEtapas = pd.DataFrame(DimEtapas)
        df_g = pd.merge(df_f, df_DimEtapas, how = "inner", on = "Stage")

        DimSelecoesA = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimSelecoes.csv', sep = ';')
        df_DimSelecoesA = pd.DataFrame(DimSelecoesA)
        df_h = pd.merge(df_g, df_DimSelecoesA, how = "inner", left_on = "Home Team Name", right_on = "Team Name")
        df_h = df_h.rename(columns={'IdSelecao': 'IdSelecaoHome'})

        DimSelecoesSiglasA = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimSelecoesSiglas.csv', sep = ';')
        df_DimSelecoesSiglasA = pd.DataFrame(DimSelecoesSiglasA)
        df_i = pd.merge(df_h, df_DimSelecoesSiglasA, how = "inner", left_on = "Home Team Initials", right_on = "Team Initials")
        df_i = df_i.rename(columns={'IdSelecaoSigla': 'IdSelecaoSiglaHome'})

        DimSelecoesB = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimSelecoes.csv', sep = ';')
        df_DimSelecoesB = pd.DataFrame(DimSelecoesB)
        df_j = pd.merge(df_i, df_DimSelecoesB, how = "inner", left_on = "Away Team Name", right_on = "Team Name")
        df_j = df_j.rename(columns={'IdSelecao': 'IdSelecaoAway'})

        DimSelecoesSiglasB = pd.read_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimSelecoesSiglas.csv', sep = ';')
        df_DimSelecoesSiglasB = pd.DataFrame(DimSelecoesSiglasB)
        df_k = pd.merge(df_j, df_DimSelecoesSiglasB, how = "inner", left_on = "Away Team Initials", right_on = "Team Initials")
        df_k = df_k.rename(columns={'IdSelecaoSigla': 'IdSelecaoSiglaAway'})

        cols_to_drop_a = ['Year', 'Datetime', 'Stage', 'Stadium', 'City', 'Home Team Name', 'Away Team Name']
        cols_to_drop_b = ['Referee', 'Assistant 1', 'Assistant 2', 'IdCidade_x']
        cols_to_drop_c = ['Home Team Initials', 'Away Team Initials', 'Team Name_x', 'Team Name_y', 'Team Initials_x', 'Team Initials_y']

        df_final = df_k.drop(cols_to_drop_a, axis=1)
        df_final = df_final.drop(cols_to_drop_b, axis=1)
        df_final = df_final.drop(cols_to_drop_c, axis=1)

        print(df_final)
        self.load(df_final)

    def load(self, p_df_s):
        df_s = p_df_s

        df_s.to_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\FatoPartidas.csv'
                  ,sep = ";"
                  ,encoding="utf-8"
                  ,index=True
                  ,index_label="IdFatoPartidas")



if __name__ == "__main__":
    Partidas = FatoPartidas()
    Partidas.extract()