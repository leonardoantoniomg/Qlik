import pandas as pd
import numpy as np

class DimSelecoes:

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
        arquivo = p_arquivo

        df_a = pd.DataFrame(arquivo)
        df_a = df_a.loc[:, ["Home Team Name"]]
        df_a = df_a.rename(columns={'Home Team Name': 'Team Name'})

        df_b = pd.DataFrame(arquivo)
        df_b = df_b.loc[:, ["Away Team Name"]]
        df_b = df_b.rename(columns={'Away Team Name': 'Team Name'})

        df = df_a.append(df_b)
        df = df["Team Name"].unique()

        df_final = pd.DataFrame(df)
        df_final = df_final.rename(columns={0: 'Team Name'})
        df_final = df_final.sort_values("Team Name")
        df_final["Team Name"] = df_final["Team Name"].str.replace('""', '')
        df_final["Team Name"] = df_final["Team Name"].str.replace('"', '')
        df_final["Team Name"] = df_final["Team Name"].str.replace('rn', '')
        df_final["Team Name"] = df_final["Team Name"].str.replace('>', '')


        self.load(df_final)

    def load(self, p_df_s):
        df_s = p_df_s

        df_s.to_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimSelecoes.csv'
                  ,sep = ";"
                  ,encoding="utf-8"
                  ,index=True
                  ,index_label="IdSelecao")



if __name__ == "__main__":
    Selecoes = DimSelecoes()
    Selecoes.extract()