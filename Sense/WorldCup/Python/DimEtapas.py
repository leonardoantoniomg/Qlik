import pandas as pd

class DimEtapas:

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
        arquivo = pd.read_csv(self.get_arquivo())
        self.transform(arquivo)


    def transform(self, p_arquivo):
        df = p_arquivo
        df = pd.DataFrame(df)
        df = df.loc[:, ["Stage"]]
        df = df.sort_values("Stage")
        df = df["Stage"].unique()

        s = pd.Series(df)

        df = pd.DataFrame(s)
        df = df.rename(columns={0: 'Stage'})


        self.load(df)

    def load(self, p_df_s):
        df_s = p_df_s

        df_s.to_csv(r'D:\Git\Qlik\Sense\WorldCup\ETL\DimEtapas.csv'
                  ,sep = ";"
                  ,encoding="utf-8"
                  ,index=True
                  ,index_label="IdEtapa")



if __name__ == "__main__":
    Etapas = DimEtapas()
    Etapas.extract()