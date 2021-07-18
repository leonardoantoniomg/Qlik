#!/usr/bin/env python
# coding: utf-8

# ### Bibliotecas

# In[1]:


import pandas as pd
import numpy as np


# ### Carrega Arquivos

# In[2]:


arquivo_contribuicoes = 'D:/Python/Fundos/Arquivos/FAP/ContribuicoesFAP_Geral.csv'
arquivo_aportes = 'D:/Python/Fundos/Arquivos/FAP/Valores_Acumulados_FAP_Geral.xlsx'
arquivo_receitas = 'D:/Python/Fundos/Arquivos/FAP/ValoresFundoFAP_Geral.csv'
arquivo_saldo = 'D:/Python/Fundos/Arquivos/FAP/SaldoFAP.csv'

contribuicoes = pd.read_csv(arquivo_contribuicoes, encoding='Latin-1', sep=';')
aportes = pd.read_excel(arquivo_aportes, sheet_name='Plan1', engine='openpyxl')
receitas = pd.read_csv(arquivo_receitas, encoding='Latin-1', sep=';')


# ### Trata Arquivo de Contribuições

# #### Limpa os campos e converte para numérico

# In[3]:


contribuicoes[' contribuição '] = contribuicoes[' contribuição '].str.replace('.','')
contribuicoes[' contribuição '] = contribuicoes[' contribuição '].str.replace(',','.')
contribuicoes[' contribuição '] = contribuicoes[' contribuição '].str.replace(' ','')
contribuicoes[' contribuição '] = contribuicoes[' contribuição '].replace('-','0')
contribuicoes['pa'] = contribuicoes['pa'].replace('-','0')
contribuicoes[' contribuição '] = contribuicoes[' contribuição '].astype(float)


# #### Cria campos Ano e Mês

# In[4]:


contribuicoes['Ano'] = pd.DatetimeIndex(contribuicoes['data']).year
contribuicoes['Mes'] = pd.DatetimeIndex(contribuicoes['data']).month


# #### Apaga colunas que não serão utilizadas

# In[5]:


contribuicoes = contribuicoes.drop(columns=['data'])


# #### Faz a soma acumulada

# In[6]:


contribuicoes['Contribuicao'] = contribuicoes.groupby(['cooperativa'])[' contribuição '].cumsum()


# #### Apaga colunas que não serão utilizadas

# In[7]:


contribuicoes = contribuicoes.drop(columns=[' contribuição '])


# #### Renomeia colunas

# In[8]:


contribuicoes = contribuicoes.rename(columns={"cooperativa": "Cooperativa", "pa": "PA"}, errors="raise")


# ### Trata Arquivo de Aportes e Reembolsos

# #### Ordena pela data e cria colunas de índice

# In[9]:


aportes = aportes.sort_values(by=['Data']).reset_index()


# In[10]:


aportes.Data = pd.to_datetime(aportes.Data, format='%Y%m%d')


# In[11]:


aportes['CoopPA'] = aportes['NumCooperativa'].map(str) + '|' + aportes['PA'].map(str)
aportes['MesAno'] = pd.DatetimeIndex(aportes['Data']).year.map(str) + '|' + pd.DatetimeIndex(aportes['Data']).month.map(str)


# In[12]:


ix = pd.date_range(aportes.Data.min(), aportes.Data.max() + pd.DateOffset(30), freq="M")


# In[13]:


idx = pd.MultiIndex.from_product([ix, 
                                  aportes.CoopPA.unique()], names=['Data', 'CoopPA'])


# #### Apaga colunas que não serão utilizadas

# In[14]:


aportes = aportes.drop(columns=['index','Data','NumCooperativa','PA','Unnamed: 5','Unnamed: 6','Unnamed: 7','Unnamed: 8','Unnamed: 9'])


# #### Agrupa por mês/pa

# In[15]:


aportes = aportes.groupby(['MesAno','CoopPA'], as_index=False)['APORTE','ValorReembolso'].sum()


# #### Transforma o Multi Index em Data Frame

# In[16]:


df = idx.to_frame(index=False)


# In[17]:


df['MesAno'] = pd.DatetimeIndex(df['Data']).year.map(str) + '|' + pd.DatetimeIndex(df['Data']).month.map(str)
df = df.drop(columns=['Data'])


# #### Cria Data Frame com todos os meses e pa's

# In[18]:


aportes_append = pd.merge(df, aportes, how='left')


# In[19]:


aportes_append = aportes_append.fillna(0)


# #### Faz a soma acumulada

# In[20]:


aportes_append['Aporte'] = aportes_append.groupby(by=['CoopPA'])['APORTE'].cumsum()
aportes_append['Reembolso'] = aportes_append.groupby(by=['CoopPA'])['ValorReembolso'].cumsum()


# #### Apaga colunas que não serão utilizadas

# In[21]:


aportes_append = aportes_append.drop(columns=['APORTE','ValorReembolso'])


# #### Cria campos Ano, Mês, Cooperatia e PA

# In[22]:


aportes_append['Ano'] = aportes_append['MesAno'].str[0:4]
aportes_append['Mes'] = aportes_append['MesAno'].str[5:7]
aportes_append['Cooperativa'] = aportes_append['CoopPA'].str[0:4]
aportes_append['PA'] = aportes_append['CoopPA'].str[5:7]


# #### Apaga colunas que não serão utilizadas

# In[23]:


aportes_append = aportes_append.drop(columns=['MesAno','CoopPA'])


# ### Trata Arquivo Receitas

# #### Limpeza dos dados

# In[24]:


receitas['receitas financeiras e demais'] = receitas['receitas financeiras e demais'].str.replace('.','')
receitas['receitas financeiras e demais'] = receitas['receitas financeiras e demais'].str.replace(',','.')
receitas['receitas financeiras e demais'] = receitas['receitas financeiras e demais'].str.replace(' ','')
receitas['receitas financeiras e demais'] = receitas['receitas financeiras e demais'].replace('-','0')
receitas['receitas financeiras e demais'] = receitas['receitas financeiras e demais'].astype(float)


# #### Cria campos Ano, Mês, Cooperatia e PA

# In[25]:


receitas['Ano'] = pd.DatetimeIndex(receitas['data']).year
receitas['Mes'] = pd.DatetimeIndex(receitas['data']).month
receitas['Cooperativa'] = 2009
receitas['PA'] = 0


# #### Faz a soma acumulada

# In[26]:


receitas['Receitas'] = receitas.groupby(['Cooperativa'])['receitas financeiras e demais'].cumsum()


# #### Apaga colunas que não serão utilizadas

# In[27]:


receitas = receitas.drop(columns=['data','receitas financeiras e demais'])


# ### Concatena os Dataframes

# In[28]:


frames = [contribuicoes, aportes_append, receitas]


# In[29]:


saldo = pd.concat(frames)


# In[30]:


# Preenche vazios com '0'
saldo = saldo.fillna(0)


# In[31]:


# Arredonda para duas casas decimais
saldo = saldo.round(decimals=2)


# In[32]:


# Adiciona a coluna 'Receitas'
#saldo['Receitas'] = 0.00


# In[33]:


saldo


# In[34]:


# Reordena as colunas
saldo = saldo[['Ano','Mes','Cooperativa','PA','Contribuicao','Aporte','Reembolso','Receitas']]


# ### Salva o Arquivo

# In[35]:


saldo.to_csv(arquivo_saldo,index=False)


# In[ ]:




