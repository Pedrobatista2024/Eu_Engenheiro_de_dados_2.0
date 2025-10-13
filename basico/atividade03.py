import pandas as pd
from datetime import datetime
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

data_hoje = datetime.now().strftime('%y%m%d')
nome_arquivo = f'noticias_brutas_{data_hoje}.json'

try:
    df_artigos_brutos = pd.read_json(nome_arquivo, orient='records')
except FileNotFoundError:
    print(f'erro: arquivo {nome_arquivo} não encontrado. execute a atividade01 primeiro.')
    exit()

df_artigos_brutos['nome_fonte'] = df_artigos_brutos['source']

df_fontes_unicas = df_artigos_brutos['nome_fonte'].drop_duplicates().to_frame()

df_fontes_unicas.reset_index(inplace=True)
df_fontes_unicas.rename(columns={'index':'id_fonte', 'nome_fonte': 'nome_fonte'}, inplace=True)

df_fontes_unicas['id_fonte'] = df_fontes_unicas['id_fonte']+1

dim_fonte = df_fontes_unicas[['id_fonte', 'nome_fonte']]

mapeamento_fonte_id = dim_fonte.set_index('nome_fonte')['id_fonte'].to_dict()

df_artigos_brutos['id_fonte_fk'] = df_artigos_brutos['nome_fonte'].map(mapeamento_fonte_id)

df_artigos_brutos.reset_index(inplace=True)
df_artigos_brutos.rename(columns={'index': 'id_artigo'}, inplace=True)

ft_artigos = df_artigos_brutos[['id_artigo', 'id_fonte_fk', 'published_at', 'title', 'url']]

ft_artigos.rename(columns={
    'published_at': 'data_publicacao',
    'title': 'titulo'
}, inplace=True)