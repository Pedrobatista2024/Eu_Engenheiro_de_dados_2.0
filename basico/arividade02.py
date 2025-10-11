#Fase 2: Transformação e Modelagem (Pilares 1, 3, 5)
#Agora que os dados brutos estão no nosso Data Lake (o arquivo JSON), o próximo passo é transformá-los, modelá-los e carregá-los em nosso Data Warehouse (o banco de dados relacional).
#
#Atividade 2/10: Modelagem e Normalização Simples (Pilares 1, 2)
#O JSON que você coletou possui dados aninhados/duplicados, o que não é ideal para análise. Nossa meta é normalizar (separar) os dados em um modelo mais eficiente para consulta.
#
#Tarefa:
#
#Carregar Dados: Carregue o arquivo JSON gerado na Atividade 1 (noticias_brutas_YYMMDD.json) em um DataFrame do Pandas.
#
#Modelagem e Normalização: Modele os dados em duas tabelas (DataFrames) separadas para simular um schema relacional simples (normalização):
#
#Tabela 1: dim_fonte (Dimensão de Fonte): Deve conter uma lista única de todas as fontes de notícias. Colunas sugeridas: id_fonte (ID único gerado por você), nome_fonte.
#
#Tabela 2: ft_artigos (Fato de Artigos): Deve conter os detalhes dos artigos. Colunas sugeridas: id_artigo (ID da notícia), id_fonte_fk (Chave estrangeira para a dim_fonte), data_publicacao, titulo, url.
#
#Criação de IDs e FKs:
#
#Crie uma coluna de id_fonte para a dim_fonte (pode ser um índice sequencial).
#
#Mapeie esse ID de volta para a tabela ft_artigos (criando a Chave Estrangeira id_fonte_fk).
#
#Para a entrega, envie o código que realiza o carregamento, a normalização e a criação dos IDs/FKs. Mostre o .head() dos dois DataFrames finais: dim_fonte e ft_artigos.

import pandas as pd
from datetime import datetime

data_hoje = datetime.now().strftime('%y%m%d')
nome_arquivo = f'noticias brutas_{data_hoje}.json'

try:
    df_artigos_brutos = pd.read_json(nome_arquivo, orient)
