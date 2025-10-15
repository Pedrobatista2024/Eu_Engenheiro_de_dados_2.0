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
#def task_transformar_modelar():
#    import pandas as pd
#    from datetime import datetime
#    pd.set_option('display.max_columns', None)
#    pd.set_option('display.max_colwidth', None)
#
#    data_hoje = datetime.now().strftime('%y%m%d')
#    nome_arquivo = f'noticias_brutas_{data_hoje}.json'
#
#    try:
#        df_artigos_brutos = pd.read_json(nome_arquivo, orient='records')
#    except FileNotFoundError:
#        print(f'erro: arquivo {nome_arquivo} não encontrado. execute a atividade01 primeiro.')
#        exit()
#
#    df_artigos_brutos['nome_fonte'] = df_artigos_brutos['source']
#
#    df_fontes_unicas = df_artigos_brutos['nome_fonte'].drop_duplicates().to_frame()
#
#    df_fontes_unicas.reset_index(inplace=True)
#    df_fontes_unicas.rename(columns={'index':'id_fonte', 'nome_fonte': 'nome_fonte'}, inplace=True)
#
#    df_fontes_unicas['id_fonte'] = df_fontes_unicas['id_fonte']+1
#
#    dim_fonte = df_fontes_unicas[['id_fonte', 'nome_fonte']]
#
#    mapeamento_fonte_id = dim_fonte.set_index('nome_fonte')['id_fonte'].to_dict()
#
#    df_artigos_brutos['id_fonte_fk'] = df_artigos_brutos['nome_fonte'].map(mapeamento_fonte_id)
#
#    df_artigos_brutos.reset_index(inplace=True)
#    df_artigos_brutos.rename(columns={'index': 'id_artigo'}, inplace=True)
#
#    ft_artigos = df_artigos_brutos[['id_artigo', 'id_fonte_fk', 'published_at', 'title', 'url']]
#
#    ft_artigos.rename(columns={
#        'published_at': 'data_publicacao',
#        'title': 'titulo'
#    }, inplace=True)
#
#
#    print("\n--- Resultado Final: dim_fonte.head() ---")
#    print(dim_fonte.head())
#    print(dim_fonte.columns.to_list())
#
#    print('*'*40)
#
#    print("\n--- Resultado Final: ft_artigos.head() ---")
#    print(ft_artigos.head())
#    print(ft_artigos.columns.to_list())
#
#    return (dim_fonte, ft_artigos)

def task_transformar_modelar(df):
    import pandas as pd
    from datetime import datetime
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)

    #data_hoje = datetime.now().strftime('%y%m%d')
    #nome_arquivo = f'noticias_brutas_{data_hoje}.json'
#
    #try:
    #    df_artigos_brutos = pd.read_json(nome_arquivo, orient='records')
    #except FileNotFoundError:
    #    print(f'erro: arquivo {nome_arquivo} não encontrado. execute a atividade01 primeiro.')
    #    exit()

    df_artigos_brutos = pd.DataFrame(df)

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


    print("\n--- Resultado Final: dim_fonte.head() ---")
    print(dim_fonte.head())
    print(dim_fonte.columns.to_list())

    print('*'*40)

    print("\n--- Resultado Final: ft_artigos.head() ---")
    print(ft_artigos.head())
    print(ft_artigos.columns.to_list())

    return (dim_fonte, ft_artigos)