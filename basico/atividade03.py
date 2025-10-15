#Atividade 3/10: Lógica ETL e Carregamento no Data Warehouse (Pilares 3, 5)
#A próxima etapa é completar o fluxo ETL (Extrair, Transformar, Carregar). Os dados transformados precisam ser persistidos em nosso Data Warehouse simulado (SQLite).
#
#Tarefa:
#
#Carregamento no Data Warehouse (DW): Crie um novo banco de dados SQLite chamado dw_noticias.db.
#
#Carregamento Inicial: Carregue os dois DataFrames (dim_fonte e ft_artigos) como duas tabelas separadas no banco de dados. Utilize o if_exists='replace' apenas nesta primeira execução.
#
#Simulação de Carregamento Incremental (Update/Insert): Em um ambiente de produção, não podemos simplesmente substituir a tabela (replace) todos os dias. Precisamos apenas inserir os novos registros e ignorar duplicatas (já que as dimensões de fonte não mudam com frequência).
#
#Simule uma segunda tentativa de carregamento (utilize o código de carregamento novamente) da dim_fonte com o parâmetro if_exists='append'.
#
#Justificativa: Me diga o que acontece com a tabela dim_fonte no SQLite se você usar append com o mesmo DataFrame (contendo as mesmas fontes). Por que um engenheiro de dados precisaria de uma lógica mais avançada (como checagem de chave única ou upsert) para evitar a duplicação?
#
#Para a entrega, envie o código que realiza o carregamento. O nome do arquivo SQLite deve ser dw_noticias.db. Responda à pergunta de justificativa em formato de texto.
#def task_carregar_dw(): 
#    import pandas as pd
#    from datetime import datetime
#    import sqlite3
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
#    nome_dw = 'dw_noticias.db'
#
#    conn = sqlite3.connect(nome_dw)
#
#    print("--- 1ª Execução: Carregamento Inicial (REPLACE) ---")
#
#    ft_artigos.to_sql(
#        'ft_artigos',
#        conn,
#        if_exists='replace',
#        index=False
#    )
#    print('ft_artigos carregada com sucesso (replace).')
#
#    dim_fonte.to_sql(
#        'dim_fonte',
#        conn,
#        if_exists='replace', 
#        index=False
#    )
#
#    df_verificacao = pd.read_sql_query('select count(*) from dim_fonte', conn)
#    total_linhas = df_verificacao.iloc[0, 0]
#
#    print("dim_fonte carregada com sucesso (REPLACE).")
#
#    #print(f"\nVerificação: Total de linhas na dim_fonte antes do APPEND: {total_linhas}")
#
#    #dim_fonte.to_sql(
#    #    'dim_fonte',
#    #    conn,
#    #    if_exists='append',
#    #    index=False
#    #)
#    #
#    #print('dim_fonte carregada com sucesso (append).')
#    #
#    #df_verificacao = pd.read_sql_query('select count(*) from dim_fonte', conn)
#    #total_linhas = df_verificacao.iloc[0, 0]
#    #
#    #print(f"\nVerificação: Total de linhas na dim_fonte após o APPEND: {total_linhas}")
#
#    conn.close()
#
#    #Justificativa: Carregamento de Dimensões
#    #O que acontece é a duplicação exata de todas as linhas da tabela dim_fonte, pois o parâmetro if_exists='append' apenas insere novos registros, ignorando a unicidade dos dados.
#    #
#    #Um Engenheiro de Dados precisa de uma lógica de UPSERT (Update or Insert) ou checagem de chave única porque:
#    #
#    #Integridade do Dado: Duplicar as dimensões (como as fontes) causa erros analíticos graves. O dado de dimensão deve ser sempre único (Chave Primária).
#    #
#    #Eficiência: O processo deve ser inteligente o suficiente para apenas inserir novas fontes que surgiram desde a última execução, em vez de carregar toda a tabela novamente, otimizando o tempo de processamento e o uso do Data Warehouse 

def task_carregar_dw(df1, df2): 
    import pandas as pd
    from datetime import datetime
    import sqlite3
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

    df1 = dim_fonte
    df2 = ft_artigos

    nome_dw = 'dw_noticias.db'

    conn = sqlite3.connect(nome_dw)

    print("--- 1ª Execução: Carregamento Inicial (REPLACE) ---")

    ft_artigos.to_sql(
        'df1',
        conn,
        if_exists='replace',
        index=False
    )
    print('ft_artigos carregada com sucesso (replace).')

    dim_fonte.to_sql(
        'df2',
        conn,
        if_exists='replace', 
        index=False
    )

    df_verificacao = pd.read_sql_query('select count(*) from dim_fonte', conn)
    total_linhas = df_verificacao.iloc[0, 0]

    print("dim_fonte carregada com sucesso (REPLACE).")

    #print(f"\nVerificação: Total de linhas na dim_fonte antes do APPEND: {total_linhas}")

    #dim_fonte.to_sql(
    #    'dim_fonte',
    #    conn,
    #    if_exists='append',
    #    index=False
    #)
    #
    #print('dim_fonte carregada com sucesso (append).')
    #
    #df_verificacao = pd.read_sql_query('select count(*) from dim_fonte', conn)
    #total_linhas = df_verificacao.iloc[0, 0]
    #
    #print(f"\nVerificação: Total de linhas na dim_fonte após o APPEND: {total_linhas}")

    conn.close()

    #Justificativa: Carregamento de Dimensões
    #O que acontece é a duplicação exata de todas as linhas da tabela dim_fonte, pois o parâmetro if_exists='append' apenas insere novos registros, ignorando a unicidade dos dados.
    #
    #Um Engenheiro de Dados precisa de uma lógica de UPSERT (Update or Insert) ou checagem de chave única porque:
    #
    #Integridade do Dado: Duplicar as dimensões (como as fontes) causa erros analíticos graves. O dado de dimensão deve ser sempre único (Chave Primária).
    #
    #Eficiência: O processo deve ser inteligente o suficiente para apenas inserir novas fontes que surgiram desde a última execução, em vez de carregar toda a tabela novamente, otimizando o tempo de processamento e o uso do Data Warehouse 