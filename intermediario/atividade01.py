#Feedback da Atividade 4/10
#Agregação Avançada (SQL): A sua consulta SQL é perfeitamente estruturada:
#
#CTE (contagemArtigos): O uso do CTE para modularizar a lógica de contagem e agregação é a prática mais limpa e moderna em SQL.
#
#GROUP BY e HAVING: Você usou o GROUP BY corretamente na tabela de fatos (ft_artigos) e aplicou o filtro HAVING total_artigos > 5 na agregação, garantindo que a consulta atenda exatamente ao requisito de negócios.
#
#JOIN: O JOIN final para trazer o nome da fonte da dim_fonte é a prova de que a modelagem relacional na Atividade 2 foi bem-sucedida.
#
#Otimização de Consulta (Índices):
#
#Você criou o comando SQL correto (CREATE INDEX idx_dim_fonte_nome ON dim_fonte (nome_fonte);).
#
#O uso do try...except para lidar com a exceção de "índice já existe" mostra uma preocupação com a robustez e a idempotência do código (capacidade de ser executado várias vezes sem causar efeitos colaterais), o que é essencial em pipelines de produção.
#
#Justificativa (Otimização): Sua justificativa é concisa e totalmente correta, definindo o Índice como um "sumário de livro" que permite ao SGBD evitar o full table scan, acelerando drasticamente o tempo de resposta das consultas analíticas.

def query():
    import sqlite3
    import pandas as pd

    nome_dw = r'C:\Users\estudante\Desktop\Pedro\projetos\Eu_engenheiro_de_dados\basico\dw_noticias.db'
    conn = sqlite3.connect(nome_dw)

    sql_consulta = """
    with contagemArtigos as (
        select
            id_fonte_fk,
            count(id_artigo) as total_artigos
        from ft_artigos
        group by id_fonte_fk
        having total_artigos > 5
    )
    select
        d.nome_fonte,
        c.total_artigos
    from dim_fonte as d
    join contagemArtigos as c
        on d.id_fonte = c.id_fonte_fk
    order by
        c.total_artigos desc;
    """

    print("--- Execução da Consulta de Agregação (CTEs e HAVING) ---")

    df_agregacao = pd.read_sql_query(sql_consulta, conn)
    print(df_agregacao)

    sql_create_index = "create index idx_dim_fonte_nome on dim_fonte (nome_fonte);"

    print("\n--- Execução da Criação de Índice ---")

    try:
        conn.execute(sql_create_index)
        conn.commit()
        print(f"Índice 'idx_dim_fonte_nome' criado com sucesso na tabela dim_fonte.")
    except sqlite3.OperationalError as e:
        if "already exists" in str(e):
            print("Índice já existe. Nenhuma ação necessária.")
        else:
            raise

    conn.close()

    #A criação do índice na coluna nome_fonte é uma prática de otimização
    #em Engenharia de Dados porque melhora drasticamente o desempenho
    #de consultas. O índice atua como um sumário de livro, permitindo
    #que o SGBD (Sistema Gerenciador de Banco de Dados) encontre linhas
    #específicas (p. ex., ao filtrar por WHERE nome_fonte = 'Exame')
    #sem ter que ler a tabela inteira. Isso é crucial para Data Warehouses
    #grandes, onde o tempo de consulta deve ser mínimo.