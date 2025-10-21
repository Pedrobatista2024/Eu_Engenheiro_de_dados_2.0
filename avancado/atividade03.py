#Tarefa:
#
#Consulta SQL com Janela: Escreva uma consulta SQL que utilize uma Função de Janela (Window Function), especificamente a ROW_NUMBER().
#
#Requisito: A consulta deve particionar (PARTITION BY) pelo nome da fonte e ordenar (ORDER BY) pela data de publicação (do mais novo para o mais antigo).
#
#Resultado Final: A consulta deve selecionar o título, nome da fonte, data de publicação e o resultado da função de janela (o número da linha, rn), e, em seguida, filtrar para reter apenas o primeiro registro (rn = 1) para cada fonte.
#
#Para a entrega, envie:
#
#O código Python que se conecta ao dw_noticias.db e executa a consulta SQL.
#
#A string completa da consulta SQL com a função de janela.
#
#O print (ou descrição) do resultado da consulta.
#
#Este é um dos conceitos mais avançados de SQL para análise em DWs.

import sqlite3
import pandas as pd

# Nome do arquivo do Data Warehouse (DW)
NOME_DW = 'dw_noticias.db'

# =========================================================
# 1. CONSULTA SQL AVANÇADA COM FUNÇÃO DE JANELA
# =========================================================

# Esta consulta encontra o artigo mais recente (ROW_NUMBER = 1) para cada fonte.
SQL_CONSULTA_JANELA = """
WITH ArtigosRankeados AS (
    -- 1. Cria uma Tabela Temporária (CTE) para ranquear os artigos
    SELECT
        f.titulo,
        d.nome_fonte,
        f.data_publicacao,
        -- Aplica a Função de Janela:
        ROW_NUMBER() OVER (
            -- Particiona os dados por fonte (cria um ranking separado para cada fonte)
            PARTITION BY d.nome_fonte 
            -- Ordena por data de publicação decrescente (o mais novo fica em 1º)
            ORDER BY f.data_publicacao DESC
        ) AS rank_noticia
    FROM ft_artigos AS f
    JOIN dim_fonte AS d 
        ON f.id_fonte_fk = d.id_fonte
)
-- 2. Seleciona o resultado final, filtrando apenas pelo ranking 1
SELECT
    nome_fonte,
    titulo,
    data_publicacao
FROM ArtigosRankeados
WHERE 
    rank_noticia = 1 
ORDER BY 
    data_publicacao DESC;
"""

print("--- Execução da Consulta com Função de Janela (ROW_NUMBER) ---")

conn = None
try:
    conn = sqlite3.connect(NOME_DW)
    
    # Executa a consulta e carrega o resultado em um DataFrame
    df_mais_recente = pd.read_sql_query(SQL_CONSULTA_JANELA, conn)
    
    print("\n[RESULTADO] Notícia Mais Recente por Fonte:")
    print("-" * 50)
    print(df_mais_recente)

except sqlite3.Error as e:
    print(f"Erro ao executar a consulta SQL: {e}")
finally:
    if conn:
        conn.close()