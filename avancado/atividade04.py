#Tarefa:
#
#Criação de Dicionário de Dados: Crie um breve Dicionário de Dados para as duas tabelas no Data Warehouse (dim_fonte e ft_artigos). Este dicionário deve listar o nome da tabela, o nome da coluna, seu tipo de dado (baseado no SQLite ou Pandas) e uma breve descrição.
#
#Resumo de Observabilidade: Crie um resumo final que reúna as três métricas de observabilidade que você implementou, simulando o relatório diário de status do pipeline:
#
#Artigos removidos por NULL/Vazio (Atividade 1)
#
#Artigos removidos por Duplicação (Atividade 8)
#
#Porcentagem de URLs Inválidas (Atividade 7)
#
#Para a entrega, envie o Dicionário de Dados e o Resumo de Observabilidade em formato de texto.


#1. Dicionário de Dados do Data Warehouse (dw_noticias.db)
#Este dicionário descreve o esquema final que foi carregado no banco de dados SQLite. Os tipos de dados são baseados na conversão do Pandas para o SQLite.

#=================================================
#DICIONÁRIO DE DADOS (DATA WAREHOUSE)
#=================================================
#
#TABELA: dim_fonte (Dimensão de Fonte)
#-------------------------------------
#| Coluna         | Tipo de Dado (SQLite) | Descrição                                  |
#|----------------|-----------------------|--------------------------------------------|
#| id_fonte       | INTEGER               | Chave Primária (PK). Identificador único da fonte. |
#| nome_fonte     | TEXT                  | Nome do veículo de notícias (ex: G1, Exame).|
#
#TABELA: ft_artigos (Tabela de Fato de Artigos)
#-----------------------------------------------
#| Coluna            | Tipo de Dado (SQLite) | Descrição                                  |
#|-------------------|-----------------------|--------------------------------------------|
#| id_artigo         | INTEGER               | Chave Primária (PK). Identificador único do artigo no Data Warehouse. |
#| id_fonte_fk       | INTEGER               | Chave Estrangeira (FK). Relaciona com a dim_fonte. |
#| data_publicacao   | TEXT                  | Data e hora da publicação da notícia.       |
#| titulo            | TEXT                  | Título completo do artigo.                 |
#| url               | TEXT                  | URL original do artigo.                    |
#| url_valida        | INTEGER               | Flag de Qualidade (0=Inválido, 1=Válido). |

#2. Resumo de Observabilidade (Relatório Diário de Status)
#Este resumo simula o relatório de monitoramento que o Engenheiro de Dados envia diariamente para o time de análise e operações. Para preencher os valores, você precisará dos resultados das variáveis no seu main_pipeline().

#=================================================
#RESUMO DE OBSERVABILIDADE DO PIPELINE ETL
#(Data: [DATA_DE_EXECUÇÃO])
#=================================================
#
#STATUS GERAL: SUCESSO (Carregamento no DW concluído)
#
#MÉTRICAS DE QUALIDADE E INTEGRIDADE:
#------------------------------------
#1. Artigos Removidos por NULL/Vazio (Title/Description):
#   -> Total Descartado: [ARTIGOS_REMOVIDOS_NULL_VAZIO] (Métrica da Task 1)
#   -> Ação: Remoção OK.
#
#2. Artigos Removidos por Duplicação (Título + Fonte):
#   -> Total Descartado: [DUPLICATAS_REMOVIDAS] (Métrica da Task 2)
#   -> Ação: Desduplicação OK. Integridade do Fato mantida.
#
#3. Porcentagem de URLs Inválidas (Regra: http/https):
#   -> Taxa de Falha: [PORCENTAGEM_INVALIDA]%
#   -> [STATUS: ALERTA URGENTE se > 10% / OK se <= 10%]
#   -> Ação: Dados carregados, mas a fonte [NOME_DA_FONTE_PROBLEMATICA] pode precisar de verificação.
#
#VOLUMETRIA:
#------------------------------------
#Artigos Carregados no DW (ft_artigos): [TOTAL_ARTIGOS_FATO_FINAL]