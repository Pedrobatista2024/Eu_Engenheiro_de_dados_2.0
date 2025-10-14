#Tarefa:
#
#Simulação de Tarefas (Funções Python): Transforme as lógicas das Atividades 1, 2 e 3 em funções Python distintas.
#
#task_extrair_dados_brutos(): Deve executar a Atividade 1 e retornar o DataFrame limpo.
#
#task_transformar_modelar(df_bruto): Deve executar a Atividade 2 (normalização) e retornar os DataFrames dim_fonte e ft_artigos.
#
#task_carregar_dw(dim_fonte, ft_artigos): Deve executar o carregamento da Atividade 3 no SQLite (use apenas if_exists='append' para dim_fonte e if_exists='replace' para ft_artigos, simplificando a simulação).
#
#Lógica de Orquestração: Crie uma função principal chamada main_pipeline() que define a ordem de execução e a passagem de dados entre as tarefas.
#
#Dependência: A task_transformar_modelar SÓ PODE rodar se a task_extrair_dados_brutos terminar com sucesso (ou seja, se o DataFrame retornado não estiver vazio).
#
#Sucesso vs. Falha: Use prints para indicar o início e o fim de cada tarefa. Se a extração falhar (DataFrame vazio), a função main_pipeline() deve parar e imprimir: "ERRO: Extração de dados falhou. Pipeline abortado."
#
#Para a entrega, envie o código Python que implementa a lógica de orquestração com as dependências.

# --- CÓDIGO PARA RESOLVER O ERRO DE IMPORTAÇÃO ---
import sys
import os

# Adiciona o diretório raiz do projeto ao caminho de busca do Python
# Ele sobe um nível (..) a partir da pasta atual (intermediario)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# -------------------------------------------------


# Agora a sua importação deve funcionar
from basico.atividade01 import task_extrair_dados_brutos

dados = task_extrair_dados_brutos()
