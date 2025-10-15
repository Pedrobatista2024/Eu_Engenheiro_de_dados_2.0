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
#import sys
#import os
#
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#
#from basico.atividade01 import task_extrair_dados_brutos
#from basico.atividade02 import task_transformar_modelar
#from basico.atividade03 import task_carregar_dw
#
#def main_pipeline():
#    print("Início do Pipeline.")
#
#    df_bruto = task_extrair_dados_brutos()
#
#    if df_bruto.empty:
#        print("ERRO: Extração falhou ou retornou DataFrame vazio. Abortando.")
#        return 
#
#    dim_fonte, ft_artigos = task_transformar_modelar(df_bruto)
#
#    task_carregar_dw(dim_fonte, ft_artigos) 
#
#    print("Pipeline concluído com sucesso.")
#
#if __name__ == '__main__':
#    main_pipeline()

# MÓDULOS ESSENCIAIS
import requests
import pandas as pd
import sqlite3
import json
from datetime import datetime, timedelta
import time
import os

# --- INÍCIO: Definições e Variáveis Globais ---
API_KEY = 'ca1d64caa68fcda80c6ffd171bf2ceb9' # Seu API Key
QUERY = 'Inteligência Artificial'
ENDPOINT = 'http://api.mediastack.com/v1/news'
NOME_DW = 'dw_noticias.db'
NOME_ARQUIVO_BRUTO = f"noticias_brutas_{datetime.now().strftime('%y%m%d')}.json"
# --- FIM: Definições e Variáveis Globais ---


# --- TAREFA 1 (ATIVIDADE 1) ---
def task_extrair_dados_brutos():
    print("TASK 1: Início da extração de dados brutos da API...")
    
    params = {
        'access_key': API_KEY,
        'keywords': QUERY,
        'countries': 'br',
        'limit': 100,
        'sort': 'published_desc'
    }
    
    try:
        response = requests.get(ENDPOINT, params=params, timeout=15)
        response.raise_for_status() 
        dados = response.json()
        artigos = dados.get('data', []) 
        
        df_bruto = pd.DataFrame(artigos)

        # 3. Qualidade de Dados (Limpeza)
        if not df_bruto.empty:
            total_antes = len(df_bruto)
            # Remove nulos e strings vazias/apenas espaços
            df_limpo = df_bruto.dropna(subset=['title', 'description'])
            df_limpo = df_limpo[
                ~( (df_limpo['title'].str.strip() == '') | (df_limpo['description'].str.strip() == '') )
            ]
            artigos_removidos = total_antes - len(df_limpo)
            
            # 4. Armazenamento Bruto (Data Lake Simulado)
            df_limpo.to_json(NOME_ARQUIVO_BRUTO, orient='records', indent=4)
            
            print(f"TASK 1: Sucesso. {len(df_limpo)} artigos limpos. {artigos_removidos} removidos por qualidade.")
            return df_limpo
        else:
            print("TASK 1: Extração retornou 0 artigos. Fim.")
            return pd.DataFrame() # Retorna DataFrame vazio em caso de falha/vazio

    except requests.exceptions.RequestException as e:
        print(f"TASK 1: Erro crítico na requisição: {e}")
        return pd.DataFrame()

# --- TAREFA 2 (ATIVIDADE 2) ---
def task_transformar_modelar(df_bruto: pd.DataFrame):
    print("TASK 2: Início da transformação e modelagem (normalização)...")
    
    # 1. Criação da Dimensão (dim_fonte)
    df_artigos = df_bruto.copy()
    df_artigos['nome_fonte'] = df_artigos['source']
    
    df_fontes_unicas = df_artigos['nome_fonte'].drop_duplicates().to_frame()
    df_fontes_unicas.reset_index(inplace=True)
    df_fontes_unicas.rename(columns={'index': 'id_fonte'}, inplace=True)
    df_fontes_unicas['id_fonte'] = df_fontes_unicas['id_fonte'] + 1 
    dim_fonte = df_fontes_unicas[['id_fonte', 'nome_fonte']]

    # 2. Criação da Tabela Fato (ft_artigos) e FK
    mapeamento_fonte_id = dim_fonte.set_index('nome_fonte')['id_fonte'].to_dict()
    df_artigos['id_fonte_fk'] = df_artigos['nome_fonte'].map(mapeamento_fonte_id)

    df_artigos.reset_index(inplace=True)
    df_artigos.rename(columns={'index': 'id_artigo'}, inplace=True)

    ft_artigos = df_artigos[[
        'id_artigo', 'id_fonte_fk', 'published_at', 'title', 'url'
    ]]
    ft_artigos.rename(columns={
        'published_at': 'data_publicacao',
        'title': 'titulo'
    }, inplace=True)
    
    print("TASK 2: Sucesso. Dimensões e Fatos criados.")
    return dim_fonte, ft_artigos

# --- TAREFA 3 (ATIVIDADE 3) ---
def task_carregar_dw(dim_fonte: pd.DataFrame, ft_artigos: pd.DataFrame):
    print(f"TASK 3: Início do carregamento no Data Warehouse ({NOME_DW})...")
    conn = None
    try:
        conn = sqlite3.connect(NOME_DW)
        
        # Carregamento da FATO (substituir, pois os dados são diários)
        ft_artigos.to_sql(
            'ft_artigos', conn, if_exists='replace', index=False
        )
        print("TASK 3: Tabela ft_artigos carregada (REPLACE).")

        # Carregamento da DIMENSÃO (Incremental/APPEND - simulando UPSERT)
        # Nota: Um UPSERT real seria necessário, mas para APPEND/simulação de inserção:
        dim_fonte.to_sql(
            'dim_fonte', conn, if_exists='append', index=False
        )
        print("TASK 3: Tabela dim_fonte carregada (APPEND).")
        
        conn.commit()
        print("TASK 3: Sucesso. Carregamento concluído.")
    
    except sqlite3.Error as e:
        print(f"TASK 3: Erro no carregamento do SQLite: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# --- LÓGICA DE ORQUESTRAÇÃO (main_pipeline) ---
def main_pipeline():
    print("--- INÍCIO DO PIPELINE GERAL (ORQUESTRAÇÃO) ---")

    # 1. EXTRATO (Tarefa 1)
    df_bruto = task_extrair_dados_brutos()

    # Checagem de Dependência
    if df_bruto.empty:
        print("\n!!! ERRO: Extração de dados falhou ou retornou vazio. Pipeline abortado. !!!")
        return 

    # 2. TRANSFORMAR E MODELAR (Tarefa 2)
    dim_fonte, ft_artigos = task_transformar_modelar(df_bruto)

    # 3. CARREGAR (Tarefa 3)
    task_carregar_dw(dim_fonte, ft_artigos)

    print("\n--- FIM DO PIPELINE. SUCESSO. ---")

if __name__ == '__main__':
    main_pipeline()
