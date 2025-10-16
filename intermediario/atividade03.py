#Atividade 6/10: Data Warehousing e Cloud Computing (Pilar 5)
#
#Agora vamos simular a fase de staging (armazenamento temporário) e o processo de Schema Evolution (evolução do esquema) no Cloud.
#
#Tarefa:
#
#
#
#Simulação de Data Lake (Cloud Storage): Simule o upload dos dados brutos do JSON para um Data Lake.
#
#Escreva uma função chamada simular_upload_s3(nome_arquivo_local) que recebe o nome do arquivo JSON e imprime a mensagem de que ele foi "movido para o bucket S3 na pasta raw/".
#
#Evolução do Schema: Em engenharia de dados, novos campos são frequentemente adicionados. A API de notícias pode adicionar o campo author (autor da notícia).
#
#Modifique a sua função task_extrair_dados_brutos para garantir que, mesmo que o campo author não exista em algumas notícias, ele seja adicionado ao DataFrame limpo (com o valor None ou Vazio) para manter o esquema consistente.
#
#Justificativa: Explique em texto, de forma concisa, por que é crucial garantir que um DataFrame tenha o mesmo schema (mesmas colunas) antes de carregá-lo em um Data Lake ou Data Warehouse.
#
#Para a entrega, envie:
#
#
#
#O código Python com a nova função simular_upload_s3.
#
#A modificação na função task_extrair_dados_brutos para incluir a coluna author.
#
#A justificativa concisa sobre a importância de manter o schema consistente.
#
#Este passo cobre o pilar de Cloud Computing e a fase inicial de Staging.

# MÓDULOS ESSENCIAIS
import requests
import pandas as pd
import sqlite3
import json
from datetime import datetime, timedelta
import time
import numpy as np
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
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

        coluna_base = list(df_bruto.columns)

        if 'author' not in coluna_base:
            coluna_base.append('author')

        df_bruto = df_bruto.reindex(columns=coluna_base)

        #df_bruto['author'] = df_bruto['author'].replace(r'^\s*$', np.nan, regex=True)

        #df_bruto['author'] = df_bruto['author'].fillna('Autor Desconhecido')

        #print(F'COLUNA AUTHOR {df_bruto.head()}')

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

def simular_upload_s3(nome_arquivo_local):
    """
    Simula o upload do arquivo JSON para um Data Lake (Bucket S3).
    """
    if os.path.exists(nome_arquivo_local):
        print("\n--- SIMULAÇÃO CLOUD ---")
        # Define o caminho padrão do Data Lake: 'raw' é a camada de dados brutos
        CAMINHO_S3 = f"s3://bucket-de-noticias-brutas/raw/{nome_arquivo_local}"
        
        print(f"Upload simulado: Arquivo '{nome_arquivo_local}' movido para o bucket S3 na pasta raw/")
        print(f"Caminho Final no Data Lake: {CAMINHO_S3}")
        print("-----------------------")
    else:
        print(f"\n[AVISO] Arquivo {nome_arquivo_local} não encontrado para upload.")

def nome_arquivo_bruto():
    NOME_ARQUIVO_BRUTO = f"noticias_brutas_{datetime.now().strftime('%y%m%d')}.json"
    return NOME_ARQUIVO_BRUTO


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

    simular_upload_s3(nome_arquivo_bruto())

    print("\n--- FIM DO PIPELINE. SUCESSO. ---")

if __name__ == '__main__':
    main_pipeline()

#Justificativa: Importância da Consistência de Schema
#É crucial manter o mesmo schema (conjunto de colunas) porque a maioria dos
#  Data Warehouses e ferramentas de análise (SQL) exige que todas as tabelas
#  e partições tenham a mesma estrutura. Se a coluna author estiver faltando
#  em algumas execuções do pipeline, o sistema falhará, pois ele espera um
#  número fixo de campos para o carregamento. O método .reindex() garante
#  que o esquema permaneça consistente, preenchendo as ausências com valores
#  None (null).