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
# --- FIM: Definições e Variáveis Globais ---

# --- FUNÇÕES AUXILIARES ---
def nome_arquivo_bruto():
    """Gera o nome do arquivo JSON com a data atual."""
    return f"noticias_brutas_{datetime.now().strftime('%y%m%d')}.json"

def simular_upload_s3(nome_arquivo_local):
    """
    Simula o upload do arquivo JSON para um Data Lake (Bucket S3).
    """
    if os.path.exists(nome_arquivo_local):
        print("\n--- SIMULAÇÃO CLOUD ---")
        CAMINHO_S3 = f"s3://bucket-de-noticias-brutas/raw/{nome_arquivo_local}"
        
        print(f"Upload simulado: Arquivo '{nome_arquivo_local}' movido para o bucket S3 na pasta raw/")
        print(f"Caminho Final no Data Lake: {CAMINHO_S3}")
        print("-----------------------")
    else:
        print(f"\n[AVISO] Arquivo {nome_arquivo_local} não encontrado para upload.")

# =========================================================
# TAREFA 1 (ATIVIDADE 1) - EXTRAÇÃO
# =========================================================
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

        # 1. EVOLUÇÃO DO SCHEMA: Garante que 'author' exista
        coluna_base = list(df_bruto.columns)
        if 'author' not in coluna_base:
            coluna_base.append('author')
        df_bruto = df_bruto.reindex(columns=coluna_base)

        # 2. QUALIDADE DE DADOS: Trata Nulos/Vazios em 'author'
        df_bruto['author'] = df_bruto['author'].replace(r'^\s*$', np.nan, regex=True)
        df_bruto['author'] = df_bruto['author'].fillna('Autor Desconhecido')


        # 3. Qualidade de Dados (Limpeza de Título/Descrição)
        if not df_bruto.empty:
            total_antes = len(df_bruto)
            
            df_limpo = df_bruto.dropna(subset=['title', 'description'])
            df_limpo = df_limpo[
                ~( (df_limpo['title'].str.strip() == '') | (df_limpo['description'].str.strip() == '') )
            ]
            artigos_removidos = total_antes - len(df_limpo)
            
            # 4. Armazenamento Bruto (Data Lake Simulado)
            nome_arquivo_salvar = nome_arquivo_bruto() 
            df_limpo.to_json(nome_arquivo_salvar, orient='records', indent=4)
            
            print(f"TASK 1: Sucesso. {len(df_limpo)} artigos limpos. {artigos_removidos} removidos por qualidade.")
            return df_limpo
        else:
            print("TASK 1: Extração retornou 0 artigos. Fim.")
            return pd.DataFrame() 

    except requests.exceptions.RequestException as e:
        print(f"TASK 1: Erro crítico na requisição: {e}")
        return pd.DataFrame()

# =========================================================
# TAREFA 2 (ATIVIDADE 2) - TRANSFORMAÇÃO E MODELAGEM
# =========================================================

def task_transformar_modelar(df_bruto: pd.DataFrame):
    print("TASK 2: Início da transformação e modelagem (normalização)...")
    
    df_artigos = df_bruto.copy()
    df_artigos['nome_fonte'] = df_artigos['source']
    
    # 1. Criação da Dimensão (dim_fonte)
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
    ]].copy() # Usando .copy() para evitar SettingWithCopyWarning
    
    ft_artigos.rename(columns={
        'published_at': 'data_publicacao',
        'title': 'titulo'
    }, inplace=True)
    
    print("TASK 2: Sucesso. Dimensões e Fatos criados.")
    return dim_fonte, ft_artigos

# =========================================================
# TAREFA 3 (ATIVIDADE 3) - CARREGAMENTO (COM UPSERT/APPEND CORRIGIDO)
# =========================================================

def task_carregar_dw(dim_fonte: pd.DataFrame, ft_artigos: pd.DataFrame):
    print(f"TASK 3: Início do carregamento no Data Warehouse ({NOME_DW})...")
    conn = None
    try:
        conn = sqlite3.connect(NOME_DW)
        
        # --- LÓGICA DE TRATAMENTO DA DIMENSÃO (UPSERT SIMULADO) ---
        
        # 1. Tenta ler as fontes que já existem no DW
        try:
            df_dim_existente = pd.read_sql_query("SELECT nome_fonte FROM dim_fonte", conn)
            # Lista de fontes que JÁ ESTÃO no DW
            fontes_existentes = set(df_dim_existente['nome_fonte'])
            
            # Filtra o DataFrame de entrada (dim_fonte) para pegar APENAS AS NOVAS FONTES
            df_novas_fontes = dim_fonte[~dim_fonte['nome_fonte'].isin(fontes_existentes)].copy()

            if df_novas_fontes.empty:
                print("TASK 3: Dimensão: Nenhuma nova fonte encontrada. Carregamento incremental ignorado.")
            else:
                # Ajusta os IDs das novas fontes para continuar a sequência do DW
                # NOTA: O SQLite tem um ROWID automático, mas para este exercício, 
                # mantemos a lógica de ID sequencial.
                
                # Carregamento da DIMENSÃO (APPEND) - Apenas as novas linhas
                df_novas_fontes.to_sql(
                    'dim_fonte', conn, if_exists='append', index=False
                )
                print(f"TASK 3: Tabela dim_fonte carregada (APPEND). {len(df_novas_fontes)} novas fontes adicionadas.")

        except pd.io.sql.DatabaseError:
            # Se a tabela dim_fonte não existir (primeira execução), carrega TUDO com REPLACE.
            dim_fonte.to_sql('dim_fonte', conn, if_exists='replace', index=False)
            print("TASK 3: Tabela dim_fonte criada pela primeira vez (REPLACE).")


        # --- CARREGAMENTO DA FATO ---
        ft_artigos.to_sql(
            'ft_artigos', conn, if_exists='replace', index=False
        )
        print("TASK 3: Tabela ft_artigos carregada (REPLACE).")
        
        conn.commit()
        print("TASK 3: Sucesso. Carregamento concluído.")
    
    except sqlite3.Error as e:
        print(f"TASK 3: Erro no carregamento do SQLite: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# =========================================================
# LÓGICA DE ORQUESTRAÇÃO (main_pipeline)
# =========================================================

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

    # 3. CARREGAR (Tarefa 3) - Agora com UPSERT funcional
    task_carregar_dw(dim_fonte, ft_artigos)

    # 4. SIMULAÇÃO DE UPLOAD PARA O DATA LAKE
    simular_upload_s3(nome_arquivo_bruto())

    print("\n--- FIM DO PIPELINE. SUCESSO. ---")

if __name__ == '__main__':
    main_pipeline()