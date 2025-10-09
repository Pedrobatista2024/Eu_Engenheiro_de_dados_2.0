#Tarefa:
#
#Escolha de Fonte (API): Utilize uma API gratuita de notícias que permita buscar artigos por palavra-chave. A NewsAPI ou a GNews API são boas opções (você precisará de uma chave de API gratuita, fácil de obter).
#
#Ingestão de Dados: Colete os artigos (notícias) publicados hoje (ou nas últimas 24h) com a palavra-chave "Mercado Financeiro" ou "Inteligência Artificial".
#
#Qualidade de Dados (Limpeza): Verifique se o título (title) e o conteúdo (content) dos artigos estão completos e sem valores nulos.
#
#Regra de Qualidade: Remova qualquer artigo onde o título ou o conteúdo esteja vazio (None ou string vazia).
#
#Armazenamento Bruto (Simulação de Data Lake): Salve o resultado (todos os artigos coletados e limpos) em um arquivo JSON chamado noticias_brutas_YYMMDD.json.
#
#Para a entrega, envie o código que realiza a coleta e a limpeza, e o nome do arquivo JSON que você gerou. Me diga quantos artigos foram removidos devido à sua regra de qualidade.
#
#Pilares Abordados:
#
#Ingestão de Dados e APIs: Conexão e coleta de dados não estruturados de uma API real.
#
#Qualidade de Dados: Implementação da primeira regra de validação (NULL check).
#
#Estou aguardando sua entrega para validar a primeira etapa do nosso pipeline. Bom trabalho!

import requests
import pandas as pd
from datetime import datetime
import json
import os 


API_KEY = 'ca1d64caa68fcda80c6ffd171bf2ceb9'  
QUERY = 'Inteligência Artificial'
ENDPOINT = 'http://api.mediastack.com/v1/news' 


params = {
    'access_key': API_KEY,          
    'keywords': QUERY,             
    'language': 'pt',
    'limit': 100,                   
    'sort': 'published_desc'       
}


try:
    response = requests.get(ENDPOINT, params=params)
    response.raise_for_status() 
    dados = response.json()

 
    artigos = dados.get('data', []) 

except requests.exceptions.RequestException as e:
    print(f"Erro na requisição da API: {e}")
    artigos = []


df_bruto = pd.DataFrame(artigos)

if df_bruto.empty:
    df_limpo = df_bruto
    artigos_removidos = 0
else:
    total_antes = len(df_bruto)
    
    df_limpo = df_bruto.dropna(subset=['title', 'description'])
    
    df_limpo = df_limpo[
        ~( (df_limpo['title'].str.strip() == '') | (df_limpo['description'].str.strip() == '') )
    ]
    
    total_depois = len(df_limpo)
    artigos_removidos = total_antes - total_depois

print(f'antes : {total_antes}')
print(f'depois : {total_depois}')
print(f'removidos : {artigos_removidos}')

#nome_arquivo = f"noticias_brutas_{datetime.now().strftime('%y%m%d')}.json"
#
#if not df_limpo.empty:
#    df_limpo.to_json(nome_arquivo, orient='records', indent=4)
#else:
#    with open(nome_arquivo, 'w') as f:
#        json.dump([], f)
#
#
#print("\n" + "="*40)
#print("RELATÓRIO DE ENTREGA")
#print(f"Arquivo JSON gerado: {nome_arquivo}")
#print(f"Total de Artigos Removidos: {artigos_removidos}")
#print("="*40)