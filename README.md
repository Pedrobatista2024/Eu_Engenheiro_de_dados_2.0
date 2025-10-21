# 🚀 PROJETO GUARDIÃO DE DADOS: Pipeline de ETL/ELT e Qualidade

Este repositório documenta a construção de um pipeline de Engenharia de Dados de ponta a ponta, simulando a infraestrutura e os desafios de um ambiente de produção (*Data Warehouse*). O projeto foi desenvolvido sob a orientação de uma "Gestora IA" e teve como objetivo cobrir os **6 Pilares Essenciais da Engenharia de Dados**, focando na **Qualidade de Dados (Data Quality)** e **Observabilidade** do processo.

---

## 🎯 Pilares Avançados da Engenharia de Dados Abordados

O projeto garantiu a aplicação prática e a consolidação dos seguintes conceitos de nível Pleno:

1.  **SQL Avançado e Modelagem:** Implementação de um esquema *Fato e Dimensão*, uso de **CTEs** e Funções de Janela (**ROW_NUMBER()**).
2.  **Lógica ETL/ELT e Orquestração:** Estruturação de *tasks* modulares com **dependências de execução** (simulação do Airflow) e lógica de carregamento **Incremental (UPSERT)**.
3.  **Qualidade e Observabilidade:** Implementação de regras de **Data Quality** (Validação de URL, *NULL check*, **Desduplicação** por Chave de Negócio) e criação de **Métricas de Alerta** em tempo real.
4.  **Data Warehousing & Cloud:** Simulação de *Data Lake* (AWS S3 `raw/` layer), Evolução de Schema, e persistência em *Data Warehouse* (SQLite).

---

## 🛠️ Tecnologias e Ferramentas

O projeto utiliza o ecossistema padrão da Engenharia de Dados, focado em robustez e eficiência:

* **Linguagem & Bibliotecas:** Python, Pandas (manipulação e transformação de dados em memória).
* **Destino (DW):** **SQLite** (`sqlite3`) para simulação de um Data Warehouse relacional.
* **Armazenamento Bruto:** JSON (simulando a camada Raw do Data Lake).
* **Conceitos Arquiteturais:** Modelagem Estrela, Lógica de *Particionamento* e *Indexing* (Otimização SQL), Tratamento de *Schema Evolution*.

---

## 🧩 Estrutura do Pipeline e Fluxo de Dados

O pipeline é modular e implementado em funções Python, simulando o fluxo de orquestração:

| Fase | Atividades Principais | Valor Agregado no Pipeline |
| :--- | :--- | :--- |
| **Ingestão (E)** | Coleta de API de notícias, Tratamento de `NULLs`. | Uso da lógica `reindex` para garantir a **consistência do schema**. |
| **Transformação (T)** | Desduplicação por **Chave de Negócio**, Criação de `dim_fonte` e `ft_artigos`. | Inclusão de **Flags de Qualidade** (Ex: `url_valida`). |
| **Carregamento (L)** | Carregamento no SQLite. | Lógica de **Carregamento Incremental** (Append para Dimensões) e Carregamento Full (Replace para Fatos). |
| **Análise (A)** | Consulta SQL com Funções de Janela (`ROW_NUMBER`). | Geração de um **Relatório de Observabilidade** com métricas diárias. |
