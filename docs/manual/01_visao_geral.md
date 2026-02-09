[Escopo](./00_escopo_e_cronograma.md) | **Visão Geral** | [Configuração](./02_configuracao_ambiente.md) | [Execução](./03_execucao_pipeline.md) | [Arquitetura](./04_arquitetura_detalhada.md) | [Troubleshooting](./05_guia_troubleshooting.md) | [Dicionário](./06_dicionario_dados.md)

---

# 1. Visão Geral do Projeto

## 🎯 Objetivo
Este projeto tem como objetivo construir um pipeline de dados escalável para processar dados públicos brasileiros:
1.  **CNPJ (Cadastro Nacional da Pessoa Jurídica)**: Dados cadastrais de empresas (abertas).
2.  **Balança Comercial**: Dados de importação e exportação (MDIC).

O sistema ingere dados brutos, limpa, transforma e os disponibiliza em um Data Lake estruturado na nuvem (Azure), seguindo as melhores práticas de Engenharia de Dados.

## 🏗️ Arquitetura Resumida
Utilizamos a arquitetura **Medallion** (Bronze, Silver, Gold):
*   **Landing**: Chegada dos arquivos brutos.
*   **Bronze**: Cópia fiel da origem, mas em formato otimizado (Delta/Parquet).
*   **Silver**: Dados limpos, tipados e com qualidade garantida.
*   **Gold**: Dados agregados para BI (Roadmap).

## 🛠️ Stack Tecnológico
*   **Linguagem**: Python 3.8+
*   **Processamento**: Apache Spark (PySpark) 3.3.2
*   **Storage**: Azure Blob Storage (Gen2)
*   **Formato de Tabela**: Delta Lake 2.2+
*   **Orquestração**: Scripts Python (podendo ser migrado para Airflow/Data Factory)
*   **Infraestrutura**: Híbrida (Roda local no Windows e no Azure Databricks)
