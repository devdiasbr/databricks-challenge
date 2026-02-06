# Projeto Integrado - Pipeline de Dados (CNPJ & Balança Comercial)

Este projeto implementa um pipeline de engenharia de dados robusto e escalável para processamento de dados públicos do CNPJ e da Balança Comercial Brasileira. O sistema utiliza **PySpark** para processamento distribuído e **Azure Blob Storage** como Data Lake, seguindo a arquitetura **Medallion (Bronze, Silver, Gold)**.

## 📋 Sobre o Projeto

O objetivo é ingerir, limpar e transformar grandes volumes de dados (Big Data) provenientes de fontes governamentais, disponibilizando-os em camadas organizadas para análise e consumo. O projeto foi desenhado para rodar tanto em clusters Databricks quanto em ambientes locais (com suporte nativo a Windows via hacks do Hadoop/Winutils).

### Arquitetura de Dados

O fluxo de dados segue o padrão Medallion:

1.  **Landing Zone (Origem)**: Dados brutos (Zips, CSVs) hospedados no Azure Blob Storage.
2.  **Bronze Layer (Raw)**: Ingestão "as-is" dos dados para formato Delta Lake ou Parquet, mantendo o histórico e rastreabilidade.
3.  **Silver Layer (Trusted)**: Dados limpos, tipados, deduplicados e enriquecidos. Aplicação de regras de negócio e validação de schema.
4.  **Gold Layer (Refined)**: (Planejado) Agregações e modelagem dimensional para BI e Analytics.

## 🚀 Funcionalidades Principais

*   **Ingestão Híbrida**: Suporte a leitura de arquivos locais e remotos (Azure Blob Storage).
*   **Orquestração Simplificada**: Script `00_setup.py` que gerencia dependências e execução do pipeline.
*   **Logging Centralizado**: Logs de execução salvos localmente e enviados automaticamente para o container `$logs` no Azure, com suporte a barras de progresso (`tqdm`) sem poluição visual.
*   **Compatibilidade Windows**: Tratamento automático de dependências do Hadoop (`winutils.exe`) para execução do Spark no Windows.
*   **Resiliência**: Mecanismos de retry, validação de caminhos e fallback para imports.

## 🛠️ Pré-requisitos

*   **Python 3.8+**
*   **Java 8 ou 11** (Necessário para o Apache Spark)
*   **Acesso ao Azure**: Connection Strings ou SAS Tokens para os containers de origem e destino.

## ⚙️ Configuração

1.  **Clone o repositório**:
    ```bash
    git clone https://github.com/devdiasbr/databricks-challenge.git
    cd databricks-challenge
    ```

2.  **Configure as Variáveis de Ambiente**:
    Crie um arquivo `.env` na raiz do projeto baseando-se no `.env.example`. Preencha com suas credenciais do Azure:

    ```ini
    # .env
    BALANCA_ACCOUNT_URL=https://landingbeca2026jan.blob.core.windows.net
    CNPJ_ACCOUNT_URL=https://landingbeca2026jan.blob.core.windows.net
    
    # SAS Tokens (Exemplos)
    AZURE_STORAGE_SAS_TOKEN_BALANCA="?sv=2022-11-02&ss=b&srt=sco..."
    AZURE_STORAGE_SAS_TOKEN_CNPJ="?sv=2022-11-02&ss=b&srt=sco..."
    
    # Targets
    AZURE_TARGET_STORAGE_RAW_URL="https://grupo4storage.blob.core.windows.net/raw?..."
    AZURE_TARGET_STORAGE_TRUSTED_URL="https://grupo4storage.blob.core.windows.net/trusted?..."
    ```

## ▶️ Como Executar

O projeto possui um orquestrador central que facilita a execução.

### Execução Completa (Setup + Pipeline)

```bash
python src/scripts/00_setup.py
```

Este comando irá:
1.  Instalar as dependências do `requirements.txt`.
2.  Executar a ingestão Bronze (Balança e CNPJ).
3.  Executar a transformação Silver (Balança e CNPJ).

### Execução Parcial (Skipping Steps)

Você pode pular etapas que já foram concluídas para ganhar tempo:

```bash
# Pular instalação de dependências e ingestão Bronze
python src/scripts/00_setup.py --skip-deps --skip-bronze

# Pular apenas a camada Silver
python src/scripts/00_setup.py --skip-silver
```

## 📂 Estrutura do Projeto

```text
/
├── docs/                   # Documentação e schemas JSON
├── hadoop/                 # Binários do Hadoop para suporte Windows
├── src/
│   ├── scripts/            # Scripts principais do pipeline
│   │   ├── 00_setup.py     # Orquestrador
│   │   ├── 03_*.py         # Ingestão Bronze
│   │   ├── 04_*.py         # Ingestão Bronze (CNPJ)
│   │   ├── 05_*.py         # Transformação Silver
│   │   └── 06_*.py         # Transformação Silver (CNPJ)
│   ├── utils/              # Módulos utilitários
│   │   ├── config.py       # Gerenciamento de configuração e .env
│   │   ├── logging_utils.py# Configuração avançada de logs
│   │   └── transformations.py # Funções de transformação Spark
├── requirements.txt        # Dependências Python
└── ingestion.ipynb         # Notebook de referência (Databricks)
```

## 🔍 Detalhes de Implementação

### Ingestão CNPJ
Os dados de CNPJ são arquivos ZIP gigantes contendo CSVs. O script `04_ingestao_bronze_cnpj.py`:
1.  Conecta no Blob Storage via `azure-storage-blob`.
2.  Baixa e extrai os arquivos ZIP em streaming/chunks para uma área de staging local.
3.  Lê os CSVs extraídos com Spark e salva em formato Delta/Parquet na camada Bronze.

### Logs e Monitoramento
Utilizamos um `TqdmLoggingHandler` customizado em `src/utils/logging_utils.py` que permite exibir barras de progresso (`tqdm`) no terminal sem que os logs de INFO/WARNING quebrem a visualização. Todos os logs são persistidos no container `$logs` do Azure para auditoria.

---
**Desenvolvido por Grupo 4 - Projeto Integrado**
