# 📊 Projeto Integrado - Pipeline de Dados (CNPJ & Balança Comercial)

![Status](https://img.shields.io/badge/Status-Concluído-brightgreen)
![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Spark](https://img.shields.io/badge/Apache_Spark-3.3.2-orange)
![Azure](https://img.shields.io/badge/Cloud-Azure_Blob_Storage-0078D4)

Este projeto implementa um pipeline de engenharia de dados robusto e escalável para processamento de dados públicos do **CNPJ** e da **Balança Comercial Brasileira**. O sistema utiliza **PySpark** para processamento distribuído e **Azure Blob Storage** como Data Lake, seguindo a arquitetura **Medallion (Bronze, Silver, Gold)**.

---

## 📑 Índice

*   [🏗️ Arquitetura e Fluxo de Dados](#-arquitetura-e-fluxo-de-dados)
*   [🚀 Funcionalidades e Diferenciais](#-funcionalidades-e-diferenciais)
*   [⚙️ Configurações de Otimização Delta Lake](#-configurações-de-otimização-delta-lake)
*   [📂 Estrutura do Projeto](#-estrutura-do-projeto)
*   [🛠️ Como Executar](#-como-executar)
*   [🧠 Decisões de Design](#-decisões-de-design)
*   [🔧 Troubleshooting](#-troubleshooting)

---

## 📚 Documentação Completa (Manual)

Para detalhes aprofundados sobre cada aspecto do projeto, acesse o **[MANUAL](docs/manual/index.md)**.

Abaixo, um resumo das seções disponíveis:

| Seção | Descrição |
| :--- | :--- |
| [00. Escopo e Cronograma](docs/manual/00_escopo_e_cronograma.md) | Visão geral dos objetivos, entregáveis e cronograma do projeto. |
| [01. Visão Geral](docs/manual/01_visao_geral.md) | Introdução ao contexto de negócio e solução técnica. |
| [02. Configuração do Ambiente](docs/manual/02_configuracao_ambiente.md) | Guia passo-a-passo para preparar o ambiente (Local/Azure/Databricks). |
| [03. Execução do Pipeline](docs/manual/03_execucao_pipeline.md) | Instruções detalhadas para rodar os scripts de ingestão e transformação. |
| [04. Arquitetura Detalhada](docs/manual/04_arquitetura_detalhada.md) | Diagramas e explicações técnicas sobre as camadas Bronze, Silver e Gold. |
| [05. Troubleshooting](docs/manual/05_guia_troubleshooting.md) | Soluções para erros comuns e problemas conhecidos. |
| [06. Dicionário de Dados](docs/manual/06_dicionario_dados.md) | Detalhamento das tabelas e colunas da camada Gold (Refined). |

---

## 🏗️ Arquitetura e Fluxo de Dados

O projeto segue o padrão Medallion para garantir qualidade e governança dos dados.

```mermaid
graph LR
    A["Landing Zone<br/>(Blob Storage)"] -->|Ingestão Raw| B[("Bronze Layer<br/>Delta/Parquet")]
    B -->|Limpeza & Schema| C[("Silver Layer<br/>Delta Lake")]
    C -->|Modelagem Star Schema| D[("Gold Layer<br/>Refined Tables")]
    
    subgraph "Fontes de Dados"
        CNPJ["Arquivos CNPJ<br/>(ZIP/CSV)"]
        BAL["Balança Comercial<br/>(CSV)"]
    end
    
    CNPJ --> A
    BAL --> A
```

1.  **Landing Zone**: Dados brutos hospedados no Azure Blob Storage (containers `landing...`).
2.  **Bronze Layer**: Dados ingeridos "as-is", convertidos para Delta/Parquet para performance, mantendo histórico.
3.  **Silver Layer**: Dados limpos, tipados (Schema Enforcement), deduplicados e enriquecidos com regras de negócio.
4.  **Gold Layer**: Dados modelados em **Star Schema** (Fatos e Dimensões) otimizados para Analytics e BI.

---

## 🚀 Funcionalidades e Diferenciais

*   **Databricks Native**: Otimizado para execução no Azure Databricks, utilizando recursos nativos como `dbutils` e `Secret Scopes`.
*   **Segurança via Key Vault**: Integração segura com Azure Key Vault para gerenciamento de credenciais, eliminando chaves hardcoded.
*   **Arquitetura Medallion Completa**: Pipeline implementado de ponta a ponta (Raw -> Trusted -> Refined).
*   **Observabilidade**: Logs detalhados são enviados para o console e persistidos.
*   **Atomicidade**: Uso de operações atômicas do Delta Lake (`overwrite` mode) e comandos `OPTIMIZE/VACUUM` para performance.
*   **Otimização Automática**: Configuração explícita de `OPTIMIZE` (Target Size: 10MB) e `VACUUM` (Retenção: 60 dias) para manutenção saudável do Data Lake.

---

## ⚙️ Configurações de Otimização Delta Lake

Para garantir alta performance de leitura e controle de custos de armazenamento, o pipeline aplica as seguintes políticas nas camadas **Silver** e **Gold**:

1.  **OPTIMIZE (Z-Order & Compaction)**:
    *   Arquivos pequenos são compactados para um tamanho alvo de **10 MB** (`10485760 bytes`).
    *   Isso evita o problema de "small files" que degrada a performance do Spark.
    *   Configuração: `DELTA_OPTIMIZE_FILE_SIZE` em `src/utils/config.py`.

2.  **VACUUM (Limpeza de Histórico)**:
    *   Versões antigas dos dados (ex: arquivos sobrescritos) são removidas fisicamente após **60 dias**.
    *   Isso permite Time Travel (voltar no tempo) por 2 meses, equilibrando segurança e custo.
    *   Configuração: `DELTA_VACUUM_RETENTION_DAYS` em `src/utils/config.py`.

---

## 📂 Estrutura do Projeto

```text
/
├── src/
│   ├── scripts/                # Scripts do Pipeline (Databricks Notebooks)
│   │   ├── 01_listagem_*.py    # 🔍 Diagnóstico: Lista arquivos na origem
│   │   ├── 02_setup_*.py       # 🛠️ Setup: Cria e valida containers de destino (Raw/Trusted/Refined)
│   │   ├── 03_ingestao_*.py    # 📥 Bronze: Ingestão Balança Comercial
│   │   ├── 04_ingestao_*.py    # 📥 Bronze: Ingestão CNPJ (extração de ZIPs)
│   │   ├── 05_transf_*.py      # 🔄 Silver: Transformação Balança (Limpeza, Tipagem)
│   │   ├── 06_transf_*.py      # 🔄 Silver: Transformação CNPJ (Schema Mapping)
│   │   └── 07_transf_*.py      # 🏆 Gold: Modelagem Dimensional (Star Schema)
│   ├── utils/                  # Bibliotecas compartilhadas
│   │   ├── config.py           # Gerenciamento de configuração e variáveis de ambiente
│   │   ├── logging_utils.py    # Handler de logs customizado
│   │   └── transformations.py  # Funções reutilizáveis Spark
├── requirements.txt            # Dependências do projeto
└── README.md                   # Este arquivo
```

---

## 🛠️ Como Executar

Este pipeline foi desenhado para ser executado exclusivamente no **Azure Databricks**.

### 1. Pré-requisitos
*   Cluster Azure Databricks (Runtime 10.4 LTS ou superior recomendado).
*   Acesso aos containers do Azure Blob Storage configurado via **Azure Key Vault**.
*   **Secret Scope** criado no Databricks apontando para o Key Vault (nome padrão: `databricks-scope-4`).

### 2. Configuração
Certifique-se de que os segredos abaixo estão configurados no Key Vault:
*   `secret-landing`: Chave de acesso (Account Key) da Storage Account de Landing.

O arquivo `src/utils/config.py` gerencia automaticamente a recuperação desses segredos.

### 3. Execução do Pipeline
Importe os scripts da pasta `src/scripts` para o seu Workspace do Databricks e execute-os como Notebooks ou Jobs na seguinte ordem:

1.  **Setup e Ingestão (Bronze)**:
    *   `03_ingestao_bronze_balanca`
    *   `04_ingestao_bronze_cnpj`
2.  **Transformação (Silver)**:
    *   `05_transformacao_silver_balanca`
    *   `06_transformacao_silver_cnpj`
3.  **Refinamento (Gold)**:
    *   `07_transformacao_gold`

---

## 🧠 Decisões de Design

### Por que Delta Lake?
Utilizamos Delta Lake nas camadas Silver e Gold para garantir **ACID Transactions**. Isso nos permite sobrescrever dados de forma segura (`overwriteSchema`) sem corromper leituras concorrentes e sem precisar deletar diretórios manualmente.

### Tratamento de Arquivos ZIP (CNPJ)
Os dados do CNPJ vêm em arquivos ZIP massivos contendo CSVs. Nossa estratégia de ingestão (Script 04):
1.  Faz o download em streaming (chunks) para evitar estouro de memória.
2.  Extrai localmente em área temporária do Driver.
3.  Lê com Spark e converte imediatamente para Parquet/Delta, descartando o CSV bruto.

### Segurança (Key Vault)
A segurança foi priorizada removendo tokens SAS hardcoded. O acesso ao Storage é feito exclusivamente através de credenciais gerenciadas no Azure Key Vault e acessadas via Databricks Secret Scopes.

---

## 🔧 Troubleshooting

| Erro | Causa Provável | Solução |
|------|----------------|---------|
| `DirectoryIsNotEmpty` | Conflito na sobrescrita de diretórios no Blob Storage. | O código já foi atualizado para usar `.mode("overwrite")` do Delta. Não apague pastas manualmente durante a execução. |
| `DBUtils not available` | Execução fora do Databricks. | Este projeto requer ambiente Databricks. |
| `Secret not found` | Segredo ausente no Key Vault ou Scope incorreto. | Verifique se o secret scope `databricks-scope-4` existe e contém a chave `secret-landing`. |

---

**Desenvolvido por Grupo 4**
