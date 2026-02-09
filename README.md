# 📊 Projeto Integrado - Pipeline de Dados (CNPJ & Balança Comercial)

![Status](https://img.shields.io/badge/Status-Concluído-brightgreen)
![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Spark](https://img.shields.io/badge/Apache_Spark-3.3.2-orange)
![Azure](https://img.shields.io/badge/Cloud-Azure_Blob_Storage-0078D4)

Este projeto implementa um pipeline de engenharia de dados robusto e escalável para processamento de dados públicos do **CNPJ** e da **Balança Comercial Brasileira**. O sistema utiliza **PySpark** para processamento distribuído e **Azure Blob Storage** como Data Lake, seguindo a arquitetura **Medallion (Bronze, Silver, Gold)**.

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

*   **Ingestão Híbrida Inteligente**: Os scripts detectam automaticamente se estão rodando no **Databricks** ou **Localmente**, ajustando caminhos e métodos de autenticação.
*   **Arquitetura Medallion Completa**: Pipeline implementado de ponta a ponta (Raw -> Trusted -> Refined).
*   **Observabilidade**: Logs detalhados são enviados para o console (com barras de progresso `tqdm`) e persistidos.
*   **Suporte a Windows**: O projeto baixa e configura automaticamente o `winutils.exe` (Hadoop binaries) para permitir a execução do Spark no Windows sem dores de cabeça.
*   **Atomicidade**: Uso de operações atômicas do Delta Lake (`overwrite` mode) e comandos `OPTIMIZE/VACUUM` para performance.

---

## 📂 Estrutura do Projeto

```text
/
├── hadoop/                     # Binários do Hadoop (winutils) gerenciados automaticamente
├── src/
│   ├── scripts/                # Scripts do Pipeline (Execução Sequencial)
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

### 1. Pré-requisitos
*   Python 3.8 ou superior.
*   Java 8 ou 11 (JRE/JDK) instalado e configurado no PATH.
*   Acesso aos containers do Azure Blob Storage (SAS Tokens).

### 2. Configuração (.env)
Crie um arquivo `.env` na raiz baseado no `.env.example` e preencha suas credenciais:

```ini
BALANCA_ACCOUNT_URL=https://seu-storage.blob.core.windows.net
AZURE_STORAGE_SAS_TOKEN_BALANCA="?sv=..."
# ... (ver .env.example para lista completa)
```

### 3. Execução do Pipeline
Os scripts devem ser executados sequencialmente para garantir a dependência dos dados:

```bash
# 1. Diagnóstico e Setup
python src/scripts/01_listagem_arquivos_azure.py
python src/scripts/02_setup_validacao_targets.py

# 2. Camada Bronze (Ingestão)
python src/scripts/03_ingestao_bronze_balanca.py
python src/scripts/04_ingestao_bronze_cnpj.py

# 3. Camada Silver (Transformação)
python src/scripts/05_transformacao_silver_balanca.py
python src/scripts/06_transformacao_silver_cnpj.py

# 4. Camada Gold (Refinamento)
python src/scripts/07_transformacao_gold.py
```

---

## 🧠 Decisões de Design

### Por que Delta Lake?
Utilizamos Delta Lake nas camadas Silver e Gold para garantir **ACID Transactions**. Isso nos permite sobrescrever dados de forma segura (`overwriteSchema`) sem corromper leituras concorrentes e sem precisar deletar diretórios manualmente.

### Tratamento de Arquivos ZIP (CNPJ)
Os dados do CNPJ vêm em arquivos ZIP massivos contendo CSVs. Nossa estratégia de ingestão (Script 04):
1.  Faz o download em streaming (chunks) para evitar estouro de memória.
2.  Extrai localmente em área temporária.
3.  Lê com Spark e converte imediatamente para Parquet/Delta, descartando o CSV bruto.

### Fallback de Caminhos (`__file__`)
Para suportar execução local (VS Code) e remota (Databricks Notebooks), usamos um padrão robusto de resolução de caminhos:
```python
try:
    base_dir = os.path.dirname(os.path.abspath(__file__)) # Funciona local
except NameError:
    base_dir = os.getcwd() # Funciona no Databricks
```

---

## 🔧 Troubleshooting

| Erro | Causa Provável | Solução |
|------|----------------|---------|
| `DirectoryIsNotEmpty` | Conflito na sobrescrita de diretórios no Blob Storage. | O código já foi atualizado para usar `.mode("overwrite")` do Delta. Não apague pastas manualmente durante a execução. |
| `Winutils not found` | Falta de binários do Hadoop no Windows. | O script baixa o `winutils.exe` automaticamente. Se falhar, verifique sua conexão ou permissões na pasta `hadoop/`. |
| `403 Forbidden` | Token SAS expirado ou incorreto. | Verifique o `.env`. O token deve começar com `?` e não deve conter quebras de linha. |

---

**Desenvolvido por Grupo 4**
