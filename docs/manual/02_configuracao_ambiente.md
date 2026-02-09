[Escopo](./00_escopo_e_cronograma.md) | [Visão Geral](./01_visao_geral.md) | **Configuração** | [Execução](./03_execucao_pipeline.md) | [Arquitetura](./04_arquitetura_detalhada.md) | [Troubleshooting](./05_guia_troubleshooting.md)

---

# 2. Configuração do Ambiente

## 📋 Pré-requisitos
Para rodar o projeto, você precisa de:
1.  **Python 3.8** ou superior.
2.  **Java JDK 8 ou 11** (O Spark precisa do Java).
3.  **Git** para clonar o repositório.

## 🚀 Instalação Local (Windows)

### 1. Clonar o Repositório
```bash
git clone https://github.com/devdiasbr/databricks-challenge.git
cd databricks-challenge
```

### 2. Ambiente Virtual (Recomendado)
```bash
python -m venv venv
.\venv\Scripts\activate
```

### 3. Instalar Dependências
Execute o comando abaixo para instalar as bibliotecas necessárias:
```bash
pip install -r requirements.txt
```

### 4. Configurar Variáveis de Ambiente (.env)
Crie um arquivo `.env` na raiz do projeto. Use o `.env.example` como base.
Você precisará dos **SAS Tokens** dos containers do Azure Blob Storage.

Exemplo de `.env`:
```ini
BALANCA_ACCOUNT_URL=https://landingbeca2026jan.blob.core.windows.net
AZURE_STORAGE_SAS_TOKEN_BALANCA="?sv=2022-11-02&ss=b&srt=sco..."
AZURE_TARGET_STORAGE_RAW_URL="https://grupo4storage.blob.core.windows.net/raw?..."
AZURE_TARGET_STORAGE_TRUSTED_URL="https://grupo4storage.blob.core.windows.net/trusted?..."
```

> **Nota sobre o Hadoop no Windows:**
> O projeto baixa automaticamente o `winutils.exe` necessário para o Spark rodar no Windows. Ele cria uma pasta `hadoop/bin` na raiz do projeto. Não é necessário instalar o Hadoop completo.

## ☁️ Execução no Databricks
1.  Importe o repositório no Databricks Repos.
2.  As variáveis de ambiente podem ser configuradas via **Databricks Secrets** ou criando um arquivo `.env` no driver (menos seguro).
3.  Os scripts são compatíveis e detectam automaticamente que não estão rodando localmente (ajustando caminhos via `os.getcwd()` em vez de `__file__`).
