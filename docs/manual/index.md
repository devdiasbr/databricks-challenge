[🏠 Home](../../README.md) | [Escopo](./00_escopo_e_cronograma.md) | [Visão Geral](./01_visao_geral.md) | [Configuração](./02_configuracao_ambiente.md) | [Execução](./03_execucao_pipeline.md) | [Arquitetura](./04_arquitetura_detalhada.md) | [Troubleshooting](./05_guia_troubleshooting.md) | [Dicionário](06_dicionario_dados.md) | **Sumário**


# 📚 SUMÁRIO

Bem-vindo ao manual completo do Projeto Integrado. Navegue pelos tópicos abaixo para acessar a documentação detalhada.

---

### **1. [ESCOPO E CRONOGRAMA](./00_escopo_e_cronograma.md)**
*   **1.1. [Escopo e Requisitos](./00_escopo_e_cronograma.md#1-escopo-e-requisitos)**
    *   1.1.1. [Requisitos Funcionais e de Dados](./00_escopo_e_cronograma.md#funcionais-e-de-dados)
    *   1.1.2. [Requisitos Técnicos e Governança](./00_escopo_e_cronograma.md#técnicos-e-governança)
*   **1.2. [Cronograma](./00_escopo_e_cronograma.md#2-cronograma)**

### **2. [VISÃO GERAL](./01_visao_geral.md)**
*   **2.1. [Objetivo](./01_visao_geral.md#🎯-objetivo)**
*   **2.2. [Arquitetura Resumida](./01_visao_geral.md#🏗️-arquitetura-resumida)**
*   **2.3. [Stack Tecnológico](./01_visao_geral.md#🛠️-stack-tecnológico)**

### **3. [CONFIGURAÇÃO DO AMBIENTE](./02_configuracao_ambiente.md)**
*   **3.1. [Pré-requisitos](./02_configuracao_ambiente.md#📋-pré-requisitos)**
*   **3.2. [Instalação Local (Windows)](./02_configuracao_ambiente.md#🚀-instalação-local-windows)**
    *   3.2.1. [Clonar o Repositório](./02_configuracao_ambiente.md#1-clonar-o-repositório)
    *   3.2.2. [Ambiente Virtual](./02_configuracao_ambiente.md#2-ambiente-virtual-recomendado)
    *   3.2.3. [Instalar Dependências](./02_configuracao_ambiente.md#3-instalar-dependências)
    *   3.2.4. [Configurar Variáveis (.env)](./02_configuracao_ambiente.md#4-configurar-variáveis-de-ambiente-env)
*   **3.3. [Execução no Databricks](./02_configuracao_ambiente.md#☁️-execução-no-databricks)**

### **4. [EXECUÇÃO DO PIPELINE](./03_execucao_pipeline.md)**
*   **4.1. [Execução Sequencial](./03_execucao_pipeline.md#📜-execução-do-pipeline)**
    *   4.1.1. [Diagnóstico](./03_execucao_pipeline.md#1-diagnóstico-opcional)
    *   4.1.2. [Validação de Setup](./03_execucao_pipeline.md#2-validação-de-setup-opcional)
    *   4.1.3. [Camada Bronze (Ingestão)](./03_execucao_pipeline.md#3-camada-bronze-ingestão)
    *   4.1.4. [Camada Silver (Transformação)](./03_execucao_pipeline.md#4-camada-silver-transformação)
*   **4.2. [Monitoramento](./03_execucao_pipeline.md#📊-monitoramento)**

### **5. [ARQUITETURA DETALHADA](./04_arquitetura_detalhada.md)**
*   **5.1. [Padrão Medallion](./04_arquitetura_detalhada.md#📐-padrão-medallion)**
    *   5.1.1. [Bronze Layer (Raw)](./04_arquitetura_detalhada.md#🥉-bronze-layer-raw)
    *   5.1.2. [Silver Layer (Trusted)](./04_arquitetura_detalhada.md#🥈-silver-layer-trusted)
    *   5.1.3. [Gold Layer (Refined)](./04_arquitetura_detalhada.md#🥇-gold-layer-refined)
    *   5.1.4. [Modelo de Dados (Star Schema)](./04_arquitetura_detalhada.md#📊-modelo-de-dados-star-schema)
    *   5.1.5. [Diagrama ER (DER)](./04_arquitetura_detalhada.md#📐-diagrama-de-entidade-relacionamento-der)
*   **5.2. [Fluxo de Dados](./04_arquitetura_detalhada.md#🔄-fluxo-de-dados)**
*   **5.3. [Segurança](./04_arquitetura_detalhada.md#🔐-segurança)**

### **6. [GUIA DE TROUBLESHOOTING](./05_guia_troubleshooting.md)**
*   **6.1. [Erros Comuns](./05_guia_troubleshooting.md#🚨-erros-comuns)**
    *   6.1.1. [DirectoryIsNotEmpty / DELTA_PATH_EXISTS](./05_guia_troubleshooting.md#1-directoryisnotempty-ou-delta_path_exists)
    *   6.1.2. [NameError: name '__file__' is not defined](./05_guia_troubleshooting.md#2-nameerror-name-__file__-is-not-defined)
    *   6.1.3. [Py4JJavaError / Winutils](./05_guia_troubleshooting.md#3-py4jjavaerror--winutils-)
    *   6.1.4. [403 Forbidden (Azure Blob)](./05_guia_troubleshooting.md#4-403-forbidden-azure-blob)
    *   6.1.5. [AnalysisException / Mismatch](./05_guia_troubleshooting.md#5-analysisexception--mismatch-)

### **7. [DICIONÁRIO DE DADOS](./06_dicionario_dados.md)**
*   **7.1. [Fato Balanço Comercial (ft_balanco_comercial)](./06_dicionario_dados.md#1-fato-balanço-comercial-ft_balanco_comercial)**
*   **7.2. [Dimensão Data (dim_data)](./06_dicionario_dados.md#2-dimensão-data-dim_data)**
*   **7.3. [Dimensão NCM (dim_ncm)](./06_dicionario_dados.md#3-dimensão-ncm-dim_ncm)**
*   **7.4. [Dimensão Países (dim_paises)](./06_dicionario_dados.md#4-dimensão-países-dim_paises)**
*   **7.5. [Dimensão UFs (dim_ufs)](./06_dicionario_dados.md#5-dimensão-ufs-dim_ufs)**
*   **7.6. [Dimensão Via Transporte (dim_via_transporte)](./06_dicionario_dados.md#6-dimensão-via-transporte-dim_via_transporte)**

---
*Documentação gerada automaticamente em 2026-02-10.*
