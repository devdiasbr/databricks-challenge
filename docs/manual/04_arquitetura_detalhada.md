[🏠 Home](../../README.md) | [Escopo](./00_escopo_e_cronograma.md) | [Visão Geral](./01_visao_geral.md) | [Configuração](./02_configuracao_ambiente.md) | [Execução](./03_execucao_pipeline.md) | **Arquitetura** | [Troubleshooting](./05_guia_troubleshooting.md) | [Dicionário](./06_dicionario_dados.md)

---

# 4. Arquitetura Detalhada

## 📐 Padrão Medallion

### 🥉 Bronze Layer (Raw)
*   **Objetivo**: Armazenar dados brutos com histórico, sem perda de informação.
*   **Formato**: Delta Lake (preferencial) ou Parquet.
*   **Estratégia CNPJ**:
    *   Os arquivos originais são ZIPs contendo CSVs.
    *   Extraímos o CSV e convertemos para Parquet/Delta.
    *   Mantemos todas as colunas como `string` para evitar erros de leitura.
*   **Estratégia Balança**:
    *   Leitura direta dos CSVs da origem.
    *   Conversão para Delta.

### 🥈 Silver Layer (Trusted)
*   **Objetivo**: Dados limpos, validados e prontos para análise (Data Quality).
*   **Formato**: Delta Lake.
*   **Transformações Aplicadas**:
    *   **Renomeação**: Colunas convertidas para *snake_case* e removidos acentos (`normalize_column_name`).
    *   **Tipagem**: Conversão de strings para Inteiros, Decimais, Datas, etc.
    *   **Limpeza**: Tratamento de nulos e espaços em branco (`trim`).
    *   **Schema Enforcement**: Validação rígida contra os schemas definidos em `docs/schemas/`.
*   **Escrita Atômica**:
    *   Usamos `.mode("overwrite")` do Delta.
    *   Isso substitui os dados atomicamente, evitando o erro clássico de `DirectoryIsNotEmpty` do Hadoop.

### 🥇 Gold Layer (Refined)
*   **Objetivo**: Dados modelados para Analytics, BI e Relatórios (Business-Ready).
*   **Modelo**: Star Schema (Modelo Estrela).
*   **Formato**: Delta Lake (Otimizado com `Z-Order` e `Partitioning`).
*   **Estratégia**:
    *   Criação de chaves substitutas (`sk_*`) para integridade referencial.
    *   Enriquecimento de dimensões (ex: Sector Econômico baseado no NCM, Regiões baseadas em UF).
    *   Unificação de Fatos (Importação + Exportação na mesma tabela).

#### 📊 Modelo de Dados (Star Schema)

As tabelas foram desenhadas para responder perguntas de negócio como *"Qual o valor FOB exportado por Estado e Setor Econômico no último trimestre?"*.

| Tabela | Tipo | Descrição | Granularidade |
| :--- | :--- | :--- | :--- |
| **ft_balanco_comercial** | Fato | Transações de Importação e Exportação unificadas. | NCM + País/UF + Via + Mês |
| **dim_data** | Dimensão | Calendário fiscal e sazonal (Ano, Mês, Trimestre). | Mês |
| **dim_ncm** | Dimensão | Detalhes do Produto, CNAE e Setor Econômico. | Código NCM |
| **dim_localidade** | Dimensão | Geografia (País, Bloco Econômico, UF, Região). | País + UF |
| **dim_via_transporte** | Dimensão | Modal logístico (Marítima, Aérea, etc.). | Código Via |

### 📐 Diagrama de Entidade-Relacionamento (DER)

Abaixo apresentamos a modelagem física da camada Gold, ilustrando as chaves primárias (PK), chaves estrangeiras (FK) e os relacionamentos entre a tabela Fato e as Dimensões.

```mermaid
erDiagram
    ft_balanco_comercial {
        bigint sk_ncm FK
        bigint sk_localidade FK
        bigint sk_via_transporte FK
        bigint sk_data FK
        string tipo_movimentacao
        decimal valor_fob
        decimal quantidade
        decimal kg_liquido
        decimal valor_unitario
        decimal preco_kg
        int flag_exportacao
        int flag_importacao
        timestamp dt_atualizacao
    }

    dim_data {
        bigint sk_data PK
        date data
        int ano
        int mes
        string nome_mes
        int trimestre
        int semestre
    }

    dim_ncm {
        bigint sk_ncm PK
        string codigo_ncm
        string descricao_ncm
        string cnae
        string descricao_cnae
        string setor_economico
    }

    dim_localidade {
        bigint sk_localidade PK
        string pais
        string uf
        string regiao
        string bloco_pais
    }

    dim_via_transporte {
        bigint sk_via_transporte PK
        int codigo_via
        string descricao_via
    }

    dim_data ||--o{ ft_balanco_comercial : "filtra por período"
    dim_ncm ||--o{ ft_balanco_comercial : "descreve produto"
    dim_localidade ||--o{ ft_balanco_comercial : "localiza origem/destino"
    dim_via_transporte ||--o{ ft_balanco_comercial : "transporta via"
```

## 🔄 Fluxo de Dados

```mermaid
flowchart TD
    %% =========================
    %% DEFINIÇÃO DE ESTILOS
    %% =========================
    classDef landing fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,rx:8,ry:8;
    classDef bronze fill:#efebe9,stroke:#5d4037,stroke-width:2px,rx:6,ry:6;
    classDef silver fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,rx:6,ry:6;
    classDef gold fill:#fff8e1,stroke:#f9a825,stroke-width:3px,rx:10,ry:10;
    classDef process fill:#ffffff,stroke:#ef6c00,stroke-width:2px,stroke-dasharray: 5 5,rx:6,ry:6;
    classDef spacer fill:transparent,stroke:transparent;

    %% LANDING
    LZ[Landing Zone]:::landing
    BR[(Bronze Layer)]:::bronze
    LZ -->|Ingestão Raw| BR

    %% SILVER
    subgraph SILVER["Processamento - Silver Layer"]
        direction TB
        DF["DataFrame (Spark)"]:::process
        CLEAN[Limpeza e Padronização]:::process
        TYPE[Tipagem Forte]:::process
        VALID[Validação de Schema]:::process
        SL[(Silver Layer)]:::silver

        DF --> CLEAN --> TYPE --> VALID -->|Escrita Delta| SL
    end

    BR -->|Leitura Spark| DF

    %% GOLD
    subgraph GOLD["Modelagem Dimensional - Gold Layer"]
        direction TB

        SPACE[ ]:::spacer
        DIMS[Dimensões]:::process
        FACT[Fatos]:::process
        STAR{{Star Schema}}:::gold
        GL[(Gold Layer)]:::gold

        SPACE --> DIMS
        SPACE --> FACT
        DIMS --> STAR
        FACT --> STAR
        STAR -->|Persistência Final| GL
    end

    SL -->|Leitura Dimensões| DIMS
    SL -->|Leitura Fatos| FACT

    linkStyle default stroke:#546e7a,stroke-width:2px;
```

## 🔐 Segurança
*   **Credenciais**: Nunca hardcoded. Sempre via variáveis de ambiente (`.env` ou Secrets).
*   **Rede**: Acesso via HTTPS (TLS 1.2+).
*   **Logs**: Não logamos dados sensíveis, apenas metadados de execução.
