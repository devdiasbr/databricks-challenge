[Escopo](./00_escopo_e_cronograma.md) | [Visão Geral](./01_visao_geral.md) | [Configuração](./02_configuracao_ambiente.md) | [Execução](./03_execucao_pipeline.md) | [Arquitetura](./04_arquitetura_detalhada.md) | [Troubleshooting](./05_guia_troubleshooting.md) | **Dicionário de Dados**

---

# 6. Dicionário de Dados (Camada Gold)

Este documento descreve detalhadamente as tabelas e colunas disponíveis na camada **Refined (Gold)** do Data Lake. Estas tabelas estão modeladas em **Star Schema** e otimizadas para consultas analíticas.

## 📋 Índice das Tabelas

1.  [Fato Balanço Comercial (`ft_balanco_comercial`)](#1-fato-balanço-comercial-ft_balanco_comercial)
2.  [Dimensão Data (`dim_data`)](#2-dimensão-data-dim_data)
3.  [Dimensão NCM (`dim_ncm`)](#3-dimensão-ncm-dim_ncm)
4.  [Dimensão Localidade (`dim_localidade`)](#4-dimensão-localidade-dim_localidade)
5.  [Dimensão Via Transporte (`dim_via_transporte`)](#5-dimensão-via-transporte-dim_via_transporte)

---

## 1. Fato Balanço Comercial (`ft_balanco_comercial`)

Tabela central que unifica transações de Importação e Exportação.
*   **Granularidade**: Uma linha por NCM, País/UF, Via e Mês.
*   **Particionamento**: `sk_data` (Mês/Ano).

| Coluna | Tipo | Chave | Descrição | Exemplo |
| :--- | :--- | :---: | :--- | :--- |
| `sk_ncm` | `BIGINT` | FK | Chave substituta para o produto (NCM). | `1012100` |
| `sk_localidade` | `BIGINT` | FK | Chave substituta para a localidade (País + UF). | `760083` |
| `sk_via_transporte` | `BIGINT` | FK | Chave substituta para a via de transporte. | `1` |
| `sk_data` | `BIGINT` | FK | Chave substituta para o período (AAAAMM). | `202401` |
| `tipo_movimentacao` | `STRING` | - | Indica se é 'IMPORTACAO' ou 'EXPORTACAO'. | `EXPORTACAO` |
| `valor_fob` | `DECIMAL(18,2)` | - | Valor da mercadoria em Dólares Americanos (FOB). | `1500.50` |
| `quantidade` | `DECIMAL(18,2)` | - | Quantidade estatística da mercadoria. | `100.00` |
| `kg_liquido` | `DECIMAL(18,2)` | - | Peso líquido da mercadoria em KG. | `120.50` |
| `valor_unitario` | `DECIMAL(18,2)` | - | Cálculo: `valor_fob / quantidade`. | `15.00` |
| `preco_kg` | `DECIMAL(18,2)` | - | Cálculo: `valor_fob / kg_liquido`. | `12.45` |
| `flag_exportacao` | `INT` | - | Flag binária (1=Sim, 0=Não) para facilitar somas. | `1` |
| `flag_importacao` | `INT` | - | Flag binária (1=Sim, 0=Não) para facilitar somas. | `0` |
| `dt_atualizacao` | `TIMESTAMP` | - | Data e hora da última atualização do registro. | `2024-02-09 10:00:00` |

---

## 2. Dimensão Data (`dim_data`)

Tabela de calendário para suporte a análises temporais.

| Coluna | Tipo | Chave | Descrição | Exemplo |
| :--- | :--- | :---: | :--- | :--- |
| `sk_data` | `BIGINT` | PK | Chave primária no formato AAAAMM. | `202401` |
| `data` | `DATE` | - | Data de referência (primeiro dia do mês). | `2024-01-01` |
| `ano` | `INT` | - | Ano com 4 dígitos. | `2024` |
| `mes` | `INT` | - | Número do mês (1-12). | `1` |
| `nome_mes` | `STRING` | - | Nome do mês em português. | `Janeiro` |
| `trimestre` | `INT` | - | Número do trimestre (1-4). | `1` |
| `semestre` | `INT` | - | Número do semestre (1-2). | `1` |

---

## 3. Dimensão NCM (`dim_ncm`)

Detalhes sobre os produtos baseados na Nomenclatura Comum do Mercosul (NCM). Enriquecida com dados de CNAE e Setor Econômico.

| Coluna | Tipo | Chave | Descrição | Exemplo |
| :--- | :--- | :---: | :--- | :--- |
| `sk_ncm` | `BIGINT` | PK | Chave primária (numérica do código NCM). | `1012100` |
| `codigo_ncm` | `STRING` | - | Código NCM original (com zeros à esquerda se houver). | `01012100` |
| `descricao_ncm` | `STRING` | - | Descrição completa do item NCM. | `Cavalos reprodutores de raça pura` |
| `cnae` | `STRING` | - | Código da Classificação Nacional de Atividades Econômicas associado. | `0151-2/01` |
| `descricao_cnae` | `STRING` | - | Descrição da atividade econômica (CNAE). | `Criação de bovinos para corte` |
| `setor_economico` | `STRING` | - | Categorização macro do setor (ex: Agropecuária, Indústria). | `Agropecuária` |

---

## 4. Dimensão Localidade (`dim_localidade`)

Normalização geográfica combinando Países e Unidades Federativas (UFs).
*   **Lógica da Chave**: `(codigo_pais * 1000) + ascii(uf)`.

| Coluna | Tipo | Chave | Descrição | Exemplo |
| :--- | :--- | :---: | :--- | :--- |
| `sk_localidade` | `BIGINT` | PK | Chave primária composta. | `760083` |
| `pais` | `STRING` | - | Nome do país (destino ou origem). | `Brasil` |
| `uf` | `STRING` | - | Sigla da Unidade Federativa (apenas para Brasil, senão 'XX'). | `SP` |
| `regiao` | `STRING` | - | Região geográfica do Brasil (Norte, Sul, etc.) ou 'Internacional'. | `Sudeste` |
| `bloco_pais` | `STRING` | - | Bloco econômico ao qual o país pertence (ex: Mercosul, UE). | `Mercosul` |

---

## 5. Dimensão Via Transporte (`dim_via_transporte`)

Modal logístico utilizado na operação.

| Coluna | Tipo | Chave | Descrição | Exemplo |
| :--- | :--- | :---: | :--- | :--- |
| `sk_via_transporte` | `BIGINT` | PK | Código da via (mesmo que Siscomex). | `1` |
| `codigo_via` | `INT` | - | Código original da via. | `1` |
| `descricao_via` | `STRING` | - | Descrição do modal (Marítima, Aérea, Rodoviária, etc.). | `MARITIMA` |
