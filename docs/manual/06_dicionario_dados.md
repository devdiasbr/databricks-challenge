[🏠 Home](../../README.md) | [Escopo](./00_escopo_e_cronograma.md) | [Visão Geral](./01_visao_geral.md) | [Configuração](./02_configuracao_ambiente.md) | [Execução](./03_execucao_pipeline.md) | [Arquitetura](./04_arquitetura_detalhada.md) | [Troubleshooting](./05_guia_troubleshooting.md) | **Dicionário** | [Sumário](./index.md)

---

# 6. Dicionário de Dados (Camada Gold)

Este documento descreve detalhadamente as tabelas e colunas disponíveis na camada **Refined (Gold)** do Data Lake. Estas tabelas estão modeladas em **Star Schema** e otimizadas para consultas analíticas.

## 📋 Índice das Tabelas

1.  [Fato Balanço Comercial (`ft_balanco_comercial`)](#1-fato-balanço-comercial-ft_balanco_comercial)
2.  [Dimensão Data (`dim_data`)](#2-dimensão-data-dim_data)
3.  [Dimensão NCM (`dim_ncm`)](#3-dimensão-ncm-dim_ncm)
4.  [Dimensão Geografia (`dim_geografia`)](#4-dimensão-geografia-dim_geografia)
5.  [Dimensão Via Transporte (`dim_via_transporte`)](#5-dimensão-via-transporte-dim_via_transporte)
6.  [Dimensão Distribuição Estabelecimentos (`dim_distribuicao_estabelecimentos`)](#6-dimensão-distribuição-estabelecimentos-dim_distribuicao_estabelecimentos)

---

## 1. Fato Balanço Comercial (`ft_balanco_comercial`)

Tabela central que unifica transações de Importação e Exportação.
*   **Granularidade**: Uma linha por NCM, Geografia (País/UF), Via e Mês.
*   **Particionamento**: `tipo_movimentacao` (EXPORTACAO/IMPORTACAO).

| Coluna | Tipo | Chave | Descrição | Exemplo |
| :--- | :--- | :---: | :--- | :--- |
| `sk_ncm` | `BIGINT` | FK | Chave substituta para o produto (NCM). | `1012100` |
| `sk_geografia` | `BIGINT` | FK | Chave substituta para o país parceiro. | `76` |
| `sk_geografia_uf` | `BIGINT` | FK | Chave substituta para a UF de origem/destino. | `8380` |
| `sk_via_transporte` | `BIGINT` | FK | Chave substituta para a via de transporte. | `1` |
| `sk_data` | `BIGINT` | FK | Chave substituta para o período (AAAAMM). | `202401` |
| `tipo_movimentacao` | `STRING` | - | Indica se é 'EXPORTACAO' ou 'IMPORTACAO'. | `EXPORTACAO` |
| `valor_fob` | `DECIMAL(18,2)` | - | Valor da mercadoria em Dólares Americanos (FOB). | `1500.50` |
| `quantidade` | `DECIMAL(18,2)` | - | Quantidade estatística da mercadoria. | `100.00` |
| `kg_liquido` | `DECIMAL(18,2)` | - | Peso líquido da mercadoria em KG. | `120.50` |
| `data_processamento_gold` | `TIMESTAMP` | - | Data e hora do processamento. | `2024-02-09 10:00:00` |

### Exemplo de Uso: Balança Comercial Mensal (Saldo)

Calcula o total exportado, importado e o saldo da balança comercial (Exportações - Importações) agrupado por mês.

```sql
SELECT 
    d.ano,
    d.mes,
    d.nome_mes,
    SUM(CASE WHEN f.tipo_movimentacao = 'EXPORTACAO' THEN f.valor_fob ELSE 0 END) as total_exportacao,
    SUM(CASE WHEN f.tipo_movimentacao = 'IMPORTACAO' THEN f.valor_fob ELSE 0 END) as total_importacao,
    (SUM(CASE WHEN f.tipo_movimentacao = 'EXPORTACAO' THEN f.valor_fob ELSE 0 END) - 
     SUM(CASE WHEN f.tipo_movimentacao = 'IMPORTACAO' THEN f.valor_fob ELSE 0 END)) as saldo_comercial
FROM gold.ft_balanco_comercial f
JOIN gold.dim_data d ON f.sk_data = d.sk_data
WHERE d.ano >= 2024
GROUP BY d.ano, d.mes, d.nome_mes
ORDER BY d.ano DESC, d.mes DESC;
```

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

### Exemplo de Uso: Análise Temporal (Anual e Trimestral)

Agrega os valores de exportação por Ano e Trimestre para identificar tendências sazonais.

```sql
SELECT 
    d.ano,
    d.trimestre,
    SUM(f.valor_fob) as total_exportado
FROM gold.ft_balanco_comercial f
JOIN gold.dim_data d ON f.sk_data = d.sk_data
WHERE f.tipo_movimentacao = 'EXPORTACAO'
GROUP BY d.ano, d.trimestre
ORDER BY d.ano DESC, d.trimestre DESC;
```

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

### Exemplo de Uso: Top 10 Produtos Exportados

Identifica quais produtos geraram maior receita de exportação em um determinado período.

```sql
SELECT 
    n.codigo_ncm,
    n.descricao_ncm,
    n.setor_economico,
    SUM(f.valor_fob) as valor_total_exportado,
    SUM(f.kg_liquido) as peso_liquido_total
FROM gold.ft_balanco_comercial f
JOIN gold.dim_ncm n ON f.sk_ncm = n.sk_ncm
WHERE f.tipo_movimentacao = 'EXPORTACAO'  -- Filtra apenas exportações
  AND f.sk_data BETWEEN 202401 AND 202412 -- Filtro de partição (Rápido)
GROUP BY n.codigo_ncm, n.descricao_ncm, n.setor_economico
ORDER BY valor_total_exportado DESC
LIMIT 10;
```

---

## 4. Dimensão Geografia (`dim_geografia`)

Tabela unificada que contém informações de Países e Unidades Federativas (UFs).

| Coluna | Tipo | Chave | Descrição | Exemplo |
| :--- | :--- | :---: | :--- | :--- |
| `sk_geografia` | `BIGINT` | PK | Chave primária (Código do País ou Hash da UF). | `76` ou `8380` |
| `codigo_referencia` | `STRING` | - | Código original (ISO do país ou Sigla da UF). | `76` ou `SP` |
| `nome_geografia` | `STRING` | - | Nome amigável da localidade. | `Brasil` ou `São Paulo` |
| `nivel` | `STRING` | - | Nível da geografia (PAIS ou UF). | `PAIS` |
| `sigla` | `STRING` | - | Sigla da localidade. | `BRA` ou `SP` |
| `regiao_ou_bloco` | `STRING` | - | Região (para UFs) ou Bloco Econômico (para Países). | `Mercosul` ou `Sudeste` |

### Exemplo de Uso: Análise de Parceiros e Origens

Analisa a movimentação por país parceiro e UF de origem/destino.

```sql
SELECT 
    g.nome_geografia,
    g.nivel,
    SUM(f.valor_fob) as valor_total
FROM gold.ft_balanco_comercial f
JOIN gold.dim_geografia g ON f.sk_geografia = g.sk_geografia
WHERE f.tipo_movimentacao = 'IMPORTACAO'
GROUP BY g.nome_geografia, g.nivel
ORDER BY valor_total DESC;
```

---

## 5. Dimensão Via Transporte (`dim_via_transporte`)

Modal logístico utilizado na operação.

| Coluna | Tipo | Chave | Descrição | Exemplo |
| :--- | :--- | :---: | :--- | :--- |
| `sk_via_transporte` | `BIGINT` | PK | Código da via (mesmo que Siscomex). | `1` |
| `codigo_via` | `INT` | - | Código original da via. | `1` |
| `descricao_via` | `STRING` | - | Descrição do modal (Marítima, Aérea, Rodoviária, etc.). | `MARITIMA` |

### Exemplo de Uso: Movimentação por Via de Transporte

Compara o volume financeiro e físico movimentado por cada modal.

```sql
SELECT 
    v.descricao_via,
    f.tipo_movimentacao,
    SUM(f.valor_fob) as valor_total,
    SUM(f.kg_liquido) as peso_total_kg
FROM gold.ft_balanco_comercial f
JOIN gold.dim_via_transporte v ON f.sk_via_transporte = v.sk_via_transporte
GROUP BY v.descricao_via, f.tipo_movimentacao
ORDER BY valor_total DESC;
```

---

## 6. Dimensão Distribuição Estabelecimentos (`dim_distribuicao_estabelecimentos`)

Tabela analítica agregada que apresenta a distribuição de empresas ativas por CNAE, Porte e UF.

*   **Objetivo**: Suportar análises de densidade empresarial e vocação econômica regional.
*   **Granularidade**: Uma linha por CNAE, Porte e UF.
*   **Particionamento**: `uf`.

| Coluna | Tipo | Chave | Descrição | Exemplo |
| :--- | :--- | :---: | :--- | :--- |
| `cnae_fiscal_principal` | `STRING` | - | Código CNAE da atividade principal. | `6204000` |
| `descricao` | `STRING` | - | Descrição da atividade CNAE. | `Consultoria em tecnologia da informação` |
| `porte_empresa` | `STRING` | - | Porte descritivo da empresa (Microempresa, Pequeno Porte, etc.). | `Empresa de Pequeno Porte` |
| `uf` | `STRING` | - | Unidade Federativa do estabelecimento. | `SP` |
| `Total_empresas` | `BIGINT` | - | Quantidade de estabelecimentos ativos neste grupo. | `150` |
| `dt_atualizacao` | `TIMESTAMP` | - | Data e hora da última atualização do registro. | `2024-02-09 10:00:00` |

### Exemplo de Uso: Vocação Econômica por Estado

Identifica quais são as atividades econômicas predominantes em um estado específico.

```sql
SELECT 
    uf,
    descricao as atividade_economica,
    SUM(Total_empresas) as total_estabelecimentos
FROM gold.dim_distribuicao_estabelecimentos
WHERE uf = 'SP'
GROUP BY uf, descricao
ORDER BY total_estabelecimentos DESC
LIMIT 10;
```
