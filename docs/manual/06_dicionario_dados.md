1→[🏠 Home](../../README.md) | [Escopo](./00_escopo_e_cronograma.md) | [Visão Geral](./01_visao_geral.md) | [Configuração](./02_configuracao_ambiente.md) | [Execução](./03_execucao_pipeline.md) | [Arquitetura](./04_arquitetura_detalhada.md) | [Troubleshooting](./05_guia_troubleshooting.md) | **Dicionário**
2→
3→---
4→
5→# 6. Dicionário de Dados (Camada Gold)
6→
7→Este documento descreve detalhadamente as tabelas e colunas disponíveis na camada **Refined (Gold)** do Data Lake. Estas tabelas estão modeladas em **Star Schema** e otimizadas para consultas analíticas.
8→
9→## 📋 Índice das Tabelas
10→
11→1.  [Fato Balanço Comercial (`ft_balanco_comercial`)](#1-fato-balanço-comercial-ft_balanco_comercial)
12→2.  [Dimensão Data (`dim_data`)](#2-dimensão-data-dim_data)
13→3.  [Dimensão NCM (`dim_ncm`)](#3-dimensão-ncm-dim_ncm)
14→4.  [Dimensão Localidade (`dim_localidade`)](#4-dimensão-localidade-dim_localidade)
15→5.  [Dimensão Via Transporte (`dim_via_transporte`)](#5-dimensão-via-transporte-dim_via_transporte)
16→6.  [Catálogo de Dados e Exemplos de Uso](#6-catálogo-de-dados-e-exemplos-de-uso)
17→
18→---
19→
20→## 1. Fato Balanço Comercial (`ft_balanco_comercial`)
21→
22→Tabela central que unifica transações de Importação e Exportação.
23→*   **Granularidade**: Uma linha por NCM, País/UF, Via e Mês.
24→*   **Particionamento**: `sk_data` (Mês/Ano).
25→
26→| Coluna | Tipo | Chave | Descrição | Exemplo |
27→| :--- | :--- | :---: | :--- | :--- |
28→| `sk_ncm` | `BIGINT` | FK | Chave substituta para o produto (NCM). | `1012100` |
29→| `sk_localidade` | `BIGINT` | FK | Chave substituta para a localidade (País + UF). | `760083` |
30→| `sk_via_transporte` | `BIGINT` | FK | Chave substituta para a via de transporte. | `1` |
31→| `sk_data` | `BIGINT` | FK | Chave substituta para o período (AAAAMM). | `202401` |
32→| `tipo_movimentacao` | `STRING` | - | Indica se é 'IMPORTACAO' ou 'EXPORTACAO'. | `EXPORTACAO` |
33→| `valor_fob` | `DECIMAL(18,2)` | - | Valor da mercadoria em Dólares Americanos (FOB). | `1500.50` |
34→| `quantidade` | `DECIMAL(18,2)` | - | Quantidade estatística da mercadoria. | `100.00` |
35→| `kg_liquido` | `DECIMAL(18,2)` | - | Peso líquido da mercadoria em KG. | `120.50` |
36→| `valor_unitario` | `DECIMAL(18,2)` | - | Cálculo: `valor_fob / quantidade`. | `15.00` |
37→| `preco_kg` | `DECIMAL(18,2)` | - | Cálculo: `valor_fob / kg_liquido`. | `12.45` |
38→| `flag_exportacao` | `INT` | - | Flag binária (1=Sim, 0=Não) para facilitar somas. | `1` |
39→| `flag_importacao` | `INT` | - | Flag binária (1=Sim, 0=Não) para facilitar somas. | `0` |
40→| `dt_atualizacao` | `TIMESTAMP` | - | Data e hora da última atualização do registro. | `2024-02-09 10:00:00` |
41→
42→---
43→
44→## 2. Dimensão Data (`dim_data`)
45→
46→Tabela de calendário para suporte a análises temporais.
47→
48→| Coluna | Tipo | Chave | Descrição | Exemplo |
49→| :--- | :--- | :---: | :--- | :--- |
50→| `sk_data` | `BIGINT` | PK | Chave primária no formato AAAAMM. | `202401` |
51→| `data` | `DATE` | - | Data de referência (primeiro dia do mês). | `2024-01-01` |
52→| `ano` | `INT` | - | Ano com 4 dígitos. | `2024` |
53→| `mes` | `INT` | - | Número do mês (1-12). | `1` |
54→| `nome_mes` | `STRING` | - | Nome do mês em português. | `Janeiro` |
55→| `trimestre` | `INT` | - | Número do trimestre (1-4). | `1` |
56→| `semestre` | `INT` | - | Número do semestre (1-2). | `1` |
57→
58→---
59→
60→## 3. Dimensão NCM (`dim_ncm`)
61→
62→Detalhes sobre os produtos baseados na Nomenclatura Comum do Mercosul (NCM). Enriquecida com dados de CNAE e Setor Econômico.
63→
64→| Coluna | Tipo | Chave | Descrição | Exemplo |
65→| :--- | :--- | :---: | :--- | :--- |
66→| `sk_ncm` | `BIGINT` | PK | Chave primária (numérica do código NCM). | `1012100` |
67→| `codigo_ncm` | `STRING` | - | Código NCM original (com zeros à esquerda se houver). | `01012100` |
68→| `descricao_ncm` | `STRING` | - | Descrição completa do item NCM. | `Cavalos reprodutores de raça pura` |
69→| `cnae` | `STRING` | - | Código da Classificação Nacional de Atividades Econômicas associado. | `0151-2/01` |
70→| `descricao_cnae` | `STRING` | - | Descrição da atividade econômica (CNAE). | `Criação de bovinos para corte` |
71→| `setor_economico` | `STRING` | - | Categorização macro do setor (ex: Agropecuária, Indústria). | `Agropecuária` |
72→
73→---
74→
75→## 4. Dimensão Localidade (`dim_localidade`)
76→
77→Normalização geográfica combinando Países e Unidades Federativas (UFs).
78→*   **Lógica da Chave**: `(codigo_pais * 1000) + ascii(uf)`.
79→
80→| Coluna | Tipo | Chave | Descrição | Exemplo |
81→| :--- | :--- | :---: | :--- | :--- |
82→| `sk_localidade` | `BIGINT` | PK | Chave primária composta. | `760083` |
83→| `pais` | `STRING` | - | Nome do país (destino ou origem). | `Brasil` |
84→| `uf` | `STRING` | - | Sigla da Unidade Federativa (apenas para Brasil, senão 'XX'). | `SP` |
85→| `regiao` | `STRING` | - | Região geográfica do Brasil (Norte, Sul, etc.) ou 'Internacional'. | `Sudeste` |
86→| `bloco_pais` | `STRING` | - | Bloco econômico ao qual o país pertence (ex: Mercosul, UE). | `Mercosul` |
87→
88→---
89→
90→## 5. Dimensão Via Transporte (`dim_via_transporte`)
91→
92→Modal logístico utilizado na operação.
93→
94→| Coluna | Tipo | Chave | Descrição | Exemplo |
95→| :--- | :--- | :---: | :--- | :--- |
96→| `sk_via_transporte` | `BIGINT` | PK | Código da via (mesmo que Siscomex). | `1` |
97→| `codigo_via` | `INT` | - | Código original da via. | `1` |
98→| `descricao_via` | `STRING` | - | Descrição do modal (Marítima, Aérea, Rodoviária, etc.). | `MARITIMA` |
99→
100→---
101→
102→## 6. Catálogo de Dados e Exemplos de Uso
103→
104→Esta seção fornece exemplos práticos de como explorar os dados da camada Gold utilizando SQL. Estes exemplos demonstram o poder do modelo **Star Schema** para responder perguntas de negócio.
105→
106→### 6.1. Balança Comercial Mensal (Saldo)
107→
108→Calcula o total exportado, importado e o saldo da balança comercial (Exportações - Importações) agrupado por mês.
109→
110→```sql
111→SELECT 
112→    d.ano,
113→    d.mes,
114→    d.nome_mes,
115→    -- Soma condicional usando as flags para performance
116→    SUM(CASE WHEN f.flag_exportacao = 1 THEN f.valor_fob ELSE 0 END) as total_exportacao,
117→    SUM(CASE WHEN f.flag_importacao = 1 THEN f.valor_fob ELSE 0 END) as total_importacao,
118→    (SUM(CASE WHEN f.flag_exportacao = 1 THEN f.valor_fob ELSE 0 END) - 
119→     SUM(CASE WHEN f.flag_importacao = 1 THEN f.valor_fob ELSE 0 END)) as saldo_comercial
120→FROM gold.ft_balanco_comercial f
121→JOIN gold.dim_data d ON f.sk_data = d.sk_data
122→WHERE d.ano >= 2024
123→GROUP BY d.ano, d.mes, d.nome_mes
124→ORDER BY d.ano DESC, d.mes DESC;
125→```
126→
127→### 6.2. Top 10 Produtos (NCM) Exportados por Valor
128→
129→Identifica quais produtos geraram maior receita de exportação em um determinado período.
130→
131→```sql
132→SELECT 
133→    n.codigo_ncm,
134→    n.descricao_ncm,
135→    n.setor_economico,
136→    SUM(f.valor_fob) as valor_total_exportado,
137→    SUM(f.kg_liquido) as peso_liquido_total
138→FROM gold.ft_balanco_comercial f
139→JOIN gold.dim_ncm n ON f.sk_ncm = n.sk_ncm
140→WHERE f.flag_exportacao = 1  -- Filtra apenas exportações
141→  AND f.sk_data BETWEEN 202401 AND 202412 -- Filtro de partição (Rápido)
142→GROUP BY n.codigo_ncm, n.descricao_ncm, n.setor_economico
143→ORDER BY valor_total_exportado DESC
144→LIMIT 10;
145→```
146→
147→### 6.3. Análise de Parceiros Comerciais (Importação por País)
148→
149→Analisa de quais países o Brasil mais importa mercadorias.
150→
151→```sql
152→SELECT 
153→    l.pais,
154→    l.bloco_pais,
155→    SUM(f.valor_fob) as valor_total_importado,
156→    AVG(f.valor_unitario) as preco_medio_item
157→FROM gold.ft_balanco_comercial f
158→JOIN gold.dim_localidade l ON f.sk_localidade = l.sk_localidade
159→WHERE f.flag_importacao = 1 -- Apenas Importações
160→GROUP BY l.pais, l.bloco_pais
161→ORDER BY valor_total_importado DESC;
162→```
163→
164→### 6.4. Movimentação por Via de Transporte (Modal Logístico)
165→
166→Compara o volume financeiro e físico movimentado por cada modal (Marítimo, Aéreo, Rodoviário, etc.).
167→
168→```sql
169→SELECT 
170→    v.descricao_via,
171→    f.tipo_movimentacao,
172→    SUM(f.valor_fob) as valor_total,
173→    SUM(f.kg_liquido) as peso_total_kg
174→FROM gold.ft_balanco_comercial f
175→JOIN gold.dim_via_transporte v ON f.sk_via_transporte = v.sk_via_transporte
176→GROUP BY v.descricao_via, f.tipo_movimentacao
177→ORDER BY valor_total DESC;
178→```
179→
180→### 6.5. Evolução Trimestral por Estado (UF)
181→
182→Monitora o desempenho das exportações de um estado específico ao longo dos trimestres.
183→
184→```sql
185→SELECT 
186→    l.uf,
187→    d.ano,
188→    d.trimestre,
189→    SUM(f.valor_fob) as total_exportado
190→FROM gold.ft_balanco_comercial f
191→JOIN gold.dim_localidade l ON f.sk_localidade = l.sk_localidade
192→JOIN gold.dim_data d ON f.sk_data = d.sk_data
193→WHERE l.uf = 'SP'        -- Filtro por Estado (São Paulo)
194→  AND f.flag_exportacao = 1
195→GROUP BY l.uf, d.ano, d.trimestre
196→ORDER BY d.ano, d.trimestre;
197→```
