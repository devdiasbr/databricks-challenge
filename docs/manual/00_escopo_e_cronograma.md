[🏠 Home](../../README.md) | **Escopo** | [Visão Geral](./01_visao_geral.md) | [Configuração](./02_configuracao_ambiente.md) | [Execução](./03_execucao_pipeline.md) | [Arquitetura](./04_arquitetura_detalhada.md) | [Troubleshooting](./05_guia_troubleshooting.md) | [Dicionário](./06_dicionario_dados.md)

---

# Projeto Integrado - Escopo e Cronograma

Este documento detalha o escopo, requisitos e o cronograma para o desenvolvimento do Projeto Integrado.

## 1. Escopo e Requisitos

O projeto visa a criação de uma solução de análise de comércio exterior com os seguintes requisitos:

### Funcionais e de Dados
- **Base de Dados Integrada:** Criação de uma base de dados robusta e atualizável mensalmente.
- **Dashboards:** Desenvolvimento de dashboards executivos e analíticos (mínimo de 4 visões/páginas) para exploração de dados de comércio exterior.
- **Capacidade Analítica:** Análise detalhada por:
  - Produto
  - Parceiro
  - Atividade econômica (quando viável).
- **Apoio Estratégico:** Suporte a estudos de mercado, inteligência econômica e estratégias de negócio.
- **Evolução Analítica:** Base para futuras implementações de IA, forecast e detecção de anomalias.

### Técnicos e Governança
- **Performance:** Uso de agregações pré-calculadas (materialized views) e partições por competência.
- **Governança:** 
  - Catálogo de dados.
  - Glossário de KPIs (Export, Import, Saldo, Corrente, YoY/MoM).
- **Segurança:** Segregação por ambientes e mascaramento de dados sensíveis (se aplicável).
- **Reprodutibilidade:** Versionamento de tabelas auxiliares e mapeamentos (ex: NCM ↔ CNAE).

## 2. Cronograma

O projeto seguirá o seguinte calendário de entregas e acompanhamentos:

| Data | Atividade | Descrição |
| :--- | :--- | :--- |
| **30/01** | **Definição e Início** | Divulgação dos grupos, compartilhamento de arquivos, definição de escopo e plano macro. Apresentação e formalização. |
| **03/02** | **Acompanhamento I** | Status report por grupo. |
| **06/02** | **Acompanhamento II** | Status report por grupo. |
| **10/02** | **Acompanhamento III** | Status report por grupo. |
| **11/02** | **Apresentação Final** | Apresentação do projeto integrado concluído. |

---
*Documento gerado em 30/01/2026.*
