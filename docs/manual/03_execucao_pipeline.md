[🏠 Home](../../README.md) | [Escopo](./00_escopo_e_cronograma.md) | [Visão Geral](./01_visao_geral.md) | [Configuração](./02_configuracao_ambiente.md) | **Execução** | [Arquitetura](./04_arquitetura_detalhada.md) | [Troubleshooting](./05_guia_troubleshooting.md) | [Dicionário](./06_dicionario_dados.md)

---

# 3. Execução do Pipeline

O pipeline é composto por scripts numerados sequencialmente em `src/scripts/`.

## 📜 Execução do Pipeline

Os scripts do pipeline foram desenhados para execução sequencial, garantindo a correta propagação dos dados entre as camadas (Bronze → Silver → Gold).

### 1. Diagnóstico (Opcional)
Verifique os arquivos disponíveis na origem:
```bash
python src/scripts/01_listagem_arquivos_azure.py
```

### 2. Validação de Setup (Opcional)
Garanta que os containers de destino existem:
```bash
python src/scripts/02_setup_validacao_targets.py
```

### 3. Camada Bronze (Ingestão)
Lê da Landing Zone e salva na Raw (Delta/Parquet).
```bash
python src/scripts/03_ingestao_bronze_balanca.py
python src/scripts/04_ingestao_bronze_cnpj.py
```
> **Nota CNPJ:** O script do CNPJ faz download e extração de ZIPs em streaming. Pode demorar dependendo da sua internet.

### 4. Camada Silver (Transformação)
Lê da Raw, limpa e salva na Trusted (Delta).
```bash
python src/scripts/05_transformacao_silver_balanca.py
python src/scripts/06_transformacao_silver_cnpj.py
```

## 📊 Monitoramento
*   Os scripts exibem barras de progresso (`tqdm`) no terminal.
*   Logs detalhados são salvos localmente na pasta `logs/`.
*   Logs também são enviados para o container `$logs` no Azure Blob Storage.
