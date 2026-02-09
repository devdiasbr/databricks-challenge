[Escopo](./00_escopo_e_cronograma.md) | [Visão Geral](./01_visao_geral.md) | [Configuração](./02_configuracao_ambiente.md) | [Execução](./03_execucao_pipeline.md) | [Arquitetura](./04_arquitetura_detalhada.md) | **Troubleshooting**

---

# 5. Guia de Troubleshooting

## 🚨 Erros Comuns

### 1. `DirectoryIsNotEmpty` ou `DELTA_PATH_EXISTS`
*   **Sintoma**: O script falha ao tentar salvar dados na camada Silver.
*   **Causa**: O diretório de destino contém arquivos de uma execução anterior (ex: `_started`, arquivos parquet antigos) e o método de escrita não está lidando com a limpeza.
*   **Solução**: O código já foi corrigido para usar `format("delta").mode("overwrite")`. Se persistir, verifique se não há processos concorrentes escrevendo no mesmo local. **Não apague pastas manualmente no Blob Storage a menos que seja o último recurso.**

### 2. `NameError: name '__file__' is not defined`
*   **Sintoma**: Erro ao rodar scripts `.py` dentro de um Notebook Databricks.
*   **Causa**: Notebooks não possuem a variável `__file__` padrão do Python.
*   **Solução**: O código usa um bloco `try-except` para detectar isso:
    ```python
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        base_dir = os.getcwd()
    ```

### 3. `Py4JJavaError: ... Winutils ...`
*   **Sintoma**: Erro ao iniciar o Spark no Windows reclamando de `winutils.exe` ou `hadoop.dll`.
*   **Solução**:
    *   Verifique se a pasta `hadoop/bin` foi criada na raiz do projeto.
    *   Verifique se o `winutils.exe` está dentro dela.
    *   O script tenta baixar automaticamente. Se sua rede bloquear, baixe manualmente do repositório `cdarlint/winutils` e coloque na pasta.

### 4. `403 Forbidden` (Azure Blob)
*   **Sintoma**: Falha de autenticação ao acessar o Storage.
*   **Solução**:
    *   Verifique se o SAS Token no `.env` está correto.
    *   Tokens expiram! Gere um novo token no portal do Azure.
    *   Garanta que o token começa com `?` (ex: `?sv=...`).

### 5. `AnalysisException: ... mismatch ...`
*   **Sintoma**: Erro na camada Silver reclamando de colunas.
*   **Solução**:
    *   O schema dos dados na Bronze mudou?
    *   Verifique os arquivos JSON em `docs/schemas/`.
    *   Se necessário, atualize o JSON para refletir as novas colunas ou ajuste o script de transformação.
