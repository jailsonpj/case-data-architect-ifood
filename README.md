# case-data-architect-ifood

## Visão Geral
Este repositório contém um projeto de arquitetura de dados para um case do iFood, organizado em uma estrutura clara que segue boas práticas de engenharia de dados. O projeto utiliza PySpark para processamento de dados e está estruturado em camadas distintas.

## Estrutura de Arquivos

### 1. Diretório `src/`
Contém todo o código fonte do projeto, organizado em subdiretórios por funcionalidade.

#### 1.1 `extraction/`
- **extraction.py**: Script principal para extração de dados
  - Implementa a classe `ExtractionTLCData` para download de dados públicos de táxi NYC
  - Conecta com S3 para armazenar os dados brutos
  - Possui métodos para:
    - Download de arquivos Parquet
    - Upload para bucket S3
    - Processamento em lotes por mês

#### 1.2 `transformation/`
- **transformation.py**: Processamento dos dados brutos
  - Classe `TransformedTLCData` realiza:
    - Conversão de tipos de dados
    - Padronização de nomes de colunas (para snake_case)
    - Criação de colunas temporais (year, month, day)
    - Filtragem por ano (2023)
  - Salva dados particionados por ano/mês

#### 1.3 `enriched/`
- **enriched.py**: Cria camada enriquecida
  - Classe `EnrichedTLCData`:
    - Seleciona colunas específicas para análise
    - Prepara dados para cálculo de métricas
    - Organiza em estrutura final

#### 1.4 `utils/`
- **utils.py**: Funções utilitárias compartilhadas
  - Contém helpers para:
    - Leitura de arquivos Parquet (`read_files_parquet`)
    - Escrita de arquivos Parquet (`save_file_parquet`)
  - Centraliza operações comuns de I/O
---
## 2. Estrutura do Data Lake

![Arquitetura Data Lake](./ifood-case/src/images/arquitetura_datalake_teste_ifood.png)

- RAW (S3 Bucket):
    - Dados brutos originais da TLC 
    - Formato preservado como recebido
    - Particionamento por ano/mês/tipo

- TRANSFORMED (S3 Bucket):
    - Dados processados com Databricks
    - Schema validado e padronizado
    - Limpeza e enriquecimento inicial

- ENRICHED (S3 Bucket):
    - Dados prontos para análise
    - Agregações e métricas calculadas
    - Otimizados para consulta SQL

---

## 3. Fluxo de Processamento

![Fluxo Pipeline de Dados](./ifood-case/src/images/fluxo_pipeline_ifood.excalidraw.png)

O fluxo de execução da pipeline de dados é linear e segue três etapas sequenciais, representadas da esquerda para a direita:

1. **EXTRAÇÃO**:  
   - **Objetivo**: Coletar dados brutos de fontes diversas (bancos de dados, APIs, logs, etc.).  
   - **Saída**: Dados são armazenados na camada **RAW**, sem modificações, preservando a forma original.  

2. **TRANSFORMAÇÃO**:  
   - **Objetivo**: Processar os dados brutos (limpeza, filtragem, estruturação, normalização).  
   - **Saída**: Dados são movidos para a camada **TRANSFORMED**.  

3. **ENRIQUECIMENTO**:  
   - **Objetivo**: Adicionar valor aos dados (agregações, cálculos, integração com outras fontes).  
   - **Saída**: Dados são armazenados na camada **ENRICHED** (enriquecidos), prontos para consumo final (relatórios, ML, etc.).  

### **Características do Fluxo**:  
- **Sequencial**: Cada etapa depende da saída da anterior.  
- **Camadas de Armazenamento**: Cada fase tem uma camada dedicada (RAW → TRANSFORMED → ENRICHED).  
- **Contexto**: Executado em um **Data Lake**, que armazena os dados em todas as etapas.

### **Códigos da Pipeline**
1. **Extração**: `extraction.py` baixa dados de táxi NYC
2. **Transformação**: `transformation.py` limpa e estrutura os dados
3. **Enriquecimento**: `enriched.py` prepara para análise
4. **Utils**: Funções compartilhadas suportam todos os passos

---

## 4. Sugestões de Melhoria
1. Adicionar tratamento de erros mais robusto
2. Implementar logging consistente
3. Adicionar testes unitários
4. Documentar requisitos/dependências
5. Configurar CI/CD para execução automatizada

## 5. Como Executar
1. Instalar dependências: `pip install pyspark boto3 requests`
2. Configurar credenciais AWS (como variáveis de ambiente)
3. Executar scripts na ordem:
   ```bash
   python src/extraction/extraction.py
   python src/transformation/transformation.py
   python src/enriched/enriched.py
   ```
