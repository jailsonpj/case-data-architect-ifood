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

### 2. Estrutura do Data Lake
- `raw/`: Dados brutos da fonte
- `transformed/`: Dados processados
- `enriched/`: Dados prontos para análise

### 3. Arquivos de Configuração
- **.gitignore**: Lista de arquivos a serem ignorados pelo Git
  - Inclui padrões para:
    - Arquivos de IDE (.vscode, .idea)
    - Cache Python (__pycache__)
    - Arquivos temporários
    - Credenciais/arquivos sensíveis

## Fluxo de Processamento
1. **Extração**: `extraction.py` baixa dados de táxi NYC
2. **Transformação**: `transformation.py` limpa e estrutura os dados
3. **Enriquecimento**: `enriched.py` prepara para análise
4. **Utils**: Funções compartilhadas suportam todos os passos

## Boas Práticas Implementadas
1. Separação clara de responsabilidades
2. Padronização de nomes (snake_case)
3. Particionamento de dados por ano/mês
4. Tratamento de tipos de dados
5. Documentação de classes e métodos

## Sugestões de Melhoria
1. Adicionar tratamento de erros mais robusto
2. Implementar logging consistente
3. Adicionar testes unitários
4. Documentar requisitos/dependências
5. Configurar CI/CD para execução automatizada

## Como Executar
1. Instalar dependências: `pip install pyspark boto3 requests`
2. Configurar credenciais AWS (como variáveis de ambiente)
3. Executar scripts na ordem:
   ```bash
   python src/extraction/extraction.py
   python src/transformation/transformation.py
   python src/enriched/enriched.py
   ```
