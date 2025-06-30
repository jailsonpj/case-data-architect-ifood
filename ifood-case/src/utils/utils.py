import logging
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("UtildSpark").getOrCreate()


def read_files_parquet(file_input):
    """Lê arquivos no formato Parquet e retorna um DataFrame Spark.
    
    Args:
        file_input (str): Caminho do(s) arquivo(s) Parquet de entrada.
            Pode ser um caminho local ou em storage distribuído (HDFS, S3, etc.).
            
    Returns:
        pyspark.sql.DataFrame: DataFrame Spark contendo os dados lidos.
        
    Examples:
        >>> df = read_files_parquet("hdfs://path/to/file.parquet")
        >>> df.show()
    """
    try:
        logger.info(f"Iniciando leitura do arquivo Parquet: {file_input}")
        df = spark.read.parquet(file_input)
        logger.info(f"Leitura concluída com sucesso. Total de linhas: {df.count()}")
        return df
    except Exception as e:
        logger.error(f"Erro ao ler arquivo Parquet {file_input}: {str(e)}")
        raise


def save_file_parquet(df, output_path, mode="overwrite"):
    """Salva um DataFrame Spark no formato Parquet com partições por ano e mês.
    
    Args:
        df (pyspark.sql.DataFrame): DataFrame Spark a ser salvo.
        output_path (str): Caminho de destino para os arquivos Parquet.
            Pode ser um caminho local ou em storage distribuído.
            
    Note:
        - Utiliza modo 'overwrite' (sobrescreve dados existentes)
        - Particiona os dados pelas colunas 'year' e 'month'
        - O formato de saída é Parquet
        
    Examples:
        >>> df = spark.createDataFrame([...])
        >>> save_file_parquet(df, "s3://bucket/output/path")
    """
    try:
        logger.info(f"Iniciando salvamento do DataFrame em: {output_path}")
        logger.info(f"Schema do DataFrame: {df.schema.json()}")
        logger.info(f"Contagem de linhas: {df.count()}")
        
        (
            df.write
            .mode(mode)
            .parquet(output_path)
        )
        
        logger.info(f"DataFrame salvo com sucesso em: {output_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar DataFrame em {output_path}: {str(e)}")
        raise