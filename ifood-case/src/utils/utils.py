from pyspark.sql import SparkSession

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
    df = spark.read.parquet(file_input)
    return df


def save_file_parquet(df, output_path):
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
    (
        df.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(output_path)
    )