from pyspark.sql.types import *
from pyspark.sql import functions as F
from utils.utils import read_files_parquet, save_file_parquet


class TransformedTLCData:
    """Classe para transformação de dados de viagens TLC (Taxi & Limousine Commission).
    
    Realiza as seguintes operações:
    - Conversão de tipos de dados
    - Padronização de nomes de colunas
    - Criação de colunas temporais
    - Processamento em lotes por mês
    
    Attributes:
        s3_bucket (str): Nome do bucket S3 para armazenamento
        path_mount_raw (str): Caminho base para dados brutos
        path_mount_transformed (str): Caminho base para dados transformados
    """
    
    def __init__(self):
        """Inicializa a classe com configurações de caminhos padrão."""
        self.s3_bucket = "transformed-tlc-trip-data-case-ifood"
        self.path_mount_raw = "/mnt/mount_raw/"
        self.path_mount_transformed = "/mnt/mount_transformed/"
        
    def convert_types_data(self, df):
        """Converte os tipos de dados das colunas conforme mapeamento.
        
        Args:
            df (pyspark.sql.DataFrame): DataFrame com dados brutos
            
        Returns:
            pyspark.sql.DataFrame: DataFrame com tipos de dados convertidos
            
        Note:
            A conversão é feita apenas para colunas existentes no DataFrame
        """
        type_conversions = {
            "VendorID": "integer",
            "tpep_pickup_datetime": "timestamp",
            "tpep_dropoff_datetime": "timestamp",
            "passenger_count": "bigint",  
            "trip_distance": "double",
            "RatecodeID": "integer",
            "PULocationID": "integer",
            "DOLocationID": "integer",
            "payment_type": "integer",
            "fare_amount": "double",
            "extra": "double",
            "mta_tax": "double",
            "tip_amount": "double",
            "tolls_amount": "double",
            "improvement_surcharge": "double",
            "total_amount": "double",
            "congestion_surcharge": "double",
            "airport_fee": "double"
        }

        for column, dtype in type_conversions.items():
            if column in df.columns:
                df = df.withColumn(column, F.col(column).cast(dtype))
            
        return df
        
    def standardize_column_names(self, df):
        """Padroniza os nomes das colunas para snake_case.
        
        Args:
            df (pyspark.sql.DataFrame): DataFrame com colunas a serem padronizadas
            
        Returns:
            pyspark.sql.DataFrame: DataFrame com nomes de colunas padronizados
            
        Note:
            1. Aplica primeiro um mapeamento específico para colunas conhecidas
            2. Para colunas não mapeadas, aplica transformações genéricas:
               - Lowercase
               - Substitui espaços e hifens por underscores
               - Remove parênteses
        """
        column_mapping = {
            'VendorID': 'vendor_id',
            'tpep_pickup_datetime': 'tpep_pickup_datetime',
            'tpep_dropoff_datetime': 'tpep_dropoff_datetime',
            'passenger_count': 'passenger_count',
            'trip_distance': 'trip_distance',
            'RatecodeID': 'rate_code_id',
            'store_and_fwd_flag': 'store_and_fwd_flag',
            'PULocationID': 'pu_location_id',
            'DOLocationID': 'do_location_id',
            'payment_type': 'payment_type',
            'fare_amount': 'fare_amount',
            'extra': 'extra',
            'mta_tax': 'mta_tax',
            'tip_amount': 'tip_amount',
            'tolls_amount': 'tolls_amount',
            'improvement_surcharge': 'improvement_surcharge',
            'total_amount': 'total_amount',
            'congestion_surcharge': 'congestion_surcharge',
            'airport_fee': 'airport_fee'
        }
        
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

        for col_name in df.columns:
            if col_name not in column_mapping.values():
                new_name = (
                    col_name.lower()
                    .replace(' ', '_')
                    .replace('-', '_')
                    .replace('(', '')
                    .replace(')', '')
                )
                if new_name != col_name:
                    df = df.withColumnRenamed(col_name, new_name)
        
        return df

    def create_columns_date(self, df, column):
        """Cria colunas temporais (year, month, day) a partir de uma coluna de data.
        
        Args:
            df (pyspark.sql.DataFrame): DataFrame de entrada
            column (str): Nome da coluna de data/timestamp
            
        Returns:
            pyspark.sql.DataFrame: DataFrame com as novas colunas temporais
        """
        df = (
            df.withColumn("year", F.year(F.col(column)))
              .withColumn("month", F.month(F.col(column)))
              .withColumn("day", F.dayofmonth(F.col(column)))
        )
        return df
    
    def transformed_data(self, color="yellow", months=5):
        """Processa os dados de viagens de táxi por meses.
        
        Args:
            color (str): Cor da frota de táxi (yellow/green)
            months (int): Número de meses a processar (1-12)
            
        Note:
            - Processa dados do ano 2023
            - Salva dados particionados por ano/mês
            - Aplica todas as transformações da pipeline
        """
        for month in range(1, months + 1):
            path = self.path_mount_raw + f"{color}_taxi/2023/{month}"
            df = read_files_parquet(file_input=path)
            df = self.convert_types_data(df)
            df = self.standardize_column_names(df=df)
            df = self.create_columns_date(df=df, column="tpep_dropoff_datetime")

            df = df.filter(F.col("year") == 2023)
            self.save_file_parquet(
                df, 
                self.path_mount_transformed + f'{color}_taxi/2023/{month}'
            )