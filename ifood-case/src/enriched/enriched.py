from pyspark.sql.types import *
from pyspark.sql import functions as F
from utils.utils import read_files_parquet, save_file_parquet


class EnrichedTLCData:
    """Classe para enriquecimento de dados TLC (Taxi & Limousine Commission).
    
    Realiza transformações adicionais nos dados já processados para criar conjuntos
    enriquecidos com métricas específicas.
    
    Attributes:
        s3_bucket (str): Nome do bucket S3 para dados enriquecidos
        path_mount_transformed (str): Caminho base para dados transformados
        path_mount_enriched (str): Caminho base para dados enriquecidos
    """

    def __init__(self):
        """Inicializa a classe com configurações de caminhos padrão."""
        self.s3_bucket = "enriched-tlc-trip-data-case-ifood"
        self.path_mount_transformed = "/mnt/mount_transformed/"
        self.path_mount_enriched = "/mnt/mount_enriched/"

    def enriched_data(self, color="yellow", months=5, enriched_type="metrics"):
        """Processa e enriquece os dados de viagens de táxi para métricas específicas.
        
        Args:
            color (str): Cor da frota de táxi ('yellow' ou 'green')
            months (int): Número de meses a processar (1-12)
            enriched_type (str): Tipo de enriquecimento a aplicar ('metrics' ou outros)
            
        Processo:
            1. Carrega dados transformados para cada mês
            2. Seleciona colunas relevantes para o enriquecimento
            3. Salva os dados enriquecidos no destino
            
        Note:
            - Atualmente implementa apenas seleção de colunas básicas
            - Pode ser estendido para cálculos de métricas mais complexas
        """
        columns_select = [
            "vendor_id", 
            "passenger_count",
            "total_amount",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime"
        ]

        for month in range(1, months + 1):
            path = self.path_mount_transformed + f"{color}_taxi/2023/*"
            df = read_files_parquet(file_input=path)
            
            df = df.select(*columns_select)
            
            save_file_parquet(
                df, 
                self.path_mount_enriched + f'{color}_taxi/{enriched_type}/taxi_trip'
            )