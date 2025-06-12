from pyspark.sql.types import *
from pyspark.sql import functions as F
from utils.utils import read_files_parquet, save_file_parquet
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
        logger.info("EnrichedTLCData inicializado. Bucket: %s", self.s3_bucket)
        logger.debug("Caminho transformado: %s", self.path_mount_transformed)
        logger.debug("Caminho enriquecido: %s", self.path_mount_enriched)

    def enriched_data(self, color="yellow", months=5, enriched_type="metrics"):
        """Processa e enriquece os dados de viagens de táxi para métricas específicas.
        
        Args:
            color (str): Cor da frota de táxi ('yellow' ou 'green')
            months (int): Número de meses a processar (1-12)
            enriched_type (str): Tipo de enriquecimento a aplicar ('metrics' ou outros)
        """
        logger.info("Iniciando enriquecimento de dados. Tipo: %s, Cor: %s, Meses: %d", 
                   enriched_type, color, months)
        
        columns_select_yellow = [
            "vendor_id", 
            "passenger_count",
            "total_amount",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime"
        ]

        columns_select_green = [
            "vendor_id", 
            "passenger_count",
            "total_amount",
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime"
        ]

        columns_select = columns_select_green if color == "green" else columns_select_yellow
        logger.debug("Colunas selecionadas para enriquecimento: %s", columns_select)

        for month in range(1, months + 1):
            try:
                path = self.path_mount_transformed + f"{color}_taxi/2023/*"
                logger.info("Processando mês %d. Lendo dados de: %s", month, path)
                
                df = read_files_parquet(file_input=path)
                logger.info("Dados lidos com sucesso. %d registros encontrados", df.count())
                
                df = df.select(*columns_select)
                logger.debug("Colunas selecionadas aplicadas. Schema atual: %s", df.schema)
                
                output_path = self.path_mount_enriched + f'{color}_taxi/{enriched_type}/taxi_trip'
                save_file_parquet(df, output_path)
                logger.info("Dados enriquecidos salvos com sucesso em: %s", output_path)
                
            except Exception as e:
                logger.error("Erro ao processar mês %d: %s", month, str(e), exc_info=True)
                raise