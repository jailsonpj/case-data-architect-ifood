import os
import requests
import boto3
import logging
from urllib.parse import urlparse
import tempfile
from datetime import datetime
from authentication.authentication import AuthenticationS3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extracao_dados_tlc.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class ExtractionTLCData:
    """Classe para extração de dados TLC (Taxi & Limousine Commission) e armazenamento no S3."""
    
    def __init__(self):
        """Inicializa a classe com configurações de conexão."""
        self.base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        self.s3_bucket = "raw-tlc-trip-data-case-ifood"
        self.dict_credentials = eval(AuthenticationS3().get_secret_value())
        self.aws_access_key = self.dict_credentials["user-dl-case-ifood"]
        self.aws_secret_key = self.dict_credentials["secret-dl-case-ifood"]
        logger.info("Classe inicializada com bucket S3: %s", self.s3_bucket)

    def extraction_download_tlc_data(self, taxi, year, month):
        """Baixa e armazena no S3 os dados de viagens TLC para um mês específico.
        
        Args:
            taxi (str): Tipo de táxi ('yellow' ou 'green')
            year (int): Ano dos dados
            month (int): Mês dos dados (1-12)
        """
        tempo_inicio = datetime.now()
        nome_arquivo = f"{taxi}_tripdata_{year}-{month:02d}.parquet"
        url_arquivo = f"{self.base_url}/{nome_arquivo}"
        
        logger.info("Iniciando processamento para táxi %s, %s-%s", taxi, year, month)
        logger.debug("URL do arquivo: %s", url_arquivo)

        try:
            logger.debug("Verificando disponibilidade do arquivo")
            resposta = requests.head(url_arquivo)
            if resposta.status_code != 200:
                logger.warning("Arquivo não encontrado (HTTP %s): %s", resposta.status_code, url_arquivo)
                return None
            
            logger.info("Arquivo encontrado, iniciando download")

            with tempfile.NamedTemporaryFile(delete=True) as arquivo_tmp:
                logger.debug("Arquivo temporário criado: %s", arquivo_tmp.name)
                
                logger.info("Baixando arquivo de %s", url_arquivo)
                inicio_download = datetime.now()
                with requests.get(url_arquivo, stream=True) as r:
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=8192):
                        arquivo_tmp.write(chunk)
                duracao_download = (datetime.now() - inicio_download).total_seconds()
                logger.info("Download concluído em %.2f segundos", duracao_download)
                
                arquivo_tmp.seek(0)
                tamanho_arquivo = os.path.getsize(arquivo_tmp.name) / (1024 * 1024)
                logger.debug("Tamanho do arquivo: %.2f MB", tamanho_arquivo)
                
                chave_s3 = f"{taxi}_taxi/{year}/{month}/{nome_arquivo}"
                logger.debug("Preparando upload para s3://%s/%s", self.s3_bucket, chave_s3)
                
                cliente_s3 = boto3.client(
                    's3',
                    aws_access_key_id=self.aws_access_key,
                    aws_secret_access_key=self.aws_secret_key
                )
                
                inicio_upload = datetime.now()
                logger.info("Iniciando upload para S3")
                cliente_s3.upload_fileobj(arquivo_tmp, self.s3_bucket, chave_s3)
                duracao_upload = (datetime.now() - inicio_upload).total_seconds()
                logger.info("Upload para S3 concluído em %.2f segundos", duracao_upload)
                
            duracao_total = (datetime.now() - tempo_inicio).total_seconds()
            logger.info("Processamento concluído com sucesso para %s em %.2f segundos", nome_arquivo, duracao_total)
            return True
            
        except requests.exceptions.RequestException as erro:
            logger.error("Falha na requisição para %s: %s", url_arquivo, str(erro))
        except boto3.exceptions.S3UploadFailedError as erro_s3:
            logger.error("Falha no upload para S3: %s", str(erro_s3))
        except Exception as erro:
            logger.error("Erro inesperado ao processar %s %s-%s: %s", 
                       taxi, year, month, str(erro), exc_info=True)
        return None

    def execute_extraction_process(self, colors=['yellow', 'green'], year=2023, months=12):
        """Executa o processo de extração para múltiplos tipos de táxi e meses."""
        logger.info("Iniciando processo de extração para ano %s, meses 1-%s", year, months)
        logger.debug("Tipos de táxi a processar: %s", colors)
        
        dict_color_month = dict()
        for color in colors:
            dict_color_month[color] = range(1, months + 1)

        total_arquivos = len(colors) * months
        arquivos_processados = 0
        arquivos_falhas = 0

        for color, months_iter in dict_color_month.items():
            logger.info("Processando dados do táxi %s", color)
            for month in months_iter:
                try:
                    resultado = self.extraction_download_tlc_data(
                        taxi=color,
                        year=year,
                        month=month
                    )
                    if resultado:
                        arquivos_processados += 1
                        logger.info("Processado com sucesso: %s - mês %s", color, month)
                    else:
                        arquivos_falhas += 1
                        logger.warning("Falha ao processar: %s - mês %s", color, month)
                except Exception as erro:
                    arquivos_falhas += 1
                    logger.error("Erro crítico ao processar %s mês %s: %s", 
                               color, month, str(erro), exc_info=True)

        logger.info("Processo de extração concluído. Sucessos: %s/%s, Falhas: %s/%s",
            arquivos_processados, total_arquivos, arquivos_falhas, total_arquivos)