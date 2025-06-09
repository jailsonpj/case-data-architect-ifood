import os
import requests
import boto3
from urllib.parse import urlparse
import tempfile


class ExtractionTLCData:
    """Classe para extração de dados TLC (Taxi & Limousine Commission) e armazenamento no S3.
    
    Realiza as seguintes operações:
    - Download de arquivos Parquet da fonte pública
    - Upload dos arquivos para um bucket S3
    - Organização dos arquivos no S3 por tipo de táxi/ano/mês
    
    Attributes:
        base_url (str): URL base dos dados públicos
        s3_bucket (str): Nome do bucket S3 de destino
        aws_access_key (str): Chave de acesso AWS (remover em produção)
        aws_secret_key (str): Chave secreta AWS (remover em produção)
    """
    
    def __init__(self):
        """Inicializa a classe com configurações de conexão."""
        self.base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        self.s3_bucket = "raw-tlc-trip-data-case-ifood"
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    def extraction_download_tlc_data(self, taxi, year, month):
        """Baixa e armazena no S3 os dados de viagens TLC para um mês específico.
        
        Args:
            taxi (str): Tipo de táxi ('yellow' ou 'green')
            year (int): Ano dos dados
            month (int): Mês dos dados (1-12)
            
        Returns:
            bool: True se o processo foi concluído com sucesso, None em caso de falha
            
        Raises:
            Exception: Captura e registra erros durante download/upload
        """
        try:
            file_name = f"{taxi}_tripdata_{year}-{month:02d}.parquet"
            file_url = f"{self.base_url}/{file_name}"

            response = requests.head(file_url)
            if response.status_code != 200:
                print(f"Arquivo não encontrado: {file_url}")
                return
            
            print(f"Processando: {file_url}")

            with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
                print("Downloading file...")
                with requests.get(file_url, stream=True) as r:
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=8192):
                        tmp_file.write(chunk)
                tmp_file.seek(0)
                
                s3_key = f"{taxi}_taxi/{year}/{month}/{file_name}"
                print(f"Uploading to s3://{self.s3_bucket}/{s3_key}")
                
                s3 = boto3.client(
                    's3',
                    aws_access_key_id=self.aws_access_key,
                    aws_secret_access_key=self.aws_secret_key
                )
                
                s3.upload_fileobj(tmp_file, self.s3_bucket, s3_key)
                
            print("Upload completed successfully")
            return True
        except Exception as e:
            print(f"Erro ao processar {taxi} {year}-{month}: {str(e)}")

    def execute_extraction_process(self, colors=['yellow', 'green'], year=2023, months=12):
        """Executa o processo de extração para múltiplos tipos de táxi e meses.
        
        Args:
            colors (list): Lista de tipos de táxi a processar
            year (int): Ano dos dados a extrair
            months (int): Número de meses a processar (1-12)
            
        Note:
            - Processa os meses sequencialmente para cada tipo de táxi
            - Imprime progresso durante a execução
        """
        dict_color_month = dict()
        for color in colors:
            dict_color_month[color] = range(1, months + 1)

        for color, months_iter in dict_color_month.items():
            for month in months_iter:
                response = self.extraction_download_tlc_data(
                    taxi=color,
                    year=year,
                    month=month
                )
                print(f"Processado: {color} - mês {month}")