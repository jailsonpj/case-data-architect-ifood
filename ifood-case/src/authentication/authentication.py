import boto3
import logging
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AuthenticationS3:
    """Classe para autenticação e obtenção de segredos do AWS Secrets Manager.
    
    Esta classe fornece métodos para conectar ao AWS Secrets Manager e recuperar
    valores de segredos armazenados.
    """
    
    def __init__(self):
        """Inicializa a classe AuthenticationS3 com configurações padrão.
        
        Configura:
        - secret_name: Nome do segredo a ser recuperado (default: "case-ifood")
        - region_name: Região AWS (default: "us-east-1")
        - session: Sessão boto3
        - client: Cliente do Secrets Manager
        """
        self.secret_name = "case-ifood"
        self.region_name = "us-east-1"
        self.session = boto3.session.Session()
        self.client = self.session.client(
            service_name='secretsmanager',
            region_name=self.region_name
        )
        logger.info("AuthenticationS3 inicializado para o segredo '%s' na região '%s'", 
                   self.secret_name, self.region_name)

    def get_secret_value(self):
        """Recupera o valor do segredo do AWS Secrets Manager.
        
        Returns:
            str: O valor do segredo como string.
            
        Raises:
            ClientError: Se ocorrer um erro ao acessar o Secrets Manager.
            
        Examples:
            >>> auth = AuthenticationS3()
            >>> secret = auth.get_secret_value()
        """
        try:
            logger.info("Tentando obter o valor do segredo '%s'", self.secret_name)
            get_secret_value_response = self.client.get_secret_value(
                SecretId=self.secret_name
            )
            logger.info("Segredo '%s' obtido com sucesso", self.secret_name)
            
        except ClientError as e:
            logger.error("Erro ao obter segredo '%s': %s", self.secret_name, str(e))
            raise e

        secret = get_secret_value_response['SecretString']
        return secret