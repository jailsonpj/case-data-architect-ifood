import boto3
from botocore.exceptions import ClientError

class AuthenticationS3:
    def __init__(self):
        self.secret_name = "case-ifood"
        self.region_name = "us-east-1"
        self.session = boto3.session.Session()
        self.client = self.session.client(
            service_name='secretsmanager',
            region_name=self.region_name
        )

    def get_secret_value(self):
        try:
            get_secret_value_response = self.client.get_secret_value(
                SecretId=self.secret_name
            )
        except ClientError as e:
            raise e

        secret = get_secret_value_response['SecretString']
        return secret