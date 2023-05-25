import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

# Load the environment variables
load_dotenv(".env")

AWS_SERVER_PUBLIC_KEY = os.getenv("AWS_SERVER_PUBLIC_KEY")
AWS_SERVER_SECRET_KEY = os.getenv("AWS_SERVER_SECRET_KEY")
REGION_NAME = os.getenv("REGION_NAME")

class OperacaoS3:

    def __init__(self):
        session = boto3.Session(
            aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
            aws_secret_access_key=AWS_SERVER_SECRET_KEY,
            region_name=REGION_NAME
        )

        self.s3_client = session.client('s3', region_name='us-east-2')
        self.s3 = session.resource('s3')

    def create_bucket(self,bucket_name):
        try:
            location = {'LocationConstraint': 'us-east-2'}
            self.s3_client.create_bucket(Bucket=bucket_name,CreateBucketConfiguration=location)
        except ClientError as e:
            raise Exception(e)
        else:
            return True

    def upload_object(self, file_name, bucket, object_name=None):
        if object_name is None:
            object_name = file_name

        try:
            response = self.s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            raise Exception(e)
        else:
            return True

    def get_url_temp(self, bucket:str, object:str):

        try:
            url = OperacaoS3().s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': object},
                ExpiresIn=604800)
        except ClientError as e:
            raise Exception(e)
        else:
            return url

    def listar_bucket(self):
        try:
            response = self.s3_client.list_buckets()
            list_buckets = []
            print('Existing buckets:')
            for bucket in response['Buckets']:
                list_buckets.append(bucket["Name"])
        except ClientError as e:
            raise Exception(e)
        else:
            return list_buckets

    def list_objects_bucket(self,bucket_name):
        try:
            response = self.s3_client.list_objects(Bucket=bucket_name)
            list_obj = []
            try:
                response['Contents']
            except:
                pass
            else:
                for object in response['Contents']:
                    list_obj.append(object['Key'])
        except ClientError as e:
            raise Exception(e)
        else:
            return list_obj

    def download_object(self, bucket_name, object_name, file_name):
        try:
            self.s3_client.download_file(bucket_name, object_name, file_name)
        except ClientError as e:
            raise Exception(e)
        else:
            return True

#t= OperacaoS3()
#p = t.list_objects_bucket('bb-movimentacoes')
#t.create_bucket('bb-movimentacoes')
#t.upload_object(file_name='./movimentacoes_22_05_2023 04-00.zip',bucket='bb-movimentacoes',object_name='22_05_2023/movimentacoes_22_05_2023 04-00.zip')
#t.upload_object(file_name='./movimentacoes_22_05_2023 05-00.zip',bucket='bb-movimentacoes',object_name='22_05_2023/movimentacoes_22_05_2023 05-00.zip')
#t.upload_object(file_name='./movimentacoes_22_05_2023 06-00.zip',bucket='bb-movimentacoes',object_name='22_05_2023/movimentacoes_22_05_2023 06-00.zip')
#t.upload_object(file_name='./Downloads.zip',bucket='bb-movimentacoes',object_name='Downloads.zip')
#t.upload_object(file_name='./Downloads.zip',bucket='bb-movimentacoes',object_name='Downloads.zip')
#t.download_object('bb-movimentacoes','requirements.txt','requirements.txt')
#print(open('requirements.txt').read())
#obj = t.s3.Object('bb-movimentacoes', 'Downloads.zip')
#print(obj)

'''BUCKET = 'bb-movimentacoes'
OBJECT = '22_05_2023/movimentacoes-22-05-2023-04-00.zip'

url = OperacaoS3().s3_client.generate_presigned_url(
    'get_object',
    Params={'Bucket': BUCKET, 'Key': OBJECT},
    ExpiresIn=604800)

print(url)'''