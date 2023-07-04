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

        self.s3 = boto3.client(
            service_name="s3",
            region_name="us-east-2",
            aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
            aws_secret_access_key=AWS_SERVER_SECRET_KEY,
        )

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
            self.s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            raise Exception(e)
        else:
            return True

    def upload_file_obj(self, file_name, bucket, object_name=None):
        if object_name is None:
            object_name = file_name

        try:
            self.s3.upload_fileobj(file_name, bucket, object_name)
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

    def list_nome_arquivos_pasta(self,bucket_name, nome_pasta):
        try:
            list_name_file = dict()
            result = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=nome_pasta)
            for item in result['Contents']:
                files = item['Key'].split("/")[1]
                if len(files.replace(' ','')) > 0:
                    print(files)
                    list_name_file[files] = item['Key']

        except ClientError as e:
            raise Exception(e)
        else:
            return list_name_file

    def download_object(self, bucket_name, object_name, file_name):
        try:
            self.s3_client.download_file(bucket_name, object_name, file_name)
        except ClientError as e:
            raise Exception(e)
        else:
            return True
