import os
import boto3
import awswrangler as wr
from boto3.dynamodb.conditions import Attr
from dotenv import load_dotenv
import pandas as pd
import datetime
from botocore.exceptions import ClientError
from inserir_colunas_complementares_df import InserirColunasComplementaresDF

# Load the environment variables
load_dotenv(".env")

AWS_SERVER_PUBLIC_KEY = os.getenv("AWS_SERVER_PUBLIC_KEY")
AWS_SERVER_SECRET_KEY = os.getenv("AWS_SERVER_SECRET_KEY")
REGION_NAME = os.getenv("REGION_NAME")

class OperacoesDynamoDB(InserirColunasComplementaresDF):

    def __init__(self):

        self.session = boto3.Session(
            aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
            aws_secret_access_key=AWS_SERVER_SECRET_KEY,
            region_name=REGION_NAME
        )
        self.dyn_resources = self.session.resource('dynamodb', region_name=REGION_NAME)


    def create_table_chave_composta(self, table_name:str, chave_particao:str="datasolicitacao", chave_classificaco:str="nprocesso_sistema_tribunal_instancia"):
        '''creates DynanmoDB table in AWS'''

        schema = {
            "AttributeDefinitions": [
                {
                    "AttributeName": chave_particao,
                    "AttributeType": "S",
                },
                {
                    "AttributeName": chave_classificaco,
                    "AttributeType": "S"
                },
            ],
            "KeySchema": [
                {"AttributeName": chave_particao, "KeyType": "HASH"},
                {"AttributeName": chave_classificaco, "KeyType": "RANGE"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "TableName": table_name
        }

        table = self.dyn_resources.create_table(**schema)
        print(f'Creating table {table_name}')
        table.wait_until_exists()
        print('Table created')
        return table


    def create_table_chave_simples(self, table_name:str, chave_particao:str):
        '''creates DynanmoDB table in AWS'''

        schema = {
            "AttributeDefinitions": [
                {
                    "AttributeName": chave_particao,
                    "AttributeType": "S",
                },
            ],
            "KeySchema": [
                {"AttributeName": chave_particao, "KeyType": "HASH"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "TableName": table_name
        }

        table = self.dyn_resources.create_table(**schema)
        print(f'Creating table {table_name}')
        table.wait_until_exists()
        print('Table created')
        return table

    def put_table(self,table_name:str,players_df):

        try:
            wr.dynamodb.put_df(
                players_df,
                table_name=table_name,
                boto3_session=self.session
            )
        except ClientError as e:
            raise Exception(f'Erro ocorreu ao fazer insert na tabela. Detalhe: {e}')

    def get_table_entre_datas(self,table_name:str,data_inicio:str,data_fim:str):

        try:
            df = wr.dynamodb.read_partiql_query(
                query=f"SELECT * FROM {table_name} WHERE datasolicitacao BETWEEN ? and ?",
                parameters=[data_inicio, data_fim],
                boto3_session=self.session
            )
        except ClientError as e:
            raise Exception(f'Erro ocorreu ao fazer select na tabela. Detalhe: {e}')
        else:
            return df

    def put_senha_login_bb_movimentacoes(self, status:str, data_hora:str, nome_responsavel:str, login:str, senha:str, data_hora_cadastro:str):

        try:
            table = self.dyn_resources.Table('SOPII_senhas_bb_movimentacoes')
            item={
                'status': status,
                'data_hora': data_hora,
                'nome_responsavel': nome_responsavel,
                'login': login,
                'senha': senha,
                'data_hora_cadastro': data_hora_cadastro
            }

            table.put_item(Item=item)
        except ClientError as e:
            raise Exception(f'Erro ocorreu ao fazer inserir na tabela. Detalhe: {e}')

    def put_registro(self, table_name:str, item:dict):

        try:
            table = self.dyn_resources.Table(table_name)
            table.put_item(Item=item)
        except ClientError as e:
            raise Exception(f'Erro ocorreu ao fazer inserir na tabela. Detalhe: {e}')

    def update_senha_login_bb_movimentacoes(self, status:str, data_hora:str, novo_nome_responsavel:str, novo_login:str, nova_senha:str, nova_data_hora_cadastro:str):

        try:
            table = self.dyn_resources.Table('SOPII_senhas_bb_movimentacoes')

            response = table.update_item(
                Key={
                     'status': str(status),
                     'data_hora': str(data_hora)
                },
                UpdateExpression='SET login = :login, #nome = :nome, senha = :senha, #data = :data',
                ExpressionAttributeNames={
                    '#nome': 'nome_responsavel',
                    '#data': 'data_hora_cadastro'
                },
                ExpressionAttributeValues={
                    ':login': novo_login,
                    ':nome': novo_nome_responsavel,
                    ':senha': nova_senha,
                    ':data': nova_data_hora_cadastro,
                }
            )

        except ClientError as e:
            raise Exception(f'Erro ocorreu ao atualizar a tabela. Detalhe: {e}')
        else:
            return response

    def get_senha_login_bb_movimentacoes(self, status:str, data_hora:str):

        try:
            table = self.dyn_resources.Table('SOPII_senhas_bb_movimentacoes')

            response = table.get_item(Key={'status': status, 'data_hora': data_hora})

        except ClientError as e:
            raise Exception(f'Erro ocorreu ao atualizar a tabela. Detalhe: {e}')
        else:
            return response['Item']


    def put_table_lista_processos(self, table_name_lista_processos:str,table_name_endereco_site:str,df_lista_pesquisa:pd.DataFrame):

        df_lista_endereco_site = self.get_table(table_name_endereco_site)
        data_atual = datetime.date.today().strftime('%Y/%m/%d')
        df_lista_pesquisa_atual = self.get_table_entre_datas(table_name_lista_processos,data_atual,data_atual)

        df = self.inserir_colunas_info_sites(
            df_lista_endereco_site=df_lista_endereco_site,
            df_lista_pesquisa=df_lista_pesquisa,
            df_lista_pesquisa_com_dados_tribunais=df_lista_pesquisa_atual,
            servico_solicitado='copia_integral'
        )

        try:
            wr.dynamodb.put_df(
                df,
                table_name=table_name_lista_processos,
                boto3_session=self.session
            )
        except ClientError as e:
            raise Exception(f'Erro ocorreu ao fazer insert na tabela. Detalhe: {e}')

    def get_table_filter(self,table_name:str,key:str,valor:str):

        try:
            df = wr.dynamodb.read_items(
                table_name=table_name,
                filter_expression=Attr(key).eq(valor),
                boto3_session=self.session
            )

        except ClientError as e:
            raise Exception(f'Erro ocorreu ao fazer select na tabela. Detalhe: {e}')
        else:
            return df

    def get_table(self, table_name:str):

        try:
            df = wr.dynamodb.read_items(
                table_name=table_name,
                allow_full_scan=True,
                boto3_session=self.session
            )
        except ClientError as e:
            raise Exception(f'Erro ocorreu ao fazer select na tabela. Detalhe: {e}')
        else:
            return df

    def delete_itens(self, table_name:str,items):
        try:
            wr.dynamodb.delete_items(items=items, table_name=table_name)
        except ClientError as e:
            raise Exception(f'Erro ocorreu ao deletar itens da tabela. Detalhe: {e}')

#OperacoesDynamoDB().create_table('SOPII_senhas_bb_movimentacoes','status','data_hora')
#data_atual = datetime.datetime.now().strftime('%Y/%m/%d')
#OperacoesDynamoDB().put_senha_login_bb_movimentacoes('ATUAL','00:00:00','Primeiro Login','teste','teste1',data_atual)
#t = OperacoesDynamoDB().get_senha_login_bb_movimentacoes('ATUAL','00:00:00')
#t = OperacoesDynamoDB().update_senha_login_bb_movimentacoes('ATUAL','00:00:00','Tamara','teste1','teste2',data_atual)
#print(t)
#df = OperacoesDynamoDB().get_table_entre_datas('SOPII_bbmovimentacoes',data_atual,data_atual)

'''df = pd.read_excel('./teste.xlsx', dtype=str)

df.loc[
    (df['hora_execucao'].fillna('') == '22/05/2023 04:05'),
    'url_pre_assinada'
] = 'https://bb-movimentacoes.s3.amazonaws.com/22_05_2023/movimentacoes-22-05-2023-04-00.zip?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAYCVNEZB53WXYPMGB%2F20230523%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230523T021002Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=a8ccd8865f2ee5d6800d5dcb62d1c38f130d60d76ec6cc1ea51c4aec3b53465d'

df.loc[
    (df['hora_execucao'].fillna('') == '22/05/2023 05:06'),
    'url_pre_assinada'
] = 'https://bb-movimentacoes.s3.amazonaws.com/22_05_2023/movimentacoes-22-05-2023-05-00.zip?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAYCVNEZB53WXYPMGB%2F20230523%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230523T020843Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=b9bd6444d252248c5273d25e099d34f70901d2a8c6f3c5f494aa0117dc455121'

df.loc[
    (df['hora_execucao'].fillna('') == '22/05/2023 06:04'),
    'url_pre_assinada'
] = 'https://bb-movimentacoes.s3.amazonaws.com/22_05_2023/movimentacoes-22-05-2023-06-00.zip?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAYCVNEZB53WXYPMGB%2F20230523%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230523T020738Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=2da18a0c45e393fd9d7ab89e5bec8f95b881b827f603aad126010b4721e152fd'

df.to_excel('./teste.xlsx', index = False)

#df = pd.read_excel('./teste.xlsx', dtype=str)

OperacoesDynamoDB().put_table('SOPII_bbmovimentacoes',df)'''

#OperacoesDynamoDB().put_registro('user_db',{'login':'barcelos_sopii','senha':'barcelos3123','email':'','nome:':'Barcelos SOPII','empresa':'Barcelos','setor':'SOPII'})