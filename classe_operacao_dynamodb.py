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
                query=f'''SELECT * FROM "{table_name}" WHERE datasolicitacao BETWEEN ? and ?''',
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

    def delete_itens(self, table_name:str,itens):
        try:
            wr.dynamodb.delete_items(items=itens, table_name=table_name)
        except ClientError as e:
            raise Exception(f'Erro ocorreu ao deletar itens da tabela. Detalhe: {e}')

    def delete_itens_usando_sql(self):
        table_name = 'depara_tipo_protocolo'
        cliente = 'barcelos-sopiii'
        wr.dynamodb.execute_statement(
            statement=f"DELETE FROM \"{table_name}\" WHERE cliente=?",
            parameters=[cliente],
            boto3_session=self.session
        )

    '''def detele_items_usando_filtro(self):
        lista_coluna = ['cliente_servico', 'sistema_tribunal_instancia']
        coluna_que_nao_pode_esta_vazia = 'cliente_servico'
        nome_tabela_dynamodb = 'lista_endereco_site'
        nome_chave = 'cliente_servico'
        valor_chave = 'rolim_copia-integral'

        df = OperacoesDynamoDB().get_table_filter(nome_tabela_dynamodb, nome_chave, valor_chave)
        df = df.loc[
            (df[coluna_que_nao_pode_esta_vazia].fillna('').str.replace(' ', '') != ''),
            lista_coluna
        ]
        dicionario = df.to_dict('records')

        OperacoesDynamoDB().delete_itens(nome_tabela_dynamodb, dicionario)'''


