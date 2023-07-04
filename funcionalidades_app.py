import pandas as pd
from googleapiclient import discovery, errors
from google.oauth2 import service_account
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
import base64
from io import BytesIO
from classe_operacao_s3 import OperacaoS3
import uuid
import re

class FuncionalidadesAPP:

    def to_excel(self, df1:pd.DataFrame, df2:pd.DataFrame):
        try:
            output = BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')
            df1.to_excel(writer, sheet_name='Resultado', index=False)
            df2.to_excel(writer, sheet_name='Estatisticas', index=False)
            writer.close()
            processed_data = output.getvalue()
        except Exception as e:
            raise Exception(f'Erro ocorreu na função "to_excel". Detalhe: {e}')
        else:
            return processed_data

    def to_excel_3_abas(self, df1:pd.DataFrame, df2:pd.DataFrame, df3:pd.DataFrame):
        try:
            output = BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')
            df1.to_excel(writer, sheet_name='Resultado', index=False)
            df2.to_excel(writer, sheet_name='Resultado detalhado', index=False)
            df3.to_excel(writer, sheet_name='Estatisticas', index=False)
            writer.close()
            processed_data = output.getvalue()
        except Exception as e:
            raise Exception(f'Erro ocorreu na função "to_excel". Detalhe: {e}')
        else:
            return processed_data

    def download_aws_object(self, bucket_name:str, key:str) -> str:
        """
        Download an object from AWS
        Example key: my/key/some_file.txt
        """
        obj = OperacaoS3().s3.Object(bucket_name, key)
        file_name = key.split('/')[-1]  # e.g. some_file.txt
        file_type = file_name.split('.')[-1]  # e.g. txt
        b64 = base64.b64encode(obj.get()['Body'].read()).decode()

        button_uuid = str(uuid.uuid4()).replace("-", "")
        button_id = re.sub("\d+", "", button_uuid)

        dl_link = f'<a download="{file_name}" id="{button_id}" href="data:file/{file_type};base64,{b64}"><button type="button">Download</button></a><br></br>'
        print(file_name)
        return dl_link

    def get_download(self, df:pd.DataFrame, arq:str) -> str:
        try:
            csv = df.to_csv(index=False)
            b64 = base64.b64encode(csv.encode()).decode()
            href = f'<a href="data:file/csv;base64,{b64}" download="' + arq + '.csv">Download</a>'

        except Exception as e:
            raise Exception(f'Erro ocorreu na função "get_download". Detalhe: {e}')
        else:
            return href

    def create_link(self, url:str) -> str:
        return f'''<a href="{url.replace(' ','')}"><button type="button">Download</button></a>'''


class TratarEmail:
    user_email = "vinicius.barcelos@bluetape.com.br"
    service_accont = {
        "type": "service_account",
        "project_id": "drive-344417",
        "private_key_id": "03e8c05133b7777c9ebbd0df4c9dc14f7a47574b",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCmHYUwWd0dYKjh\nEiY2u5l1fJDqfm11u7+bg3fe6Z5YKnpc+nDq0aGhWdhDl/Y17EXbkmQVUZ55FvqH\nCw7YBd3ngxSaRpySvfYlNfGNenrXEL+V85PK65F6Q56tOOaixwFqC3vNH1GfjSKS\nScjsufaai1I3f6C5T/KBknRkhK8CUtllQ2jJT0Hft+/D3BXbMCvYPH1qJRWyUk0K\nfraEWdV+QuuP1qVAHPvp5m1mLFVLs9lTP9ULL4dBEaYTOuDYYwPLYZt3mDogIlh2\nSsxK/gxDxHSjoJoB3e3TQG7IlVmHkTJFz0BdoI1HoX+MoZcWBGdw8h/PVS6AOv8m\nj77tGtxfAgMBAAECggEARA4NDmmKkqoSlh8K49qIvN7bOg6wxo60KcC7P+qDMki4\n19XMpA9dQg7ecJbVynKxjgrUEP3eyGo3GlNX8f25U0tbCfeK1v2XVdhbTWST5f9Z\nFlRzxKss7yO74ujQaHjSZgEtZ7SAirbWJouvEvj/BIK2nMEfdjxANIBtSe8oCfGB\nS8QKDf5h/Z3F3JMETRWLI3LOuUvdW0Hz0rNbRe2qBSRKu+dqV0cpn0SlIPsg057F\n6GtpT/UsDY8mhU8IjvRKtw/qr1jsby6dZ2DUhhowUPVDL8s8PmXMuwYOVL2UUFI/\nATthRkfUQAHf20AQ+ESsYktAMi6zFjRr+5PEWPVnpQKBgQDU4jMyzk9swDoEWSUt\nriL5jKnFvP6hg54O8RPVy/iiY5O5QmBJ5sZRKfgnCaj/Kl9oeSZMvrdk3TEJLol4\nYNa3vvR1Fw0oNtBTaR3jRkR4QHqdZwBHidx8mKhQEEPmUgIs4VDgHKt8CC+7oCqM\nhU4De/0DCworRJ/K3rh+arH63QKBgQDHwm6ZwpG6DeD/t7BOLmW818yl81Q5LlWD\n6Q5fg4ZrJVdSYA2tt39rP/CsfYdsgqdTOKReYr9AxlleQE+LwTgBK3Nm+zLaV7PK\nsKigsu4Zty1x/PH58Xi7y88DIiwmadsEuug3Ds8V/PQXN45CwSH5FWIqLsC+X+n/\nIT4Fjf3qawKBgHefK5nO6D55vaAX35ZNlYWYjwV377UeWkCXIsguN2Y4ghfFvomI\nTcPpy3FUMkw/qSDxgp35ROA+koFQTxr/f2f8uyzYaSJBuspD9PFy2KKhyMSNzlBk\nnSq+NUXX/e98AZDqgyGLuqiViQzrMT2I4o9+qmt9Vpd9ljTO9ejDV4NZAoGBAI5P\nXP3bmHfk5VMoKMk50q77Skc7l9f4w2FrShsPl1aDzrbXyUbmADeG4M3xy+WpGs11\n/9tiHABojkhQJptYtq5WpytJcAwPCP4wZqG9d1UIo66eVkELH0kixELmOG/RX//5\nq+91dGCkybw0jvvNnYdXDY0hq5y0tf5VT1sjsi4rAoGATZC7VUu0uNy0TCWbFzEu\nPa9/6KjLIxCVWv/sa5b8wjGk9piS0TZZsaH6pYdb6aVuPWy1oCq1imZuts/w37kM\nnHl206xa8ylX3t4v7FxGGBWIvtH1xTxdeipHU7quzozLrCTSmNlchHsKAwSUYNnu\nvAzf+UvAcywvBg/BxlYojEk=\n-----END PRIVATE KEY-----\n",
        "client_email": "drive-bluetape@drive-344417.iam.gserviceaccount.com",
        "client_id": "106636666073957208326",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/drive-bluetape%40drive-344417.iam.gserviceaccount.com"
    }
    def __init__(self):
        try:
            credentials = self.validar_servico()
            # Informar qual endereço de email é o dono das credenciais
            delegated_credentials = credentials.with_subject(self.user_email)
            service = discovery.build('gmail', 'v1', credentials=delegated_credentials)
            self.service = service
        except Exception as e:
            raise Exception("Erro correu no função '__init__' da classe 'TratarEmail'. Exception: " + str(e))
        else:
            print('A classe Tratar email iniciou de forma correta!')

    def validar_servico(self):
        try:
            SERVICE_ACCOUNT_FILE = 'Classes_complementares/Creds/service_account.json'
            SCOPES = ['https://mail.google.com/']
            # Configurar as credenciais
            credentials = service_account.Credentials.from_service_account_info(self.service_accont, scopes=SCOPES)

            #credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        except Exception as e:
            raise Exception("Erro correu no função 'validar_servico' da classe 'TratarEmail'. Exception: " + str(e))
        else:
            return credentials

    def formatar_msg(self, ls_destinatario, assunto, mensagem, dc_arquivo_anexo):
        try:
            message = MIMEMultipart()
            message['to'] = (ls_destinatario[0] if len(ls_destinatario) == 1 else ",".join(ls_destinatario))
            message['from'] = self.user_email
            message['subject'] = assunto
            parte_texto = MIMEText(mensagem)
            message.attach(parte_texto)
            if dc_arquivo_anexo != None:
                for nome_arquivo, diretorio in dc_arquivo_anexo.items():
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload((open(diretorio, 'rb')).read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', "attachment; filename=" + nome_arquivo)
                    message.attach(part)
            create_message = {'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}
        except Exception as e:
            raise Exception("Erro correu no função 'formatar_msg' da classe 'TratarEmail'. Exception: " + str(e))
        else:
            print('O email foi formatado com sucesso!')
            return create_message

    def enviar_email(self, ls_destinatario, assunto, mensagem, dc_arquivo_anexo=None,n_enviar:int=0):
        try:
            message = self.formatar_msg(ls_destinatario, assunto, mensagem, dc_arquivo_anexo)
            email_sent = self.service.users().messages().send(userId="me", body=message).execute()
        except Exception as e:
            try:
                _quant = 3
                if n_enviar < _quant:
                    n_enviar += 1
                    print("Erro correu no função 'enviar_email' da classe 'TratarEmail'. Exception: " + str(e) + ". Ainda será feito '" + str(_quant - n_enviar)+"'")
                    credentials = self.validar_servico()
                    # Informar qual endereço de email é o dono das credenciais
                    delegated_credentials = credentials.with_subject(self.user_email)
                    service = discovery.build('gmail', 'v1', credentials=delegated_credentials)
                    self.service = service
                    self.enviar_email(ls_destinatario, assunto, mensagem, dc_arquivo_anexo, n_enviar)
                else:
                    raise Exception("Erro correu no função 'enviar_email' da classe 'TratarEmail'. Exception: " + str(e) + "foram feitas '" + str(n_enviar) + "' tentativas")
            except Exception as e:
                raise Exception("Erro correu no função 'enviar_email' dentro do bloco Exception da classe 'TratarEmail'. Exception: " + str(e))
        else:
            print('Email enviado!! Id do email:'+email_sent['id'])
            print("A instancia da classe TratarEmail foi finalizada.")
            return