from tratativa_barcelos_sopii import ClienteBarcelosSOPII
from classe_operacao_s3 import OperacaoS3
import pandas as pd
from datetime import datetime, timedelta
import pytz

class ClienteLasmar(ClienteBarcelosSOPII):

    lista_possiveis_servicos = ['copia_integral', 'protocolo']
    nome_cliente = 'lasmar'
    nome_base_protocolo = 'lasmar_protocolo'
    nome_base_lista_endereco_site = 'lista_endereco_site'
    nome_base_depara_tipo_protocolo = 'depara_tipo_protocolo'
    nome_base_depara_tipo_arquivo = 'depara_tipo_arquivo'
    nome_bucket_protocolo = 'lasmar-protocolo'

    def identificar_informacoes_no_nome_dos_arquivos(self, dict_nome_arquivos: list) -> pd.DataFrame:

        try:
            df = pd.DataFrame(dict_nome_arquivos, columns=['Nome do arquivo', 'diretorio arquivo'])
            if len(df) == 0:
                raise Exception(f'Nao foi identificado nenhum protocolo inseido para o "{self.nome_cliente}".')

            dict_arquivos_gerais = OperacaoS3().list_nome_arquivos_pasta(self.nome_bucket_protocolo, 'arquivos_gerais')

            df_arquivos_gerais = pd.DataFrame(list(dict_arquivos_gerais.items()), columns=['Nome do arquivo', 'diretorio arquivo'])

            if len(df_arquivos_gerais) == 0:
                raise Exception(f'Nao foi identificado os arquivos gerais do cliente "{self.nome_cliente}"')

            df_arquivos_gerais = self.gerar_excel_arquivos_gerais(df_arquivos_gerais=df_arquivos_gerais)

            if 'protocolo' not in df.columns:
                df['protocolo'] = ''

            if 'status_comprovante' not in df.columns:
                df['status_comprovante'] = ''

            if 'n_processo' not in df.columns:
                df['n_processo'] = ''

            if 'Ordem do arquivo' not in df.columns:
                df['Ordem do arquivo'] = ''

            if 'nome_arquivo_original' not in df.columns:
                df['nome_arquivo_original'] = ''

            if 'nome_arquivo' not in df.columns:
                df['nome_arquivo'] = ''

            if 'Identificador' not in df.columns:
                df['Identificador'] = ''

            if 'cod_tribunal' not in df.columns:
                df['cod_tribunal'] = ''

            if 'lista_arquivos_para_anexar' not in df.columns:
                df['lista_arquivos_para_anexar'] = ''

            df.loc[
                (df["protocolo"].fillna('').str.replace(' ', '') == '') &
                (df['Nome do arquivo'].str.count('-') != 2),
                'protocolo'
            ] = 'NOME DO ARQUIVO FORA DO PADRÃO.'

            # tratativas do numero do protocolo
            df.loc[
                ~(df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[0].isna()),
                'n_processo'
            ] = df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[0]

            # adicionar regex para retirar os pontos
            df.loc[
                (df['n_processo'].fillna('').str.replace(' ', '') != ''),
                'n_processo'
            ] = df['n_processo'].str.replace(' ', '')

            df.loc[
                (df['n_processo'].fillna('').str.replace(' ', '') != ''),
                'n_processo'
            ] = df['n_processo'].str.replace('[^0-9]', '', regex=True)

            df.loc[
                (df["protocolo"].fillna('').str.replace(' ', '') == '') &
                (
                        (df['n_processo'].str.len() < 15) |
                        (df['n_processo'].str.len() > 20)
                ),
                'protocolo'
            ] = 'NOME DO ARQUIVO FORA DO PADRÃO.'

            # acrescentar zeros a esquerda
            df.loc[
                (df['n_processo'].str.len() >= 16),
                'n_processo'
            ] = df['n_processo'].fillna('').str.replace(' ', '').apply(lambda x: x.zfill(20))

            # tratativas do cod_tribunal
            df.loc[
                (df['n_processo'].str.len() == 20),
                'cod_tribunal'
            ] = df['n_processo'].fillna('').str[-7:-4]

            # tratativas do Ordem do arquivo
            df.loc[
                ~(df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[1].isna()),
                'Ordem do arquivo'
            ] = df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[1].replace(' ', '')

            df.loc[
                (df['Ordem do arquivo'].fillna('').str.replace(' ', '') != ''),
                'Ordem do arquivo'
            ] = df['Ordem do arquivo'].str.replace(' ', '')

            df.loc[
                (df["protocolo"].fillna('').str.replace(' ', '') == '') &
                (df['Ordem do arquivo'].str.len() > 3),
                ['protocolo', 'Ordem do arquivo']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '']

            df.loc[
                (df["protocolo"].fillna('').str.replace(' ', '') == '') &
                (df['Ordem do arquivo'].fillna('').str.replace(' ', '') == ''),
                ['protocolo', 'Ordem do arquivo']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '']

            # tratativas do nome_arquivo
            df.loc[
                ~(df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[2].isna()),
                'nome_arquivo'
            ] = df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[2]

            df.loc[
                ~(df['nome_arquivo'].str.split(".", n=-1, expand=False).str[0].isna()),
                'nome_arquivo'
            ] = df['nome_arquivo'].str.split(".", n=-1, expand=False).str[0]

            df.loc[
                (df["protocolo"].fillna('').str.replace(' ', '') == '') &
                (df['nome_arquivo'].str.split(".", n=-1, expand=False).str[0].isna()),
                ['protocolo', 'nome_arquivo']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '']

            # tratativas do identificador
            df.loc[
                (df['protocolo'].fillna('').str.replace(' ', '') == '') &
                (df['Ordem do arquivo'].fillna('') == '1'),
                'Identificador'
            ] = ((df['protocolo'].fillna('').str.replace(' ', '') == '') & (df['Ordem do arquivo'].fillna('') == '1')).cumsum()

            df.loc[
                (df['nome_arquivo'].str.len() > 0),
                'nome_arquivo_original'
            ] = df['nome_arquivo'].fillna('').str.replace('\d+', '', regex=True).replace('_', '')

            df.loc[
                (df['nome_arquivo_original'].fillna('').str.replace(' ', '') != ''),
                'nome_arquivo'
            ] = df['nome_arquivo_original'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode(
                'utf-8').str.replace(' ', '').str.lower()

            df_ordem_1 = df.loc[
                (df["Ordem do arquivo"].fillna('') == '1'),
                [
                    'Nome do arquivo',
                    'protocolo',
                    'n_processo',
                    'Ordem do arquivo',
                    'nome_arquivo_original',
                    'nome_arquivo',
                    'Identificador',
                    'cod_tribunal'
                ]
            ].fillna('')

            df["Ordem do arquivo"] = pd.to_numeric(df["Ordem do arquivo"], errors='coerce')
            for index, row in df_ordem_1.iterrows():

                df_arquivos_gerais.loc[df_arquivos_gerais['Nome do arquivo'].fillna('').str.replace(' ', '') != '',
                    ['n_processo', 'cod_tribunal']
                ] = [row['n_processo'], row['cod_tribunal']]

                maior_identificador = df.loc[
                    (df['Identificador'].fillna('') == row['Identificador']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'Ordem do arquivo'
                ].max()

                qt_identificador = len(df.loc[
                                           (df['Identificador'].fillna('') == row['Identificador']) &
                                           (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                                           'Ordem do arquivo'
                                       ])

                qt_valores_duplicados = (df.loc[
                                             (df['Identificador'].fillna('') == row['Identificador']) &
                                             (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                                             'Ordem do arquivo'
                                         ].duplicated()).sum()

                df.loc[
                    (df['Identificador'].fillna('') == row['Identificador']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'protocolo'
                ] = (maior_identificador != qt_identificador and "ORDEM DOS ARQUIVOS INCORRETA." or "")

                df.loc[
                    (df['Identificador'].fillna('') == row['Identificador']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'protocolo'
                ] = (qt_valores_duplicados > 0 and "ORDEM DOS ARQUIVOS INCORRETA." or "")

                df_arquivos_gerais['Ordem do arquivo'] = df_arquivos_gerais.index + maior_identificador
                df = pd.concat([df, df_arquivos_gerais])

                df.loc[
                    (df['protocolo'].fillna('').str.replace(' ', '') == '') &
                    (df['n_processo'].fillna('') == row['n_processo']),
                                       'Identificador'
                ] = row['Identificador']

            df['Ordem do arquivo'] = df['Ordem do arquivo'].astype(str).str.split(".", n=-1, expand=False).str[0]
            df.sort_values(['n_processo', 'Ordem do arquivo'], inplace=True)
            df.reset_index(inplace=True, drop=True)

            df_final = df.loc[
                (df["Nome do arquivo"].fillna('').str.replace(' ', '') != ''),
                df.columns
            ].fillna('')

        except Exception as e:
            raise Exception(f'Ocorreu um erro no método "identificar_informacoes_no_nome_dos_arquivos". Exception:{e}.')
        else:
            print("A função 'identificar_informacoes_no_nome_dos_arquivos' executou com sucesso!")
            return df_final

    def gerar_excel_arquivos_gerais(self, df_arquivos_gerais: pd.DataFrame) -> pd.DataFrame:
        try:
            if 'protocolo' not in df_arquivos_gerais.columns:
                df_arquivos_gerais['protocolo'] = ''

            if 'n_processo' not in df_arquivos_gerais.columns:
                df_arquivos_gerais['n_processo'] = ''

            if 'Ordem do arquivo' not in df_arquivos_gerais.columns:
                df_arquivos_gerais['Ordem do arquivo'] = ''

            if 'nome_arquivo_original' not in df_arquivos_gerais.columns:
                df_arquivos_gerais['nome_arquivo_original'] = ''

            if 'nome_arquivo' not in df_arquivos_gerais.columns:
                df_arquivos_gerais['nome_arquivo'] = ''

            if 'Identificador' not in df_arquivos_gerais.columns:
                df_arquivos_gerais['Identificador'] = ''

            if 'cod_tribunal' not in df_arquivos_gerais.columns:
                df_arquivos_gerais['cod_tribunal'] = ''

            # tratativas do Ordem do arquivo
            df_arquivos_gerais.loc[
                ~(df_arquivos_gerais['Nome do arquivo'].str.split("-", n=-1, expand=False).str[0].isna()),
                'Ordem do arquivo'
            ] = df_arquivos_gerais['Nome do arquivo'].str.split("-", n=-1, expand=False).str[0].replace(' ', '')
            df_arquivos_gerais.loc[df_arquivos_gerais['Ordem do arquivo'].str.len() > 0,
                                   'Ordem do arquivo'
            ] = df_arquivos_gerais['Ordem do arquivo'].replace(' ', '')

            # tratativas do nome_arquivo_original
            df_arquivos_gerais.loc[
                ~(df_arquivos_gerais['Nome do arquivo'].str.split("-", n=-1, expand=False).str[1].isna()),
                'nome_arquivo_original'
            ] = df_arquivos_gerais['Nome do arquivo'].str.split("-", n=-1, expand=False).str[1]

            df_arquivos_gerais.loc[
                ~(df_arquivos_gerais['nome_arquivo_original'].str.split(".", n=-1, expand=False).str[0].isna()),
                'nome_arquivo_original'
            ] = df_arquivos_gerais['nome_arquivo_original'].str.split(".", n=-1, expand=False).str[0]

            # tratativas do nome_arquivo
            df_arquivos_gerais.loc[df_arquivos_gerais['nome_arquivo_original'].str.len() > 0,
                                   'nome_arquivo'
            ] = df_arquivos_gerais['nome_arquivo_original'].str.normalize('NFKD').str.encode('ascii',
                                                                                             errors='ignore').str.decode(
                'utf-8').str.lower()

            df_arquivos_gerais.loc[df_arquivos_gerais['nome_arquivo'].str.len() > 0,
                                   'nome_arquivo'
            ] = df_arquivos_gerais['nome_arquivo'].str.replace(' ', '')

            df_verifica_erro = df_arquivos_gerais.loc[
                (df_arquivos_gerais["Nome do arquivo"].fillna('') != '') &
                (
                        (df_arquivos_gerais["Ordem do arquivo"].fillna('') == '') |
                        (df_arquivos_gerais["nome_arquivo_original"].fillna('') == '') |
                        (df_arquivos_gerais["nome_arquivo"].fillna('') == '')
                ),
                [
                    'Nome do arquivo'
                ]
            ].fillna('')

            if len(df_verifica_erro) > 0:
                raise Exception(f'A nomeclatura de {len(df_verifica_erro)} arquivos gerais está errada.')

            df_arquivos_gerais.index = list(pd.to_numeric(df_arquivos_gerais['Ordem do arquivo'], errors='coerce'))
            df_arquivos_gerais['Ordem do arquivo'] = ''

            df_arquivos_gerais = df_arquivos_gerais.loc[
                (df_arquivos_gerais["Nome do arquivo"].fillna('') != ''),
                df_arquivos_gerais.columns
            ].fillna('')

        except Exception as e:
            raise Exception(f'Ocorreu um erro no método "gerar_excel_arquivos_gerais". Exception:{e}')
        else:
            print("A função 'gerar_excel_arquivos_gerais' executou com sucesso!")
            return df_arquivos_gerais

    def finalizar_tratativas_protocolo(self, df: pd.DataFrame) -> pd.DataFrame:

        df.loc[
            (df['Ordem do arquivo'].fillna('') == '1'),
            'protocolo'
        ] = df.groupby(
            [
                'Identificador',
                'n_processo',
                'tribunal',
                'sistema',
                'instancia'
            ])['protocolo'].transform(lambda x: ' '.join(x))

        df.loc[
            (df['Ordem do arquivo'].fillna('') == '1'),
            'lista_arquivos_para_anexar'
        ] = df.groupby(
            [
                'Identificador',
                'n_processo',
                'tribunal',
                'sistema',
                'instancia'
            ])['Nome do arquivo'].transform(lambda x: ', '.join(x))

        df.loc[
            (df['protocolo'].fillna('').str.replace(' ', '') != ''),
            ['data_resultado', 'status_comprovante']
        ] = [datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%d/%m/%Y %H:%M:%S'), 'ERRO']

        df['datasolicitacao'] = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y/%m/%d')
        df['horasolicitacao'] = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%H:%M:%S')
        df['nome_arquivo_hora_solicitacao_sistema_tribunal_instancia'] = df['Identificador'].map(str) + '_' + df['Nome do arquivo'].map(str) + '_' + df[
            'horasolicitacao'].map(str) + '_' + df['sistema'].map(str) + '_' + df['tribunal'].map(str) + '_' + df[
                                                                             'instancia'].map(str)
        df.drop_duplicates(subset=['nome_arquivo_hora_solicitacao_sistema_tribunal_instancia', 'datasolicitacao'],
                           keep=False, inplace=True)

        df = df.fillna('')

        return df

