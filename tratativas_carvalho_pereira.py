from tratativa_barcelos_sopii import ClienteBarcelosSOPII
import pandas as pd

class ClienteCarvalhoPereira(ClienteBarcelosSOPII):

    lista_possiveis_servicos = ['copia_integral', 'protocolo']
    nome_cliente = 'carvalho-pereira'
    nome_base_protocolo = 'carvalho-pereira_protocolo'
    nome_base_lista_endereco_site = 'lista_endereco_site'
    nome_base_depara_tipo_protocolo = 'depara_tipo_protocolo'
    nome_base_depara_tipo_arquivo = 'depara_tipo_arquivo'
    nome_bucket_protocolo = 'carvalho-pereira-protocolo'

    def identificar_informacoes_no_nome_dos_arquivos(self, dict_nome_arquivos: list) -> pd.DataFrame:

        try:
            df = pd.DataFrame(dict_nome_arquivos, columns=['Nome do arquivo', 'diretorio arquivo'])
            if len(df) == 0:
                raise Exception('Nao foi identificado nenhum protocolo na pasta "Barcelos".')

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
                (df['nome_arquivo'].str.replace(' ', '').str.len() > 0),
                'Identificador'
            ] = df['n_processo'].fillna('')

            df.loc[
                (df["protocolo"].fillna('').str.replace(' ', '') == '') &
                (df['nome_arquivo'].str[-8:].str.replace(' ', '').str.len() == 0),
                'protocolo'
            ] = 'NOME DO ARQUIVO FORA DO PADRÃO.'

            df.loc[
                (df['Identificador'].fillna('').str.replace(' ', '') != ''),
                'Identificador'
            ] = df['Identificador'].str.replace('[^0-9]', '', regex=True)

            df.loc[
                (df['nome_arquivo'].str.len() > 0),
                'nome_arquivo'
            ] = df['nome_arquivo'].fillna('').str.replace('\d+', '', regex=True).replace('_', '')

            df.loc[
                (df['nome_arquivo'].fillna('').str.replace(' ', '') != ''),
                'nome_arquivo_original'
            ] = df['nome_arquivo'].str.replace('_', '')

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