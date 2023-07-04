from tratativa_barcelos_sopii import ClienteBarcelosSOPII
from classe_operacao_dynamodb import OperacoesDynamoDB
import metodos_com_cache_data as mcd
from classe_operacao_s3 import OperacaoS3
import pandas as pd
import streamlit as st
from funcionalidades_app import *
from datetime import datetime, timedelta
import pytz

class ClienteBarcelosSOPIII(ClienteBarcelosSOPII):

    lista_possiveis_servicos = ['copia_integral', 'protocolo']
    nome_cliente = 'barcelos-sopiii'
    nome_base_protocolo = 'barcelos-sopiii_protocolo'
    nome_base_lista_endereco_site = 'lista_endereco_site'
    nome_base_depara_tipo_protocolo = 'depara_tipo_protocolo'
    nome_base_depara_tipo_arquivo = 'depara_tipo_arquivo'
    nome_bucket_protocolo = 'barcelos-sopiii-protocolo'

    def realizar_upload_arq_protocolo(self):

        container_upload_protocolo = st.expander('Insira os arquivos para protocolar.')

        with container_upload_protocolo.form(key='form_upload_arquivos_protocolo', clear_on_submit=True):
            opcao_caso = ['Esperado', 'Esporádico']

            opcao_caso_selecionado = st.radio("Selecione o caso:", opcao_caso, horizontal=True)

            # Allow the user to upload a files
            uploaded_files = st.file_uploader(label='Carregue os arquivos aqui...',
                                              type='pdf',
                                              accept_multiple_files=True)

            # If a files was uploaded, display its contents
            submit_btn = st.form_submit_button('📤 Realizar upload dos arquivos')
            if submit_btn and len(uploaded_files) > 0:
                try:
                    prg = st.progress(0)
                    qt_arquivos = len(uploaded_files)
                    num_lines = qt_arquivos + 6
                    # df_depara_tipoprotocolo = OperacoesDynamoDB().get_table_filter(self.nome_base_depara_tipo_protocolo,'cliente',self.nome_cliente)
                    df_depara_tipoprotocolo = mcd.selecionar_infomacoes_depara_tipo_de_protocolo(self.nome_base_depara_tipo_protocolo, self.nome_cliente)
                    if len(df_depara_tipoprotocolo) > 0:
                        df_depara_tipoprotocolo = df_depara_tipoprotocolo.loc[
                            (df_depara_tipoprotocolo["tribunal"].fillna('').str.replace(' ', '') != ''),
                            [
                                "tribunal",
                                "sistema",
                                "nome_arquivo",
                                "Tipo do protocolo"
                            ]
                        ].drop_duplicates().fillna('')
                    else:
                        raise Exception('A tabela tipo de protocolo esta vazia.')

                    # df_depara_tipoarquivo = OperacoesDynamoDB().get_table_filter(self.nome_base_depara_tipo_arquivo,'cliente',self.nome_cliente)
                    df_depara_tipoarquivo = mcd.selecionar_infomacoes_depara_tipo_de_arquivo(self.nome_base_depara_tipo_arquivo,self.nome_cliente)
                    if len(df_depara_tipoarquivo):
                        df_depara_tipoarquivo = df_depara_tipoarquivo.loc[
                            (df_depara_tipoarquivo["tribunal"].fillna('').str.replace(' ', '') != ''),
                            [
                                "tribunal",
                                "sistema",
                                "nome_arquivo",
                                "Tipo arquivo"
                            ]
                        ].drop_duplicates().fillna('')
                    else:
                        df_depara_tipoarquivo = pd.DataFrame(columns=[
                                "tribunal",
                                "sistema",
                                "nome_arquivo",
                                "Tipo arquivo"
                            ]
                        )

                    # df_lista_endereco_site = OperacoesDynamoDB().get_table_filter(self.nome_base_lista_endereco_site,'cliente_servico',self.nome_base_protocolo)
                    df_lista_endereco_site = mcd.selecionar_infomacoes_tribunais(self.nome_base_lista_endereco_site, self.nome_base_protocolo)
                    if len(df_lista_endereco_site) > 0:
                        df_lista_endereco_site = df_lista_endereco_site.loc[
                            (df_lista_endereco_site["tribunal"].fillna('').str.replace(' ', '') != ''),
                            [
                                "tribunal",
                                "sistema",
                                "instancia",
                                "cod_tribunal",
                                "url",
                                "login",
                                "senha",
                                "validado",
                                "habilitado"
                            ]
                        ].drop_duplicates().fillna('')
                    else:
                        raise Exception('A tabela de endereço de site está vazia.')

                    files_name = []
                    i = 0
                    data_hora_br = datetime.now(pytz.timezone('America/Sao_Paulo'))
                    pasta_data = data_hora_br.strftime('%d_%m_%Y')
                    pasta_hora = data_hora_br.strftime('%H:%M:%S')
                    for file in uploaded_files:
                        i += 1
                        object_name = f'{pasta_data}/{pasta_hora}/arquivos_para_protocolar/{file.name}'
                        OperacaoS3().upload_file_obj(
                            file_name=file,
                            bucket=self.nome_bucket_protocolo,
                            object_name=object_name
                        )
                        files_name.append([file.name, object_name])
                        prg.progress(int(100 * i / num_lines))
                    if opcao_caso_selecionado == 'Esporádico':
                        df = self.identificar_informacoes_no_nome_dos_arquivos_esporadico(
                            dict_nome_arquivos=files_name
                        )
                    else:
                        df = self.identificar_informacoes_no_nome_dos_arquivos(
                            dict_nome_arquivos=files_name
                        )
                    i += 1
                    prg.progress(int(100 * i / num_lines))

                    df = self.inserir_informacoes_de_tribunais(
                        df_lista_endereco_site=df_lista_endereco_site,
                        df_lista_pesquisa=df,
                        servico_solicitado='protocolo'
                    )

                    i += 1
                    prg.progress(int(100 * i / num_lines))
                    df = self.realizar_depara(
                        df_original=df,
                        df_depara_tipoprotocolo=df_depara_tipoprotocolo,
                        df_depara_tipoarquivo=df_depara_tipoarquivo
                    )
                    i += 1
                    prg.progress(int(100 * i / num_lines))

                    df = self.finalizar_tratativas_protocolo(df=df)

                    i += 1
                    prg.progress(int(100 * i / num_lines))

                    try:
                        OperacoesDynamoDB().put_table(self.nome_base_protocolo, df)
                    except Exception as e_put_table:
                        print(e_put_table)
                        raise Exception(f'Erro ao gravar os dados.')
                    i += 1

                    prg.progress(int(100 * i / num_lines))

                    try:
                        TratarEmail().enviar_email(
                            ls_destinatario=self.ls_destinatario,
                            assunto=f'NOVA BASE PROTOCOLO - {self.nome_cliente}'.upper(),
                            mensagem=f'\n\nAcabou de chegar "{qt_arquivos}" arquivos para fazer a protocolo. \n'
                                     f'Acesse a plataforma.'
                        )
                    except Exception as e_enviar_email:
                        print(f'Ocorrreu um erro ao enviar um email. Detalhe: {e_enviar_email}')

                    i += 1

                    prg.progress(int(100 * i / num_lines))

                except Exception as erro:
                    st.error(erro)
                else:
                    st.success(f'Upload realizado com sucesso!')
            else:
                st.write(f'Insira os arquivos para fazer upload.')

    def identificar_informacoes_no_nome_dos_arquivos_esporadico(self, dict_nome_arquivos: list) -> pd.DataFrame:

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