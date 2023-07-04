from tratativa_barcelos_sopii import ClienteBarcelosSOPII
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import pytz
from classe_operacao_dynamodb import OperacoesDynamoDB
from funcionalidades_app import *


class ClienteRolim(ClienteBarcelosSOPII):
    nome_cliente = 'rolim'
    nome_base_copia_integral = 'rolim_copia-integral'

    def realizar_upload_base_dados_copia_integral(self):

        container_upload = st.expander('Upload base de dados para copia integral. ')
        container_upload.write('Atenção, ao fazer upload da base da cópia integrel, verifique se a base possuí as seguintes características:')
        container_upload.write(
            '- Nome da coluna "n_processo". (COLUNA OBRIGATÓRIA)')
        container_upload.write('- Nome da coluna "instancia". (COLUNA NÃO OBRIGATÓRIA)')
        container_upload.write(
            '- Nome da coluna "tribunal". (COLUNA NÃO OBRIGATÓRIA)')
        container_upload.write(
            '- Nome da coluna "sistema". (COLUNA NÃO OBRIGATÓRIA)')

        with container_upload.form("my-form", clear_on_submit=True):
            uploaded_file = st.file_uploader('Upload base de dados para copia integral.', type='xlsx')
            submitted = st.form_submit_button("UPLOAD", help='Realizar o upload!')
            if submitted and uploaded_file is not None:
                dataframe = pd.read_excel(uploaded_file, dtype=str)
                lista_colunas_existentes = []
                if 'n_processo' not in dataframe.columns:
                    st.error(f'O excel não possui a coluna obrigatória "n_processo".')
                    return
                else:
                    lista_colunas_existentes.append('n_processo')

                if 'instancia' in dataframe.columns:
                    lista_colunas_existentes.append('instancia')
                if 'tribunal' in dataframe.columns:
                    lista_colunas_existentes.append('tribunal')
                if 'sistema' in dataframe.columns:
                    lista_colunas_existentes.append('sistema')
                if 'Cliente' in dataframe.columns:
                    lista_colunas_existentes.append('Cliente')

                dataframe = dataframe.loc[
                    (dataframe["n_processo"].fillna('').str.replace(' ', '') != ''),
                    lista_colunas_existentes
                ].drop_duplicates().fillna('')
                tamanho_dataframe = len(dataframe)
                if tamanho_dataframe == 0:
                    st.error('Erro: Não identificou nenhum processo no excel inserido.')
                    return

                df_lista_endereco_site = OperacoesDynamoDB().get_table_filter(self.nome_base_lista_endereco_site,
                                                                              'cliente_servico',
                                                                              self.nome_base_copia_integral)
                data_atual = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y/%m/%d')
                df_lista_pesquisa_atual = OperacoesDynamoDB().get_table_filter(self.nome_base_copia_integral,
                                                                               'datasolicitacao', data_atual)
                try:
                    df = self.inserir_informacoes_de_tribunais(df_lista_endereco_site, dataframe, 'copia_integral')
                    df = self.finalizar_tratativas_copia_integral(df, df_lista_pesquisa_atual)
                except Exception as erro_tratativas:
                    st.error(erro_tratativas)
                    return
                try:
                    OperacoesDynamoDB().put_table(self.nome_base_copia_integral, df)
                    try:
                        TratarEmail().enviar_email(
                            ls_destinatario=self.ls_destinatario,
                            assunto=f'NOVA BASE CÓPIA INTEGRAL - {self.nome_cliente}'.upper(),
                            mensagem=f'\n\nAcabou de chegar "{tamanho_dataframe}" processos para fazer a cópia integral. \n'
                                     f'Acesse a plataforma.'
                        )
                    except Exception as e_enviar_email:
                        print(f'Ocorrreu um erro ao enviar um email. Detalhe: {e_enviar_email}')
                except Exception as e_put_table:
                    st.error(f'Erro: Não foi possivel gravar os dados do excel. Detalhe: {e_put_table}')
                else:
                    st.success('Dados do excel foram gravados com sucesso!')

            elif submitted and uploaded_file is None:
                st.error('Upload não realizado. Para fazer upload insira um arquivo valido.')

    def tratar_dados_copia_integral(self, data_inicio, data_fim) -> pd.DataFrame:
        try:
            data = pd.DataFrame(
                columns=['DATA DA SOLICITAÇÃO','CLIENTE PARCEIRO', 'Nº DO PROCESSO', 'CÓD. TRIBUNAL', 'SISTEMA', 'TRIBUNAL', 'INSTÂNCIA', 'DATA RESULTADO',
                         'RESULTADO', 'ERRO'])
            data_inicio_usada = data_inicio.strftime('%Y/%m/%d')
            data_fim_usada = data_fim.strftime('%Y/%m/%d')
            if data_inicio_usada <= data_fim_usada:
                data_1 = OperacoesDynamoDB().get_table_entre_datas(self.nome_base_copia_integral, data_inicio_usada, data_fim_usada)
                if len(data_1) > 0:
                    data = data_1.loc[
                        (data_1["n_processo"].fillna('') != ''),
                        [
                            'datasolicitacao',
                            'Cliente',
                            'n_processo_original',
                            'cod_tribunal',
                            'sistema',
                            'tribunal',
                            'instancia',
                            'copia_integral',
                            'detalhe',
                            'data_resultado',
                            'url_pre_assinada'
                        ]
                    ].fillna('')

                    data = data.rename(
                        columns=
                        {
                            'datasolicitacao': 'DATA DA SOLICITAÇÃO',
                            'Cliente': 'CLIENTE PARCEIRO',
                            'n_processo_original': 'Nº DO PROCESSO',
                            'cod_tribunal': 'CÓD. TRIBUNAL',
                            'sistema': 'SISTEMA',
                            'tribunal': 'TRIBUNAL',
                            'instancia': 'INSTÂNCIA',
                            'data_resultado': 'DATA RESULTADO',
                            'copia_integral': 'RESULTADO',
                            'detalhe': 'ERRO',
                            'url_pre_assinada': 'DOWNLOAD'
                        }
                    )
                    data['DATA DA SOLICITAÇÃO'] = pd.to_datetime(data['DATA DA SOLICITAÇÃO'], errors='coerce')
                    data['DATA DA SOLICITAÇÃO'] = data["DATA DA SOLICITAÇÃO"].dt.strftime("%d/%m/%Y")

        except Exception as e:
            print(f'Erro ocorreu na função "load_data_copia_integral". Detalhe: {e}')
            raise Exception('Erro ao consultar no banco de dados.')
        else:
            return data