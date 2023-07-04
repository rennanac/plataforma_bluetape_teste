import pandas as pd
import streamlit as st
import metodos_com_cache_data as mcd
from datetime import datetime, timedelta
import pytz
from classe_operacao_dynamodb import OperacoesDynamoDB
from classe_operacao_s3 import OperacaoS3
from funcionalidades_app import *
from st_aggrid import AgGrid, JsCode, GridOptionsBuilder
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)



class ClienteBarcelosSOPII:
    ls_destinatario = ['robo@bluetape.com.br']
    nome_base_senha_bb_movimentacoes = 'SOPII_senhas_bb_movimentacoes'
    lista_possiveis_servicos = ['copia_integral', 'protocolo']
    nome_base_copia_integral = 'barcelos-sopii_copia-integral'
    nome_base_bbmovimentacoes = 'SOPII_bbmovimentacoes'
    nome_cliente = 'barcelos-sopii'
    nome_base_protocolo = 'barcelos-sopii_protocolo'
    nome_base_lista_endereco_site = 'lista_endereco_site'
    nome_base_depara_tipo_protocolo = 'depara_tipo_protocolo'
    nome_base_depara_tipo_arquivo = 'depara_tipo_arquivo'
    nome_bucket_protocolo = 'barcelos-sopii-protocolo'

    def painel_copia_integral(self):

        self.inserir_infomacoes_tribunais(self.nome_base_copia_integral)

        self.realizar_upload_base_dados_copia_integral()

        df = self.filtros_obrigatorios(servico='copia_integral')

        self.mostrar_resultado_copia_integral(df=df)

    def painel_bb_movimentacoes(self, nome_responsavel):

        self.alterar_senha_portal_bb_movimentacoes(nome_responsavel=nome_responsavel)

        df = self.filtros_obrigatorios(servico='bb_movimentacoes')

        self.mostrar_resultado_bb_movimentacoes(df=df)

    def painel_protocolo(self):

        self.inserir_infomacoes_tribunais(self.nome_base_protocolo)

        self.inserir_infomacoes_depara_tipo_de_protocolo()

        self.inserir_infomacoes_depara_tipo_de_arquivo()

        self.realizar_upload_arq_protocolo()

        df = self.filtros_obrigatorios(servico='protocolo')

        self.mostrar_resultado_protocolo(df=df)

    def styles(self):

        th_props = [
            ('font-size', '14px'),
            ('text-align', 'center'),
            ('font-weight', 'bold'),
            ('color', '#6d6d6d'),
            ('background-color', '#f7ffff')
        ]

        td_props = [
            ('font-size', '12px')
        ]

        styles = [
            dict(selector="th", props=th_props),
            dict(selector="td", props=td_props)
        ]

        return styles

    def alterar_senha_portal_bb_movimentacoes(self, nome_responsavel:str):

        data = mcd.selecionar_informacoes_de_acesso_ao_site_bb_movimentacoes(self.nome_base_senha_bb_movimentacoes)

        container_alterar_senha = st.expander("Alterar a senha de acesso ao portal BB Movimentações: ")
        if container_alterar_senha.button('🔁 ', key='atualizar_senha_bb_mov', help="Atualizar tabela de senha de acesso BB Movimentações."):
            mcd.selecionar_informacoes_de_acesso_ao_site_bb_movimentacoes.clear()
        container_alterar_senha.table(data.style.set_properties(**{'text-align': 'left'}).set_table_styles(self.styles()))

        with container_alterar_senha.form(key='insert', clear_on_submit=True):

            col5, col6 = st.columns(2)
            col7, col8 = st.columns(2)

            input_login = col5.text_input(label='Insira o novo login:', key='login')
            input_senha = col6.text_input(label='Insira a nova senha:', key='senha')
            button_submit = col7.form_submit_button('Enviar')

            if button_submit:
                if input_login.replace(' ', '') == '' or input_senha.replace(' ', '') == '':
                    container_alterar_senha.error('É obrigatório inserir o login e a senha!')
                else:
                    try:
                        data_atual = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%d/%m/%Y %H:%M:%S')
                        #st.write(f'ATUAL, 00:00:00, {nome_responsavel},{input_login}, {input_senha}, {data_atual}')
                        OperacoesDynamoDB().update_senha_login_bb_movimentacoes('ATUAL', '00:00:00', nome_responsavel,input_login, input_senha, data_atual)
                        try:
                            TratarEmail().enviar_email(
                                ls_destinatario=self.ls_destinatario,
                                assunto=f'NOVA ALTERAÇÃO DE SENHA BB-MOVIMENTAÇÕES - {self.nome_cliente}'.upper(),
                                mensagem=f'\n\nOcorreu nova alteração de login e senha de acesso ao site bb_movimentacoes, para validar acesse a plataforma...'

                            )
                        except Exception as e_enviar_email:
                            print(f'Ocorrreu um erro ao enviar um email. Detalhe: {e_enviar_email}')
                    except Exception:
                        container_alterar_senha.success('Erro ao inserir a senha e login!')
                    else:
                        mcd.selecionar_informacoes_de_acesso_ao_site_bb_movimentacoes.clear()
                        container_alterar_senha.success('Senha e login inseridos com sucesso!')

    def realizar_upload_base_dados_copia_integral(self):

        container_upload = st.expander('Upload base de dados para copia integral. ')
        container_upload.write(
            'Atenção, ao fazer upload da base da cópia integrel, verifique se a base possuí as seguintes características:')
        container_upload.write(
            '- Nome da coluna "n_processo". (COLUNA OBRIGATÓRIA)')
        container_upload.write('- Nome da coluna "instancia". (COLUNA NÃO OBRIGATÓRIA)')
        container_upload.write(
            '- Nome da coluna "tribunal". (COLUNA NÃO OBRIGATÓRIA)')
        container_upload.write(
            '- Nome da coluna "sistema". (COLUNA NÃO OBRIGATÓRIA)')

        with container_upload.form("my-form", clear_on_submit=True):
            uploaded_file = st.file_uploader('Upload base de dados para copia integral.', type='xlsx')
            submitted = st.form_submit_button("📤 UPLOAD", help='Realizar o upload!')
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

                dataframe = dataframe.loc[
                    (dataframe["n_processo"].fillna('').str.replace(' ', '') != ''),
                    lista_colunas_existentes
                ].drop_duplicates().fillna('')
                tamanho_dataframe = len(dataframe)
                if tamanho_dataframe == 0:
                    st.error('Erro: Não identificou nenhum processo no excel inserido.')
                    return

                df_lista_endereco_site = OperacoesDynamoDB().get_table_filter(self.nome_base_lista_endereco_site, 'cliente_servico', self.nome_base_copia_integral)
                data_atual = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y/%m/%d')
                df_lista_pesquisa_atual = OperacoesDynamoDB().get_table_filter(self.nome_base_copia_integral, 'datasolicitacao', data_atual)

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
                    return
                else:
                    st.success('Dados do excel foram gravados com sucesso!')


            elif submitted and uploaded_file is None:
                st.error('Upload não realizado. Para fazer upload insira um arquivo valido.')

    def finalizar_tratativas_copia_integral(self, df_a_ser_tratado: pd.DataFrame, df_ja_tratado: pd.DataFrame) -> pd.DataFrame:

        df_a_ser_tratado['datasolicitacao'] = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y/%m/%d')
        df_a_ser_tratado['nprocesso_sistema_tribunal_instancia'] = df_a_ser_tratado['n_processo'].map(str) + '_' + \
                                                                    df_a_ser_tratado['sistema'].map(str) + '_' + \
                                                                    df_a_ser_tratado['tribunal'].map(str) + '_' + \
                                                                    df_a_ser_tratado['instancia'].map(str)
        df_a_ser_tratado.drop_duplicates(subset=['nprocesso_sistema_tribunal_instancia', 'datasolicitacao'],keep=False, inplace=True)

        if len(df_ja_tratado) == 0:
            return df_a_ser_tratado

        df_a_ser_tratado.drop(columns=['copia_integral', 'data_resultado', 'detalhe', 'url_pre_assinada'], inplace=True)

        df = pd.merge(
            df_ja_tratado.loc[
                (df_ja_tratado["n_processo"].fillna('').str.replace(' ', '') != ''),
                df_ja_tratado.columns
            ],
            df_a_ser_tratado, how='outer', on=list(df_a_ser_tratado.columns))

        df = df.fillna('')

        return df

    def tratar_dados_copia_integral(self, data_inicio, data_fim) -> pd.DataFrame:
        try:
            data = pd.DataFrame(
                columns=['DATA DA SOLICITAÇÃO', 'Nº DO PROCESSO', 'CÓD. TRIBUNAL', 'SISTEMA', 'TRIBUNAL', 'INSTÂNCIA', 'DATA RESULTADO',
                         'RESULTADO', 'ERRO', 'DOWNLOAD'])
            data_inicio_usada = data_inicio.strftime('%Y/%m/%d')
            data_fim_usada = data_fim.strftime('%Y/%m/%d')
            if data_inicio_usada <= data_fim_usada:
                data_1 = mcd.selecionar_resultado(self.nome_base_copia_integral, data_inicio_usada, data_fim_usada)
                if len(data_1) > 0:
                    data = data_1.loc[
                        (data_1["n_processo"].fillna('') != ''),
                        [
                            'datasolicitacao',
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
                    #data['DATA RESULTADO'] = pd.to_datetime(data['DATA RESULTADO'], errors='coerce')
                    #data['DATA RESULTADO'] = data["DATA RESULTADO"].dt.strftime("%d/%m/%Y %H:%M:%S")

        except Exception as e:
            print(f'Erro ocorreu na função "load_data_copia_integral". Detalhe: {e}')
            raise Exception('Erro ao consultar no banco de dados.')
        else:
            return data

    def gerar_estatistica_copia_integral(self, df:pd.DataFrame) -> pd.DataFrame:
        try:
            if len(df) > 0:
                df_estatistica = df.groupby(['RESULTADO', 'DATA DA SOLICITAÇÃO'], as_index=False)['TRIBUNAL'].count()
                df_estatistica = pd.DataFrame(df_estatistica)
                df_estatistica['PORCENTAGEM (%)'] = (df_estatistica['TRIBUNAL'] / df_estatistica[
                    'TRIBUNAL'].sum()) * 100

            else:
                df_estatistica = pd.DataFrame(
                    columns=['RESULTADO', 'DATA DA SOLICITAÇÃO', 'TRIBUNAL', 'PORCENTAGEM (%)'])

            df_estatistica.columns = ['STATUS', 'DATA DA SOLICITAÇÃO', 'QUANTIDADE', 'PORCENTAGEM (%)']
            df_estatistica.loc[
                (df_estatistica['STATUS'].fillna('').str.replace(' ', '') == ''),
                'STATUS'
            ] = 'PENDENTE'
        except Exception as e:
            raise Exception(f'Erro ocorreu na função "load_data_estatistica_copia_integral". Detalhe: {e}')
        else:
            return df_estatistica

    def tratar_dados_bb_movimentacoes(self, data_inicio, data_fim) -> pd.DataFrame:
        try:
            data = pd.DataFrame(
                columns=['DATA DA SOLICITAÇÃO','DATA RESULTADO', 'CONTRATO', 'TIPO MOVIMENTAÇÃO', 'RESULTADO','DOWNLOAD'])
            data_inicio_usada = data_inicio.strftime('%Y/%m/%d')
            data_fim_usada = data_fim.strftime('%Y/%m/%d')
            if data_inicio_usada <= data_fim_usada:
                data_1 = mcd.selecionar_resultado(self.nome_base_bbmovimentacoes, data_inicio_usada, data_fim_usada)
                if 'url_pre_assinada' not in data_1.columns:
                    data_1['url_pre_assinada'] = ''
                if len(data_1) > 0:
                    data = data_1.loc[
                        (data_1["datasolicitacao"].fillna('').str.replace(' ','') != ''),
                        [
                            'datasolicitacao',
                            'status_resultado',
                            'hora_execucao',
                            'contrato',
                            'tipo_movimentacao',
                            'url_pre_assinada'
                        ]
                    ].fillna('')
                    data = data.rename(
                        columns=
                        {
                            'datasolicitacao':'DATA DA SOLICITAÇÃO',
                            'hora_execucao':'HORA RESULTADO',
                            'contrato': 'CONTRATO',
                            'tipo_movimentacao': 'TIPO MOVIMENTAÇÃO',
                            'status_resultado':'RESULTADO',
                            'url_pre_assinada':'DOWNLOAD'
                        }
                    )
                    data['HORA RESULTADO'] = pd.to_datetime(data['HORA RESULTADO'], errors='coerce')
                    data['HORA RESULTADO'] = data["HORA RESULTADO"].dt.strftime("%H:%M")
                    data['DATA DA SOLICITAÇÃO'] = pd.to_datetime(data['DATA DA SOLICITAÇÃO'], errors='coerce')
                    data['DATA DA SOLICITAÇÃO'] = data['DATA DA SOLICITAÇÃO'].dt.strftime("%d/%m/%Y")

        except Exception as e:
            raise Exception(f'Erro ocorreu na função "load_data_bb_movimentacoes". Detalhe: {e}')
        else:
            return data

    def gerar_estatistica_bb_movimentacoes(self, df:pd.DataFrame) -> pd.DataFrame:
        try:
            if len(df) > 0:
                df['QUANTIDADE'] = df['RESULTADO']
                df_estatistica = df.groupby(['RESULTADO','DATA DA SOLICITAÇÃO'], as_index=False)['QUANTIDADE'].count()
                df_estatistica = pd.DataFrame(df_estatistica)

                df_estatistica['PORCENTAGEM (%)'] = 100 * df_estatistica['QUANTIDADE'] / df_estatistica.groupby('DATA DA SOLICITAÇÃO')[
                    'QUANTIDADE'].transform('sum')
                df_estatistica['PORCENTAGEM (%)'] = df_estatistica['PORCENTAGEM (%)'].round(2)

                df_estatistica = df_estatistica.sort_values(by=['DATA DA SOLICITAÇÃO'])
                df_estatistica.reset_index(drop=True, inplace=True)
            else:
                df_estatistica = pd.DataFrame(
                    columns=['RESULTADO','DATA DA SOLICITAÇÃO','QUANTIDADE','PORCENTAGEM (%)'])

            df_estatistica.columns = ['RESULTADO','DATA DA SOLICITAÇÃO','QUANTIDADE','PORCENTAGEM (%)']
            df_estatistica = df_estatistica.loc[
                        (df_estatistica['RESULTADO'].fillna('').str.replace(" ", "") != ''),
                        [
                            'RESULTADO',
                            'DATA DA SOLICITAÇÃO',
                            'QUANTIDADE',
                            'PORCENTAGEM (%)'
                        ]
                    ].drop_duplicates()

            df_estatistica.loc[
                (df_estatistica['RESULTADO'].fillna('').str.replace(' ', '') == ''),
                'RESULTADO'
            ] = 'PENDENTE'
        except Exception as e:
            raise Exception(f'Erro ocorreu na função "load_data_estatistica_bb_movimentacoes". Detalhe: {e}')
        else:
            return df_estatistica

    def agrupar_dados_bb_movimentacoes(self, df:pd.DataFrame) -> pd.DataFrame:
        try:
            if len(df) > 0:

                df_estatistica = df.groupby(['RESULTADO','DATA DA SOLICITAÇÃO','HORA RESULTADO','DOWNLOAD'], as_index=False)['CONTRATO'].count()
                df_estatistica = pd.DataFrame(df_estatistica)

                df_estatistica=df_estatistica.sort_values(by=['DATA DA SOLICITAÇÃO','HORA RESULTADO'])
                df_estatistica.reset_index(drop=True, inplace=True)
            else:
                df_estatistica = pd.DataFrame(
                    columns=['RESULTADO','DATA DA SOLICITAÇÃO','HORA RESULTADO','QUANTIDADE','DOWNLOAD'])

            df_estatistica.columns = ['RESULTADO','DATA DA SOLICITAÇÃO','HORA RESULTADO','DOWNLOAD','QUANTIDADE']
            df_estatistica = df_estatistica.loc[
                        (df_estatistica['RESULTADO'].fillna('').str.replace(" ", "") != ''),
                        [
                            'RESULTADO',
                            'DATA DA SOLICITAÇÃO',
                            'HORA RESULTADO',
                            'QUANTIDADE',
                            'DOWNLOAD'
                        ]
                    ].drop_duplicates()

            df_estatistica.loc[
                (df_estatistica['RESULTADO'].fillna('').str.replace(' ', '') == ''),
                'RESULTADO'
            ] = 'PENDENTE'
        except Exception as e:
            raise Exception(f'Erro ocorreu na função "load_data_estatistica_bb_movimentacoes". Detalhe: {e}')
        else:
            return df_estatistica

    def filtros_obrigatorios(self, servico:str) -> pd.DataFrame:
        df = pd.DataFrame()
        try:
            st.subheader("Filtros Obrigatórios: ")
            col3, col4 = st.columns(2)

            data_fim = datetime.now(pytz.timezone('America/Sao_Paulo'))
            data_inicio = data_fim + timedelta(days=-1)
            start_date = col3.date_input('Data inicio', data_inicio)
            end_date = col4.date_input('Data fim', data_fim)
            if start_date <= end_date:
                col3.text(f"Data inicio: {start_date.strftime('%d/%m/%Y')}")
                col4.text(f"Data Fim: {end_date.strftime('%d/%m/%Y')}")
            else:
                st.error('Erro: A data inicio não pode suceder a data fim.')

            try:
                df = self.__getattribute__(f"tratar_dados_{str(servico)}")(start_date, end_date)
            except Exception as e_load_data:
                print(f'Erro: {e_load_data}')
                raise Exception(e_load_data)
        except Exception as e_filtro:
            st.error(f'Erro: {e_filtro}')

        return df

    def mostrar_resultado_bb_movimentacoes(self, df:pd.DataFrame):

        st.subheader("Resultado BB movimentações: ")
        df = df.fillna('')
        try:
            df_resultado = self.agrupar_dados_bb_movimentacoes(df)
        except Exception as e_df_estatistica:
            st.sidebar.error(f'Erro1: {e_df_estatistica}.')
            df_resultado = pd.DataFrame()

        gb = GridOptionsBuilder.from_dataframe(df_resultado)

        gb.configure_column(
            "DOWNLOAD",
            headerName="DOWNLOAD",
            cellRenderer=JsCode("""
                class UrlCellRenderer {
                  init(params) {
                    this.eGui = document.createElement('a');
                    if (params.data.DOWNLOAD != ''){
                        this.eGui.innerText = '📥';
                        this.eGui.setAttribute('href', params.value);
                        this.eGui.setAttribute('style', "text-decoration:none");
                        this.eGui.setAttribute('target', "_blank");
                    }else{
                        this.eGui.innerText = '❌';
                    }
                  }
                  getGui() {
                    return this.eGui;
                  }
                }
            """)
        )

        change_color = JsCode("""
                                                  function(params){
                                                    if(params.data.RESULTADO == 'ERRO'){

                                                           return {
                                                                'color':'#FF0000'
                                                           }
                                                   }           
                                                   }
                                        """)

        gridOptions = gb.build()
        gridOptions['getRowStyle'] = change_color

        AgGrid(df_resultado, gridOptions=gridOptions, allow_unsafe_jscode=True, fit_columns_on_grid_load=True)

        st.subheader("Detalhes do resultado BB movimentações: ")
        # table
        try:
            df = self.filtros_complementares(df, 'HORA RESULTADO', "%H:%M")
        except Exception as e_filter:
            st.sidebar.error(f'Erro3: {e_filter}.')
        else:
            pass

        try:
            df_estatistica = self.gerar_estatistica_bb_movimentacoes(df_resultado)
        except Exception as e_df_estattistica:
            st.sidebar.error(f'Erro1: {e_df_estattistica}.')
            df_estatistica = pd.DataFrame()
        try:
            df.drop(columns=['DOWNLOAD'], inplace=True)
        except:
            pass
        try:
            df_xlsx = FuncionalidadesAPP().to_excel(df, df_estatistica)
        except Exception as e_to_excel:
            df_xlsx = pd.DataFrame()
            st.sidebar.error(f'Erro4: {e_to_excel}.')
        if len(df_estatistica) > 0:
            desabilitar = False
            msg = 'Download Disponível.'
        else:
            desabilitar = True
            msg = 'A tabela de resultados esta vizia.'

        st.download_button(label='📥 Download Excel', data=df_xlsx,
                           file_name=f"resultado_bb_movimentacoes.xlsx",
                           disabled=desabilitar, help=msg)

        container_resultado = st.expander("Resultado: ")
        container_resultado.table(df.style.set_properties(**{'text-align': 'left'}).set_table_styles(self.styles()))
        container_estatistica = st.expander("Estatísticas: ")
        container_estatistica.table(
            df_estatistica.style.set_properties(**{'text-align': 'left'}).set_table_styles(self.styles()))

    def mostrar_resultado_copia_integral(self, df:pd.DataFrame):
        df = df.fillna('')
        st.subheader("Resultado cópia integral: ")
        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_column(
            "DOWNLOAD",
            headerName="DOWNLOAD",
            cellRenderer=JsCode("""
                        class UrlCellRenderer {
                          init(params) {
                            this.eGui = document.createElement('a');
                            if (params.data.DOWNLOAD != ''){
                                this.eGui.innerText = '📥';
                                this.eGui.setAttribute('href', params.value);
                                this.eGui.setAttribute('style', "text-decoration:none");
                                this.eGui.setAttribute('target', "_blank");
                            }else{
                                this.eGui.innerText = '❌';
                            }
                          }
                          getGui() {
                            return this.eGui;
                          }
                        }
                    """)
        )

        change_color = JsCode("""
                                              function(params){
                                                if(params.data.ERRO != ''){

                                                       return {
                                                            'color':'#FF0000'
                                                       }
                                               }           
                                               }
                                    """)

        gridOptions = gb.build()
        gridOptions['getRowStyle'] = change_color

        AgGrid(df, gridOptions=gridOptions, allow_unsafe_jscode=True, fit_columns_on_grid_load=True)
        st.subheader("Detalhes do resultado cópia integral: ")

        # table
        try:
            df_resultado = self.filtros_complementares(df, 'DATA RESULTADO', "%d/%m/%Y %H:%M:%S")
        except Exception as e_filter:
            st.sidebar.error(f'Erro: {e_filter}.')
            df_resultado = pd.DataFrame()
        try:
            df_estatistica = self.gerar_estatistica_copia_integral(df_resultado)
        except Exception as e_df_estattistica:
            st.sidebar.error(f'Erro: {e_df_estattistica}.')
            df_estatistica = pd.DataFrame()
            
        try:
            df_resultado.drop(columns=['DOWNLOAD'], inplace=True)
        except:
            pass

        try:
            df_xlsx = FuncionalidadesAPP().to_excel(df_resultado, df_estatistica)
        except Exception as e_to_excel:
            st.sidebar.error(f'Erro: {e_to_excel}.')
            df_xlsx = pd.DataFrame()
        if len(df_estatistica) > 0:
            desabilitar = False
            msg = 'Download Disponível.'
        else:
            desabilitar = True
            msg = 'A tabela de resultados esta vizia.'

        st.download_button(label='📥 Download Excel', data=df_xlsx,
                           file_name=f"resultado_copia_integral.xlsx",
                           disabled=desabilitar, help=msg)

        container_resultado = st.expander("Resultado: ")
        container_resultado.table(df_resultado.style.set_properties(**{'text-align': 'left'}).set_table_styles(self.styles()))

        container_estatistica = st.expander("Estatísticas: ")
        container_estatistica.table(
            df_estatistica.style.set_properties(**{'text-align': 'left'}).set_table_styles(self.styles()))

    def filtros_complementares(self, df:pd.DataFrame, coluna_complementar_data:str, mascara_data:str) -> pd.DataFrame:
        """
        Adds a UI on top of a dataframe to let viewers filter columns

        Args:
            df (pd.DataFrame): Original dataframe

        Returns:
            pd.DataFrame: Filtered dataframe
        """
        modify = st.checkbox("Filtros Complementares")

        if not modify:
            return df

        df = df.copy()

        # Try to convert datetimes into a standard format (datetime, no timezone)
        for col in df.columns:
            if is_object_dtype(df[col]):
                try:
                    df[col] = pd.to_datetime(df[col])
                except Exception:
                    pass

            if is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(None)

        modification_container = st.container()

        with modification_container:
            to_filter_columns = st.multiselect("Selecione colunas para filtrar:", df.columns)
            for column in to_filter_columns:
                left, right = st.columns((1, 20))
                # Treat columns with < 10 unique values as categorical
                if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                    df['DATA DA SOLICITAÇÃO'] = pd.to_datetime(df['DATA DA SOLICITAÇÃO'], errors='coerce')
                    df['DATA DA SOLICITAÇÃO'] = df['DATA DA SOLICITAÇÃO'].dt.strftime("%d/%m/%Y")
                    df[coluna_complementar_data] = pd.to_datetime(df[coluna_complementar_data], errors='coerce')
                    df[coluna_complementar_data] = df[coluna_complementar_data].dt.strftime(mascara_data)
                    user_cat_input = right.multiselect(
                        f"Marque o/a {column}",
                        df[column].unique(),
                        default=list(df[column].unique()),
                    )
                    df = df[df[column].isin(user_cat_input)]
                elif is_numeric_dtype(df[column]):
                    _min = float(df[column].min())
                    _max = float(df[column].max())
                    step = (_max - _min) / 100
                    user_num_input = right.slider(
                        f"Marque o/a {column}",
                        min_value=_min,
                        max_value=_max,
                        value=(_min, _max),
                        step=step,
                    )
                    df = df[df[column].between(*user_num_input)]
                elif is_datetime64_any_dtype(df[column]):
                    user_date_input = right.date_input(
                        f"Marque o/a {column}",
                        value=(
                            df[column].min(),
                            df[column].max(),
                        ),
                    )
                    if len(user_date_input) == 2:
                        user_date_input = tuple(map(pd.to_datetime, user_date_input))
                        start_date, end_date = user_date_input
                        df = df.loc[df[column].between(start_date, end_date)]
                else:
                    user_text_input = right.text_input(
                        f"Informe o {column}",
                    )
                    if user_text_input:
                        df = df[df[column].astype(str).str.contains(user_text_input)]
        return df

    def realizar_upload_arq_protocolo(self):

        container_upload_protocolo = st.expander('Insira os arquivos para protocolar.')

        with container_upload_protocolo.form(key='form_upload_arquivos_protocolo', clear_on_submit=True):

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

    def reset_button(self):
        st.session_state['edit_table'] = False

    def inserir_infomacoes_tribunais(self, nome_base_cliente_servico:str):

        container_insert = st.expander('Inserir infomacões tribunais. ')

        with container_insert.form("form_inserir_informacoes_tribunais_protocolo", clear_on_submit=True):
            uploaded_file = st.file_uploader('Upload informações de tribanais. ', type='xlsx')
            submitted = st.form_submit_button("UPLOAD", help='Realizar o upload!')
            if submitted and uploaded_file is not None:
                dataframe = pd.read_excel(uploaded_file, dtype=str)
                try:
                    if 'instancia' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "instancia".')
                    if 'tribunal' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "tribunal".')
                    if 'sistema' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "sistema".')
                    if 'cod_tribunal' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "cod_tribunal".')
                    if 'url' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "url".')
                    if 'login' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "login".')
                    if 'senha' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "senha".')
                    if 'validado' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "validado".')
                    if 'habilitado' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "habilitado".')

                    dataframe = dataframe.loc[
                        (dataframe["tribunal"].fillna('').str.replace(' ', '') != ''),
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
                    dataframe['sistema_tribunal_instancia'] = dataframe['sistema'].map(str) + '_' + dataframe[
                        'tribunal'].map(
                        str) + '_' + dataframe['instancia'].map(str)
                    dataframe['cliente_servico'] = nome_base_cliente_servico
                    try:
                        OperacoesDynamoDB().put_table(self.nome_base_lista_endereco_site, dataframe)
                    except Exception as e_put_table:
                        print(e_put_table)
                        raise Exception(f'Erro: Não foi possivel gravar os dados do excel.')
                except Exception as e:
                    st.error(e)
                else:
                    st.success('Dados do excel foram gravados com sucesso!')

            elif submitted and uploaded_file is None:
                st.error('Upload não realizado. Para fazer upload insira um arquivo valido.')

        if container_insert.button('🔁 ', key='atualizar_tribunais', help="Atualizar dados tribunais.", on_click=self.reset_button):
            mcd.selecionar_infomacoes_tribunais.clear()

        alterar_table = container_insert.checkbox("Editar tabela.", key='edit_table')

        if alterar_table:
            container_insert.write('Atenção: As células com a cor cinza claro são editáveis.')
            reload_data = False
            use_checkbox = True
            editable = True
            disabled_button = False
            cellsytle_jscode = JsCode("""
                        function(params) {

                            return {
                                'color': 'black',
                                'backgroundColor': 'lightgray'
                            }

                        };
                        """)
        else:
            reload_data = True
            use_checkbox = False
            editable = False
            disabled_button = True
            cellsytle_jscode = JsCode("""
                        function(params) {

                            return {
                                'color': 'black',
                                'backgroundColor': 'white'
                            }

                        };
                        """)



        df = mcd.selecionar_infomacoes_tribunais(self.nome_base_lista_endereco_site, nome_base_cliente_servico)
        with container_insert.form("form_alterar_informacoes_tribunais_protocolo"):
            builder = GridOptionsBuilder.from_dataframe(df)

            builder.configure_column("sistema_tribunal_instancia", hide=True)
            builder.configure_column("cliente_servico", hide=True)
            builder.configure_column("validado", header_name="validado", editable=editable, cellStyle=cellsytle_jscode)
            builder.configure_column("habilitado", header_name="habilitado", editable=editable, cellStyle=cellsytle_jscode)
            builder.configure_column("senha", header_name="senha", editable=editable, cellStyle=cellsytle_jscode)
            builder.configure_column("login", header_name="login", editable=editable, cellStyle=cellsytle_jscode)
            builder.configure_selection('multiple', use_checkbox=use_checkbox)
            builder.configure_grid_options()

            go = builder.build()

            # uses the gridOptions dictionary to configure AgGrid behavior.
            grid_response = AgGrid(
                df,
                gridOptions=go,
                height=200,
                width='100%',
                reload_data=reload_data,
                theme='fresh',
                allow_unsafe_jscode=True
            )

            bt_gravar = st.form_submit_button("GRAVAR", help='Gravar resultados!', disabled=disabled_button)

            if bt_gravar:
                try:
                    OperacoesDynamoDB().put_table(self.nome_base_lista_endereco_site, grid_response['data'])
                except Exception as e_put_table:
                    print(e_put_table)
                    raise Exception(f'Erro: Não foi possivel gravar os dados do excel.')
                else:
                    mcd.selecionar_infomacoes_tribunais.clear()
                    st.success('Os dados  de acesso dos tribunais foram alterados com sucesso!')

            bt_deletar = st.form_submit_button("DELETAR", help='Deletar resultados relecionados!',disabled=disabled_button)
            if bt_deletar:
                df_deletar = pd.DataFrame(grid_response['selected_rows'])
                st.write(df_deletar)
                if len(df_deletar) > 0:
                    df_deletar = df_deletar[[
                            "sistema_tribunal_instancia",
                            "cliente_servico"
                        ]
                    ]
                    dicionario = df_deletar.to_dict('records')
                    try:
                        OperacoesDynamoDB().delete_itens(self.nome_base_lista_endereco_site, dicionario)
                    except Exception as e_delete:
                        print(e_delete)
                        st.error('Erro ao excluir os dados de acesso dos tribunais.')
                    else:
                        mcd.selecionar_infomacoes_tribunais.clear()
                        st.success('Os dados  de acesso dos tribunais foram excluídos com sucesso!')
                else:
                    st.error('Não foi indicado nenhum tribunal para ser excluido.')

    def inserir_infomacoes_depara_tipo_de_protocolo(self):

        container_upload = st.expander('Upload infomacões depara tipo de protocolo. ')

        with container_upload.form("form_inserir_informacoes_depara_tipo_protocolo", clear_on_submit=True):

            uploaded_file = st.file_uploader('Upload informações depara tipo de protocolo. ', type='xlsx')
            submitted = st.form_submit_button("UPLOAD", help='Realizar o upload!')
            if submitted and uploaded_file is not None:
                dataframe = pd.read_excel(uploaded_file, dtype=str)
                try:

                    if 'tribunal' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "tribunal".')
                    if 'sistema' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "sistema".')
                    if 'nome_arquivo_original' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "nome_arquivo_original".')
                    if 'Tipo do protocolo' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "Tipo do protocolo".')

                    dataframe = dataframe.loc[
                        (dataframe["tribunal"].fillna('').str.replace(' ', '') != ''),
                        [
                            "tribunal",
                            "sistema",
                            "nome_arquivo_original",
                            "Tipo do protocolo"
                        ]
                    ].drop_duplicates().fillna('')
                    dataframe['nome_arquivo'] = dataframe['nome_arquivo_original'].str.normalize('NFKD').str.encode(
                        'ascii',
                        errors='ignore').str.decode(
                        'utf-8').str.replace(' ', '').str.lower()

                    dataframe['sistema_tribunal_nome_arquivo'] = dataframe['sistema'].map(str) + '_' + dataframe[
                        'tribunal'].map(str) + '_' + dataframe['nome_arquivo'].map(str)
                    dataframe['cliente'] = self.nome_cliente

                    dataframe['valores duplicados'] = dataframe['sistema_tribunal_nome_arquivo']

                    df_cont_duplicados = dataframe.groupby(['sistema_tribunal_nome_arquivo'])['valores duplicados'].count().reset_index()
                    df_cont_duplicados = df_cont_duplicados.loc[
                        (df_cont_duplicados["valores duplicados"] > 1),
                        [
                            "sistema_tribunal_nome_arquivo"
                        ]
                    ]
                    dataframe.drop(columns=['valores duplicados'], inplace=True)

                    if len(df_cont_duplicados) > 0:
                        container_upload.dataframe(df_cont_duplicados)
                        raise Exception(f'Não foi possivel inserir dados do de/para tipo de protocolo. Foi identificados {len(df_cont_duplicados)} dados  de '
                                        f'"sistema_tribunal_nome_arquivo" iguais.')
                    else:
                        try:
                            OperacoesDynamoDB().put_table(self.nome_base_depara_tipo_protocolo, dataframe)
                        except Exception as e_put_table:
                            print(e_put_table)
                            raise Exception('Erro: Não foi possivel gravar os dados do excel.')
                except Exception as e:
                    st.error(e)
                else:
                    st.success('Dados do excel foram gravados com sucesso!')
            elif submitted and uploaded_file is None:
                st.error('Upload não realizado. Para fazer upload insira um arquivo valido.')
        if container_upload.button('🔁 ', key='atualizar_tipo_protocolo', help="Atualizar tabela de/para tipo de prrotocolo."):
            mcd.selecionar_infomacoes_depara_tipo_de_protocolo.clear()

        alterar_table = container_upload.checkbox("Editar tabela.", key='edit_table_tipo_protocolo')

        if alterar_table:
            container_upload.write('Atenção: As células com a cor cinza claro são editáveis.')
            reload_data = False
            use_checkbox = True
            editable = True
            disabled_button = False
            cellsytle_jscode = JsCode("""
                        function(params) {

                            return {
                                'color': 'black',
                                'backgroundColor': 'lightgray'
                            }

                        };
                        """)
        else:
            reload_data = True
            use_checkbox = False
            editable = False
            disabled_button = True
            cellsytle_jscode = JsCode("""
                        function(params) {

                            return {
                                'color': 'black',
                                'backgroundColor': 'white'
                            }

                        };
                        """)

        df = mcd.selecionar_infomacoes_depara_tipo_de_protocolo(self.nome_base_depara_tipo_protocolo, self.nome_cliente)

        with container_upload.form("form_alterar_informacoes_tipo_protocolo"):
            builder = GridOptionsBuilder.from_dataframe(df)
            builder.configure_column("sistema_tribunal_nome_arquivo", hide=True)
            builder.configure_column("nome_arquivo", hide=True)
            builder.configure_column("cliente", hide=True)
            builder.configure_column("Tipo do protocolo", header_name="Tipo do protocolo", editable=editable, cellStyle=cellsytle_jscode)
            builder.configure_selection('multiple', use_checkbox=use_checkbox)
            builder.configure_grid_options()

            go = builder.build()

            # uses the gridOptions dictionary to configure AgGrid behavior.
            grid_response = AgGrid(
                df,
                gridOptions=go,
                height=200,
                width='100%',
                reload_data=reload_data,
                theme='fresh',
                allow_unsafe_jscode=True
            )

            bt_gravar = st.form_submit_button("GRAVAR", help='Gravar resultados!', disabled=disabled_button)

            if bt_gravar:
                try:
                    OperacoesDynamoDB().put_table(self.nome_base_depara_tipo_protocolo, grid_response['data'])
                except Exception as e_put_table:
                    print(e_put_table)
                    raise Exception(f'Erro: Não foi possivel gravar os dados do excel.')
                else:
                    mcd.selecionar_infomacoes_tribunais.clear()
                    st.success('Os dados  de acesso dos tribunais foram alterados com sucesso!')

            bt_deletar = st.form_submit_button("DELETAR", help='Deletar resultados relecionados!',
                                               disabled=disabled_button)
            if bt_deletar:
                df_deletar = pd.DataFrame(grid_response['selected_rows'])
                st.write(df_deletar)
                if len(df_deletar) > 0:
                    df_deletar = df_deletar[[
                        "sistema_tribunal_nome_arquivo",
                        "cliente"
                    ]
                    ]
                    dicionario = df_deletar.to_dict('records')
                    try:
                        OperacoesDynamoDB().delete_itens(self.nome_base_depara_tipo_protocolo, dicionario)
                    except Exception as e_delete:
                        print(e_delete)
                        st.error('Erro ao excluir os dados de acesso dos tribunais.')
                    else:
                        mcd.selecionar_infomacoes_tribunais.clear()
                        st.success('Os dados  de acesso dos tribunais foram excluídos com sucesso!')
                else:
                    st.error('Não foi indicado nenhum tribunal para ser excluido.')


    def inserir_infomacoes_depara_tipo_de_arquivo(self):

        container_upload = st.expander('Upload infomacões depara tipo de arquivo. ')

        with container_upload.form("form_inserir_informacoes_depara_tipo_arquivo", clear_on_submit=True):

            uploaded_file = st.file_uploader('Upload informações depara tipo de arquivo. ', type='xlsx')
            submitted = st.form_submit_button("UPLOAD", help='Realizar o upload!')
            if submitted and uploaded_file is not None:
                dataframe = pd.read_excel(uploaded_file, dtype=str)
                try:

                    if 'tribunal' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "tribunal".')
                    if 'sistema' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "sistema".')
                    if 'nome_arquivo_original' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "nome_arquivo_original".')
                    if 'Tipo arquivo' not in dataframe.columns:
                        raise Exception(f'O excel não possui a coluna obrigatória "Tipo arquivo".')

                    dataframe = dataframe.loc[
                        (dataframe["tribunal"].fillna('').str.replace(' ', '') != ''),
                        [
                            "tribunal",
                            "sistema",
                            "nome_arquivo_original",
                            "Tipo arquivo"
                        ]
                    ].drop_duplicates().fillna('')
                    dataframe['nome_arquivo'] = dataframe['nome_arquivo_original'].str.normalize('NFKD').str.encode(
                        'ascii',
                        errors='ignore').str.decode(
                        'utf-8').str.replace(' ', '').str.lower()

                    dataframe['sistema_tribunal_nome_arquivo'] = dataframe['sistema'].map(str) + '_' + dataframe[
                        'tribunal'].map(
                        str) + '_' + dataframe['nome_arquivo'].map(str)
                    dataframe['cliente'] = self.nome_cliente
                    dataframe.drop_duplicates(subset=['sistema_tribunal_nome_arquivo', 'cliente'],keep=False, inplace=True)
                    try:
                        OperacoesDynamoDB().put_table(self.nome_base_depara_tipo_arquivo, dataframe)
                    except Exception as e_put_table:
                        print(e_put_table)
                        raise Exception(f'Erro: Não foi possivel gravar os dados do excel.')
                except Exception as e:
                    st.error(e)
                else:
                    st.success('Dados do excel foram gravados com sucesso!')
            elif submitted and uploaded_file is None:
                st.error('Upload não realizado. Para fazer upload insira um arquivo valido.')
        if container_upload.button('🔁 ', key='atualizar_tipo_arquivo', help="Atualizar tabela de/para tipo arquivo."):
            mcd.selecionar_infomacoes_depara_tipo_de_arquivo.clear()


        alterar_table = container_upload.checkbox("Editar tabela.", key='edit_table_tipo_arquivo')

        if alterar_table:
            container_upload.write('Atenção: As células com a cor cinza claro são editáveis.')
            reload_data = False
            use_checkbox = True
            editable = True
            disabled_button = False
            cellsytle_jscode = JsCode("""
                                function(params) {

                                    return {
                                        'color': 'black',
                                        'backgroundColor': 'lightgray'
                                    }

                                };
                                """)
        else:
            reload_data = True
            use_checkbox = False
            editable = False
            disabled_button = True
            cellsytle_jscode = JsCode("""
                                function(params) {

                                    return {
                                        'color': 'black',
                                        'backgroundColor': 'white'
                                    }

                                };
                                """)

        df = mcd.selecionar_infomacoes_depara_tipo_de_arquivo(self.nome_base_depara_tipo_arquivo,self.nome_cliente)

        with container_upload.form("form_alterar_informacoes_tipo_arquivo"):
            builder = GridOptionsBuilder.from_dataframe(df)
            builder.configure_column("sistema_tribunal_nome_arquivo", hide=True)
            builder.configure_column("nome_arquivo", hide=True)
            builder.configure_column("cliente", hide=True)
            builder.configure_column("Tipo arquivo", header_name="Tipo arquivo", editable=editable,
                                     cellStyle=cellsytle_jscode)
            builder.configure_selection('multiple', use_checkbox=use_checkbox)
            builder.configure_grid_options()

            go = builder.build()

            # uses the gridOptions dictionary to configure AgGrid behavior.
            grid_response = AgGrid(
                df,
                gridOptions=go,
                height=200,
                width='100%',
                reload_data=reload_data,
                theme='fresh',
                allow_unsafe_jscode=True
            )

            bt_gravar = st.form_submit_button("GRAVAR", help='Gravar resultados!', disabled=disabled_button)

            if bt_gravar:
                try:
                    OperacoesDynamoDB().put_table(self.nome_base_depara_tipo_protocolo, grid_response['data'])
                except Exception as e_put_table:
                    print(e_put_table)
                    raise Exception(f'Erro: Não foi possivel gravar os dados do excel.')
                else:
                    mcd.selecionar_infomacoes_tribunais.clear()
                    st.success('Os dados  de acesso dos tribunais foram alterados com sucesso!')

            bt_deletar = st.form_submit_button("DELETAR", help='Deletar resultados relecionados!',
                                               disabled=disabled_button)
            if bt_deletar:
                df_deletar = pd.DataFrame(grid_response['selected_rows'])
                st.write(df_deletar)
                if len(df_deletar) > 0:
                    df_deletar = df_deletar[[
                        "sistema_tribunal_nome_arquivo",
                        "cliente"
                    ]
                    ]
                    dicionario = df_deletar.to_dict('records')
                    try:
                        OperacoesDynamoDB().delete_itens(self.nome_base_depara_tipo_protocolo, dicionario)
                    except Exception as e_delete:
                        print(e_delete)
                        st.error('Erro ao excluir os dados de acesso dos tribunais.')
                    else:
                        mcd.selecionar_infomacoes_tribunais.clear()
                        st.success('Os dados  de acesso dos tribunais foram excluídos com sucesso!')
                else:
                    st.error('Não foi indicado nenhum tribunal para ser excluido.')

    def inserir_informacoes_de_tribunais(self, df_lista_endereco_site: pd.DataFrame, df_lista_pesquisa: pd.DataFrame,
                                         servico_solicitado: str) -> pd.DataFrame:
        '''

        Funcao que faz um merge dataframe entre dados de processos e dados de acesso dos tribunais.

        :param df_lista_endereco_site: dataframe que possui os dados de acesso tribunais.
        Colunas df_lista_endereco_site: tribunal; sistema; instancia; cod_tribunal; url; login; senha; validado; habilitado
        :param df_lista_pesquisa: dataframe com os dados dos processos solicitados
        Colunas  df_lista_pesquisa: n_processo e a unica coluna obrigatoria.
        :param servico_solicitado: lista de servicos.
        Possiveis servicos: ['protocolo', 'busca_movimentacoes', 'busca_informacao_capa', 'copia_integral']

        :return: dataframe que possui o merge entre df_lista_pesquisa e df_lista_endereco_site

        '''

        try:
            try:
                df_lista_endereco_site['cod_tribunal']
                df_lista_endereco_site['tribunal']
                df_lista_endereco_site['sistema']
                df_lista_endereco_site['instancia']
                df_lista_endereco_site['url']
                df_lista_endereco_site['login']
                df_lista_endereco_site['senha']
            except Exception as e1:
                raise Exception(
                    f'O dataframe "lista_endereco_site" não possui todas as colunas obrigatórias. Detalhe: {e1}.')

            df_lista_endereco_site['instancia'] = df_lista_endereco_site['instancia'].str.split('.',
                                                                                                expand=True).reindex(
                [0], axis=1)
            df_lista_endereco_site['cod_tribunal'] = df_lista_endereco_site['cod_tribunal'].str.split('.',
                                                                                                      expand=True).reindex(
                [0], axis=1)

            if 'n_processo' not in df_lista_pesquisa.columns:
                raise Exception(f'O dataframe "lista_pesquisa" não possui a coluna "n_processo" que é obrigatória.')

            if 'cod_tribunal' in df_lista_pesquisa.columns:
                df_lista_pesquisa['cod_tribunal'] = df_lista_pesquisa['cod_tribunal'].str.split('.',
                                                                                                expand=True).reindex(
                    [0], axis=1)
            else:
                df_lista_pesquisa['cod_tribunal'] = ''

            if 'data_resultado' not in df_lista_pesquisa.columns:
                df_lista_pesquisa['data_resultado'] = ''

            if 'detalhe' not in df_lista_pesquisa.columns:
                df_lista_pesquisa['detalhe'] = ''

            if 'Cliente' not in df_lista_pesquisa.columns:
                df_lista_pesquisa['Cliente'] = ''

            if 'url_pre_assinada' not in df_lista_pesquisa.columns:
                df_lista_pesquisa['url_pre_assinada'] = ''

            if isinstance(servico_solicitado, str):
                if servico_solicitado in self.lista_possiveis_servicos:
                    if servico_solicitado not in df_lista_pesquisa.columns:
                        df_lista_pesquisa[servico_solicitado] = ''
                else:
                    raise Exception(
                        f'Serviço "{servico_solicitado}" não foi encontrado em "{self.lista_possiveis_servicos}"')
            else:
                raise Exception('Tipo de dados do "servico_solicitado" não foi identificado.')

            df_lista_pesquisa.loc[
                (df_lista_pesquisa[str(servico_solicitado)].fillna('').str.replace(' ', '') == ''),
                'n_processo_original'
            ] = df_lista_pesquisa['n_processo']

            df_lista_pesquisa.loc[
                (df_lista_pesquisa[str(servico_solicitado)].fillna('').str.replace(' ', '') == ''),
                'n_processo'
            ] = df_lista_pesquisa['n_processo'].str.replace('[^0-9]', '', regex=True).str.replace(' ', '')

            df_lista_pesquisa.loc[
                (df_lista_pesquisa[str(servico_solicitado)].fillna('').str.replace(' ', '') == '') &
                (df_lista_pesquisa['n_processo'].fillna('').str.replace(' ', '').str.len() >= 16),
                'n_processo'
            ] = df_lista_pesquisa['n_processo'].fillna('').str.replace(' ', '').apply(lambda x: x.zfill(20))

            df_lista_pesquisa.loc[
                (df_lista_pesquisa[str(servico_solicitado)].fillna('').str.replace(' ', '') == '') &
                (df_lista_pesquisa['n_processo'].fillna('').str.replace(' ', '').str.len() >= 20),
                'cod_tribunal'
            ] = df_lista_pesquisa['n_processo'].str[13:16]

            if 'instancia' in df_lista_pesquisa.columns:

                df_lista_pesquisa['instancia'] = df_lista_pesquisa['instancia'].str.split('.', expand=True).reindex([0],axis=1)
                df_lista_pesquisa['instancia'] = df_lista_pesquisa['instancia'].str.lower().str.replace(' ', '')

                df_lista_pesquisa.loc[
                    (df_lista_pesquisa[str(servico_solicitado)].fillna('').str.replace(' ', '') == '') &
                    (
                            (df_lista_pesquisa['instancia'].fillna('').str.replace(' ', '') == 'stj') |
                            (df_lista_pesquisa['instancia'].fillna('').str.replace(' ', '') == 'stf')
                    ),
                    'cod_tribunal'
                ] = '-'

            df_lista_pesquisa.loc[
                (df_lista_pesquisa[str(servico_solicitado)].fillna('').str.replace(' ', '') == '') &
                (df_lista_pesquisa['n_processo'].fillna('').str.replace(' ', '') == ''),
                'cod_tribunal'
            ] = '*'

            df_lista_pesquisa.loc[
                (df_lista_pesquisa[str(servico_solicitado)].fillna('').str.replace(' ', '') == '') &
                (df_lista_pesquisa['n_processo'].fillna('').str.replace(' ', '').str.len() < 16) &
                (df_lista_pesquisa['cod_tribunal'].fillna('').str.replace(' ', '') == ''),
                'cod_tribunal'
            ] = '-'

            lt_merge = []

            if 'url' in df_lista_pesquisa.columns:
                lt_merge.append('url')

            if 'cod_tribunal' in df_lista_pesquisa.columns:
                lt_merge.append('cod_tribunal')

            if 'instancia' in df_lista_pesquisa.columns:
                lt_merge.append('instancia')

            if 'sistema' in df_lista_pesquisa.columns:
                lt_merge.append('sistema')

            if 'tribunal' in df_lista_pesquisa.columns:
                lt_merge.append('tribunal')

            df_lista_pesquisa = pd.merge(df_lista_pesquisa, df_lista_endereco_site, how='left', on=lt_merge)

            df_lista_pesquisa = df_lista_pesquisa.fillna('')

            df_lista_pesquisa.loc[(
                                          (df_lista_pesquisa['habilitado'] == 'n') |
                                          (df_lista_pesquisa['validado'] == 'n') |
                                          (df_lista_pesquisa['cod_tribunal'] == '*') |
                                          (df_lista_pesquisa['url'] == '')
                                  ) &
                                  (df_lista_pesquisa[servico_solicitado].fillna('').str.replace(' ', '') == ''),
                                  servico_solicitado
            ] = 'TRIBUNAL NÃO IDENTIFICADO.'

            try:
                df_lista_pesquisa[str(servico_solicitado)]
                df_lista_pesquisa['n_processo']
                df_lista_pesquisa['n_processo_original']
                df_lista_pesquisa['cod_tribunal']
                df_lista_pesquisa['tribunal']
                df_lista_pesquisa['sistema']
                df_lista_pesquisa['instancia']
                df_lista_pesquisa['url']
                df_lista_pesquisa['data_resultado']
                df_lista_pesquisa['detalhe']
                df_lista_pesquisa['Cliente']
            except Exception as e4:
                raise Exception(f'O dataframe gerado não possui todas as colunas obrigatórias. Exception: {e4}')
        except Exception as e:
            raise Exception(f'Ocorreu um erro no método "inserir_informacoes_de_tribunais". Exception:{e}')
        else:
            return df_lista_pesquisa

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
                (df['nome_arquivo'].str[-8:].str.replace(' ', '').str.len() > 0),
                'Identificador'
            ] = df['nome_arquivo'].fillna('').str[-8:]

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
                (df['nome_arquivo'].str[:-8].str.len() > 0),
                'nome_arquivo'
            ] = df['nome_arquivo'].fillna('').str[:-8]

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

                '''
                df.loc[
                    (df['protocolo'].fillna('').str.replace(' ', '') == '') &
                    (df['n_processo'].fillna('') == row['n_processo']),
                    'Identificador'
                ] = row['Identificador']
                '''

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

    def realizar_depara(self, df_original: pd.DataFrame, df_depara_tipoprotocolo: pd.DataFrame,
                        df_depara_tipoarquivo: pd.DataFrame) -> pd.DataFrame:
        '''

        :param df_original:
        :param df_depara_tipoprotocolo:
        :param df_depara_tipoarquivo:
        :return:
        '''
        try:

            lista_colunas = list(df_original.columns)
            lista_colunas.append('Tipo do protocolo')

            df_original.loc[
                (df_original["Ordem do arquivo"].fillna('').str.replace(' ', '') == '1'),
                lista_colunas
            ] = pd.merge(df_original, df_depara_tipoprotocolo, how='left', on=['sistema', 'tribunal', 'nome_arquivo'])

            lista_colunas.append('Tipo arquivo')
            df_original.loc[
                df_original['nome_arquivo'].fillna('').str.replace(' ', '') != '',
                lista_colunas
            ] = pd.merge(df_original, df_depara_tipoarquivo, how='left', on=['sistema', 'tribunal', 'nome_arquivo'])

            df_original.loc[
                (df_original["protocolo"].fillna('').str.replace(' ', '') == '') &
                (df_original['Ordem do arquivo'].fillna('').str.replace(' ', '') == '1') &
                (df_original['Tipo do protocolo'].fillna('').str.replace(' ', '') == ''),
                'protocolo'
            ] = 'TIPO DE PROTOCOLO NÃO IDENTIFICADO NO "DE/PARA".'

            df_original.sort_values(['n_processo', 'cod_tribunal', 'instancia', 'Ordem do arquivo'], inplace=True)

        except Exception as e:
            raise Exception(f'Ocorreu um erro no método "realizar_depara". Exception:{e}')
        else:
            print("A função 'realizar_depara' executou com sucesso!")
            return df_original

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
        df['nome_arquivo_hora_solicitacao_sistema_tribunal_instancia'] = df['Nome do arquivo'].map(str) + '_' + df[
            'horasolicitacao'].map(str) + '_' + df['sistema'].map(str) + '_' + df['tribunal'].map(str) + '_' + df[
                                                                             'instancia'].map(str)
        df.drop_duplicates(subset=['nome_arquivo_hora_solicitacao_sistema_tribunal_instancia', 'datasolicitacao'],
                           keep=False, inplace=True)

        df = df.fillna('')

        return df

    def tratar_dados_protocolo(self, data_inicio, data_fim) -> pd.DataFrame:
        try:
            data = pd.DataFrame(
                columns=[
                    'NOME DO ARQUIVO',
                    'DATA DA SOLICITAÇÃO',
                    'HORA SOLICITAÇÃO',
                    'Nº DO PROCESSO',
                    'CÓD. TRIBUNAL',
                    'SISTEMA',
                    'TRIBUNAL',
                    'INSTÂNCIA',
                    'DATA RESULTADO',
                    'NOME ARQUIVO',
                    'IDENTIFICADOR',
                    'ORDEM DO ARQUIVO',
                    'TIPO DO PROTOCOLO',
                    'LISTA DE ARQUIVOS',
                    'STATUS GERAL',
                    'STATUS DETALHADO',
                    'DOWNLOAD'
                ]
            )
            data_inicio_usada = data_inicio.strftime('%Y/%m/%d')
            data_fim_usada = data_fim.strftime('%Y/%m/%d')
            if data_inicio_usada <= data_fim_usada:
                data_1 = mcd.selecionar_resultado(self.nome_base_protocolo, data_inicio_usada, data_fim_usada)

                print(len(data_1))
                if len(data_1) > 0:
                    data = data_1.loc[
                        (data_1["n_processo"].fillna('') != ''),
                        [
                            'Nome do arquivo',
                            'datasolicitacao',
                            'horasolicitacao',
                            'n_processo_original',
                            'cod_tribunal',
                            'sistema',
                            'tribunal',
                            'instancia',
                            'nome_arquivo_original',
                            'Identificador',
                            'Ordem do arquivo',
                            'Tipo do protocolo',
                            'lista_arquivos_para_anexar',
                            'status_comprovante',
                            'protocolo',
                            'data_resultado',
                            'url_pre_assinada'
                        ]
                    ].fillna('')

                    data = data.rename(
                        columns=
                        {
                            "Nome do arquivo": "NOME DO ARQUIVO",
                            'datasolicitacao': 'DATA DA SOLICITAÇÃO',
                            'horasolicitacao': 'HORA SOLICITAÇÃO',
                            'n_processo_original': 'Nº DO PROCESSO',
                            'cod_tribunal': 'CÓD. TRIBUNAL',
                            'sistema': 'SISTEMA',
                            'tribunal': 'TRIBUNAL',
                            'instancia': 'INSTÂNCIA',
                            'nome_arquivo_original': 'NOME ARQUIVO',
                            'Identificador': 'IDENTIFICADOR',
                            'Ordem do arquivo': 'ORDEM DO ARQUIVO',
                            'Tipo do protocolo': 'TIPO DO PROTOCOLO',
                            'lista_arquivos_para_anexar': 'LISTA DE ARQUIVOS',
                            'status_comprovante': 'STATUS GERAL',
                            'protocolo': 'STATUS DETALHADO',
                            'data_resultado': 'DATA RESULTADO',
                            'url_pre_assinada': 'DOWNLOAD'
                        }
                    )
                    data['DATA DA SOLICITAÇÃO'] = pd.to_datetime(data['DATA DA SOLICITAÇÃO'], errors='coerce')
                    data['DATA DA SOLICITAÇÃO'] = data["DATA DA SOLICITAÇÃO"].dt.strftime("%d/%m/%Y")
            data.loc[
                (data['ORDEM DO ARQUIVO'].fillna('').str.replace(" ", "") == '1') &
                (data['STATUS GERAL'].fillna('').str.replace(' ', '') == '') &
                (data['STATUS DETALHADO'].fillna('').str.replace(' ', '') != ''),
                'STATUS GERAL'
            ] = data['STATUS DETALHADO']

            data.loc[
                (data['ORDEM DO ARQUIVO'].fillna('').str.replace(" ", "") == '1') &
                (data['STATUS GERAL'].fillna('').str.replace(' ', '') == ''),
                'STATUS GERAL'
            ] = 'PENDENTE'
            data.sort_values(by=['DATA DA SOLICITAÇÃO', 'HORA SOLICITAÇÃO', 'IDENTIFICADOR'], inplace=True)
            data.reset_index(drop=True, inplace=True)
        except Exception as e:
            print(f'Erro ocorreu na função "tratar_dados_protocolo". Detalhe: {e}')
            raise Exception('Erro ao consultar no banco de dados.')
        else:
            data.reset_index(drop=True, inplace=True)
            return data

    def mostrar_resultado_protocolo(self, df: pd.DataFrame):
        try:
            st.subheader("Resultado protocolo: ")

            df_resultado = self.gerar_resultado_principal_protocolo(df)

            gb = GridOptionsBuilder.from_dataframe(df_resultado)
            gb.configure_column(
                "DOWNLOAD",
                headerName="DOWNLOAD",
                cellRenderer=JsCode("""
                            class UrlCellRenderer {
                              init(params) {
                                this.eGui = document.createElement('a');
                                if (params.data.DOWNLOAD != ''){
                                    this.eGui.innerText = '📥';
                                    this.eGui.setAttribute('href', params.value);
                                    this.eGui.setAttribute('style', "text-decoration:none");
                                    this.eGui.setAttribute('target', "_blank");
                                }else{
                                    this.eGui.innerText = '❌';
                                }
                              }
                              getGui() {
                                return this.eGui;
                              }
                            }
                        """)
            )

            change_color = JsCode("""
                                                function(params){
                                                    if(params.data['STATUS GERAL'] == 'ERRO'){
                                                           return {
                                                                'color':'#FF0000'
                                                           }
                                                   }else if (params.data['STATUS GERAL'] != 'ERRO' && params.data['STATUS GERAL'] != 'PENDENTE'){
                                                        return {
                                                                'color':'#007FFF'
                                                           }
                                                   }  

                                                }        
                                        """)

            gridOptions = gb.build()
            gridOptions['getRowStyle'] = change_color

            AgGrid(
                df_resultado,
                theme='fresh',
                gridOptions=gridOptions,
                allow_unsafe_jscode=True,
                fit_columns_on_grid_load=True
            )

            st.subheader("Detalhes do resultado protocolo: ")
            # table
            try:
                df_detalhado = self.filtros_complementares(df, 'HORA SOLICITAÇÃO', '%H:%M:%S')
            except Exception as e_filter:
                st.sidebar.error(f'Erro: {e_filter}.')
                df_detalhado = pd.DataFrame()

            try:
                df_cliente = self.gerar_resultado_protocolo(df_detalhado)
            except Exception as e_df_cliente:
                st.sidebar.error(f'Erro: {e_df_cliente}.')
                df_cliente = pd.DataFrame()

            try:
                df_estatistica = self.gerar_estatistica_protocolo(df_cliente)
            except Exception as e_df_estattistica:
                st.sidebar.error(f'Erro: {e_df_estattistica}.')
                df_estatistica = pd.DataFrame()
            try:
                df_detalhado.drop(columns=['DOWNLOAD'], inplace=True)
            except:
                pass
            try:
                df_cliente.drop(columns=['DOWNLOAD', 'QUANTIDADE'], inplace=True)
            except:
                pass

            try:
                df_xlsx = FuncionalidadesAPP().to_excel_3_abas(df_cliente, df_detalhado, df_estatistica)
            except Exception as e_to_excel:
                df_xlsx = pd.DataFrame()
                st.sidebar.error(f'Erro4: {e_to_excel}.')

            if len(df_detalhado) > 0:
                desabilitar = False
                msg = 'Download Disponível.'
            else:
                desabilitar = True
                msg = 'A tabela de resultados esta vizia.'

            st.download_button(label='📥 Download Excel', data=df_xlsx,
                               file_name=f"resultado_protocolo.xlsx",
                               disabled=desabilitar, help=msg)

            container_resultado = st.expander("Resultado: ")
            container_resultado.table(
                df_cliente.style.set_properties(**{'text-align': 'left'}).set_table_styles(self.styles()))
            container_resultado_detalhado = st.expander("Resultado Detalhado: ")
            container_resultado_detalhado.table(
                df_detalhado.style.set_properties(**{'text-align': 'left'}).set_table_styles(self.styles()))
            container_estatistica = st.expander("Estatísticas: ")
            container_estatistica.table(
                df_estatistica.style.set_properties(**{'text-align': 'left'}).set_table_styles(self.styles()))

        except Exception as e_final:
            raise Exception(f'Erro: {e_final}')

    def gerar_resultado_principal_protocolo(self, df: pd.DataFrame) -> pd.DataFrame:

        try:
            if len(df) > 0:
                df = self.gerar_resultado_protocolo(df)

                df['QUANTIDADE'] = df['STATUS GERAL']
                df_estatistica = \
                df.groupby(['STATUS GERAL', 'DATA DA SOLICITAÇÃO', 'HORA SOLICITAÇÃO', 'DOWNLOAD'], as_index=False)[
                    'QUANTIDADE'].count()
                df_estatistica = pd.DataFrame(df_estatistica)

                df_estatistica['PORCENTAGEM (%)'] = 100 * df_estatistica['QUANTIDADE'] / \
                                                    df_estatistica.groupby('HORA SOLICITAÇÃO')[
                                                        'QUANTIDADE'].transform('sum')
                df_estatistica['PORCENTAGEM (%)'] = df_estatistica['PORCENTAGEM (%)'].round(2)

                df_estatistica.sort_values(by=['DATA DA SOLICITAÇÃO', 'HORA SOLICITAÇÃO'], inplace=True)
                df_estatistica.reset_index(drop=True, inplace=True)
            else:
                df_estatistica = pd.DataFrame(
                    columns=['STATUS GERAL', 'DATA DA SOLICITAÇÃO', 'HORA SOLICITAÇÃO', 'DOWNLOAD', 'QUANTIDADE',
                             'PORCENTAGEM (%)'])

            df_estatistica.columns = ['STATUS GERAL', 'DATA DA SOLICITAÇÃO', 'HORA SOLICITAÇÃO', 'DOWNLOAD',
                                      'QUANTIDADE', 'PORCENTAGEM (%)']
            df_estatistica = df_estatistica.loc[
                (df_estatistica['STATUS GERAL'].fillna('').str.replace(" ", "") != ''),
                [
                    'STATUS GERAL',
                    'DATA DA SOLICITAÇÃO',
                    'HORA SOLICITAÇÃO',
                    'QUANTIDADE',
                    'PORCENTAGEM (%)',
                    'DOWNLOAD'
                ]
            ].drop_duplicates()

        except Exception as e:
            raise Exception(f'Erro ocorreu na função "gerar_resultado_principal_protocolo". Detalhe: {e}')
        else:
            return df_estatistica

    def gerar_estatistica_protocolo(self, df: pd.DataFrame) -> pd.DataFrame:

        try:
            if len(df) > 0:

                df['QUANTIDADE'] = df['STATUS GERAL']
                df_estatistica = df.groupby(['STATUS GERAL', 'DATA DA SOLICITAÇÃO'], as_index=False)[
                    'QUANTIDADE'].count()

                df_estatistica = pd.DataFrame(df_estatistica)

                df_estatistica['PORCENTAGEM (%)'] = 100 * df_estatistica['QUANTIDADE'] / \
                                                    df_estatistica.groupby('DATA DA SOLICITAÇÃO')[
                                                        'QUANTIDADE'].transform('sum')
                df_estatistica['PORCENTAGEM (%)'] = df_estatistica['PORCENTAGEM (%)'].round(2)

                df_estatistica.sort_values(by='DATA DA SOLICITAÇÃO', inplace=True)
                df_estatistica.reset_index(drop=True, inplace=True)
            else:
                df_estatistica = pd.DataFrame(
                    columns=['STATUS GERAL', 'DATA DA SOLICITAÇÃO', 'QUANTIDADE', 'PORCENTAGEM (%)'])

            df_estatistica.columns = ['STATUS GERAL', 'DATA DA SOLICITAÇÃO', 'QUANTIDADE', 'PORCENTAGEM (%)']
            df_estatistica = df_estatistica.loc[
                (df_estatistica['STATUS GERAL'].fillna('').str.replace(" ", "") != ''),
                [
                    'STATUS GERAL',
                    'DATA DA SOLICITAÇÃO',
                    'QUANTIDADE',
                    'PORCENTAGEM (%)'
                ]
            ].drop_duplicates()

        except Exception as e:
            raise Exception(f'Erro ocorreu na função "gerar_estatistica_protocolo". Detalhe: {e}')
        else:
            return df_estatistica

    def gerar_resultado_protocolo(self, df: pd.DataFrame) -> pd.DataFrame:

        try:
            if len(df) > 0:
                df = df.fillna('')
                df = df.loc[
                    df['ORDEM DO ARQUIVO'].fillna('').str.replace(" ", "") == '1',
                    [
                        'NOME DO ARQUIVO',
                        'DATA DA SOLICITAÇÃO',
                        'HORA SOLICITAÇÃO',
                        'Nº DO PROCESSO',
                        'CÓD. TRIBUNAL',
                        'SISTEMA',
                        'TRIBUNAL',
                        'INSTÂNCIA',
                        'DATA RESULTADO',
                        'NOME ARQUIVO',
                        'IDENTIFICADOR',
                        'TIPO DO PROTOCOLO',
                        'LISTA DE ARQUIVOS',
                        'STATUS GERAL',
                        'STATUS DETALHADO',
                        'DOWNLOAD'
                    ]
                ].fillna('')
            else:
                df = pd.DataFrame(
                    columns=[
                        'NOME DO ARQUIVO',
                        'DATA DA SOLICITAÇÃO',
                        'HORA SOLICITAÇÃO',
                        'Nº DO PROCESSO',
                        'CÓD. TRIBUNAL',
                        'SISTEMA',
                        'TRIBUNAL',
                        'INSTÂNCIA',
                        'DATA RESULTADO',
                        'NOME ARQUIVO',
                        'IDENTIFICADOR',
                        'TIPO DO PROTOCOLO',
                        'LISTA DE ARQUIVOS',
                        'STATUS GERAL',
                        'STATUS DETALHADO',
                        'DOWNLOAD'
                    ]
                ).fillna('')
            df.reset_index(drop=True, inplace=True)
        except Exception as e:
            raise Exception(f'Erro ocorreu na função "gerar_resultado_protocolo". Detalhe: {e}')
        else:
            return df

