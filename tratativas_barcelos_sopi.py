from tratativa_barcelos_sopii import ClienteBarcelosSOPII
from classe_operacao_dynamodb import OperacoesDynamoDB
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, JsCode, GridOptionsBuilder
from funcionalidades_app import FuncionalidadesAPP

class ClienteBarcelosSOPI(ClienteBarcelosSOPII):

    nome_cliente = 'barcelos-sopi'
    nome_base_intimacoes = 'barcelos-sopi_intimacoes'
    nome_base_lista_endereco_site = 'lista_endereco_site'
    nome_bucket_protocolo = 'barcelos-sopi-intimacoes'

    def painel_intimacoes(self):

        self.inserir_infomacoes_tribunais(self.nome_base_intimacoes)
        df = self.filtros_obrigatorios(servico='intimacoes')
        self.mostrar_resultado_intimacoes(df=df)

    def tratar_dados_intimacoes(self, data_inicio, data_fim) -> pd.DataFrame:
        try:
            data = pd.DataFrame(
                columns=[
                    'DATA DA SOLICITAÇÃO',
                    'CÓD. TRIBUNAL',
                    'SISTEMA',
                    'TRIBUNAL',
                    'INSTÂNCIA',
                    'QUANTIDADE DE DIAS',
                    'DATA RESULTADO',
                    'STATUS',
                    'STATUS GERAL',
                    'DOWNLOAD'
                ]
            )
            data_inicio_usada = data_inicio.strftime('%Y/%m/%d')
            data_fim_usada = data_fim.strftime('%Y/%m/%d')
            if data_inicio_usada <= data_fim_usada:
                data_1 = OperacoesDynamoDB().get_table_entre_datas(self.nome_base_intimacoes, data_inicio_usada,
                                                                   data_fim_usada)

                print(len(data_1))
                if len(data_1) > 0:
                    data = data_1.loc[
                        (data_1["tribunal"].fillna('') != ''),
                        [
                            'datasolicitacao',
                            'cod_tribunal',
                            'sistema',
                            'tribunal',
                            'instancia',
                            'intimacoes',
                            'status_final',
                            'data_resultado',
                            'url_pre_assinada',
                            'qt_dias_consulta'
                        ]
                    ].fillna('')

                    data = data.rename(
                        columns=
                        {
                            'datasolicitacao': 'DATA DA SOLICITAÇÃO',
                            'cod_tribunal': 'CÓD. TRIBUNAL',
                            'sistema': 'SISTEMA',
                            'tribunal': 'TRIBUNAL',
                            'instancia': 'INSTÂNCIA',
                            'qt_dias_consulta': 'QUANTIDADE DE DIAS',
                            'intimacoes': 'STATUS',
                            'status_final': 'STATUS GERAL',
                            'data_resultado': 'DATA RESULTADO',
                            'url_pre_assinada': 'DOWNLOAD'
                        }
                    )
                    data['DATA DA SOLICITAÇÃO'] = pd.to_datetime(data['DATA DA SOLICITAÇÃO'], errors='coerce')
                    data['DATA DA SOLICITAÇÃO'] = data["DATA DA SOLICITAÇÃO"].dt.strftime("%d/%m/%Y")
            data.loc[
                (data['STATUS GERAL'].fillna('').str.replace(' ', '') == '') &
                (data['STATUS'].fillna('').str.replace(' ', '') != ''),
                'STATUS GERAL'
            ] = data['STATUS']

            data.loc[
                (data['STATUS'].fillna('').str.replace(' ', '') == ''),
                'STATUS GERAL'
            ] = 'PENDENTE'
            data.drop(columns=['STATUS'], inplace=True)
            data.sort_values(by=['DATA DA SOLICITAÇÃO', 'SISTEMA', 'CÓD. TRIBUNAL', 'INSTÂNCIA'], inplace=True)
            data.reset_index(drop=True, inplace=True)
        except Exception as e:
            print(f'Erro ocorreu na função "tratar_dados_intimacoes". Detalhe: {e}')
            raise Exception('Erro ao consultar no banco de dados.')
        else:
            data.reset_index(drop=True, inplace=True)
            return data

    def mostrar_resultado_intimacoes(self, df: pd.DataFrame):
        try:
            st.subheader("Resultado intimações: ")

            df_resultado = self.gerar_resultado_intimacoes(df)

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

            AgGrid(df_resultado, gridOptions=gridOptions, allow_unsafe_jscode=True, fit_columns_on_grid_load=True)

            st.subheader("Detalhes do resultado intimações: ")
            # table
            try:
                df_detalhado = self.filtros_complementares(df.drop(columns=['DOWNLOAD']), 'DATA RESULTADO', "%d/%m/%Y %H:%M:%S")
            except Exception as e_filter:
                st.sidebar.error(f'Erro: {e_filter}.')
                df_detalhado = pd.DataFrame()

            try:
                df_estatistica = self.gerar_estatistica_intimacoes(df_detalhado)
            except Exception as e_df_estattistica:
                st.sidebar.error(f'Erro: {e_df_estattistica}.')
                df_estatistica = pd.DataFrame()
            try:
                df_detalhado.drop(columns=['QUANTIDADE'], inplace=True)
            except:
                pass
            try:
                df_xlsx = FuncionalidadesAPP().to_excel(df_detalhado, df_estatistica)
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

            container_resultado = st.expander("Resultado Detalhado: ")
            container_resultado.table(
                df_detalhado.style.set_properties(**{'text-align': 'left'}).set_table_styles(self.styles()))
            container_estatistica = st.expander("Estatísticas: ")
            container_estatistica.table(
                df_estatistica.style.set_properties(**{'text-align': 'left'}).set_table_styles(self.styles()))

        except Exception as e_final:
            raise Exception(f'Erro: {e_final}')

    def gerar_resultado_intimacoes(self, df: pd.DataFrame) -> pd.DataFrame:

        try:
            if len(df) > 0:

                df['QUANTIDADE'] = df['STATUS GERAL']
                df_resultado = df.groupby(['STATUS GERAL', 'DOWNLOAD', 'DATA DA SOLICITAÇÃO'], as_index=False)[
                    'QUANTIDADE'].count()

                df_resultado = pd.DataFrame(df_resultado)

                df_resultado['PORCENTAGEM (%)'] = 100 * df_resultado['QUANTIDADE'] / \
                                                    df_resultado.groupby('DATA DA SOLICITAÇÃO')[
                                                        'QUANTIDADE'].transform('sum')
                df_resultado['PORCENTAGEM (%)'] = df_resultado['PORCENTAGEM (%)'].round(2)

                df_resultado.sort_values(by='DATA DA SOLICITAÇÃO', inplace=True)
                df_resultado.reset_index(drop=True, inplace=True)
            else:
                df_resultado = pd.DataFrame(
                    columns=['STATUS GERAL', 'DOWNLOAD', 'DATA DA SOLICITAÇÃO', 'QUANTIDADE', 'PORCENTAGEM (%)'])

            df_resultado.columns = ['STATUS GERAL','DOWNLOAD', 'DATA DA SOLICITAÇÃO', 'QUANTIDADE', 'PORCENTAGEM (%)']
            df_resultado = df_resultado.loc[
                (df_resultado['STATUS GERAL'].fillna('').str.replace(" ", "") != ''),
                [
                    'STATUS GERAL',
                    'DATA DA SOLICITAÇÃO',
                    'QUANTIDADE',
                    'PORCENTAGEM (%)',
                    'DOWNLOAD'
                ]
            ].drop_duplicates()

        except Exception as e:
            raise Exception(f'Erro ocorreu na função "gerar_estatistica_protocolo". Detalhe: {e}')
        else:
            return df_resultado

    def gerar_estatistica_intimacoes(self, df: pd.DataFrame) -> pd.DataFrame:

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