import pandas as pd
import streamlit as st
import datetime
from classe_operacao_dynamodb import OperacoesDynamoDB
from funcionalidades_app import FuncionalidadesAPP
import plotly.graph_objects as go
import streamlit_authenticator as stauth
from st_aggrid import AgGrid, JsCode, GridOptionsBuilder
from DB_postgres import DB_bots

st.set_page_config(page_title="Plataforma BlueTape", page_icon=":bar_chart:", layout="wide")
# ------------------------- #
# --- USER AUTHENTICATION ---
try:
    df_users = OperacoesDynamoDB().get_table('user_db')

    usernames = list(df_users['login'])
    names = list(df_users['nome'])
    passwords = list(df_users['senha'])
    hashed_passwords = stauth.Hasher(passwords).generate()

    authenticator = stauth.Authenticate(names, usernames, hashed_passwords, "Plataforma BlueTape", "abcdef", cookie_expiry_days=1)

    name, authentication_status, username = authenticator.login("Login", "main")

    if authentication_status == False:
        st.error("Usuário ou senha incorreto.")

    if authentication_status == None:
        st.warning("Por favor, insira o nome do usuário e senha!")

    if authentication_status:
        st.sidebar.title(f"Bem vindo {name}")

        authenticator.logout('Logout', 'main')

        opcao = st.sidebar.radio("Selecione o serviço",
                                 ('Resultados BB Movimentações', 'Resultado Cópia Integral', 'Upload Cópia Integral'))

        if opcao == 'Upload Cópia Integral':
            uploaded_file = st.file_uploader('Upload base de dados para copia integral.', type='xlsx')
            if uploaded_file is not None:
                dataframe = pd.read_excel(uploaded_file, dtype=str)
                lista_colunas_existentes = []
                if 'n_processo' not in dataframe.columns:
                    st.error(f'O excel não possui a coluna "n_processo" que é obrigatória.')
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
                    try:
                        OperacoesDynamoDB().put_table_lista_processos('SOPII_copia_integral',
                                                                      'SOPII_lista_endereco_site', dataframe)
                    except Exception as e_put_table:
                        st.error(f'Erro: Não foi possivel gravar os dados do excel. Detalhe: {e_put_table}')
                    else:
                        st.table(dataframe)
                        st.sidebar.success('Dados do excel foram gravados com sucesso!')

        elif opcao == 'Resultado Cópia Integral':
            st.subheader("Filtros Obrigatórios: ")
            col1, col2 = st.columns(2)
            data_fim = datetime.date.today()
            data_inicio = data_fim + datetime.timedelta(days=-1)

            start_date = col1.date_input('Data inicio', data_inicio)
            end_date = col2.date_input('Data fim', data_fim)
            try:
                df = FuncionalidadesAPP().load_data_copia_integral(start_date, end_date)
            except Exception as e_load_data:
                df = pd.DataFrame()
                st.sidebar.error(f'Erro: {e_load_data}.')
            else:
                if start_date <= end_date:
                    col1.success('Data inicio: `%s`' % (data_inicio.strftime('%d/%m/%Y')))
                    col2.success('Data Fim: `%s`' % (data_fim.strftime('%d/%m/%Y')))
                else:
                    col1.error('Erro: A data inicio não pode suceder a data fim.')

            st.subheader("Resultado Cópia Integral: ")

            # styl
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

            # table
            try:
                df = FuncionalidadesAPP().filter_dataframe_copia_integral(df, st)
            except Exception as e_filter:
                st.sidebar.error(f'Erro: {e_filter}.')

            try:
                df_estatistica = FuncionalidadesAPP().load_data_estatistica_copia_integral(df)
            except Exception as e_df_estattistica:
                st.sidebar.error(f'Erro: {e_df_estattistica}.')
                df_estatistica = pd.DataFrame()

            try:
                df_xlsx = FuncionalidadesAPP().to_excel(df, df_estatistica)
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
                               file_name=f"resultado_copia_integral_{data_inicio.strftime('%d_%m_%Y')}.xlsx",
                               disabled=desabilitar, help=msg)

            container_resultado = st.expander("Resultado: ")
            container_resultado.table(df.style.set_properties(**{'text-align': 'left'}).set_table_styles(styles))
            # st.dataframe(filter_dataframe(df))

            container_estatistica = st.expander("Estatísticas: ")
            container_estatistica.table(
                df_estatistica.style.set_properties(**{'text-align': 'left'}).set_table_styles(styles))

        else:
            # styl
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
            container_alterar_senha = st.expander("Alterar a senha de acesso ao portal BB Movimentações: ")

            with container_alterar_senha.form(key='insert'):
                def clear_form():
                    st.session_state["login"] = ""
                    st.session_state["senha"] = ""

                col5, col6 = st.columns(2)
                col7, col8 = st.columns(2)

                input_login = col5.text_input(label='Insira o novo login:', key='login')
                input_senha = col6.text_input(label='Insira a nova senha:', key='senha')
                button_submit = col7.form_submit_button('Enviar')
                button_limpar = col8.form_submit_button('Limpar', on_click=clear_form)

                if button_submit:
                    if input_login.replace(' ', '') == '' or input_senha.replace(' ', '') == '':
                        container_alterar_senha.error('É obrigatório inserir o login e a senha!')
                    else:
                        try:
                            data_atual = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
                            OperacoesDynamoDB().update_senha_login_bb_movimentacoes('ATUAL', '00:00:00', name,
                                                                                    input_login, input_senha,
                                                                                    data_atual)
                        except Exception:
                            container_alterar_senha.success('Erro ao inserir a senha e login!')
                        else:
                            container_alterar_senha.success('Senha e login inseridos com sucesso!')

            data = OperacoesDynamoDB().get_table_filter('SOPII_senhas_bb_movimentacoes', 'status', 'ATUAL')
            data = data.loc[
                (data["login"].fillna('') != ''),
                [
                    'login',
                    'senha',
                    'data_hora_cadastro',
                    'nome_responsavel'
                ]
            ].fillna('')

            data = data.rename(
                columns=
                {
                    'login': 'LOGIN ATUAL',
                    'senha': 'SENHA ATUAL',
                    'data_hora_cadastro': 'DATA DA ATUALIZAÇÃO',
                    'nome_responsavel': 'RESPONSÁVEL PELA ATUALIZAÇÃO'
                }
            )
            container_alterar_senha.table(data.style.set_properties(**{'text-align': 'left'}).set_table_styles(styles))

            st.subheader("Filtros Obrigatórios: ")
            col3, col4 = st.columns(2)
            data_fim = datetime.date.today()
            data_inicio = data_fim + datetime.timedelta(days=-1)
            start_date = col3.date_input('Data inicio', data_inicio)
            end_date = col4.date_input('Data fim', data_fim)
            try:
                df = FuncionalidadesAPP().load_data_bb_movimentacoes(start_date, end_date)
            except Exception as e_load_data:
                df = pd.DataFrame()
                st.sidebar.error(f'Erro2: {e_load_data}.')
            else:
                if start_date <= end_date:
                    col3.success('Data inicio: `%s`' % (data_inicio.strftime('%d/%m/%Y')))
                    col4.success('Data Fim: `%s`' % (data_fim.strftime('%d/%m/%Y')))
                else:
                    col3.error('Erro: A data inicio não pode suceder a data fim.')

            st.subheader("Resultado BB Movimentações: ")

            try:
                df_resultado = FuncionalidadesAPP().load_data_estatistica_bb_movimentacoes(df)
            except Exception as e_df_estattistica:
                st.sidebar.error(f'Erro1: {e_df_estattistica}.')
                df_resultado = pd.DataFrame()

            gb = GridOptionsBuilder.from_dataframe(df_resultado)

            gb.configure_column("DOWNLOAD",
                                headerName="DOWNLOAD",
                                cellRenderer=JsCode(
                                    """
                                    function(params) {
                                        if (params.value != ''){
                                            return '<a href=' + params.value + '> 💾 </a>'
                                        }else{
                                            return 'Donwload indisponível.'
                                        }
                                    }
                                    """)
                                )

            ''' change_color = JsCode("""
                      function(params){
                        if(params.data.RESULTADO == 'ERRO'){

                               return {
                                    'color':'#FF0000'
                               }
                       }           
                       }
            """)'''

            gridOptions = gb.build()
            #gridOptions['getRowStyle'] = change_color

            AgGrid(df_resultado, gridOptions=gridOptions, allow_unsafe_jscode=True, fit_columns_on_grid_load=True)

            st.subheader("Detalhes do Resultado BB Movimentações: ")
            # table
            try:
                df = FuncionalidadesAPP().filter_dataframe_bb_movimentacoes(df, st)
            except Exception as e_filter:
                st.sidebar.error(f'Erro3: {e_filter}.')
            else:
                pass

            try:
                df_estatistica = FuncionalidadesAPP().load_data_estatistica_bb_movimentacoes(df)
            except Exception as e_df_estattistica:
                st.sidebar.error(f'Erro1: {e_df_estattistica}.')
                df_estatistica = pd.DataFrame()
            df = df.drop(columns=['DOWNLOAD'])
            df_estatistica = df_estatistica.drop(columns=['DOWNLOAD'])
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
                               file_name=f"resultado_bb_movimentacoes_{data_inicio.strftime('%d_%m_%Y')}.xlsx",
                               disabled=desabilitar, help=msg)

            container_resultado = st.expander("Resultado: ")
            container_resultado.table(df.style.set_properties(**{'text-align': 'left'}).set_table_styles(styles))
            container_estatistica = st.expander("Estatísticas: ")
            container_estatistica.table(
            df_estatistica.style.set_properties(**{'text-align': 'left'}).set_table_styles(styles))

except Exception as e_final:
    st.error(f'Ocorreu um erro. Detalhe: {e_final}')