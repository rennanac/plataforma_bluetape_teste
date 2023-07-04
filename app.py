import pandas as pd
import streamlit as st
from classe_operacao_dynamodb import OperacoesDynamoDB
import streamlit_authenticator as stauth
from todo_clientes import TodoClientes
from PIL import Image
from datetime import datetime
import pytz

st.set_page_config(page_title="Plataforma BlueTape", page_icon=":judge:", layout="wide")

def add_logo(logo_path, width, height):
    """Read and return a resized logo"""
    logo = Image.open(logo_path)
    #modified_logo = logo.resize((width, height))
    return logo

try:
    df_users = OperacoesDynamoDB().get_table('user_db')

    usernames = list(df_users['login'])
    names = list(df_users['nome'])
    passwords = list(df_users['senha'])
    hashed_passwords = stauth.Hasher(passwords).generate()
    try:
        authenticator = stauth.Authenticate(names, usernames, hashed_passwords, "Plataforma BlueTape", "abcdef", cookie_expiry_days=1)

        name, authentication_status, username = authenticator.login("Login", "main")
    except:
        st.error('Ops, algo inesperado aconteceu, para retonar atualize a página.')
    else:
        if authentication_status == False:
            st.error("Usuário ou senha incorreto.")

        if authentication_status == None:
            st.warning("Por favor, insira o nome do usuário e senha!")

        if authentication_status:
            authenticator.logout('Logout', 'main')
            col_img_1, col_img_2, col_img_3 = st.sidebar.columns(3)

            with col_img_1:
                st.write("")

            with col_img_2:
                st.image(add_logo(logo_path="logo.jpeg", width=3500, height=648))

            with col_img_3:
                st.write("")

            #st.sidebar.image(add_logo(logo_path="logo.png", width=100, height=100))
            st.sidebar.title(f"Bem vindo {name}.")
            if username == 'admin':
                nomes_empresas = list(df_users.loc[
                    (df_users['login'].fillna('') != 'admin'),
                    'empresa'
                ].drop_duplicates())
                empresa_selecionada = st.sidebar.selectbox("Cliente: ", nomes_empresas)
            else:
                empresa_selecionada = list(df_users.loc[
                    (df_users['login'].fillna('') == username),
                    'empresa'
                ].drop_duplicates())[0]

            dados_cliente = list(df_users.loc[
                (df_users['empresa'].fillna('') == empresa_selecionada),
                'servicos'
            ].drop_duplicates())[0]

            opcao = st.sidebar.radio("Selecione o serviço", dados_cliente)

            if opcao == 'Painel Cópia Integral':
                TodoClientes().__getattribute__(f"cliente_{str(empresa_selecionada).replace(' ','').lower()}").painel_copia_integral()

            elif opcao == 'Painel BB Movimentações':
                TodoClientes().__getattribute__(f"cliente_{str(empresa_selecionada).replace(' ', '').lower()}").painel_bb_movimentacoes(
                    nome_responsavel=name
                )
            elif opcao == 'Painel Protocolo':
                TodoClientes().__getattribute__(f"cliente_{str(empresa_selecionada).replace(' ', '').lower()}").painel_protocolo()

            elif opcao == "Painel Intimações":
                TodoClientes().__getattribute__(f"cliente_{str(empresa_selecionada).replace(' ', '').lower()}").painel_intimacoes()

except Exception as e:
    st.error(f'Erro: {e}')