import streamlit as st
import pandas as pd
from classe_operacao_dynamodb import OperacoesDynamoDB

@st.cache_data(show_spinner=False)
def selecionar_resultado(nome_base, data_inicio, data_fim) -> pd.DataFrame:
    try:
        data = OperacoesDynamoDB().get_table_entre_datas(nome_base, data_inicio,data_fim)
    except Exception as e:
        print(
            f'Ocorreu um erro ao acessar o banco de dados. Nome da tabela: {nome_base}. detelhe do erro: {e}')
        st.error('Ocorreu um erro ao acessar o banco de dados.')
    else:
        return data

@st.cache_data(show_spinner=False)
def selecionar_infomacoes_tribunais(nome_base_lista_endereco_site, nome_base_protocolo) -> pd.DataFrame:
    try:
        df = OperacoesDynamoDB().get_table_filter(nome_base_lista_endereco_site, 'cliente_servico', nome_base_protocolo)
    except Exception as e:
        print(f'Ocorreu um erro ao acessar o banco de dados. Nome da tabela: {nome_base_lista_endereco_site}. detelhe do erro: {e}')
        st.error('Ocorreu um erro ao acessar o banco de dados.')

    if len(df) > 0:
        df = df.loc[
            (df["tribunal"].fillna('').str.replace(' ', '') != ''),
            [
                "tribunal",
                "sistema",
                "instancia",
                "cod_tribunal",
                "url",
                "login",
                "senha",
                "validado",
                "habilitado",
                "sistema_tribunal_instancia",
                "cliente_servico",
            ]
        ].drop_duplicates().fillna('')
    else:
        df = pd.DataFrame(columns=[
            "sistema_tribunal_instancia",
            "cliente_servico",
            "tribunal",
            "sistema",
            "instancia",
            "cod_tribunal",
            "url",
            "login",
            "senha",
            "validado",
            "habilitado"
        ])
    return df

@st.cache_data(show_spinner=False)
def selecionar_infomacoes_depara_tipo_de_protocolo(nome_base_depara_tipo_protocolo, nome_cliente) -> pd.DataFrame:
    try:
        df = OperacoesDynamoDB().get_table_filter(nome_base_depara_tipo_protocolo, 'cliente', nome_cliente)
    except Exception as e:
        print(f'Ocorreu um erro ao acessar o banco de dados. Nome da tabela: {nome_base_depara_tipo_protocolo}. detelhe do erro: {e}')
        df = pd.DataFrame()
        st.error('Ocorreu um erro ao acessar o banco de dados.')

    if len(df) > 0:
        df = df.loc[
            (df["tribunal"].fillna('').str.replace(' ', '') != ''),
            [
                "tribunal",
                "sistema",
                "nome_arquivo_original",
                "nome_arquivo",
                "Tipo do protocolo",
                "sistema_tribunal_nome_arquivo",
                "cliente"
            ]
        ].drop_duplicates().fillna('')
    else:
        df = pd.DataFrame(columns=[
            "tribunal",
            "sistema",
            "nome_arquivo_original",
            "nome_arquivo",
            "Tipo do protocolo",
            "sistema_tribunal_nome_arquivo",
            "cliente"
        ])
    return df

@st.cache_data(show_spinner=False)
def selecionar_infomacoes_depara_tipo_de_arquivo(nome_base_depara_tipo_arquivo, nome_cliente) -> pd.DataFrame:
    try:
        df = OperacoesDynamoDB().get_table_filter(nome_base_depara_tipo_arquivo, 'cliente', nome_cliente)
    except Exception as e:
        print(f'Ocorreu um erro ao acessar o banco de dados. Nome da tabela: {nome_base_depara_tipo_arquivo}. detelhe do erro: {e}')
        df = pd.DataFrame()
        st.error('Ocorreu um erro ao acessar o banco de dados.')

    if len(df):
        df = df.loc[
            (df["tribunal"].fillna('').str.replace(' ', '') != ''),
            [
                "tribunal",
                "sistema",
                "nome_arquivo_original",
                "nome_arquivo",
                "Tipo arquivo"
            ]
        ].drop_duplicates().fillna('')
    else:
        df = pd.DataFrame(columns=[
            "tribunal",
            "sistema",
            "nome_arquivo_original",
            "nome_arquivo",
            "Tipo arquivo"
        ])
    return df

@st.cache_data(show_spinner=False)
def selecionar_informacoes_de_acesso_ao_site_bb_movimentacoes(nome_base_senha_bb_movimentacoes) -> pd.DataFrame:
    try:
        data = OperacoesDynamoDB().get_table_filter(nome_base_senha_bb_movimentacoes, 'status', 'ATUAL')
    except Exception as e:
        print(f'Ocorreu um erro ao acessar o banco de dados. Nome da tabela: {nome_base_senha_bb_movimentacoes}. detelhe do erro: {e}')
        data = pd.DataFrame()
        st.error('Ocorreu um erro ao acessar o banco de dados.')
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
    return data