import pandas as pd
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)
import base64
import datetime
from io import BytesIO
import io
from classe_operacao_dynamodb import OperacoesDynamoDB
from classe_operacao_s3 import OperacaoS3
import uuid
import re


class FuncionalidadesAPP:

    def filter_dataframe_copia_integral(self, df:pd.DataFrame, st) -> pd.DataFrame:
        """
        Adds a UI on top of a dataframe to let viewers filter columns

        Args:
            df (pd.DataFrame): Original dataframe

        Returns:
            pd.DataFrame: Filtered dataframe
        """
        modify = st.checkbox("Filtros Complementares")

        if not modify:
            if len(df) > 0:
                df['DATA DA SOLICITAÇÃO'] = pd.to_datetime(df['DATA DA SOLICITAÇÃO'], errors='coerce')
                df['DATA DA SOLICITAÇÃO'] = df["DATA DA SOLICITAÇÃO"].dt.strftime("%d/%m/%Y")
                df['DATA RESULTADO'] = pd.to_datetime(df['DATA RESULTADO'], errors='coerce')
                df['DATA RESULTADO'] = df['DATA RESULTADO'].dt.strftime("%d/%m/%Y %H:%M:%S")
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
                    df['DATA DA SOLICITAÇÃO'] = df["DATA DA SOLICITAÇÃO"].dt.strftime("%d/%m/%Y")
                    df['DATA RESULTADO'] = pd.to_datetime(df['DATA RESULTADO'], errors='coerce')
                    df['DATA RESULTADO'] = df['DATA RESULTADO'].dt.strftime("%d/%m/%Y %H:%M:%S")
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
        if len(df) > 0:
            df['DATA DA SOLICITAÇÃO'] = pd.to_datetime(df['DATA DA SOLICITAÇÃO'], errors='coerce')
            df['DATA DA SOLICITAÇÃO'] = df["DATA DA SOLICITAÇÃO"].dt.strftime("%d/%m/%Y")
            df['DATA RESULTADO'] = pd.to_datetime(df['DATA RESULTADO'], errors='coerce')
            df['DATA RESULTADO'] = df['DATA RESULTADO'].dt.strftime("%d/%m/%Y %H:%M:%S")
        return df

    def filter_dataframe_bb_movimentacoes(self, df:pd.DataFrame, st) -> pd.DataFrame:
        """
        Adds a UI on top of a dataframe to let viewers filter columns

        Args:
            df (pd.DataFrame): Original dataframe

        Returns:
            pd.DataFrame: Filtered dataframe
        """
        modify = st.checkbox("Filtros Complementares")

        if not modify:
            if len(df) > 0:
                df['DATA DA SOLICITAÇÃO'] = pd.to_datetime(df['DATA DA SOLICITAÇÃO'], errors='coerce')
                df['DATA DA SOLICITAÇÃO'] = df['DATA DA SOLICITAÇÃO'].dt.strftime("%d/%m/%Y")
                df['HORA RESULTADO'] = pd.to_datetime(df['HORA RESULTADO'], errors='coerce')
                df['HORA RESULTADO'] = df['HORA RESULTADO'].dt.strftime("%H:%M")
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
                    df['HORA RESULTADO'] = pd.to_datetime(df['HORA RESULTADO'], errors='coerce')
                    df['HORA RESULTADO'] = df['HORA RESULTADO'].dt.strftime("%H:%M")
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
        if len(df) > 0:
            df['DATA DA SOLICITAÇÃO'] = pd.to_datetime(df['DATA DA SOLICITAÇÃO'], errors='coerce')
            df['DATA DA SOLICITAÇÃO'] = df['DATA DA SOLICITAÇÃO'].dt.strftime("%d/%m/%Y")
            df['HORA RESULTADO'] = pd.to_datetime(df['HORA RESULTADO'], errors='coerce')
            df['HORA RESULTADO'] = df['HORA RESULTADO'].dt.strftime("%H:%M")
        return df

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

    def download_aws_object(self, bucket_name='bb-movimentacoes', key='22_05_2023/movimentacoes-22-05-2023-04-00.zip'):
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

    def load_data_copia_integral(self, data_inicio, data_fim) -> pd.DataFrame:
        try:
            data = pd.DataFrame(
                columns=['Nº DO PROCESSO', 'CÓD. TRIBUNAL', 'SISTEMA', 'TRIBUNAL', 'INSTÂNCIA', 'DATA RESULTADO',
                         'RESULTADO', 'ERRO'])
            data_inicio_usada = data_inicio.strftime('%Y/%m/%d')
            data_fim_usada = data_fim.strftime('%Y/%m/%d')
            if data_inicio_usada <= data_fim_usada:
                data_1 = OperacoesDynamoDB().get_table_entre_datas('SOPII_copia_integral', data_inicio_usada, data_fim_usada)
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
                            'data_resultado'
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
                            'detalhe': 'ERRO'
                        }
                    )
                    data['DATA DA SOLICITAÇÃO'] = pd.to_datetime(data['DATA DA SOLICITAÇÃO'], errors='coerce')
                    data['DATA DA SOLICITAÇÃO'] = data["DATA DA SOLICITAÇÃO"].dt.strftime("%d/%m/%Y")
                    data['DATA RESULTADO'] = pd.to_datetime(data['DATA RESULTADO'], errors='coerce')
                    data['DATA RESULTADO'] = data["DATA RESULTADO"].dt.strftime("%d/%m/%Y %H:%M:%S")

        except Exception as e:
            raise Exception(f'Erro ocorreu na função "load_data_copia_integral". Detalhe: {e}')
        else:
            return data

    def load_data_estatistica_copia_integral(self, df:pd.DataFrame) -> pd.DataFrame:
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

    def load_data_bb_movimentacoes(self, data_inicio, data_fim) -> pd.DataFrame:
        try:
            data = pd.DataFrame(
                columns=['DATA DA SOLICITAÇÃO','DATA RESULTADO', 'CONTRATO', 'TIPO MOVIMENTAÇÃO', 'RESULTADO','URL'])
            data_inicio_usada = data_inicio.strftime('%Y/%m/%d')
            data_fim_usada = data_fim.strftime('%Y/%m/%d')
            if data_inicio_usada <= data_fim_usada:
                data_1 = OperacoesDynamoDB().get_table_entre_datas('SOPII_bbmovimentacoes', data_inicio_usada, data_fim_usada)
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

    def load_data_estatistica_bb_movimentacoes(self, df:pd.DataFrame) -> pd.DataFrame:
        try:
            if len(df) > 0:

                df_estatistica = df.groupby(['RESULTADO','DATA DA SOLICITAÇÃO','HORA RESULTADO','DOWNLOAD'], as_index=False)['CONTRATO'].count()
                df_estatistica = pd.DataFrame(df_estatistica)

                df_estatistica['PORCENTAGEM (%)'] = 100 * df_estatistica['CONTRATO'] / df_estatistica.groupby('DATA DA SOLICITAÇÃO')[
                    'CONTRATO'].transform('sum')
                df_estatistica['PORCENTAGEM (%)'] = df_estatistica['PORCENTAGEM (%)'].round(2)

                df_estatistica=df_estatistica.sort_values(by=['DATA DA SOLICITAÇÃO','HORA RESULTADO'])
                df_estatistica.reset_index(drop=True, inplace=True)
            else:
                df_estatistica = pd.DataFrame(
                    columns=['RESULTADO','DATA DA SOLICITAÇÃO','HORA RESULTADO','QUANTIDADE','PORCENTAGEM (%)','DOWNLOAD'])

            df_estatistica.columns = ['RESULTADO','DATA DA SOLICITAÇÃO','HORA RESULTADO','DOWNLOAD','QUANTIDADE','PORCENTAGEM (%)']
            df_estatistica = df_estatistica.loc[
                        (df_estatistica['RESULTADO'].fillna('').str.replace(" ", "") != ''),
                        [
                            'RESULTADO',
                            'DATA DA SOLICITAÇÃO',
                            'HORA RESULTADO',
                            'QUANTIDADE',
                            'PORCENTAGEM (%)',
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
