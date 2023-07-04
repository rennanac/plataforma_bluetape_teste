import pandas as pd
import datetime

class InserirColunasComplementaresDF:

    def inserir_colunas_info_sites(self, df_lista_endereco_site:pd.DataFrame, df_lista_pesquisa:pd.DataFrame, df_lista_pesquisa_com_dados_tribunais:pd.DataFrame, servico_solicitado:str) -> pd.DataFrame:
        lista_possiveis_servicos = ['copia_integral','protocolo']
        try:
            if len(df_lista_pesquisa) == 0:
                print('Todos os processos ja possuem dados de acesso.')
            else:
                try:
                    df_lista_endereco_site['cod_tribunal']
                    df_lista_endereco_site['tribunal']
                    df_lista_endereco_site['sistema']
                    df_lista_endereco_site['instancia']
                    df_lista_endereco_site['url']
                except Exception as e1:
                    raise Exception(
                        f'O dataframe "lista_endereco_site" não possui todas as colunas obrigatórias. Detalhe: {e1}.')
                try:
                    df_lista_endereco_site.drop(columns=['cliente_servico','sistema_tribunal_instancia'], inplace=True)
                except:
                    pass

                df_lista_endereco_site['instancia'] = df_lista_endereco_site['instancia'].str.split('.', expand=True).reindex(
                    [0], axis=1)
                df_lista_endereco_site['cod_tribunal'] = df_lista_endereco_site['cod_tribunal'].str.split('.',
                                                                                                          expand=True).reindex(
                    [0], axis=1)


                if 'n_processo' not in df_lista_pesquisa.columns:
                    raise Exception(f'O dataframe "lista_pesquisa" não possui a coluna "n_processo" que é obrigatória.')

                if 'url_pre_assinada' not in df_lista_pesquisa.columns:
                    df_lista_pesquisa['url_pre_assinada'] = ''

                if 'dir_arquivo_pasta_s3' not in df_lista_pesquisa.columns:
                    df_lista_pesquisa['dir_arquivo_pasta_s3'] = ''

                if 'Cliente' not in df_lista_pesquisa.columns:
                    df_lista_pesquisa['Cliente'] = ''

                if 'cod_tribunal' in df_lista_pesquisa.columns:
                    df_lista_pesquisa['cod_tribunal'] = df_lista_pesquisa['cod_tribunal'].str.split('.', expand=True).reindex(
                        [0], axis=1)
                else:
                    df_lista_pesquisa['cod_tribunal'] = ''

                if 'tribunal' in df_lista_pesquisa.columns:
                    df_lista_pesquisa['tribunal'] = df_lista_pesquisa['tribunal'].str.lower().str.replace(' ', '')


                if 'sistema' in df_lista_pesquisa.columns:
                    df_lista_pesquisa['sistema'] = df_lista_pesquisa['sistema'].str.lower().str.replace(' ', '')


                if 'instancia' in df_lista_pesquisa.columns:
                    df_lista_pesquisa['instancia'] = df_lista_pesquisa['instancia'].str.split('.', expand=True).reindex([0],
                                                                                                                        axis=1)
                    df_lista_pesquisa['instancia'] = df_lista_pesquisa['instancia'].str.lower().str.replace(' ', '')

                if 'n_processo_original' not in df_lista_pesquisa.columns:
                    df_lista_pesquisa["n_processo_original"] = ''

                df_lista_pesquisa.loc[
                    (df_lista_pesquisa['n_processo'].fillna('').str.replace(' ', '') != '') &
                    (df_lista_pesquisa['n_processo_original'].fillna('').str.replace(' ', '') == ''),
                    'n_processo_original'
                ] = df_lista_pesquisa['n_processo']

                df_lista_pesquisa.loc[
                    (df_lista_pesquisa['n_processo'].fillna('').str.replace(' ', '') != ''),
                    'n_processo'
                ] = df_lista_pesquisa['n_processo'].str.replace('[^0-9]', '', regex=True).str.replace(' ', '')

                df_lista_pesquisa.loc[
                    (df_lista_pesquisa['n_processo'].fillna('').str.replace(' ', '').str.len() >= 16),
                    'n_processo'
                ] = df_lista_pesquisa['n_processo'].fillna('').str.replace(' ', '').apply(lambda x: x.zfill(20))

                df_lista_pesquisa.loc[
                    (df_lista_pesquisa['n_processo'].fillna('').str.replace(' ', '').str.len() >= 20) &
                    (df_lista_pesquisa['cod_tribunal'].fillna('').str.replace(' ', '') == ''),
                    'cod_tribunal'
                ] = df_lista_pesquisa['n_processo'].str[13:16]

                df_lista_pesquisa.loc[
                    (df_lista_pesquisa['cod_tribunal'].fillna('').str.replace(' ', '') == '') &
                    (
                        (df_lista_pesquisa['instancia'].fillna('').str.replace(' ', '') == 'stj') |
                        (df_lista_pesquisa['instancia'].fillna('').str.replace(' ', '') == 'stf')
                    ),
                    'cod_tribunal'
                ] = '-'

                df_lista_pesquisa.loc[
                    (df_lista_pesquisa['cod_tribunal'].fillna('').str.replace(' ', '') == '') &
                    (df_lista_pesquisa['n_processo'].fillna('').str.replace(' ', '') == ''),
                    'cod_tribunal'
                ] = '*'

                df_lista_pesquisa.loc[
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


                df_lista_pesquisa_pendente = pd.merge(df_lista_pesquisa, df_lista_endereco_site, how='left',
                                                      on=lt_merge)


                if len(df_lista_pesquisa_com_dados_tribunais) > 0:

                    df_lista_pesquisa = pd.merge(
                        df_lista_pesquisa_com_dados_tribunais.loc[
                            (df_lista_pesquisa_com_dados_tribunais["n_processo"].fillna('').str.replace(' ', '') != ''),
                            df_lista_pesquisa_com_dados_tribunais.columns
                        ],
                        df_lista_pesquisa_pendente, how='outer', on=df_lista_pesquisa_pendente.columns.tolist())
                else:
                    df_lista_pesquisa = df_lista_pesquisa_pendente
                    if 'data_resultado' not in df_lista_pesquisa.columns:
                        df_lista_pesquisa['data_resultado'] = ''

                    if 'detalhe' not in df_lista_pesquisa.columns:
                        df_lista_pesquisa['detalhe'] = ''

                    if isinstance(servico_solicitado, str):
                        if servico_solicitado in lista_possiveis_servicos:
                            if servico_solicitado not in df_lista_pesquisa.columns:
                                df_lista_pesquisa[servico_solicitado] = ''
                        else:
                            raise Exception(
                                f'Serviço "{servico_solicitado}" não foi encontrado em "{lista_possiveis_servicos}"')
                    else:
                        raise Exception('Tipo de dados do "servico_solicitado" não foi identificado.')

                df_lista_pesquisa['nprocesso_sistema_tribunal_instancia'] = df_lista_pesquisa['n_processo'].map(str) + '_' + df_lista_pesquisa['sistema'].map(str) + '_' + df_lista_pesquisa['tribunal'].map(str) + '_' + df_lista_pesquisa['instancia'].map(str)
                df_lista_pesquisa['datasolicitacao'] = datetime.date.today().strftime('%Y/%m/%d')
                df_lista_pesquisa.drop_duplicates(subset=['nprocesso_sistema_tribunal_instancia', 'datasolicitacao'], keep=False, inplace=True)

                df_lista_pesquisa.loc[
                    (df_lista_pesquisa['copia_integral'].fillna('').str.replace(' ', '') == 'RETIRADO'),
                    ['copia_integral', 'data_resultado']
                ] = ['', '']

                df_lista_pesquisa.loc[
                    (df_lista_pesquisa['copia_integral'].fillna('').str.replace(' ', '') == '') &
                    (df_lista_pesquisa['url'].fillna('').str.replace(' ', '') == ''),
                    ['copia_integral', 'data_resultado']
                ] = ['RETIRADO', datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')]
                df_lista_pesquisa = df_lista_pesquisa.fillna('')

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
                except Exception as e4:
                    raise Exception(f'O dataframe gerado não possui todas as colunas obrigatórias. Exception: {e4}')
        except Exception as e:
            raise Exception(f'Ocorreu um erro no método "inserir_colunas_info_sites". Exception:{e}')
        else:
            return df_lista_pesquisa