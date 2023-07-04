import pandas as pd

class TratarDFRolim:
    lista_possiveis_servicos = ['protocolo', 'copia_integral']

    def inserir_na_coluna_tratativa_concluida(self, df:pd.DataFrame, coluna_validacao:str) -> pd.DataFrame:
        try:
            if 'tratativa_concluida' not in df.columns:
                df['tratativa_concluida'] = ''

            df.loc[
                df[coluna_validacao].fillna('').str.replace(' ', '') != '',
                'tratativa_concluida'
            ] = 'sim'
        except Exception as e:
            raise Exception(f'Ocorreu um erro no método "inserir_na_coluna_tratativa_concluida". Exception:{e}')
        else:
            print("A função 'inserir_na_coluna_tratativa_concluida' executou com sucesso!")
            return df

    def inserir_colunas_info_sites(self, df_lista_endereco_site:pd.DataFrame, df_lista_pesquisa:pd.DataFrame, servico_solicitado:str) -> pd.DataFrame:
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
            except Exception as e1:
                raise Exception(f'O dataframe "lista_endereco_site" não possui todas as colunas obrigatórias. Detalhe: {e1}.')

            if 'copia1' in df_lista_endereco_site.columns:
                df_lista_endereco_site = df_lista_endereco_site.drop(columns=['copia1'])

            if 'copia2' in df_lista_endereco_site.columns:
                df_lista_endereco_site = df_lista_endereco_site.drop(columns=['copia2'])

            df_lista_endereco_site['instancia'] = df_lista_endereco_site['instancia'].str.split('.', expand=True).reindex([0], axis=1)
            df_lista_endereco_site['cod_tribunal'] = df_lista_endereco_site['cod_tribunal'].str.split('.', expand=True).reindex([0], axis=1)

            if 'n_processo' not in df_lista_pesquisa.columns:
                raise Exception(f'O dataframe "lista_pesquisa" não possui a coluna "n_processo" que é obrigatória.')
            if 'tratativa_concluida' not in df_lista_pesquisa.columns:
                df_lista_pesquisa['tratativa_concluida'] = ''

            if 'cod_tribunal' in df_lista_pesquisa.columns:
                df_lista_pesquisa['cod_tribunal'] = df_lista_pesquisa['cod_tribunal'].str.split('.', expand=True).reindex([0], axis=1)
            else:
                df_lista_pesquisa['cod_tribunal'] = ''

            if 'tribunal' in df_lista_pesquisa.columns:
                df_lista_pesquisa['tribunal'] = df_lista_pesquisa['tribunal'].str.lower().str.replace(' ', '')
            else:
                df_lista_pesquisa['tribunal'] = ''

            if 'sistema' in df_lista_pesquisa.columns:
                df_lista_pesquisa['sistema'] = df_lista_pesquisa['sistema'].str.lower().str.replace(' ', '')
            else:
                df_lista_pesquisa['sistema'] = ''

            if 'instancia' in df_lista_pesquisa.columns:
                df_lista_pesquisa['instancia'] = df_lista_pesquisa['instancia'].str.split('.', expand=True).reindex([0], axis=1)
                df_lista_pesquisa['instancia'] = df_lista_pesquisa['instancia'].str.lower().str.replace(' ', '')
            else:
                df_lista_pesquisa['instancia'] = ''

            if 'url' not in df_lista_pesquisa.columns:
                df_lista_pesquisa["url"] = ''

            if 'login' not in df_lista_pesquisa.columns:
                df_lista_pesquisa["login"] = ''

            if 'senha' not in df_lista_pesquisa.columns:
                df_lista_pesquisa["senha"] = ''

            if 'validado' not in df_lista_pesquisa.columns:
                df_lista_pesquisa["validado"] = ''

            if 'habilitado' not in df_lista_pesquisa.columns:
                df_lista_pesquisa["habilitado"] = ''

            if 'data_resultado' not in df_lista_pesquisa.columns:
                df_lista_pesquisa['data_resultado'] = ''

            if isinstance(servico_solicitado, str):
                if servico_solicitado in self.lista_possiveis_servicos:
                    if servico_solicitado not in df_lista_pesquisa.columns:
                        df_lista_pesquisa[servico_solicitado] = ''
                else:
                    raise Exception(f'Serviço "{servico_solicitado}" não foi encontrado em "{self.lista_possiveis_servicos}"')
            else:
                raise Exception('Tipo de dados do "servico_solicitado" não foi identificado.')

            if 'detalhe' not in df_lista_pesquisa.columns:
                df_lista_pesquisa['detalhe'] = ''

            if 'Cliente' not in df_lista_pesquisa.columns:
                df_lista_pesquisa['Cliente'] = ''

            df_lista_pesquisa.loc[
                (df_lista_pesquisa['tratativa_concluida'].fillna('').str.replace(' ', '') == ''),
                'n_processo_original'
            ]= df_lista_pesquisa['n_processo']

            df_lista_pesquisa.loc[
                (df_lista_pesquisa['tratativa_concluida'].fillna('').str.replace(' ', '') == ''),
                'n_processo'
            ] = df_lista_pesquisa['n_processo'].str.replace('[^0-9]', '', regex=True).str.replace(' ', '')

            df_lista_pesquisa.loc[
                (df_lista_pesquisa['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df_lista_pesquisa['n_processo'].fillna('').str.replace(' ','').str.len() >= 16),
                'n_processo'
            ] = df_lista_pesquisa['n_processo'].fillna('').str.replace(' ', '').apply(lambda x: x.zfill(20))

            df_lista_pesquisa.loc[
                (df_lista_pesquisa['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df_lista_pesquisa['n_processo'].fillna('').str.replace(' ','').str.len() >= 20),
                'cod_tribunal'
            ] = df_lista_pesquisa['n_processo'].str[13:16]

            df_lista_pesquisa.loc[
                (df_lista_pesquisa['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (
                        (df_lista_pesquisa['instancia'].fillna('').str.replace(' ','') == 'stj') |
                        (df_lista_pesquisa['instancia'].fillna('').str.replace(' ','') == 'stf')
                ),
                'cod_tribunal'
            ] = '-'

            df_lista_pesquisa.loc[
                (df_lista_pesquisa['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df_lista_pesquisa['n_processo'].fillna('').str.replace(' ','') == ''),
                'cod_tribunal'
            ] = '*'


            df_lista_pesquisa.loc[
                (df_lista_pesquisa['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df_lista_pesquisa['n_processo'].fillna('').str.replace(' ','').str.len() < 16) &
                (df_lista_pesquisa['cod_tribunal'].fillna('').str.replace(' ','') == ''),
                'cod_tribunal'
            ] = '-'

            df_lista_pesquisa_pendente_de_dados_tribunais = df_lista_pesquisa.loc[
                (df_lista_pesquisa["tratativa_concluida"].fillna('').str.replace(' ','') == '') &
                (df_lista_pesquisa["n_processo"].fillna('').str.replace(' ','') != ''),
                df_lista_pesquisa.columns.tolist()
            ].fillna('')

            if len(df_lista_pesquisa_pendente_de_dados_tribunais) == 0:
                print('Todos os processos ja possuem dados de acesso.')
            else:
                lt_drop = ['login','senha','validado','habilitado']

                lt_merge = []
                if '' not in list(df_lista_pesquisa_pendente_de_dados_tribunais['url']):
                    lt_merge.append('url')
                else:
                    lt_drop.append('url')

                if '' not in list(df_lista_pesquisa_pendente_de_dados_tribunais['cod_tribunal']):
                    lt_merge.append('cod_tribunal')
                else:
                    lt_drop.append('cod_tribunal')

                if '' not in list(df_lista_pesquisa_pendente_de_dados_tribunais['instancia']):
                    lt_merge.append('instancia')
                else:
                    lt_drop.append('instancia')

                if '' not in list(df_lista_pesquisa_pendente_de_dados_tribunais['sistema']):
                    lt_merge.append('sistema')
                else:
                    lt_drop.append('sistema')

                if '' not in list(df_lista_pesquisa_pendente_de_dados_tribunais['tribunal']):
                    lt_merge.append('tribunal')
                else:
                    lt_drop.append('tribunal')

                df_lista_pesquisa_sem_site = df_lista_pesquisa_pendente_de_dados_tribunais.drop(columns=lt_drop)

                df_lista_pesquisa_pendente = pd.merge(df_lista_pesquisa_sem_site, df_lista_endereco_site, how='left', on=lt_merge)

                df_lista_pesquisa = pd.merge(
                    df_lista_pesquisa.loc[
                        (df_lista_pesquisa["tratativa_concluida"].fillna('').str.replace(' ', '') != '') &
                        (df_lista_pesquisa["n_processo"].fillna('').str.replace(' ', '') != ''),
                        df_lista_pesquisa.columns
                    ],
                    df_lista_pesquisa_pendente
                    , how='outer', on=df_lista_pesquisa_pendente.columns.tolist())

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
                df_lista_pesquisa['Cliente']
                df_lista_pesquisa['tratativa_concluida']
            except Exception as e4:
                raise Exception(f'O dataframe gerado não possui todas as colunas obrigatórias. Exception: {e4}')
        except Exception as e:
            raise Exception(f'Ocorreu um erro no método "inserir_colunas_info_sites". Exception:{e}')
        else:
            return df_lista_pesquisa

class TratarDFBarcelos(TratarDFRolim):
    # Possiveis status: "ORDEM DOS ARQUIVOS INCORRETA.", 'NOME DO ARQUIVO FORA DO PADRÃO.'
    def gerar_excel_principal(self, list_nome_arquivos:list, df_buscado:pd.DataFrame) -> pd.DataFrame:

        try:
            df = pd.DataFrame(list_nome_arquivos, columns=['Nome do arquivo'])
            if len(df) == 0:
                raise Exception('Nao foi identificado nennhum protocolo na pasta "Barcelos".')
            if len(df_buscado) > 0:
                df = pd.merge(df, df_buscado, how='outer', on=['Nome do arquivo'])

            if 'protocolo' not in df.columns:
                df['protocolo'] = ''

            if 'tratativa_concluida' not in df.columns:
                df['tratativa_concluida'] = ''

            if 'status_comprovante' not in df.columns:
                df['status_comprovante'] = ''

            if len(df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == ''),
                ['Nome do arquivo']
            ]) == 0:
                df_final = df.loc[
                    (df["Nome do arquivo"].fillna('') != ''),
                    df.columns
                ].fillna('')
                return df_final

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

            if 'Tipo do protocolo' not in df.columns:
                df['Tipo do protocolo'] = ''

            if 'Tipo arquivo' not in df.columns:
                df['Tipo arquivo'] = ''

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['Nome do arquivo'].str.count('-') != 2),
                ['protocolo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.',TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            # tratativas do numero do protocolo
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                ~(df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[0].isna()),
                   'n_processo'
            ] = df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[0]

            # adicionar regex para retirar os pontos
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['n_processo'].fillna('').str.replace(' ','') != ''),
                   'n_processo'
            ] = df['n_processo'].str.replace(' ', '')

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['n_processo'].fillna('').str.replace(' ', '') != ''),
                   'n_processo'
            ] = df['n_processo'].str.replace('[^0-9]', '', regex=True)

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (
                        (df['n_processo'].str.len() < 15) |
                        (df['n_processo'].str.len() > 20)
                ),
                ['protocolo', 'n_processo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]


            # acrescentar zeros a esquerda
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['n_processo'].str.len() >= 16),
                   'n_processo'
            ] = df['n_processo'].fillna('').str.replace(' ', '').apply(lambda x: x.zfill(20))

            # tratativas do cod_tribunal
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['n_processo'].str.len() == 20),
                   'cod_tribunal'
            ] = df['n_processo'].fillna('').str[-7:-4]

            # tratativas do Ordem do arquivo
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                ~(df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[1].isna()),
                   'Ordem do arquivo'
            ] = df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[1].replace(' ', '')

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['Ordem do arquivo'].fillna('').str.replace(' ','') != ''),
                   'Ordem do arquivo'
            ] = df['Ordem do arquivo'].str.replace(' ', '')

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['Ordem do arquivo'].str.len() > 3),
                ['protocolo', 'Ordem do arquivo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['Ordem do arquivo'].fillna('').str.replace(' ', '') == ''),
                ['protocolo', 'Ordem do arquivo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            # tratativas do nome_arquivo
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                ~(df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[2].isna()),
                   'nome_arquivo'
            ] = df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[2]

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                ~(df['nome_arquivo'].str.split(".", n=-1, expand=False).str[0].isna()),
                   'nome_arquivo'
            ] = df['nome_arquivo'].str.split(".", n=-1, expand=False).str[0]

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['nome_arquivo'].str.split(".", n=-1, expand=False).str[0].isna()),
                ['protocolo', 'nome_arquivo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            # tratativas do identificador
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['nome_arquivo'].str[-8:].str.replace(' ','').str.len() > 0),
                   'Identificador'
            ] = df['nome_arquivo'].fillna('').str[-8:]

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['nome_arquivo'].str[-8:].str.replace(' ','').str.len() == 0),
                ['protocolo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['Identificador'].fillna('').str.replace(' ','') != ''),
                   'Identificador'
            ] = df['Identificador'].str.replace('[^0-9]', '', regex=True)


            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['nome_arquivo'].str[:-8].str.len() > 0),
                   'nome_arquivo'
            ] = df['nome_arquivo'].fillna('').str[:-8]

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['nome_arquivo'].str.len() > 0),
                   'nome_arquivo'
            ] = df['nome_arquivo'].fillna('').str.replace('\d+', '', regex=True).replace('_', '')

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['nome_arquivo'].fillna('').str.replace(' ','') != ''),
                   'nome_arquivo_original'
            ] = df['nome_arquivo'].str.replace('_', '')

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['nome_arquivo_original'].fillna('').str.replace(' ','') != ''),
                   'nome_arquivo'
            ] = df['nome_arquivo_original'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode(
                'utf-8').str.lower()

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['nome_arquivo'].fillna('').str.replace(' ','') != ''),
                   'nome_arquivo'
            ] = df['nome_arquivo'].str.replace(' ', '')

            df_ordem_1 = df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df["Ordem do arquivo"].fillna('') == '1'),
                [
                    'Nome do arquivo'
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
                    (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                    (df['n_processo'].fillna('') == row['n_processo']),
                    'Ordem do arquivo'
                ].max()

                qt_identificador = len(df.loc[
                                           (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                                           (df['n_processo'].fillna('') == row['n_processo']),
                                           'Ordem do arquivo'
                                       ])

                qt_valores_duplicados = (df.loc[
                                             (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                                             (df['n_processo'].fillna('') == row['n_processo']),
                                             'Ordem do arquivo'
                                         ].duplicated()).sum()

                df.loc[
                    (df['n_processo'].fillna('') == row['n_processo']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'data_resultado'
                ] = (maior_identificador != qt_identificador and TratarTempo().tempo_atual().strftime(
                    '%d/%m/%Y %H:%M:%S') or "")

                df.loc[
                    (df['n_processo'].fillna('') == row['n_processo']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'protocolo'
                ] = (maior_identificador != qt_identificador and "ORDEM DOS ARQUIVOS INCORRETA." or "")

                df.loc[
                    (df['n_processo'].fillna('') == row['n_processo']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'data_resultado'
                ] = (qt_valores_duplicados > 0 and TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S') or "")

                df.loc[
                    (df['n_processo'].fillna('') == row['n_processo']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'protocolo'
                ] = (qt_valores_duplicados > 0 and "ORDEM DOS ARQUIVOS INCORRETA." or "")

                df.loc[
                    (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                    (df['protocolo'].fillna('').str.replace(' ', '') == '') &
                    (df['n_processo'].fillna('') == row['n_processo']),
                    'Identificador'
                ] = row['Identificador']

            df['Ordem do arquivo'] = df['Ordem do arquivo'].astype(str).str.split(".", n=-1, expand=False).str[0]
            df.sort_values(['n_processo', 'Ordem do arquivo'], inplace=True)
            df.reset_index(inplace=True, drop=True)

            df_final = df.loc[
                (df["Nome do arquivo"].fillna('') != ''),
                df.columns
            ].fillna('')

        except Exception as e:
            raise Exception(f'Ocorreu um erro no método "gerar_excel_principal". Exception:{e}')
        else:
            print("A função 'gerar_excel_principal' executou com sucesso!")
            return df_final

    def depara_tipo(self, df_original:pd.DataFrame, df_depara_tipoprotocolo:pd.DataFrame, df_depara_tipoarquivo:pd.DataFrame) -> pd.DataFrame:
        '''

        :param df_original:
        :param df_depara_tipoprotocolo:
        :param df_depara_tipoarquivo:
        :return:
        '''
        try:
            try:
                df_depara_tipoprotocolo['nome_arquivo']
            except:
                df_depara_tipoprotocolo['nome_arquivo'] = ''

            df_depara_tipoprotocolo.loc[df_depara_tipoprotocolo['nome_arquivo_original'].fillna('') != '',
                                        'nome_arquivo'
            ] = df_depara_tipoprotocolo['nome_arquivo_original'].str.normalize('NFKD').str.encode('ascii',
                                                                                                  errors='ignore').str.decode(
                'utf-8').str.lower()

            df_depara_tipoprotocolo.loc[df_depara_tipoprotocolo['nome_arquivo'].fillna('') != '',
                                        'nome_arquivo'
            ] = df_depara_tipoprotocolo['nome_arquivo'].str.replace(' ', '')

            df_depara_tipoprotocolo = df_depara_tipoprotocolo.loc[
                (df_depara_tipoprotocolo["Tipo do protocolo"].fillna('') != ''),
                [
                    'sistema',
                    'tribunal',
                    'nome_arquivo',
                    'Tipo do protocolo'
                ]
            ].drop_duplicates().fillna('')

            df_copia_df = df_original
            df_copia_df = df_copia_df.drop(columns=['Tipo do protocolo','Tipo arquivo'])

            df_original.loc[
                (df_original["Ordem do arquivo"].fillna('') == '1') &
                (df_original['tratativa_concluida'].fillna('').str.replace(' ', '') == ''),
                df_original.columns
            ] = pd.merge(df_copia_df, df_depara_tipoprotocolo, how='left', on=['sistema', 'tribunal', 'nome_arquivo'])

            try:
                df_depara_tipoarquivo['nome_arquivo']
            except:
                df_depara_tipoarquivo['nome_arquivo'] = ''

            df_depara_tipoarquivo.loc[
                (df_depara_tipoarquivo['nome_arquivo_original'].fillna('') != ''),
                'nome_arquivo'
            ] = df_depara_tipoarquivo['nome_arquivo_original'].str.normalize('NFKD').str.encode('ascii',
                                                                                                errors='ignore').str.decode(
                'utf-8').str.lower()

            df_depara_tipoarquivo.loc[
                (df_depara_tipoarquivo['nome_arquivo'].fillna('') != ''),
                'nome_arquivo'
            ] = df_depara_tipoarquivo['nome_arquivo'].str.replace(' ', '')

            df_depara_tipoarquivo = df_depara_tipoarquivo.loc[
                (df_depara_tipoarquivo["nome_arquivo"].fillna('') != ''),
                [
                    'sistema',
                    'tribunal',
                    'nome_arquivo',
                    'Tipo arquivo'
                ]
            ].drop_duplicates().fillna('')


            df_copia_df = df_original
            df_copia_df = df_copia_df.drop(columns=['Tipo arquivo'])

            df_original.loc[
                df_original['tratativa_concluida'].fillna('').str.replace(' ', '') == '',
                df_original.columns
            ] = pd.merge(df_copia_df, df_depara_tipoarquivo, how='left', on=['sistema','tribunal','nome_arquivo'])


            df_original.loc[
                (df_original['Ordem do arquivo'].fillna('').str.replace(' ', '') == '1') &
                (df_original['Tipo do protocolo'].fillna('').str.replace(' ', '') == '') &
                (df_original['tratativa_concluida'].fillna('').str.replace(' ', '') == ''),
                   'protocolo'
            ] = 'TIPO DE PROTOCOLO NÃO IDENTIFICADO NO "DE/PARA".'

            df_original.sort_values(['n_processo', 'cod_tribunal', 'instancia', 'Ordem do arquivo'], inplace=True)

            df_original.loc[
                (df_original['Ordem do arquivo'].fillna('') == '1') &
                (df_original['tratativa_concluida'].fillna('').str.replace(' ', '') == ''),
                   'protocolo'
            ] = df_original.groupby(
                [
                    'n_processo',
                    'tribunal',
                    'sistema',
                    'instancia'
                ])['protocolo'].transform(lambda x: ' '.join(x))


            #df['protocolo'] = df['protocolo'].str.replace(' ', '')

            df_original.loc[
                (df_original['Ordem do arquivo'].fillna('') == '1') &
                (df_original['tratativa_concluida'].fillna('').str.replace(' ', '') == ''),
                   'lista_arquivos_para_anexar'
            ] = df_original.groupby(
                [
                    'n_processo',
                    'tribunal',
                    'sistema',
                    'instancia'
                ])['Nome do arquivo'].transform(lambda x: ', '.join(x))

        except Exception as e:
            raise Exception(f'Ocorreu um erro no método "depara_tipo". Exception:{e}')
        else:
            print("A função 'depara_tipo' executou com sucesso!")
            return df_original

class TratarDFBarcelosEsporadico(TratarDFBarcelos):
    # Possiveis status: "ORDEM DOS ARQUIVOS INCORRETA.", 'NOME DO ARQUIVO FORA DO PADRÃO.'
    def gerar_excel_principal(self, list_nome_arquivos:list, df_buscado:pd.DataFrame) -> pd.DataFrame:

        try:
            df_dict = pd.DataFrame(list_nome_arquivos, columns=['Nome do arquivo'])
            if len(df_dict) == 0:
                raise Exception('Nao foi identificado nennhum protocolo na pasta "Barcelos".')
            if len(df_buscado) > 0:
                df = pd.merge(df_dict, df_buscado, how='outer', on=['Nome do arquivo'])
            else:
                df = df_dict

            if 'protocolo' not in df.columns:
                df['protocolo'] = ''

            if 'tratativa_concluida' not in df.columns:
                df['tratativa_concluida'] = ''

            if len(df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == ''),
                ['Nome do arquivo']
            ]) == 0:
                df_final = df.loc[
                    (df["Nome do arquivo"].fillna('') != ''),
                    df.columns
                ].fillna('')
                return df_final

            if 'n_processo' not in df.columns:
                df['n_processo'] = ''

            if 'status_comprovante' not in df.columns:
                df['status_comprovante'] = ''

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

            if 'Tipo do protocolo' not in df.columns:
                df['Tipo do protocolo'] = ''

            if 'Tipo arquivo' not in df.columns:
                df['Tipo arquivo'] = ''

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['Nome do arquivo'].str.count('-') != 2),
                ['protocolo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            # tratativas do numero do protocolo
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                ~(df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[0].isna()),
                   'n_processo'
            ] = df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[0]

            # adicionar regex para retirar os pontos
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['n_processo'].fillna('').str.replace(' ','') != ''),
                   'n_processo'
            ] = df['n_processo'].str.replace(' ', '')

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['n_processo'].fillna('').str.replace(' ', '') != ''),
                   'n_processo'
            ] = df['n_processo'].str.replace('[^0-9]', '', regex=True)

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (
                        (df['n_processo'].str.len() < 15) |
                        (df['n_processo'].str.len() > 20)
                ),
                ['protocolo', 'n_processo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            # acrescentar zeros a esquerda
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['n_processo'].str.len() >= 16),
                   'n_processo'
            ] = df['n_processo'].fillna('').str.replace(' ', '').apply(lambda x: x.zfill(20))

            # tratativas do cod_tribunal
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['n_processo'].str.len() == 20),
                   'cod_tribunal'
            ] = df['n_processo'].fillna('').str[-7:-4]

            # tratativas do Ordem do arquivo
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                ~(df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[1].isna()),
                   'Ordem do arquivo'
            ] = df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[1].replace(' ', '')

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['Ordem do arquivo'].fillna('').str.replace(' ','') != ''),
                   'Ordem do arquivo'
            ] = df['Ordem do arquivo'].str.replace(' ', '')

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['Ordem do arquivo'].str.len() > 3),
                ['protocolo', 'Ordem do arquivo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['Ordem do arquivo'].fillna('').str.replace(' ', '') == ''),
                ['protocolo', 'Ordem do arquivo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            # tratativas do nome_arquivo
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                ~(df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[2].isna()),
                   'nome_arquivo'
            ] = df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[2]

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                ~(df['nome_arquivo'].str.split(".", n=-1, expand=False).str[0].isna()),
                   'nome_arquivo'
            ] = df['nome_arquivo'].str.split(".", n=-1, expand=False).str[0]

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['nome_arquivo'].str.split(".", n=-1, expand=False).str[0].isna()),
                ['protocolo', 'nome_arquivo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            # tratativas do identificador
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['nome_arquivo'].str.replace(' ','').str.len() > 0),
                   'Identificador'
            ] = df['n_processo'].fillna('')

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['nome_arquivo'].str.len() > 0),
                   'nome_arquivo'
            ] = df['nome_arquivo'].fillna('').str.replace('\d+', '', regex=True).replace('_', '')

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['nome_arquivo'].fillna('').str.replace(' ','') != ''),
                   'nome_arquivo_original'
            ] = df['nome_arquivo'].str.replace('_', '')

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['nome_arquivo_original'].fillna('').str.replace(' ','') != ''),
                   'nome_arquivo'
            ] = df['nome_arquivo_original'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode(
                'utf-8').str.lower()

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['nome_arquivo'].fillna('').str.replace(' ','') != ''),
                   'nome_arquivo'
            ] = df['nome_arquivo'].str.replace(' ', '')

            df_ordem_1 = df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
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
                    (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                    (df['n_processo'].fillna('') == row['n_processo']),
                    'Ordem do arquivo'
                ].max()

                qt_identificador = len(df.loc[
                                           (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                                           (df['n_processo'].fillna('') == row['n_processo']),
                                           'Ordem do arquivo'
                                       ])

                qt_valores_duplicados = (df.loc[
                                             (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                                             (df['n_processo'].fillna('') == row['n_processo']),
                                             'Ordem do arquivo'
                                         ].duplicated()).sum()

                df.loc[
                    (df['n_processo'].fillna('') == row['n_processo']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'data_resultado'
                ] = (maior_identificador != qt_identificador and TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S') or "")

                df.loc[
                    (df['n_processo'].fillna('') == row['n_processo']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'protocolo'
                ] = (maior_identificador != qt_identificador and "ORDEM DOS ARQUIVOS INCORRETA." or "")

                df.loc[
                    (df['n_processo'].fillna('') == row['n_processo']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'data_resultado'
                ] = (qt_valores_duplicados > 0 and TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S') or "")

                df.loc[
                    (df['n_processo'].fillna('') == row['n_processo']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'protocolo'
                ] = (qt_valores_duplicados > 0 and "ORDEM DOS ARQUIVOS INCORRETA." or "")

                df.loc[
                    (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                    (df['protocolo'].fillna('').str.replace(' ', '') == '') &
                    (df['n_processo'].fillna('') == row['n_processo']),
                    'Identificador'
                ] = row['Identificador']

            df['Ordem do arquivo'] = df['Ordem do arquivo'].astype(str).str.split(".", n=-1, expand=False).str[0]
            df.sort_values(['n_processo', 'Ordem do arquivo'], inplace=True)
            df.reset_index(inplace=True, drop=True)

            df_final = df.loc[
                (df["Nome do arquivo"].fillna('') != ''),
                df.columns
            ].fillna('')

        except Exception as e:
            raise Exception(f'Ocorreu um erro no método "gerar_excel_principal". Exception:{e}')
        else:
            print("A função 'gerar_excel_principal' executou com sucesso!")
            return df_final

class TratarDFCarvalhoPereira(TratarDFBarcelosEsporadico):
    pass

class TratarDFLasmar(TratarDFBarcelos):

    def gerar_excel_principal(self, list_nome_arquivos:list, df_buscado:pd.DataFrame) -> pd.DataFrame:

        try:
            df = pd.DataFrame(list_nome_arquivos, columns=['Nome do arquivo'])

            df_arquivos_gerais = df[-4:]
            df = df[0:-4]
            if len(df) == 0:
                raise Exception('Nao foi identificado nennhum protocolo na pasta "lasmar".')
            df_arquivos_gerais = self.gerar_excel_arquivos_gerais(df_arquivos_gerais=df_arquivos_gerais)

            if len(df_buscado) > 0:
                df = pd.merge(df, df_buscado, how='outer', on=['Nome do arquivo'])

            if 'protocolo' not in df.columns:
                df['protocolo'] = ''

            if 'tratativa_concluida' not in df.columns:
                df['tratativa_concluida'] = ''

            if len(df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == ''),
                ['Nome do arquivo']
            ]) == 0:
                df_final = df.loc[
                    (df["Nome do arquivo"].fillna('') != ''),
                    df.columns
                ].fillna('')
                return df_final

            if 'n_processo' not in df.columns:
                df['n_processo'] = ''

            if 'status_comprovante' not in df.columns:
                df['status_comprovante'] = ''

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

            if 'Tipo do protocolo' not in df.columns:
                df['Tipo do protocolo'] = ''

            if 'Tipo arquivo' not in df.columns:
                df['Tipo arquivo'] = ''

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
                (df['Nome do arquivo'].str.count('-') != 2),
                   ['protocolo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            # tratativas do numero do protocolo
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                ~(df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[0].isna()),
                   'n_processo'
            ] = df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[0]

            # adicionar regex para retirar os pontos
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['n_processo'].fillna('').str.replace(' ', '') != ''),
                'n_processo'
            ] = df['n_processo'].str.replace(' ', '')

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['n_processo'].fillna('').str.replace(' ', '') != ''),
                'n_processo'
            ] = df['n_processo'].str.replace('[^0-9]', '', regex=True)

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (
                        (df['n_processo'].str.len() < 15) |
                        (df['n_processo'].str.len() > 20)
                ),
                ['protocolo', 'n_processo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            # acrescentar zeros a esquerda
            df.loc[
                (df['n_processo'].fillna('').str.replace(' ', '').str.len() >= 16) &
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == ''),
                   'n_processo'
            ] = df['n_processo'].fillna('').str.replace(' ', '').apply(lambda x: x.zfill(20))

            # tratativas do cod_tribunal
            df.loc[
                (df['n_processo'].fillna('').str.replace(' ', '').str.len() == 20) &
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == ''),
                   'cod_tribunal'
            ] = df['n_processo'].fillna('').str[-7:-4]

            # tratativas do Ordem do arquivo
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                ~(df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[1].isna()),
                   'Ordem do arquivo'
            ] = df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[1].replace(' ', '')

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['Ordem do arquivo'].str.len() > 0),
                   'Ordem do arquivo'
            ] = df['Ordem do arquivo'].str.replace(' ', '')

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['Ordem do arquivo'].str.len() > 3),
                ['protocolo', 'Ordem do arquivo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['Ordem do arquivo'].fillna('').str.replace(' ', '') == ''),
                ['protocolo', 'Ordem do arquivo', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            # tratativas do nome_arquivo
            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                ~(df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[2].isna()),
                   'nome_arquivo_original'
            ] = df['Nome do arquivo'].str.split("-", n=-1, expand=False).str[2]

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                ~(df['nome_arquivo_original'].str.split(".", n=-1, expand=False).str[0].isna()),
                   'nome_arquivo_original'
            ] = df['nome_arquivo_original'].str.split(".", n=-1, expand=False).str[0]

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['nome_arquivo_original'].str.split(".", n=-1, expand=False).str[0].isna()),
                ['protocolo', 'nome_arquivo_original', 'data_resultado']
            ] = ['NOME DO ARQUIVO FORA DO PADRÃO.', '', TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S')]

            # tratativas do identificador

            df.loc[
                (df['protocolo'].fillna('').str.replace(' ', '') == '') &
                (df['Ordem do arquivo'].fillna('') == '1'),
                   'Identificador'
            ] = ((df['protocolo'].fillna('').str.replace(' ','') == '') & (df['Ordem do arquivo'].fillna('') == '1')).cumsum()

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                ((df['nome_arquivo_original'].str.len()) > 0),
                   'nome_arquivo_original'
            ] = df['nome_arquivo_original'].fillna('').str.replace('\d+', '', regex=True)

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['nome_arquivo_original'].fillna('') != ''),
                   'nome_arquivo'
            ] = df['nome_arquivo_original'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode(
                'utf-8').str.lower()

            df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                (df['nome_arquivo'].fillna('') != ''),
                   'nome_arquivo'
            ] = df['nome_arquivo'].str.replace(' ', '')

            df_ordem_1 = df.loc[
                (df['tratativa_concluida'].fillna('').str.replace(' ','') == '') &
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
                df_arquivos_gerais.loc[df_arquivos_gerais['Nome do arquivo'].fillna('') != '',
                       ['n_processo', 'cod_tribunal']
                ] = [row['n_processo'], row['cod_tribunal']]

                maior_identificador = df.loc[
                    (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                    (df['n_processo'].fillna('') == row['n_processo']),
                       'Ordem do arquivo'
                ].max()

                qt_identificador = len(df.loc[
                   (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                   (df['n_processo'].fillna('') == row['n_processo']),
                     'Ordem do arquivo'
                ])

                qt_valores_duplicados = (df.loc[
                                             (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                                             (df['n_processo'].fillna('') == row['n_processo']),
                                              'Ordem do arquivo'
                                       ].duplicated()).sum()

                df.loc[
                    (df['n_processo'].fillna('') == row['n_processo']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'data_resultado'
                ] = (maior_identificador != qt_identificador and TratarTempo().tempo_atual().strftime(
                    '%d/%m/%Y %H:%M:%S') or "")

                df.loc[
                    (df['n_processo'].fillna('') == row['n_processo']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'protocolo'
                ] = (maior_identificador != qt_identificador and "ORDEM DOS ARQUIVOS INCORRETA." or "")

                df.loc[
                    (df['n_processo'].fillna('') == row['n_processo']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'data_resultado'
                ] = (qt_valores_duplicados > 0 and TratarTempo().tempo_atual().strftime('%d/%m/%Y %H:%M:%S') or "")

                df.loc[
                    (df['n_processo'].fillna('') == row['n_processo']) &
                    (df['protocolo'].fillna('').str.replace(' ', '') == ''),
                    'protocolo'
                ] = (qt_valores_duplicados > 0 and "ORDEM DOS ARQUIVOS INCORRETA." or "")

                df_arquivos_gerais['Ordem do arquivo'] = df_arquivos_gerais.index + maior_identificador
                df = pd.concat([df, df_arquivos_gerais])

                df.loc[
                    (df['tratativa_concluida'].fillna('').str.replace(' ', '') == '') &
                    (df['protocolo'].fillna('').str.replace(' ', '') == '') &
                    (df['n_processo'].fillna('') == row['n_processo']),
                                       'Identificador'
                ] = row['Identificador']

            df['Ordem do arquivo'] = df['Ordem do arquivo'].astype(str).str.split(".", n=-1, expand=False).str[0]
            df.sort_values(['n_processo','Ordem do arquivo'], inplace=True)
            df.reset_index(inplace=True, drop=True)
            df_final = df.loc[
                (df["Nome do arquivo"].fillna('') != ''),
                df.columns
            ].fillna('')

        except Exception as e:
            raise Exception(f'Ocorreu um erro no método "gerar_excel_principal". Exception:{e}')
        else:
            print("A função 'gerar_excel_principal' executou com sucesso!")
            return df_final

    def gerar_excel_arquivos_gerais(self, df_arquivos_gerais:pd.DataFrame) -> pd.DataFrame:

        try:
            try:
                df_arquivos_gerais['protocolo']
            except:
                df_arquivos_gerais['protocolo'] = ''

            try:
                df_arquivos_gerais['n_processo']
            except:
                df_arquivos_gerais['n_processo'] = ''

            try:
                df_arquivos_gerais['Ordem do arquivo']
            except:
                df_arquivos_gerais['Ordem do arquivo'] = ''

            try:
                df_arquivos_gerais['nome_arquivo_original']
            except:
                df_arquivos_gerais['nome_arquivo_original'] = ''

            try:
                df_arquivos_gerais['nome_arquivo']
            except:
                df_arquivos_gerais['nome_arquivo'] = ''

            try:
                df_arquivos_gerais['Identificador']
            except:
                df_arquivos_gerais['Identificador'] = ''

            try:
                df_arquivos_gerais['cod_tribunal']
            except:
                df_arquivos_gerais['cod_tribunal'] = ''

            # tratativas do Ordem do arquivo
            df_arquivos_gerais.loc[~(df_arquivos_gerais['Nome do arquivo'].str.split("-", n=-1, expand=False).str[0].isna()),
                   'Ordem do arquivo'
            ] = df_arquivos_gerais['Nome do arquivo'].str.split("-", n=-1, expand=False).str[0].replace(' ', '')
            df_arquivos_gerais.loc[df_arquivos_gerais['Ordem do arquivo'].str.len() > 0,
                   'Ordem do arquivo'
            ] = df_arquivos_gerais['Ordem do arquivo'].replace(' ', '')
            '''df_arquivos_gerais.loc[
                (df_arquivos_gerais['Ordem do arquivo'].str.len() > 3) |
                (df_arquivos_gerais['Ordem do arquivo'].fillna('') == ''),
                   ['protocolo', 'Ordem do arquivo']
            ] = ['retirado. Motivo: Ordem do arquivo fora do padrão.', '']'''

            # tratativas do nome_arquivo_original
            df_arquivos_gerais.loc[~(df_arquivos_gerais['Nome do arquivo'].str.split("-", n=-1, expand=False).str[1].isna()),
                   'nome_arquivo_original'
            ] = df_arquivos_gerais['Nome do arquivo'].str.split("-", n=-1, expand=False).str[1]
            df_arquivos_gerais.loc[~(df_arquivos_gerais['nome_arquivo_original'].str.split(".", n=-1, expand=False).str[0].isna()),
                   'nome_arquivo_original'
            ] = df_arquivos_gerais['nome_arquivo_original'].str.split(".", n=-1, expand=False).str[0]

            '''df_arquivos_gerais.loc[df_arquivos_gerais['nome_arquivo'].str.split(".", n=-1, expand=False).str[0].isna(),
                   ['protocolo', 'nome_arquivo']
            ] = ['retirado. Motivo: Tipo de arquivo não identificado.', '']'''

            # tratativas do nome_arquivo
            df_arquivos_gerais.loc[df_arquivos_gerais['nome_arquivo_original'].str.len() > 0,
                   'nome_arquivo'
            ] = df_arquivos_gerais['nome_arquivo_original'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode(
                'utf-8').str.lower()

            df_arquivos_gerais.loc[df_arquivos_gerais['nome_arquivo'].str.len() > 0,
                                   'nome_arquivo'
            ] = df_arquivos_gerais['nome_arquivo'].str.replace(' ', '')

            df_verifica_erro = df_arquivos_gerais.loc[
                (df_arquivos_gerais["Nome do arquivo"].fillna('') != '') &
                (
                    (df_arquivos_gerais["Ordem do arquivo"].fillna('') == '') |
                    (df_arquivos_gerais["nome_arquivo_original"].fillna('') == '') |
                    (df_arquivos_gerais["nome_arquivo"].fillna('') == '')
                ),
                [
                    'Nome do arquivo'
                ]
            ].fillna('')

            if len(df_verifica_erro) > 0:
                raise Exception(f'A nomeclatura de {len(df_verifica_erro)} arquivos gerais est[a errada.')

            #df_arquivos_gerais.sort_values(['Ordem do arquivo'], inplace=True)
            df_arquivos_gerais.index = list(pd.to_numeric(df_arquivos_gerais['Ordem do arquivo'], errors='coerce'))
            df_arquivos_gerais['Ordem do arquivo'] = ''

            df_arquivos_gerais = df_arquivos_gerais.loc[
                (df_arquivos_gerais["Nome do arquivo"].fillna('') != ''),
                df_arquivos_gerais.columns
            ].fillna('')

        except Exception as e:
            raise Exception(f'Ocorreu um erro no método "gerar_excel_arquivos_gerais". Exception:{e}')
        else:
            print("A função 'gerar_excel_arquivos_gerais' executou com sucesso!")
            return df_arquivos_gerais


class TratarDFPorCliente:
    tratar_df_rolim = TratarDFRolim()
    tratar_df_barcelos = TratarDFBarcelos()
    tratar_df_lasmar = TratarDFLasmar()
    tratar_df_barcelosesporadico = TratarDFBarcelosEsporadico()
    tratar_df_carvalhopereira = TratarDFCarvalhoPereira()
