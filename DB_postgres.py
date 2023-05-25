import pyodbc
import pandas as pd

class DB_bots:
    cnxn = None
    cursor = None
    cnxn_str = (
        "DRIVER={PostgreSQL Unicode};"
        "DATABASE=painelbots;"
        "UID=painelbots;"
        "PWD=painelbots;"
        "SERVER=painelbots.cfos7cdftipk.us-east-2.rds.amazonaws.com;"
        "PORT=5432;"
    )

    def abertura_conexao(self):
        try:
            self.cnxn = pyodbc.connect(self.cnxn_str)
            self.cursor = self.cnxn.cursor()
        except pyodbc.NotSupportedError as e:
            raise Exception('Erro ocorreu na função "abertura_conexao" da classe "DB_bots". Exceção: Erro de dados. ' + str(e))
        except pyodbc.IntegrityError as e:
            raise Exception('Erro ocorreu na função "abertura_conexao" da classe "DB_bots". Exceção: Erro operacional. ' + str(e))
        except pyodbc.DataError as e:
            raise Exception('Erro ocorreu na função "abertura_conexao" da classe "DB_bots". Exceção: Erro de integridade. ' + str(e))
        except pyodbc.ProgrammingError as e:
            raise Exception('Erro ocorreu na função "abertura_conexao" da classe "DB_bots". Exceção: Erro interno. ' + str(e))
        except pyodbc.OperationalError as e:
            raise Exception('Erro ocorreu na função "abertura_conexao" da classe "DB_bots". Exceção: Erro de programação.' + str(e))
        except Exception as e:
            raise Exception('Erro ocorreu na função "abertura_conexao" da classe "DB_bots". Exceção: ' + str(e))
        else:
            print('Função "abertura_conexao" da classe "DB_bots" executou com sucesso!!')

    def encerrar_conexao(self):
        try:
            if self.cnxn != None and self.cursor != None:
                self.cursor.close()
                self.cnxn.close()
        except pyodbc.NotSupportedError as e:
            raise Exception('Erro ocorreu na função "encerrar_conexao" da classe "DB_bots". Exceção: Erro de dados. ' + str(e))
        except pyodbc.IntegrityError as e:
            raise Exception('Erro ocorreu na função "encerrar_conexao" da classe "DB_bots". Exceção: Erro operacional. ' + str(e))
        except pyodbc.DataError as e:
            raise Exception('Erro ocorreu na função "encerrar_conexao" da classe "DB_bots". Exceção: Erro de integridade. ' + str(e))
        except pyodbc.ProgrammingError as e:
            raise Exception('Erro ocorreu na função "encerrar_conexao" da classe "DB_bots". Exceção: Erro interno. ' + str(e))
        except pyodbc.OperationalError as e:
            raise Exception('Erro ocorreu na função "encerrar_conexao" da classe "DB_bots". Exceção: Erro de programação.' + str(e))
        except Exception as e:
            raise Exception('Erro ocorreu na função "encerrar_conexao" da classe "DB_bots". Exceção: ' + str(e))
        else:
            self.cnxn = None
            self.cursor = None
            print('Função "encerrar_conexao" da classe "DB_bots" executou com sucesso!!')

    def get_user(self):
        try:
            self.abertura_conexao()
            query = 'SELECT * FROM usuario;'
            #self.cursor.execute(query)
            #response = self.cursor.fetchall()
            response = pd.read_sql(query, self.cnxn)
        except pyodbc.NotSupportedError as e:
            self.encerrar_conexao()
            raise Exception(
                'Erro ocorreu na função "select_empresa" da classe "DB_bots". Exceção: Erro de dados. ' + str(e))
        except pyodbc.IntegrityError as e:
            self.encerrar_conexao()
            raise Exception(
                'Erro ocorreu na função "select_empresa" da classe "DB_bots". Exceção: Erro operacional. ' + str(e))
        except pyodbc.DataError as e:
            self.encerrar_conexao()
            raise Exception(
                'Erro ocorreu na função "select_empresa" da classe "DB_bots". Exceção: Erro de integridade. ' + str(e))
        except pyodbc.ProgrammingError as e:
            self.encerrar_conexao()
            raise Exception(
                'Erro ocorreu na função "select_empresa" da classe "DB_bots". Exceção: Erro interno. ' + str(e))
        except pyodbc.OperationalError as e:
            self.encerrar_conexao()
            raise Exception(
                'Erro ocorreu na função "select_empresa" da classe "DB_bots". Exceção: Erro de programação.' + str(e))
        except Exception as e:
            self.encerrar_conexao()
            raise Exception('Erro ocorreu na função "select_empresa" da classe "DB_bots". Exceção: ' + str(e))
        else:
            print('Função "%s" da classe "DB_bots" executou com sucesso!!' % __name__)
            self.encerrar_conexao()
            return response

