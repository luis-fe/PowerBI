import pyodbc

def Conexao():
    #Conectado ao Banco DE DADOS do ERP DA EMPRESA VIA ODBC
    conn = pyodbc.connect(dsn = 'CONSISTEM', user ='root', password = 'ccscache')

    return conn