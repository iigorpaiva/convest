import sqlite3 as sql
import pandas as pd
from alpha_vantage.timeseries import TimeSeries 

from datetime import datetime

import traceback
import sys

API_KEY = open("KEY.txt").read()

ts = TimeSeries(key = API_KEY, output_format = 'pandas')

B3SA3, MetaDados_B3SA3 = ts.get_daily('B3SA3.SAO', outputsize='full')
PETR4, MetaDados_PETR4 = ts.get_daily('PETR4.SAO', outputsize='full')

B3SA3.columns = ['open', 'high', 'low', 'close', 'volume']
B3SA3.insert(5, "simbolo", "B3SA3")
B3SA3.insert(6, "nome", "Brasil, Bolsa Balcao")
B3SA3.insert(7, "habilitado", True)

PETR4.columns = ['open', 'high', 'low', 'close', 'volume']
PETR4.insert(5, "simbolo", "PETR4")
PETR4.insert(6, "nome", "Petrobras")
PETR4.insert(7, "habilitado", True)

try:
    bancoDeDados = sql.connect("storage.db")
    cursor = bancoDeDados.cursor()
    
    DataFrame_B3SA3 = pd.DataFrame(B3SA3.iloc[:5], columns= ['close', 'nome', 'simbolo', 'habilitado'])
    DataFrame_PETR4 = pd.DataFrame(PETR4.iloc[:7], columns= ['close', 'nome', 'simbolo', 'habilitado'])

    cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='B3SA3' ''')

    if cursor.fetchone()[0] == 0:
        print("NÃO EXISTE TABELA B3SA3!")
        DataFrame_B3SA3.to_sql("B3SA3", bancoDeDados, if_exists="replace", index=True) 
    else:
        print('EXISTE TABELA B3SA3!')

        dataBancoDeDados_B3SA3 = cursor.execute("SELECT * FROM B3SA3;")
        dataBancoDeDados_B3SA3 = cursor.fetchmany(7)

        for index_B3SA3, row in DataFrame_B3SA3.iterrows():
            for i in range(len(dataBancoDeDados_B3SA3)):

                igual = False
                datas = str(index_B3SA3)
                comparaDatas = "'" + datas + "'" 
                precoAtual = str(row['close'])

                if dataBancoDeDados_B3SA3[i][0] == datas:
                    igual = True
                    break
            if igual == False:
                cursor.execute("INSERT INTO B3SA3 VALUES (?, ?, ?, ?, ?);", (datas, precoAtual, 'Brasil, Bolsa Balcao', 'B3SA3', 1))
            else:
                cursor.execute("UPDATE B3SA3 SET close = " + precoAtual + " WHERE date = " + comparaDatas + ";")

            bancoDeDados.commit()

    cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='PETR4' ''')

    if cursor.fetchone()[0] == 0:
        print("NÃO EXISTE TABELA PETR4!")
        DataFrame_PETR4.to_sql("PETR4", bancoDeDados, if_exists="replace", index=True) 

    else:
        print('EXISTE TABELA PETR4!')

        dataBancoDeDados_PETR4 = cursor.execute("SELECT * FROM PETR4;")
        dataBancoDeDados_PETR4 = cursor.fetchmany(7)

        for index_PETR4, row in DataFrame_PETR4.iterrows():
            for i in range(len(dataBancoDeDados_PETR4)):

                igual = False
                datas = str(index_PETR4)
                comparaDatas = "'" + datas + "'" 
                precoAtual = str(row['close'])

                if dataBancoDeDados_PETR4[i][0] == datas:
                    igual = True
                    break
            if igual == False:
                cursor.execute("INSERT INTO PETR4 VALUES (?, ?, ?, ?, ?);", (datas, precoAtual, 'Petrobras', 'PETR4', 1))
            else:
                cursor.execute("UPDATE PETR4 SET close = " + precoAtual + " WHERE date = " + comparaDatas + ";")

            bancoDeDados.commit()
 
except sql.Error as error:

    print("Erro na conexão com sqlite.")

finally:
    
    print("Ativos importados com sucesso!")
    bancoDeDados.close()