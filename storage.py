import sqlite3 as sql
import pandas as pd
from alpha_vantage.timeseries import TimeSeries 

from datetime import datetime

import traceback
import sys

API_KEY = open("KEY.txt").read()

ts = TimeSeries(key = API_KEY, output_format = 'pandas')

ativos = [("B3SA3", True), ("PETR4", True)]

for a in ativos:

    tabelaAtivo, MetaDados = ts.get_daily(str(a[0]) + ".SAO", outputsize='full')

    simbolo = str(MetaDados['2. Symbol'])[0:5]

    if simbolo == "B3SA3":
        nome = "Brasil, Bolsa Balcao"
    elif simbolo == "PETR4":
        nome = "Petrobras"

    tabelaAtivo.columns = ['open', 'high', 'low', 'close', 'volume']
    tabelaAtivo.insert(5, "simbolo", simbolo)
    tabelaAtivo.insert(5, "nome", nome)
    tabelaAtivo.insert(6, "habilitado", a[1])

    if tabelaAtivo['habilitado'].values[0] == True:

        try:
            bancoDeDados = sql.connect("storage.db")
            cursor = bancoDeDados.cursor()

            DataFrame = pd.DataFrame(tabelaAtivo.iloc[:7], columns= ['close', 'nome', 'simbolo', 'habilitado'])

            strSimbolo = "'" + simbolo + "'"

            cursor.execute(("SELECT count(name) FROM sqlite_master WHERE type='table' AND name = "+ strSimbolo + ";"))
                                                    
            if cursor.fetchone()[0] == 0:
                print("NÃO EXISTE TABELA DO ATIVO " + simbolo)
                DataFrame.to_sql(simbolo, bancoDeDados, if_exists="replace", index=True) 
            else:
                print("EXISTE TABELA DO ATIVO " + simbolo)

                dataBancoDeDados = cursor.execute("SELECT * FROM B3SA3;")
                dataBancoDeDados = cursor.fetchmany(7)

                for index, row in DataFrame.iterrows():
                    for i in range(len(dataBancoDeDados)):

                        igual = False
                        datas = str(index)
                        strDatas = "'" + datas + "'" 
                        precoAtual = str(row['close'])
                        nomeAtivo = str(row['nome'])
                        simboloAtivo = str(row['simbolo'])

                        if dataBancoDeDados[i][0] == datas:
                            igual = True
                            break
                    if igual == False:
                        cursor.execute("INSERT INTO " + simbolo + " VALUES (?, ?, ?, ?, ?);", (datas, precoAtual, nomeAtivo, simboloAtivo, 1))
                    else:
                        cursor.execute("UPDATE " + simbolo + " SET close = " + precoAtual + " WHERE date = " + strDatas + ";")
                                                
                    bancoDeDados.commit()
        
        except sql.Error as error:

            print("Erro na conexão com sqlite.")

        finally:
            
            print(simbolo +" importado com sucesso!")
            bancoDeDados.close()
    
    else:
        print("Esse ativo nao esta habilitado para importacao!")