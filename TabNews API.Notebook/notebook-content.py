# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "23cc9a3d-2cf7-4620-b0e4-e7dc5d339b52",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "0b451587-eab4-4658-a425-3e76245475b4"
# META     }
# META   }
# META }

# CELL ********************

#%%

import requests
import pandas as pd
import datetime
import json
import os
import time

#%%

#Busco a API utilizando o kwargs para puxar todos os parâmetros
def get_response(**kwargs):
    url = "https://www.tabnews.com.br/api/v1/contents/"
    resp = requests.get(url, params=kwargs)
    return resp


# Salvo os dados no formato json e se caso não for salvo em json, salvo em parquet, faço as duas opções
# aqui eu busco o caminho do arquivo e salvo com a data atual
def save_data(data, option='parquet'):

    now = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S_%f")

    json_dir = "/lakehouse/default/Files/data/contents/json/"
    parquet_dir = "/lakehouse/default/Files/data/contents/parquet/"


    if option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f"{parquet_dir}{now}.parquet",index=False)

    elif option == 'json':
        with open(f"{json_dir}{now}.json",'w') as open_file:
            
            json.dump(data, open_file, indent=4)

#%%

# Aqui é o ponto principal: especifíco quais os parametros quero da API;
# enquanto for verdade, vou rodar os dados. se chegar em 100 conteudos e a qtde for menor que 100 e
# a data for < que estipulei, ele para, já que nao precisa mais rodar.
page = 1 
date_stop = pd.to_datetime('2024-10-18').date()
while True:
    print(page)
    resp = get_response(page=2, per_page=100, strategy="new")

    if resp.status_code == 200:
        data = resp.json()  

        # Verifica se os dados foram obtidos corretamente
        if data:
            save_data(data, option='parquet')  # Salva os dados em parquet
        else:
            break
    
        if len(data) < 100:
            print("Menos de 100 conteúdos retornados, parando...")
            break

        page += 1
        time.sleep(5) #aguarda 5 seg para fazer a prox requisição

    else:
        print(resp.status_code)
        print(resp.json())
        time.sleep(60 * 15) #se o código nao der certo, vai tentar novamente depois de 5 min

#%%
data = resp.json() 

#%%
save_data(data, option='parquet')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#%%

import requests
import pandas as pd
import datetime
import json
import os
import time

#%%

#Busco a API utilizando o kwargs para puxar todos os parâmetros
def get_response(**kwargs):
    url = "https://www.tabnews.com.br/api/v1/contents/"
    resp = requests.get(url, params=kwargs)
    return resp


# Salvo os dados no formato json e se caso não for salvo em json, salvo em parquet, faço as duas opções
# aqui eu busco o caminho do arquivo e salvo com a data atual
def save_data(data, option='parquet'):

    now = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S_%f")

    json_dir = "/lakehouse/default/Files/data/contents/json/"
    parquet_dir = "/lakehouse/default/Files/data/contents/parquet/"


    if option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f"{parquet_dir}{now}.parquet",index=False)

    elif option == 'json':
        with open(f"{json_dir}{now}.json",'w') as open_file:
            
            json.dump(data, open_file, indent=4)

#%%

# Aqui é o ponto principal: especifíco quais os parametros quero da API;
# enquanto for verdade, vou rodar os dados. se chegar em 100 conteudos e a qtde for menor que 100 e
# a data for < que estipulei, ele para, já que nao precisa mais rodar.
page = 1 
date_stop = pd.to_datetime('2024-10-18').date()
while True:
    print(page)
    resp = get_response(page=1, per_page=100, strategy="new")

    if resp.status_code == 200:
        data = resp.json()  

        # Verifica se os dados foram obtidos corretamente
        if data:
            save_data(data, option='parquet')  # Salva os dados em parquet
        else:
            break
    
        if len(data) < 100:
            print("Menos de 100 conteúdos retornados, parando...")
            break

        page += 1
        time.sleep(5) #aguarda 5 seg para fazer a prox requisição

    else:
        print(resp.status_code)
        print(resp.json())
        time.sleep(60 * 15) #se o código nao der certo, vai tentar novamente depois de 5 min

#%%
data = resp.json() 

#%%
save_data(data, option='parquet')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
