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

import requests

url = "https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0000021&Dim1=S7A1986&Dim2=1&Dim3=101&lang=EN"

requisicao = requests.get(url)

print (requisicao.json())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Consumption Table - Meat

# CELL ********************

import datetime
import requests
import pandas as pd
import json

def get_response(dim1, dim3):
    url = f"https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0000211&Dim1=S7A{dim1}&Dim2=PT&Dim3={dim3}&lang=EN"
    response = requests.get(url)
    return response

def save_data(data, option='json'):

    now = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S_%f")

    json_dir = "/lakehouse/default/Files/data/ine_data/json/meat_consumption/"
    parquet_dir = "/lakehouse/default/Files/data/ine_data/parquet/meat_consumption/"

    if option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f"{parquet_dir}{now}_{dim1}.parquet",index=False)

    elif option == 'json':
        with open(f"{json_dir}{now}_{dim1}.json",'w') as open_file:
            
            json.dump(data, open_file, indent=4)

dim1_years = [f"{year}" for year in range(2003, 2025)]
dim3_species = [f"{number}" for number in [41, 411, 412, 413, 414, 415, 416, 420]]

data = []

for dim1 in dim1_years:
    for dim3 in dim3_species:
        response = get_response(dim1, dim3)
            
        if response.status_code == 200:
            data = response.json()
            save_data(data)

        else:
            print(response.status_code)

if data:
    save_data(data)
else:
    print("no data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH_Bronze.dbo.tb_meat_consumption LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Consumption Table - Egg

# CELL ********************

import datetime
import requests
import pandas as pd
import json

def get_response(dim1):
    url = f"https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0000216&Dim1=S7A{dim1}&Dim2=PT&lang=EN"
    response = requests.get(url)
    return response

def save_data(data, option='json'):

    now = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S_%f")

    json_dir = "/lakehouse/default/Files/data/ine_data/json/egg_consumption/"
    parquet_dir = "/lakehouse/default/Files/data/ine_data/parquet/egg_consumption/"

    if option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f"{parquet_dir}{now}_{dim1}.parquet",index=False)

    elif option == 'json':
        with open(f"{json_dir}{now}_{dim1}.json",'w') as open_file:
            
            json.dump(data, open_file, indent=4)

dim1_years = [f"{year}" for year in range(2003, 2025)]

data = []

for dim1 in dim1_years:
        response = get_response(dim1)
            
        if response.status_code == 200:
            data = response.json()
            save_data(data)

        else:
            print(response.status_code)

if data:
    save_data(data)
else:
    print("no data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH_Bronze.dbo.tb_ine_egg_consumption LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Consumption Table - Sugar

# CELL ********************

import datetime
import requests
import pandas as pd
import json

def get_response(dim1):
    url = f"https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0000198&Dim1=7A{dim1}_2&Dim2=PT&lang=EN"
    response = requests.get(url)
    return response

def save_data(data, option='json'):

    now = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S_%f")

    json_dir = "/lakehouse/default/Files/data/ine_data/json/sugar_consumption/"
    parquet_dir = "/lakehouse/default/Files/data/ine_data/parquet/sugar_consumption/"

    if option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f"{parquet_dir}{now}_{dim1}.parquet",index=False)

    elif option == 'json':
        with open(f"{json_dir}{now}_{dim1}.json",'w') as open_file:
            
            json.dump(data, open_file, indent=4)

dim1_years = [f"{year}" for year in range(1991, 2023)]

data = []

for dim1 in dim1_years:
        response = get_response(dim1)
            
        if response.status_code == 200:
            data = response.json()
            save_data(data)

        else:
            print(response.status_code)

if data:
    save_data(data)
else:
    print("no data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Consumption Table - Fruit

# CELL ********************

import datetime
import requests
import pandas as pd
import json

def get_response(dim1, dim3):
    url = f"https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0000166&Dim1=7A{dim1}_2&Dim2=PT&lang=EN"
    response = requests.get(url)
    return response

def save_data(data, option='json'):

    now = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S_%f")

    json_dir = "/lakehouse/default/Files/data/ine_data/json/fruit_consumption/"
    parquet_dir = "/lakehouse/default/Files/data/ine_data/parquet/fruit_consumption/"

    if option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f"{parquet_dir}{now}_{dim1}.parquet",index=False)

    elif option == 'json':
        with open(f"{json_dir}{now}_{dim1}.json",'w') as open_file:
            
            json.dump(data, open_file, indent=4)

dim1_years = [f"{year}" for year in range(1990, 2023)]
dim3_species = [f"{number}" for number in [19, 191, 192, 193, 194]]

data = []

for dim1 in dim1_years:
    for dim3 in dim3_species:
        response = get_response(dim1, dim3)
            
        if response.status_code == 200:
            data = response.json()
            save_data(data)

        else:
            print(response.status_code)

if data:
    save_data(data)
else:
    print("no data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Workload Table

# CELL ********************

import datetime
import requests
import pandas as pd
import json

def get_response(dim1, dim2, dim3):
    url = f"https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0013158&Dim1=S7A{dim1}&Dim2={dim2}&Dim3={dim3}&lang=EN"
    response = requests.get(url)
    return response

def save_data(data, option='json'):

    now = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S_%f")

    json_dir = "/lakehouse/default/Files/data/ine_data/json/workload/"
    parquet_dir = "/lakehouse/default/Files/data/ine_data/parquet/workload/"

    if option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f"{parquet_dir}{now}_{dim1}.parquet",index=False)

    elif option == 'json':
        with open(f"{json_dir}{now}_{dim1}.json",'w') as open_file:
            
            json.dump(data, open_file, indent=4)

dim1_years = [f"{year}" for year in [1989, 1993, 1995, 1997, 1999, 2003, 2005, 2007, 2009, 2013, 2016, 2019]]
dim2_location = [f"{number}" for number in ["1", "11", "19", "1D", "1A", "1B", "1C", "15", "2", "20", "3"]]
dim3_workforce = [f"{number}" for number in [1, 2]]

data = []

for dim1 in dim1_years:
    for dim2 in dim2_location:
        for dim3 in dim3_workforce:
            response = get_response(dim1, dim2, dim3)
            
            if response.status_code == 200:
                data = response.json()
                save_data(data)

            else:
                print(response.status_code)

if data:
    save_data(data)
else:
    print("no data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Workforce Table

# CELL ********************

import datetime
import requests
import pandas as pd
import json

def get_response(dim1, dim2, dim3):
    url = f"https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0000032&Dim1=S7A{dim1}&Dim2={dim2}&Dim3={dim3}&Dim4=T&lang=EN"
    response = requests.get(url)
    return response

def save_data(data, option='json'):

    now = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S_%f")

    json_dir = "/lakehouse/default/Files/data/ine_data/json/workforce/"
    parquet_dir = "/lakehouse/default/Files/data/ine_data/parquet/workforce/"

    if option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f"{parquet_dir}{now}_{dim1}.parquet",index=False)

    elif option == 'json':
        with open(f"{json_dir}{now}_{dim1}.json",'w') as open_file:
            
            json.dump(data, open_file, indent=4)

dim1_years = [f"{year}" for year in [1989, 1993, 1995, 1997, 1999, 2003, 2005, 2007, 2009, 2013, 2016, 2019]]
dim2_location = [f"{number}" for number in [1, 11, 16, 17, 18, 15, 2, 20, 3, 30]]
dim3_type_labor = [f"{number}" for number in [1, 2]]

data = []

for dim1 in dim1_years:
    for dim2 in dim2_location:
        for dim3 in dim3_type_labor:
            response = get_response(dim1, dim2, dim3)
                    
            if response.status_code == 200:
                data = response.json()
                save_data(data)

            else:
                print(response.status_code)

if data:
    save_data(data)
else:
    print("no data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Exploration Table

# CELL ********************

import datetime
import requests
import pandas as pd
import json

def get_response(dim1, dim2, dim3, dim4):
    url = f"https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0010821&Dim1=S7A{dim1}&Dim2={dim2}&Dim3={dim3}&Dim4={dim4}&lang=EN"
    response = requests.get(url)
    return response

def save_data(data, option='json'):

    now = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S_%f")

    json_dir = "/lakehouse/default/Files/data/ine_data/json/exploration/"
    parquet_dir = "/lakehouse/default/Files/data/ine_data/parquet/exploration/"

    if option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f"{parquet_dir}{now}_{dim1}.parquet",index=False)

    elif option == 'json':
        with open(f"{json_dir}{now}_{dim1}.json",'w') as open_file:
            
            json.dump(data, open_file, indent=4)

dim1_years = [f"{year}" for year in [1989, 1999, 2009, 2019]]
dim2_location = [f"{number}" for number in [1, 101, 102, 103, 104, 105, 106, 107, 2, 241, 242, 243, 244, 245, 246, 247, 248, 249, 3, 331, 332]]
dim3_classes = [f"{number}" for number in range(1, 6)]
dim4_work_unit = [f"{number}" for number in range(1, 6)]

data = []

for dim1 in dim1_years:
    for dim2 in dim2_location:
        for dim3 in dim3_classes:
            for dim4 in dim4_work_unit:
                response = get_response(dim1, dim2, dim3, dim4)
                    
                if response.status_code == 200:
                    data = response.json()
                    save_data(data)

                else:
                    print(response.status_code)

if data:
    save_data(data)
else:
    print("no data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Milk Consumption

# CELL ********************

import datetime
import requests
import pandas as pd
import json

def get_response(dim1):
    url = f"https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0000213&Dim1=S7A{dim1}&Dim2=PT&Dim3=44&lang=EN"
    response = requests.get(url)
    return response

def save_data(data, option='json'):

    now = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S_%f")

    json_dir = "/lakehouse/default/Files/data/ine_data/json/milk_consumption/"
    parquet_dir = "/lakehouse/default/Files/data/ine_data/parquet/milk_consumption/"

    if option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f"{parquet_dir}{now}_{dim1}.parquet",index=False)

    elif option == 'json':
        with open(f"{json_dir}{now}_{dim1}.json",'w') as open_file:
            
            json.dump(data, open_file, indent=4)

dim1_years = [f"{year}" for year in range (2003, 2025)]

data = []

for dim1 in dim1_years:
    response = get_response(dim1)
                    
    if response.status_code == 200:
        data = response.json()
        save_data(data)

    else:
        print(response.status_code)

if data:
    save_data(data)
else:
    print("no data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Milk Production

# CELL ********************

import datetime
import requests
import pandas as pd
import json

def get_response(dim1):
    url = f"https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0008609&Dim1=S7A{dim1}&Dim2=PT&Dim3=T&lang=EN"
    response = requests.get(url)
    return response

def save_data(data, option='json'):

    now = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S_%f")

    json_dir = "/lakehouse/default/Files/data/ine_data/json/milk_production/"
    parquet_dir = "/lakehouse/default/Files/data/ine_data/parquet/milk_production/"

    if option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f"{parquet_dir}{now}_{dim1}.parquet",index=False)

    elif option == 'json':
        with open(f"{json_dir}{now}_{dim1}.json",'w') as open_file:
            
            json.dump(data, open_file, indent=4)

dim1_years = [f"{year}" for year in range (2003, 2025)]

data = []

for dim1 in dim1_years:
    response = get_response(dim1)
                    
    if response.status_code == 200:
        data = response.json()
        save_data(data)

    else:
        print(response.status_code)

if data:
    save_data(data)
else:
    print("no data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Meat Production

# CELL ********************

import datetime
import requests
import pandas as pd
import json

def get_response(dim1):
    url = f"https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0008953&Dim1=S7A{dim1}&Dim2=PT&Dim3=T&lang=EN"
    response = requests.get(url)
    return response

def save_data(data, option='json'):

    now = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S_%f")

    json_dir = "/lakehouse/default/Files/data/ine_data/json/meat_production/"
    parquet_dir = "/lakehouse/default/Files/data/ine_data/parquet/meat_production/"

    if option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f"{parquet_dir}{now}_{dim1}.parquet",index=False)

    elif option == 'json':
        with open(f"{json_dir}{now}_{dim1}.json",'w') as open_file:
            
            json.dump(data, open_file, indent=4)

dim1_years = [f"{year}" for year in range (2003, 2025)]

data = []

for dim1 in dim1_years:
    response = get_response(dim1)
                    
    if response.status_code == 200:
        data = response.json()
        save_data(data)

    else:
        print(response.status_code)

if data:
    save_data(data)
else:
    print("no data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Egg Production

# CELL ********************

import datetime
import requests
import pandas as pd
import json

def get_response(dim1):
    url = f"https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0008954&Dim1=S7A{dim1}&Dim2=PT&Dim3=T&lang=EN"
    response = requests.get(url)
    return response

def save_data(data, option='json'):

    now = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S_%f")

    json_dir = "/lakehouse/default/Files/data/ine_data/json/egg_prod/"
    parquet_dir = "/lakehouse/default/Files/data/ine_data/parquet/egg_prod/"

    if option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f"{parquet_dir}{now}_{dim1}.parquet",index=False)

    elif option == 'json':
        with open(f"{json_dir}{now}_{dim1}.json",'w') as open_file:
            
            json.dump(data, open_file, indent=4)

dim1_years = [f"{year}" for year in range (2003, 2025)]

data = []

for dim1 in dim1_years:
    response = get_response(dim1)
                    
    if response.status_code == 200:
        data = response.json()
        save_data(data)

    else:
        print(response.status_code)

if data:
    save_data(data)
else:
    print("no data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
