import zipfile
import os
import requests
import pandas as pd
import re
import pymongo
from pymongo import MongoClient

client = MongoClient('mongodb+srv://pi4_writer:pi4_writer@cluster0.tz7tehv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client['proj_integrador_iv']
collection = db['historico_inmet']

urls = ['https://portal.inmet.gov.br/uploads/dadoshistoricos/2020.zip',
        'https://portal.inmet.gov.br/uploads/dadoshistoricos/2021.zip',
        'https://portal.inmet.gov.br/uploads/dadoshistoricos/2022.zip',
        'https://portal.inmet.gov.br/uploads/dadoshistoricos/2023.zip',
        'https://portal.inmet.gov.br/uploads/dadoshistoricos/2024.zip']

def json_serializer(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(obj, float) and (pd.isna(obj)):
        return None
    raise TypeError(f"Tipo {type(obj)} não serializável")

for url in urls:
    arquivo_zip = url.split("/")[-1]
    
    response = requests.get(url)
    with open(arquivo_zip, 'wb') as f:
        f.write(response.content)

    with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
        zip_ref.extractall('dados_extraidos')

    arquivos_csv = [f for f in os.listdir('dados_extraidos') if f.endswith('.CSV') and 'SAO PAULO' in f.upper()]
    
    for file in arquivos_csv:
        file_path = os.path.join('dados_extraidos', file)
        _id = file.replace('.CSV', '')
        existing_document = collection.find_one({"_id": _id})
        if existing_document:
            print(f"Documento com _id '{_id}' já existe no banco de dados. Skipping file.")
            continue

        try:
            df = pd.read_csv(file_path, skiprows=8, header=0, encoding='latin1', sep=';', decimal=',')
            df = df.loc[:, ~df.columns.str.contains('^unnamed', case=False)]
            df.columns = [re.sub(r'[^a-zA-Z0-9 ]', '', col) for col in df.columns]
            df.columns = [col.replace(' ', '_').lower() for col in df.columns]
            df['hora_utc'] = df['hora_utc'].str.replace(' UTC', '', regex=False)
            df['hora_utc'] = df['hora_utc'].str.zfill(4)
            df['data_hora_utc'] = pd.to_datetime(df['data'] + ' ' + df['hora_utc'], 
                                                  errors='coerce',
                                                  format='%Y/%m/%d %H%M')

            df['data_hora_utc'] = df['data_hora_utc'].dt.strftime('%Y-%m-%d %H:%M:%S')            
            df = df.drop(columns=['data', 'hora_utc'])
            df = df.replace({float('nan'): None})
            data_dict = df.to_dict(orient='records')
            json_data = {"_id": _id, "data": data_dict}
            collection.insert_one(json_data)
            print(f"Dados inseridos no database: {json_data['_id']}")

        except Exception as e:
            print(f"Erro ao processar o arquivo {file}: {e}")

    os.remove(arquivo_zip)
    print(f"Arquivo ZIP {arquivo_zip} excluído com sucesso.")
