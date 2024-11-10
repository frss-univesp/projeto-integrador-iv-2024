import pymongo
import pandas as pd

client = pymongo.MongoClient('mongodb+srv://pi4_writer:pi4_writer@cluster0.tz7tehv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client['proj_integrador_iv']
collection = db['historico_inmet']

documents = collection.find()
all_data = []

for document in documents:
    if 'data' in document:
        data = document['data']
        for entry in data:
            entry['_id_document'] = document['_id']
            all_data.append(entry)

df = pd.DataFrame(all_data)
df