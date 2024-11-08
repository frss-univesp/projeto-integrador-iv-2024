import pymongo
import pandas as pd

uri = "mongodb+srv://pipt:HhnQbl12R8FS0S67@cluster0.tz7tehv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = pymongo.MongoClient(uri)
db = client["proj_integrador_iv"]
collection = db["previsao"]

def extracao_previsao(document):
    data = document.get("data", [])
    extracted_data_list = []
    
    for entry in data:
        extracted_data = {
            "data": entry["date"],
            "data_br": entry["date_br"],
            "umidade_minima": entry["humidity"]["min"],
            "umidade_maxima": entry["humidity"]["max"],
            "umidade_alvorecer_minima": entry["humidity"]["dawn"]["min"],
            "umidade_alvorecer_maxima": entry["humidity"]["dawn"]["max"],
            "umidade_manha_minima": entry["humidity"]["morning"]["min"],
            "umidade_manha_maxima": entry["humidity"]["morning"]["max"],
            "umidade_tarde_minima": entry["humidity"]["afternoon"]["min"],
            "umidade_tarde_maxima": entry["humidity"]["afternoon"]["max"],
            "umidade_noite_minima": entry["humidity"]["night"]["min"],
            "umidade_noite_maxima": entry["humidity"]["night"]["max"],
            "pressao": entry["pressure"]["pressure"],
            "precipitacao_chuva": entry["rain"]["precipitation"],
            "probabilidade_chuva": entry["rain"]["probability"],
            "vento_velocidade_minima": entry["wind"]["velocity_min"],
            "vento_velocidade_maxima": entry["wind"]["velocity_max"],
            "vento_velocidade_media": entry["wind"]["velocity_avg"],
            "vento_direcao": entry["wind"]["direction"],
            "rajada_vento_maximo": entry["wind"]["gust_max"],
            "uv_maximo": entry["uv"]["max"],
            "sensacao_termica_minima": entry["thermal_sensation"]["min"],
            "sensacao_termica_maxima": entry["thermal_sensation"]["max"],
            "temperatura_minima": entry["temperature"]["min"],
            "temperatura_maxima": entry["temperature"]["max"],
            "temperatura_alvorecer_minima": entry["temperature"]["dawn"]["min"],
            "temperatura_alvorecer_maxima": entry["temperature"]["dawn"]["max"],
            "temperatura_manha_minima": entry["temperature"]["morning"]["min"],
            "temperatura_manha_maxima": entry["temperature"]["morning"]["max"],
            "temperatura_tarde_minima": entry["temperature"]["afternoon"]["min"],
            "temperatura_tarde_maxima": entry["temperature"]["afternoon"]["max"],
            "temperatura_noite_minima": entry["temperature"]["night"]["min"],
            "temperatura_noite_maxima": entry["temperature"]["night"]["max"],
            "nuvens_cobertura_baixa": entry["cloud_coverage"]["low"],
            "nuvens_cobertura_media": entry["cloud_coverage"]["mid"],
            "nuvens_cobertura_alta": entry["cloud_coverage"]["high"],
            "horario_nascer_sol": entry["sun"]["sunrise"],
            "horario_por_sol": entry["sun"]["sunset"]
        }
        extracted_data_list.append(extracted_data)
    
    return extracted_data_list

documents = collection.find()
all_extracted_data = []

for document in documents:
    extracted_data_list = extracao_previsao(document)
    all_extracted_data.extend(extracted_data_list)

df = pd.DataFrame(all_extracted_data)
df
