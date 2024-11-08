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
            "date": entry["date"],
            "date_br": entry["date_br"],
            "humidity_min": entry["humidity"]["min"],
            "humidity_max": entry["humidity"]["max"],
            "humidity_dawn_min": entry["humidity"]["dawn"]["min"],
            "humidity_dawn_max": entry["humidity"]["dawn"]["max"],
            "humidity_morning_min": entry["humidity"]["morning"]["min"],
            "humidity_morning_max": entry["humidity"]["morning"]["max"],
            "humidity_afternoon_min": entry["humidity"]["afternoon"]["min"],
            "humidity_afternoon_max": entry["humidity"]["afternoon"]["max"],
            "humidity_night_min": entry["humidity"]["night"]["min"],
            "humidity_night_max": entry["humidity"]["night"]["max"],
            "pressure": entry["pressure"]["pressure"],
            "rain_precipitation": entry["rain"]["precipitation"],
            "rain_probability": entry["rain"]["probability"],
            "wind_velocity_min": entry["wind"]["velocity_min"],
            "wind_velocity_max": entry["wind"]["velocity_max"],
            "wind_velocity_avg": entry["wind"]["velocity_avg"],
            "wind_direction": entry["wind"]["direction"],
            "wind_gust_max": entry["wind"]["gust_max"],
            "uv_max": entry["uv"]["max"],
            "thermal_sensation_min": entry["thermal_sensation"]["min"],
            "thermal_sensation_max": entry["thermal_sensation"]["max"],
            "temperature_min": entry["temperature"]["min"],
            "temperature_max": entry["temperature"]["max"],
            "temperature_dawn_min": entry["temperature"]["dawn"]["min"],
            "temperature_dawn_max": entry["temperature"]["dawn"]["max"],
            "temperature_morning_min": entry["temperature"]["morning"]["min"],
            "temperature_morning_max": entry["temperature"]["morning"]["max"],
            "temperature_afternoon_min": entry["temperature"]["afternoon"]["min"],
            "temperature_afternoon_max": entry["temperature"]["afternoon"]["max"],
            "temperature_night_min": entry["temperature"]["night"]["min"],
            "temperature_night_max": entry["temperature"]["night"]["max"],
            "cloud_coverage_low": entry["cloud_coverage"]["low"],
            "cloud_coverage_mid": entry["cloud_coverage"]["mid"],
            "cloud_coverage_high": entry["cloud_coverage"]["high"],
            "sunrise": entry["sun"]["sunrise"],
            "sunset": entry["sun"]["sunset"]
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
