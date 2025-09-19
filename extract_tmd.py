import requests
import csv
import os
import pandas as pd
import json as json
import datetime as timestamp,datetime
from datetime import datetime

def Get_Data_WeatherToday(url_point):
    url = f"{url_point}"
    print(f"End Point: {url}")
    output_dir = "C:\\Users\\chuwo\\OneDrive\\เอกสาร\\Desktop\\all_project\\Project_ETL_Data_From_REST_API\\tmd_cli"
    output_file = f"output_WeatherToday.csv"
    output_path = os.path.join(output_dir, output_file)
    #Make the API request
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed to fetch data. Status code: {response.status_code}")
    else:
        data = response.json()
        print(type(data))

         # Check if data is a list of dictionaries
        if isinstance(data, list) and all(isinstance(item, dict) for item in data):
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)

            # Get headers from the first item
            headers = data[0].keys()

            print(f"Data successfully saved to {output_path}")
            print(f"Total records: {len(data)}")

        else:
           #Case if data is dictionaries
           print("Unexpected data format. Expected a list of dictionaries.")

           if isinstance(data, dict) and "Stations" in data:
            records = data["Stations"]["Station"]  # ดึง list ออกมา

            #list สำหรับกำหนด Header และข้อมูล
            list_header_air = []
            for items in records:
               # แปลง string DateTime เป็น object datetime
               dt_obj = datetime.strptime(items["Observation"]["DateTime"], "%Y-%m-%d %H:%M")  
              # แปลง datetime เป็น format ใหม่ เช่น "dd/mm/yyyy HH:MM"
               formatted_dt = dt_obj.strftime("%m/%d/%Y %H:%M:%S")
               record = {
                  "WmoStationNumber": items["WmoStationNumber"],
                  "StationNameThai": items["StationNameThai"],
                  "StationNameEnglish": items["StationNameEnglish"],
                  "Province": items["Province"],
                  "Latitude": float(items["Latitude"]),
                  "Longitude": float(items["Longitude"]),
                  "DateTime": formatted_dt,
                  "MeanSeaLevelPressure": items["Observation"]["MeanSeaLevelPressure"],
                  "Temperature": items["Observation"]["Temperature"],
                  "MaxTemperature": items["Observation"]["MaxTemperature"],
                  "DifferentFromMaxTemperature": items["Observation"]["DifferentFromMaxTemperature"],
                  "MinTemperature": items["Observation"]["MinTemperature"],
                  "DifferentFromMinTemperature": items["Observation"]["DifferentFromMinTemperature"],
                  "RelativeHumidity": items["Observation"]["RelativeHumidity"],
                  "WindDirection": items["Observation"]["WindDirection"],
                  "WindSpeed": items["Observation"]["WindSpeed"],
                  "Rainfall": items["Observation"]["Rainfall"],

               }
               list_header_air.append(record)
               df = pd.DataFrame(list_header_air)
               #(df)
               #print(f"Station is {items["stationID"]} PM 2.5 Is: {items["AQILast"]["PM25"]["value"]}")
            #json_air = list_header_air.json()
            #print(type(json_air))
            headers = df.columns.tolist()
            print(f"Data Is: {headers}")

            # Write to CSV
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                #records เป็นการชี้ว่าให้เริ่มการเขียนลงไฟล์ CSV เริ่มต้นจากลำดับที่ 1
                writer.writerows(df.to_dict(orient="records"))

            print(f"Data successfully saved to {output_path}")
            print(f"Total records: {len(records)}")


def Get_Data_Weather3Hours(url_point):
    url = f"{url_point}"
    print(f"End Point: {url}")
    output_dir = "C:\\Users\\chuwo\\OneDrive\\เอกสาร\\Desktop\\all_project\\Project_ETL_Data_From_REST_API\\tmd_cli"
    output_file = f"output_Weather3Hours.csv"
    output_path = os.path.join(output_dir, output_file)
    #Make the API request
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed to fetch data. Status code: {response.status_code}")
    else:
        data = response.json()
        print(type(data))

         # Check if data is a list of dictionaries
        if isinstance(data, list) and all(isinstance(item, dict) for item in data):
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)

            # Get headers from the first item
            headers = data[0].keys()

            # Write to CSV
           # with open(output_path, "w", newline="", encoding="utf-8") as f:
           #     writer = csv.DictWriter(f, fieldnames=headers)
           #     writer.writeheader()
           #     writer.writerows(data)

            print(f"Data successfully saved to {output_path}")
            print(f"Total records: {len(data)}")

        else:
           #Case if data is dictionaries
           print("Unexpected data format. Expected a list of dictionaries.")

           if isinstance(data, dict) and "Stations" in data:
            records = data["Stations"]["Station"]  # ดึง list ออกมา

            #list สำหรับกำหนด Header และข้อมูล
            list_header_air = []
            for items in records:
               record = {
                  "WmoStationNumber": items["WmoStationNumber"],
                  "StationNameThai": items["StationNameThai"],
                  "StationNameEnglish": items["StationNameEnglish"],
                  "Province": items["Province"],
                  "Latitude": float(items["Latitude"]),
                  "Longitude": float(items["Longitude"]),
                  "DateTime": items["Observation"]["DateTime"],
                  "StationPressure": items["Observation"]["StationPressure"],
                  "MeanSeaLevelPressure": items["Observation"]["MeanSeaLevelPressure"],
                  "AirTemperature": items["Observation"]["AirTemperature"],
                  "DewPoint": items["Observation"]["DewPoint"],
                  "RelativeHumidity": items["Observation"]["RelativeHumidity"],
                  "VaporPressure": items["Observation"]["VaporPressure"],
                  "LandVisibility": items["Observation"]["LandVisibility"],
                  "WindDirection": items["Observation"]["WindDirection"],
                  "WindSpeed": items["Observation"]["WindSpeed"],
                  "Rainfall": items["Observation"]["Rainfall"],
                  "Rainfall24Hr": items["Observation"]["Rainfall24Hr"],

               }
               list_header_air.append(record)
               df = pd.DataFrame(list_header_air)
               #(df)
               #print(f"Station is {items["stationID"]} PM 2.5 Is: {items["AQILast"]["PM25"]["value"]}")
            #json_air = list_header_air.json()
            #print(type(json_air))
            headers = df.columns.tolist()
            print(f"Data Is: {headers}")

            # Write to CSV
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                #records เป็นการชี้ว่าให้เริ่มการเขียนลงไฟล์ CSV เริ่มต้นจากลำดับที่ 1
                writer.writerows(df.to_dict(orient="records"))

            print(f"Data successfully saved to {output_path}")
            print(f"Total records: {len(records)}")


url_end_point_WeatherToday = "https://data.tmd.go.th/api/WeatherToday/V2/?uid=demo&ukey=demokey&format=json"
url_end_point_Weather3Hours = "https://data.tmd.go.th/api/Weather3Hours/V2/?uid=demo&ukey=demokey&format=json"
Get_Data_WeatherToday(url_end_point_WeatherToday)
Get_Data_Weather3Hours(url_end_point_Weather3Hours)