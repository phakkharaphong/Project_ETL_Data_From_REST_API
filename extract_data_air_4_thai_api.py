##################################################################
# Program Name:  GET API And writer CSV
# Program Purpose:
#
# Fetches data from the specified API
# Validates the response
# Converts the JSON to a UTF-8 encoded CSV
# Saves it to the specified directory
# Displays the total number of records
# Date created : Aug 21, 2025 6.00pm
#################################################################
import requests
import csv
import os
import pandas as pd
import json as json
import datetime as timestamp
from bs4 import BeautifulSoup

def Get_Data_PM25_and_PM10(url_point):
    url = f"{url_point}"
    print(f"End Point: {url}")
    output_dir = "C:\\Users\\chuwo\\OneDrive\\เอกสาร\\Desktop\\all_project\\Project_ETL_Data_From_REST_API\\4Airthai"
    output_file = f"output_4Air.csv"
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

           if isinstance(data, dict) and "stations" in data:
            records = data["stations"]  # ดึง list ออกมา

            #list สำหรับกำหนด Header และข้อมูล
            list_header_air = []
            for items in records:
               record = {
                  "stationID": items["stationID"],
                  "nameTH": items["nameTH"],
                  "nameEN": items["nameEN"],
                  "areaTH": items["areaTH"],
                  "areaEN": items["areaEN"],
                  "stationType": items["stationType"],
                  "lat": float(items["lat"]),
                  "long": float(items["long"]),
                  "date": items["AQILast"]["date"],
                  "time": items["AQILast"]["time"],
                  "PM25_color_id": items["AQILast"]["PM25"]["color_id"],
                  "PM25_aqi": items["AQILast"]["PM25"]["aqi"],
                  "PM25_value": items["AQILast"]["PM25"]["value"],
                  "PM10_color_id": items["AQILast"]["PM10"]["color_id"],
                  "PM10_aqi": items["AQILast"]["PM10"]["aqi"],
                  "PM10_value": items["AQILast"]["PM10"]["value"],
                  "O3_color_id": items["AQILast"]["O3"]["color_id"],
                  "O3_aqi": items["AQILast"]["O3"]["aqi"],
                  "O3_value": items["AQILast"]["O3"]["value"],
                  "CO_color_id": items["AQILast"]["CO"]["color_id"],
                  "CO_aqi": items["AQILast"]["CO"]["aqi"],
                  "CO_value": items["AQILast"]["CO"]["value"],
                  "NO2_color_id": items["AQILast"]["NO2"]["color_id"],
                  "NO2_aqi": items["AQILast"]["NO2"]["aqi"],
                  "NO2_value": items["AQILast"]["NO2"]["value"],
                  "SO2_color_id": items["AQILast"]["SO2"]["color_id"],
                  "SO2_aqi": items["AQILast"]["SO2"]["aqi"],
                  "SO2_value": items["AQILast"]["SO2"]["value"],
                  "AQI_color_id": items["AQILast"]["AQI"]["color_id"],
                  "AQI_aqi": items["AQILast"]["AQI"]["aqi"],
                  "AQI_value": items["AQILast"]["AQI"]["param"],
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


    url = f"{url_point}"
    print(f"End Point: {url}")
    output_dir = "C:\\Users\\chuwo\\OneDrive\\เอกสาร\\Desktop\\all_project\\Project_ETL_Data_From_REST_API\\Data_hidxbc"
    output_file = f"hidxbc.csv"
    output_path = os.path.join(output_dir, output_file)
    response = requests.get(url)

    #Use BeautifulSoup Ref Chat GPT
    soup = BeautifulSoup(response.text, "html.parser")

    #Loop search and append to array file .csv in endpoint URL
    csv_links = [a["href"] for a in soup.find_all("a") if a["href"].endswith(".csv")]
    
    for link in csv_links:
        file_url = url + link
        file_path = os.path.join(output_dir, link)

        print(f"Downloading {file_url}")
        r = requests.get(file_url)
        with open(file_path, "wb") as f:
            f.write(r.content)

    print("✅ All CSV downloaded!")


    print(f"Data successfully saved to {output_path}")


url_end_point_ = "http://air4thai.pcd.go.th/services/getNewAQI_JSON.php"
url_end_point_hidxbc_predict = "http://www.rnd.tmd.go.th/heatindex_API/hidxbc/"
url_end_point_hidxbc_real = "http://www.rnd.tmd.go.th/hi_monitor/data/"


#Excel path
#path_excel_read = "C:\\Users\\chuwo\\OneDrive\\เอกสาร\\Desktop\\all_project\\Project_ETL_Data_From_REST_API\\Data_hidxbc_predict"
#Get_Data_PM25_and_PM10(url_end_point_)
#Get_Data_CSV_hidxbc_predic(url_end_point_hidxbc_predict)
#read_csv_and_transform(path_excel_read)
#Get_Data_CSV_hidxbc_real(url_end_point_hidxbc_real)