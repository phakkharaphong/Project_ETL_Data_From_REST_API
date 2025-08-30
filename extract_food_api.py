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



# usecols =["จังหวัด","อำเภอ", "code", "ตำบล"]
def Read_excel(path_file_excel, sheet_namee,url_API):
   i = 0
   df = pd.read_excel(path_file_excel, sheet_name=sheet_namee, usecols=["no","name"])
   row_index = pd.DataFrame(df)
   for idx, row in row_index.iterrows():
       if pd.notnull(row["name"]):
        i += 1
        name = row["name"]
        print(f"=========Check Code{i}=============")
        print(f"แถว {idx} {name}")
        Get_API_Food_Province(url_API,name)

def Get_API_Food_Province(url_point,province_name):
    # API endpoint
    #url = f"https://foodhandler.anamai.moph.go.th/webapp/webservice/qa?province=ขอนแก่น"
    url = f"{url_point}{province_name}"
    print(f"{url}")
# Output directory and file name
    output_dir = "C:\\Users\\chuwo\\OneDrive\\เอกสาร\\Desktop\\all_project\\ETL\\ผู้ประกอบการ"
    output_file = f"output_{province_name}.csv"
    output_path = os.path.join(output_dir, output_file)

    #Make the API request
    response = requests.get(url)

    # Validate the response
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
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(data)

            print(f"Data successfully saved to {output_path}")
            print(f"Total records: {len(data)}")
        else:
           print("Unexpected data format. Expected a list of dictionaries.")
           if isinstance(data, dict) and "data" in data:
            records = data["data"]  # ดึง list ออกมา
            #records ตอนนี้เป็น list of dict
            headers = records[1].keys()
            print(f"Data Is: {headers}")

            # Write to CSV
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                #records เป็นการชี้ว่าให้เริ่มการเขียนลงไฟล์ CSV เริ่มต้นจากลำดับที่ 1
                writer.writerows(records[1:])

            print(f"Data successfully saved to {output_path}")
            print(f"Total records: {len(records)}")


#GET_API Ony
def Get_API_Toilet_Only(url_point):
    url = f"{url_point}"
    print(f"Fetching: {url}")

    output_dir = "C:\\Users\\tyuoi\\OneDrive\\เดสก์ท็อป\\ETLDATA\\Project_ETL_Data_From_REST_API\\APIระบบส้วม"
    output_file = "output_Result.csv"
    output_path = os.path.join(output_dir, output_file)

    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return

    data = response.json()
    print(f"Response type: {type(data)}")

    # --- Case 1: ถ้า data เป็น list of dict ---
    if isinstance(data, list) and all(isinstance(item, dict) for item in data):
        records = data

    # --- Case 2: ถ้า data เป็น dict และมี "data" ---
    elif isinstance(data, dict) and "data" in data and "items" in data["data"]:
        #เลือกรายการ Array
        records = data["data"]["items"]
    else:
        print("Unexpected data format")
        return

    # --- Flatten Array ---
    flattened_records = []
    all_assessment_headers = set()

    for item in records:
        base = {k: v for k, v in item.items() if k != "assessments"}
        if "assessments" in item and isinstance(item["assessments"], list):
            for assess in item["assessments"]:
                # ใช้ wcStandardCriteriaName เป็น header
                base[assess["wcStandardCriteriaName"]] = assess["isPass"]
                all_assessment_headers.add(assess["wcStandardCriteriaName"])
        flattened_records.append(base)

    # สร้าง headers = key ปกติ + key จาก assessments
    normal_headers = [k for k in flattened_records[0].keys() if k not in all_assessment_headers]
    headers = normal_headers + list(all_assessment_headers)

    # --- Export To CSV ---
    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(flattened_records)

    print(f"✅ Data successfully saved to {output_path}")
    print(f"Total records: {len(flattened_records)}")



def ETL_API_System_Food_Only(url_point, month, year):
    url = f"{url_point}?month={month}&year={year}"
    print(f"{url}")
    output_dir = "C:\\Users\\chuwo\\OneDrive\\เอกสาร\\Desktop\\all_project\\Project_ETL_Data_From_REST_API\\APIผู้ผ่านการอบรม"
    output_file = f"output_Result_ผู้ผ่านการอบรม_{month}{year}.csv"
    output_path = os.path.join(output_dir, output_file)
     #Make the API request
    response = requests.get(url)
    print(type(response))

    # Validate the response
    if response.status_code != 200:
        print(f"Failed to fetch data. Status code: {response.status_code}")
    else:
        #Convert Response To text for skip tag php
        raw_text = response.text
        json_text = raw_text[raw_text.find("{"):]
        data = json.loads(json_text)
        print(type(data))
    
        # Check if data is a list of dictionaries
        if isinstance(data, list) and all(isinstance(item, dict) for item in data):
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)

            # Get headers from the first item
            headers = data[2].keys()

            # Write to CSV
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(data)

            print(f"Data successfully saved to {output_path}")
            print(f"Total records: {len(data)}")
        else:
            print("Unexpected data format. Expected a list of dictionaries.")
            if isinstance(data, dict) and "data" in data:
                records = data["data"]  # ดึง list ออกมา
                #records ตอนนี้เป็น list of dict
                headers = records[1].keys()
                print(f"Data Is: {headers}")

                # Write to CSV
                with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=headers, delimiter=';')
                    writer.writeheader()
                    #records เป็นการชี้ว่าให้เริ่มการเขียนลงไฟล์ CSV เริ่มต้นจากลำดับที่ 1
                    writer.writerows(records[1:])

                print(f"Data successfully saved to {output_path}")
                print(f"Total records: {len(records)}")




#========================== Input ===================================
#path_file_excel = input("Enter your path file excel for read: ")
#sheet_name = input("Enter your sheet_name in excel : ")
url_API = input("Enter your path API End point: ")
month_in = input("Enter Month: ")
year_in = input("Enter year: ")
#sheet_name = input("Enter your sheet_name in excel : ")

#========================= Call Method ================
ETL_API_System_Food_Only(url_API,month_in,year_in)
#Read_excel(path_file_excel,sheet_name,url_API)
#Get_APIEndPoint_Only(url_API)
#Get_APIEndPoint_Province(url_API,"ขอนแก่น")
