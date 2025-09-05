import requests
import csv
import os
import pandas as pd
from datetime import date


url = "https://opendata.moph.go.th/api/report_data"

path_file_excel = r"C:\Users\chuwo\OneDrive\เอกสาร\Desktop\all_project\Project_ETL_Data_From_REST_API\จังหวัด.xlsx"
sheet_namee = "Sheet1"


def Read_excel(path_file_excel, sheet_namee, url_API, last_budget_year,current_date):
    i = 0
    df = pd.read_excel(path_file_excel, sheet_name=sheet_namee, usecols=["name", "moph_id"])
    row_index = pd.DataFrame(df)
    while(last_budget_year <= current_date):
        for idx, row in row_index.iterrows():
            if pd.notnull(row["name"]):
                i += 1
                name = row["name"]
                moph_province_id = row["moph_id"]
                print(f"=========Check Code{i}=============")
                print(f"แถว {idx} {name} {moph_province_id} Last Budget year Is {last_budget_year}")
                getAPI_pm25_age_sex(url_API, name, moph_province_id, last_budget_year)
        i = 0
        last_budget_year = last_budget_year+1
        print(f"last IS {last_budget_year}")
        

def getAPI_pm25_age_sex(end_point_pai, name_province, moph_province_id, last_budget_year):
    payload = {
        "province": str(moph_province_id),
        "tableName": "s_pm25_age_sex",
        "type": "json",
        "year": str(last_budget_year)
    }
    output_dir = r"C:\Users\chuwo\OneDrive\เอกสาร\Desktop\all_project\Project_ETL_Data_From_REST_API\โรค"
    
    year = payload["year"]
    output_file = f"จำนวนป่วย_รายโรค_จำแนกตามกลุ่มอายุและเพศ_รายสัปดาห์ด้วยโรคที่เกี่ยวข้องกับมลพิษทางอากาศ_ปี_{year}_ประจำจังหวัด{name_province}.csv"
    output_path = os.path.join(output_dir, output_file)

    headers = {"Content-Type": "application/json"}
    response = requests.post(end_point_pai, json=payload, headers=headers)

    if response.status_code not in [200, 201]:
        print(f"Failed to fetch data. Status code: {response.status_code}")
    else:
        data = response.json()

        if isinstance(data, list) and all(isinstance(item, dict) for item in data):
            os.makedirs(output_dir, exist_ok=True)

            #เช็คว่ามีข้อมูลหรือไม่เพื่อนป้องกัน ERR ไม่สามารถกำหนด Header ได้
            if len(data) != 0:
                headers_csv = data[0].keys()

                with open(output_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=headers_csv, delimiter=',')
                    writer.writeheader()
                    writer.writerows(data)

                print(f"Data successfully saved to {output_path}")
                print(f"Total records: {len(data)}")
            else:
                print("Province Not Found")
                print("Unexpected data format. Expected a list of dictionaries.")

# === Run ===
last_b_year = 2563
current_date = date.today().year + 543
print(f"{current_date}")
print(f"Budget year Is {last_b_year}")
Read_excel(path_file_excel, sheet_namee, url, last_b_year,current_date)
