
import requests
import pandas as pd
import os
import csv

URL_API = "https://ehdc.anamai.moph.go.th/ehdc/4.4.8.3/api/v1/doh-eforms/find-report?page=1&limit=10&joinTable=true&is_status=true"
output_dir = "C:\\Users\\chuwo\\OneDrive\\เอกสาร\\Desktop\\all_project\\Project_ETL_Data_From_REST_API\\shert_niche"
output_file = f"extract_shert.csv"
output_path = os.path.join(output_dir, output_file)


def GetAPIShert(base_url,output):
    url_api = base_url
    response = requests.get(url_api)
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
        # เลือกเฉพาะ column
        selected_columns = [
            "eform_id", 
            "section_id", 
            "section_title", 
            "header_dohAreaName", 
            "header_dohProvinceName",
            "header_situation_date",
            "header_situation_time",
            "master_type_danger_id",
            "position_dohProvince_name",
            "position_dohDistrict_name",
            "position_dohSubDistrict_name",
            "events_description",
            "life_impact",
            "amount_injured",
            "amount_dead",
            "amount_evacuate",
            ]
        selected_columns_labs = [
            "eform_id",
            "parameter_type_id",
            "status_result",
        ]
        all_records = []
        reocrd_lab = []
        for row in records:
            records_eform_section1 = row.get("Eform_section1", [])
            #records_eform_lab = row.get("lab_results", [])
            all_records.extend(records_eform_section1)
            #reocrd_lab.extend(records_eform_lab)


        #นำ Data จาก API มาใส่ในรูปแบบขของ  DataFrame
        
        data_frame = pd.DataFrame(all_records)

        # เก็บเฉพาะ column ที่เลือก
        data_frame = data_frame.reindex(columns=selected_columns)

        # ลบเรคคอร์ดที่ซ้ำออก
        data_frame = data_frame.drop_duplicates()

        print(f"จำนวนเรคคอร์ดหลังลบซ้ำ: {len(data_frame)}")
        #lab_frame = pd.DataFrame(reocrd_lab)
        #กำนหด columns ที่ต้องการ insert to csv
        data_frame = data_frame.reindex(columns=selected_columns)
        #lab_frame = lab_frame.reindex(columns=selected_columns_labs)
        
        print(f"Data Is : {data_frame}")
        #print(f"Lab Is : {lab_frame}")
        
        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=selected_columns)
            writer.writeheader()
            writer.writerows(data_frame.to_dict(orient="records"))
    else:
        print("Unexpected data format")
        return



GetAPIShert(URL_API,output_path)