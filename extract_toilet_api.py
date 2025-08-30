import requests
import csv
import os
import json as json

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

