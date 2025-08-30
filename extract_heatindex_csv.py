import requests
import os
import pandas as pd
import json as json
from bs4 import BeautifulSoup

#Method transform csv heatindex
def read_csv_and_transform(path_file_excel):
   
   list_header_heatindex = [
        "stationID",
        "lat",
        "long",
        "hidxbc_date_1_time_00Z",
        "hidxbc_date_1_time_03Z",
        "hidxbc_date_1_time_06Z",
        "hidxbc_date_1_time_09Z",
        "hidxbc_date_1_time_12Z",
        "hidxbc_date_1_time_15Z",
        "hidxbc_date_1_time_18Z",
        "hidxbc_date_1_time_21Z",
        "hidxbc_date_2_time_00Z",
        "hidxbc_date_2_time_03Z",
        "hidxbc_date_2_time_06Z",
        "hidxbc_date_2_time_09Z",
        "hidxbc_date_2_time_12Z",
        "hidxbc_date_2_time_15Z",
        "hidxbc_date_2_time_18Z",
        "hidxbc_date_2_time_21Z",
        "hidxbc_date_3_time_00Z",
        "hidxbc_date_3_time_03Z",
        "hidxbc_date_3_time_06Z",
        "hidxbc_date_3_time_09Z",
        "hidxbc_date_3_time_12Z",
        "hidxbc_date_3_time_15Z",
        "hidxbc_date_3_time_18Z",
        "hidxbc_date_3_time_21Z",
        "hidxbc_date_4_time_00Z",
        "hidxbc_date_4_time_03Z",
        "hidxbc_date_4_time_06Z",
        "hidxbc_date_4_time_09Z",
        "hidxbc_date_4_time_12Z",
        "hidxbc_date_4_time_15Z",
        "hidxbc_date_4_time_18Z",
        "hidxbc_date_4_time_21Z",
        "hidxbc_date_5_time_00Z",
        "hidxbc_date_5_time_03Z",
        "hidxbc_date_5_time_06Z",
        "hidxbc_date_5_time_09Z",
        "hidxbc_date_5_time_12Z",
        "hidxbc_date_5_time_15Z",
        "hidxbc_date_5_time_18Z",
        "hidxbc_date_5_time_21Z",
        "hidxbc_date_6_time_00Z",
        "hidxbc_date_6_time_03Z",
        "hidxbc_date_6_time_06Z",
        "hidxbc_date_6_time_09Z",
        "hidxbc_date_6_time_12Z",
        "hidxbc_date_6_time_15Z",
        "hidxbc_date_6_time_18Z",
        "hidxbc_date_6_time_21Z",
        "hidxbc_date_7_time_00Z",
        "hidxbc_date_7_time_03Z",
        "hidxbc_date_7_time_06Z",
        "hidxbc_date_7_time_09Z",
        "hidxbc_date_7_time_12Z",
        "hidxbc_date_7_time_15Z",
        "hidxbc_date_7_time_18Z",
        "hidxbc_date_7_time_21Z",
        "hidxbc_date_8_time_00Z",
        "hidxbc_date_8_time_03Z",
        "hidxbc_date_8_time_06Z",
        "hidxbc_date_8_time_09Z",
        "hidxbc_date_8_time_12Z",
        "hidxbc_date_8_time_15Z",
        "hidxbc_date_8_time_18Z",
        "hidxbc_date_8_time_21Z",
        "hidxbc_date_9_time_00Z",
        "hidxbc_date_9_time_03Z",
        "hidxbc_date_9_time_06Z",
        "hidxbc_date_9_time_09Z",
        "hidxbc_date_9_time_12Z",
        "hidxbc_date_9_time_15Z",
        "hidxbc_date_9_time_18Z",
        "hidxbc_date_9_time_21Z",
        "hidxbc_date_10_time_00Z",
        "hidxbc_date_10_time_03Z",
        "hidxbc_date_10_time_06Z",
        "hidxbc_date_10_time_09Z",
        "hidxbc_date_10_time_12Z",
        "hidxbc_date_10_time_15Z",
        "hidxbc_date_10_time_18Z",
        "hidxbc_date_10_time_21Z",
    ]



    #อ่านรายการไฟล์ที่อยู่ใน Data_hidxbc_predict
   dir_list = os.listdir(path_file_excel)
   print(f"List File in predict {dir_list}")

   #Loop อ่านไฟล์ csv ที่มีชื่ออยู่ใน dir_list
   for row in dir_list:
    print(f"Path is [{path_file_excel}/{row}]")

    # อ่านcsv และกำหนดให้header เป็น noneและเพิ่มลงใน Datafram
    read_excel_extract = pd.read_csv(f"{path_file_excel}/{row}", header=None)
    data_frame = pd.DataFrame(read_excel_extract)
    print(f"Data frame is {data_frame}")
    print(f"{list_header_heatindex}")


    # ใส่ header ใหม่ที่เรากำหนดเองใน list_header_air
    data_frame.columns = list_header_heatindex[:len(data_frame.columns)]

    #output path file.
    output_dir = "C:\\Users\\chuwo\\OneDrive\\เอกสาร\\Desktop\\all_project\\Project_ETL_Data_From_REST_API\\Data_heatindex_transform_header"
    output_file = f"tran_from_{row}"
    output_path = os.path.join(output_dir, output_file)

    # บันทึกไฟล์ใหม่
    if data_frame.to_csv(output_path, index=False, encoding="utf-8-sig"):
        print("✅ บันทึกไฟล์ใหม่พร้อม header แล้ว")
        print(f"✅ บันทึกไฟล์ CSV พร้อม header สำเร็จ: {output_dir}")        
    
           

#Download liat file csv in Directory in URL
def Get_Data_CSV_hidxbc_predic(url_point):
    url = f"{url_point}"
    print(f"End Point: {url}")
    output_dir = "C:\\Users\\chuwo\\OneDrive\\เอกสาร\\Desktop\\all_project\\Project_ETL_Data_From_REST_API\\Data_hidxbc_predict"
    output_file = f"hidxbc.csv"
    output_path = os.path.join(output_dir, output_file)
    response = requests.get(url)

    soup = BeautifulSoup(response.text, "html.parser")

    csv_links = [a["href"] for a in soup.find_all("a") if a["href"].endswith(".csv")]
    
    for link in csv_links:
        file_url = url + link
        file_path = os.path.join(output_dir, link)

        print(f"Downloading {file_url}")
        r = requests.get(file_url)
        with open(file_path, "wb") as f:
            f.write(r.content)

    print("✅ All CSV downloaded!")
    print(f"✅ Data successfully saved to {file_url}")
    print(f"Total records: {len(csv_links)}")

def Get_Data_CSV_hidxbc_real(url_point):
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


url_end_point_hidxbc_predict = "http://www.rnd.tmd.go.th/heatindex_API/hidxbc/"
url_end_point_hidxbc_real = "http://www.rnd.tmd.go.th/hi_monitor/data/"


#Excel path
path_excel_read = "C:\\Users\\chuwo\\OneDrive\\เอกสาร\\Desktop\\all_project\\Project_ETL_Data_From_REST_API\\Data_hidxbc_predict"

Get_Data_CSV_hidxbc_real(url_end_point_hidxbc_real)
Get_Data_CSV_hidxbc_predic(url_end_point_hidxbc_predict )
read_csv_and_transform(path_excel_read)
 