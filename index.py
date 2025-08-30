
import extract_heatindex_csv as ex_heatindex
import extract_data_air_4_thai_api as ex_air_4_thai
import extract_food_api as ex_roomlist


url_end_point_air_4_thai = "http://air4thai.pcd.go.th/services/getNewAQI_JSON.php"
url_end_point_toilet = "https://ehdc.anamai.moph.go.th/ehdc/4.4.3/api/v1/widget/establishment/dashboard"
url_end_point_hidxbc_predict = "http://www.rnd.tmd.go.th/heatindex_API/hidxbc/"
url_end_point_hidxbc_real = "http://www.rnd.tmd.go.th/hi_monitor/data/"
url_end_point_training = "https://foodhandler.anamai.moph.go.th/webapp/webservice/training"
url_end_point_qa = "https://foodhandler.anamai.moph.go.th/webapp/webservice/qa"

print("===== Start App ========")
ex_air_4_thai.Get_Data_PM25_and_PM10(url_end_point_air_4_thai)