# 內政部房子資料抓取

## 介紹
到內政部不動產網站，使用Python資料下載和選擇台北市,新北市,桃園市,台中市和高雄市的資料，並且使用Python和Pyspark做資料處理和資料篩選，把資料儲存成Json檔案，並且設計RESTful API，資料能以鄉鎮市區、總樓層數和建物型態做資料搜尋，網頁會顯示搜尋的資料

datascrape.py: 資料爬蟲
dataproc_pyspark.py: 使用Pyspark做資料處理
houseweb.py: 設計Restful API，使用Post方式，做資料搜尋
