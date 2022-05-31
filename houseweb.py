from flask import Flask, request, jsonify
import os,json
from pathlib import Path
import dataproc
from flask_cors import CORS

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False
CORS(app)

file_dir =os.path.join(Path.home(), "Downloads/lvr_landcsv.zip")


@app.route('/')
def index():
    return '這是內政部不動產網站的資料搜尋！'

@app.route('/search',methods=['POST'])
def getvalue():
    # 取得前端傳過來的數值
    insertValues = request.get_json()
    city =insertValues['city']
    town = insertValues['area']
    totalfloor = insertValues['totalfloor']
    housetype = insertValues['housetype']
    data_json = dataproc.data_proc(city,town,totalfloor,housetype)
    house_search =json.loads(data_json)
    result = jsonify(house_search)

    return result

if __name__ =='__main__':
    app.run(host='0.0.0.0', port=3000, debug=True)

