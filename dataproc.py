import pandas as pd
from zipfile import ZipFile
import os
from pathlib import Path

def engnum(val):
    '''
    做資料處理，把中文的數字轉成英文
    '''
    num_dict = {}
    num_chinese=['一','二','三','四','五','六','七','八','九','十','十一','十二','十三','十四','十五','十六','十七','十八','十九','二十',
                 '二十一','二十二','二十三','二十四','二十五','二十六','二十七','二十八','二十九','三十','三十一','三十二','三十三']
    num = [i for i in range(1,34)]
    for a,b in zip(num_chinese, num):
        num_dict[a] =b
    num_dict['四十二']=42
    for num_chinese in num_dict.keys():
        if val==num_chinese:
            return num_dict[val]

def data_proc(city, town,totalfloor,housetype):
    # 解壓縮檔案
    # with ZipFile(str(file_dir), 'r') as zipobj:
    #     zipobj.extractall('data')
    city_lst = ['台北市','台中市','高雄市','新北市','桃園市']
    file_lst = ['a_lvr_land_a.csv', 'b_lvr_land_a.csv', 'e_lvr_land_a.csv','f_lvr_land_a.csv','h_lvr_land_a.csv']
    for i in range(len(city_lst)):
        if city_lst[i]==city:
            file = file_lst[i]
    # 讀取資料
    df = pd.read_csv(f'data/{file}',header=0,skiprows=[1])
    house_tpidf = df.loc[df['總樓層數'].notnull()]
    # 把中文的數字轉成英文
    house_tpidf['總樓層數Eng'] = house_tpidf['總樓層數'].str.split('層', 1).str[0]
    house_tpidf['總樓層數Eng']=house_tpidf['總樓層數Eng'].apply(engnum)
    house_tpidf.to_csv('data/process/a_lvr_land_a_proc.csv', header=True, encoding="utf_8_sig",index=False)
    # 塞選資料
    house_tpidf1 = house_tpidf.loc[(house_tpidf['鄉鎮市區']==town) & (house_tpidf['總樓層數Eng']==totalfloor) & (house_tpidf['建物型態']==housetype)]
    house_tpidf_update = house_tpidf1[['鄉鎮市區','土地位置建物門牌','總樓層數','建物型態','交易標的','土地移轉總面積平方公尺', '交易年月日','交易筆棟數',
                  '移轉層次','主要用途','主要建材','建築完成年月','建物移轉總面積平方公尺','建物現況格局-房',
                  '建物現況格局-廳','建物現況格局-衛','建物現況格局-隔間','總價元', '單價元平方公尺', '車位類別', '車位移轉總面積(平方公尺)',
                  '主建物面積','總樓層數Eng']]
    house_search = house_tpidf_update.to_json(orient='records')
    # print(house_tpidf1[['鄉鎮市區','總樓層數＿去除','建物型態','土地位置建物門牌','土地移轉總面積平方公尺']])

    return house_search
