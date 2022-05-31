from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col,to_date,array
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName('PySpark DataFrame From External Files').getOrCreate()
# read files
house_df=spark.read.csv('data/process/a_lvr_land_a_proc.csv', header=True)
house_df=house_df.withColumn("城市", lit("台北市"))
house_df1=spark.read.csv('data/process/b_lvr_land_a_proc.csv', header=True)
house_df1=house_df1.withColumn("城市", lit("台中市"))
house_df2=spark.read.csv('data/process/e_lvr_land_a_proc.csv', header=True)
house_df2=house_df2.withColumn("城市", lit("高雄市"))
house_df3=spark.read.csv('data/process/f_lvr_land_a_proc.csv', header=True)
house_df3=house_df3.withColumn("城市", lit("新北市"))
house_df4=spark.read.csv('data/process/h_lvr_land_a_proc.csv', header=True)
house_df4=house_df4.withColumn("城市", lit("桃園市"))
# 資料合併
housecomb_df = house_df.union(house_df1)
housecomb_df1=housecomb_df.union(house_df2)
housecomb_df2=housecomb_df1.union(house_df3)
housecomb_df3=housecomb_df2.union(house_df4)

housecomb_df3=housecomb_df3.withColumn("總樓層數Eng", col("總樓層數Eng").cast(IntegerType()))
housecomb_df3=housecomb_df3.withColumn("交易年月日", col("交易年月日").cast(IntegerType()))
housecomb_df3=housecomb_df3.withColumn("交易月日",col("交易年月日"))
housecomb_df4 = housecomb_df3.select('城市','鄉鎮市區','主要用途','建物型態','總樓層數Eng','交易年月日','交易月日')
housecomb_df4 = housecomb_df4.filter((housecomb_df4['主要用途']=='住家用') &(housecomb_df4['建物型態']=='住宅大樓(11層含以上有電梯)') & (housecomb_df4['總樓層數Eng']>=13))
# 欄位名稱更改
housecomb_df4=housecomb_df4.withColumnRenamed("城市","City")
housecomb_df4=housecomb_df4.withColumnRenamed("鄉鎮市區","Area")
housecomb_df4=housecomb_df4.withColumnRenamed("主要用途","main_apply")
housecomb_df4=housecomb_df4.withColumnRenamed("建物型態","housetype")
housecomb_df4 = housecomb_df4.withColumnRenamed("總樓層數Eng","Floor_total")
housecomb_df4=housecomb_df4.withColumnRenamed("交易年月日","Date")
housecomb_df4=housecomb_df4.withColumnRenamed("交易月日","MonthDate")
# 年份改成西元年
housecomb_df4.createOrReplaceTempView("housetable")
newDF = spark.sql("SELECT City,main_apply,Area,housetype,CONCAT(FLOOR(Substring(Date,1,3)+1911),'-',Substring(MonthDate,4,2),'-',Substring(MonthDate,6,2)) AS YEARMONTHDATE, \
                  Floor_total FROM housetable \
                  WHERE FLOOR(Substring(Date,1,3)+1911)<=2022")
# newDF1 = newDF.select('City','main_apply','Area','housetype','YEARMONTHDATE','Floor_total').orderBy("YEARMONTHDATE", ascending=False)
newDF1=newDF.select('City',to_date('YEARMONTHDATE','yyyy-MM-dd').alias('Date'),array(newDF.Area,newDF.housetype).alias("events")).sort(F.desc("Date"))
output = newDF1.coalesce(2).write.format('json').save('data/process/output')


