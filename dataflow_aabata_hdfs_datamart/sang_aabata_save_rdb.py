from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

spark = SparkSession.builder \
        .master('yarn') \
        .appName('daflow_aabata_hdfs_rdb') \
        .getOrCreate()

# MaríaDB 연결 정보 설정
mariadb_url = "jdbc:mysql://172.31.41.158:3306/donpa_datamart1"
mariadb_properties = {
    "user": "root",
    "password": "1234",
    "driver": "org.mariadb.jdbc.Driver"
}

# HDFS에서 CSV 파일 읽어오기
csv_path = "hdfs:///user/ubuntu/donpa_aabata_sang.csv/*.csv"
csv_df = spark.read.option("header", "true").csv(csv_path)

csv_df.show()

# MaríaDB에 데이터프레임 쓰기
csv_df.write \
    .jdbc(url=mariadb_url, table="sang_aabata", mode="overwrite", properties=mariadb_properties)

# 스파크 애플리케이션 종료
spark.stop()
