from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

spark = SparkSession.builder \
        .master('yarn') \
        .appName('show_df') \
        .getOrCreate()

# MaríaDB 연결 정보 설정
mariadb_url = "jdbc:mysql://172.31.41.158:3306/donpa_datamart1"
mariadb_properties = {
    "user": "root",
    "password": "1234",
    "driver": "org.mariadb.jdbc.Driver"
}

import subprocess
import datetime

# 현재 시간과 30분 뺀 시간 계산
current_time = datetime.datetime.now()
time_threshold = current_time - datetime.timedelta(hours=1)

# HDFS 디렉토리 경로
hdfs_directory = "/donpa3_spark_stream"
# HDFS 디렉토리 내 파일 목록 가져오기
hdfs_ls_command = f"hdfs dfs -ls {hdfs_directory}"
result = subprocess.run(hdfs_ls_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

# 가져온 파일 목록을 줄 단위로 분할
file_lines = result.stdout.strip().split('\n')
# 파일 목록을 시간 역순으로 정렬
sorted_files = sorted(file_lines, key=lambda line: line.split()[-2], reverse=True)[1:]


# 스키마 정의
schema = StructType([
    StructField("soldDate", TimestampType(), True),
    StructField("itemName", StringType(), True),
    StructField("count", IntegerType(), True),
    StructField("unitPrice", DoubleType(), True)
])

# 빈 데이터 프레임 생성
result_df = spark.createDataFrame([], schema)

index_num = 2
hdfs_path_list = []
while index_num < len(sorted_files):
    # 파일 가져오기
    file = sorted_files[index_num]

    # 파일 생성시간 추출
    file_time_str = file.split()[-3] + ' ' + file.split()[-2]
    file_time = datetime.datetime.strptime(file_time_str, '%Y-%m-%d %H:%M')

    if file_time >= time_threshold:
        # 파일 경로 파싱
        file_path = file.split()[-1]
        hdfs_path_list.append(file_path)
        # index_num 증감
        index_num += 1
    else:
        break

for i in hdfs_path_list:
    df = spark.read.option('header', 'true').csv(f'hdfs://{i}')
    result_df = df.union(result_df)

# DataFrame을 사용하여 원하는 작업을 수행할 수 있습니다.
result_df.show()

# MaríaDB에 데이터프레임 쓰기
result_df.write \
    .jdbc(url=mariadb_url, table="donpa_item03", mode="overwrite", properties=mariadb_properties)

# 스파크 애플리케이션 종료
spark.stop()
