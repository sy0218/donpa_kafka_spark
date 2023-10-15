from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
import requests
import os


# 스파크 세션 생성
spark = SparkSession.builder \
        .master("yarn") \
        .appName("aabataProcessing") \
        .getOrCreate()

# API 엔드포인트 URL
api_url = "https://api.neople.co.kr/df/avatar-market/sold"

# 파라미터 설정
params = {
        "limit": 50,
        "q": 'avatarSet:true,avatarRarity:상급',
        "apikey": "Q3A8yWb7Un1oXuM7uC5nIRV6zzGL23YP"
}

# GET 요청 보내기
response = requests.get(api_url, params=params)

# 응답 데이터 확인
if response.status_code == 200:
    data = response.json()
    # 데이터 처리 및 출력
    #print(data)
else:
    print("API 요청 실패:", response.status_code)

test1 = data.get("rows")

# 필요한 필드만 추출하여 저장할 리스트
extracted_data = []

for item in test1:
    title = item['title']
    jobname = item['jobName']
    price = item['price']
    ava_rit = item['avatarRarity']
    emblem = item['emblem']['name']
    soldDate = item['soldDate']

    extracted_data.append({'title': title, 'price': price, 'ava_rit': ava_rit, 'jobname': jobname,
                           'emblem': emblem, 'soldDate': soldDate})

# 데이터 프레임 생성
df = spark.createDataFrame(extracted_data)

# 원하는 순서로 컬럼 선택
df = df.select('soldDate', 'title', 'ava_rit', 'jobname', 'emblem', 'price')

# 중복 레코드 제거
df = df.dropDuplicates()

# soldDate 컬럼을 기준으로 정렬
df = df.orderBy("soldDate")

try:
    previous_df = spark.read.option("header", "true").csv("hdfs:///user/ubuntu/donpa_aabata_sang.csv/*.csv")
except:
    previous_df = None

if previous_df is not None:
    merged_df = previous_df.union(df)

    # 중복 레코드 제거
    merged_df = merged_df.dropDuplicates()

    # soldDate 컬럼을 기준으로 정렬
    merged_df = merged_df.orderBy("soldDate")

    # csv파일로 저장
    merged_df.write.csv("new_donpa_aabata_sang.csv", header=True, mode="overwrite")

    # 기존 디렉토리 삭제
    os.system("hdfs dfs -rm -r donpa_aabata_sang.csv")

    # 새로운 디렉터리 이름 변경
    os.system("hdfs dfs -mv new_donpa_aabata_sang.csv donpa_aabata_sang.csv")

else:
    # 바로저장
    df.write.csv("donpa_aabata_sang.csv", header=True, mode="overwrite")

print("성공")

# 스파크 세션 종료
spark.stop()
