from confluent_kafka import Producer
import requests
import json
import time
import pandas as pd  # pandas 라이브러리 임포트

# 이전 데이터를 추적하기 위한 변수 초기화
last_data = None

# Kafka 설정
kafka_config = {
    'bootstrap.servers': 'kafka01:9092,kafka02:9092,kafka03:9092',
    'client.id': 'api-producer3'
}

# Kafka 프로듀서 생성
producer = Producer(kafka_config)

# API 엔드포인트 및 파라미터 설정
api_url = "https://api.neople.co.kr/df/auction-sold"
params = {
    "itemName": "무색 큐브 조각",
    "wordType": "match",
    "wordShort": "false",
    "limit": 1,
    "apikey": "JUG54ELPbKttanbis2VPFNqC9LJOM7v4"
}

# 주기적으로 API 호출 및 데이터를 Kafka 토픽에 전송
while True:
    try:
        response = requests.get(api_url, params=params)
        data = response.json()
        data = data.get('rows')
        data = [{"soldDate": row["soldDate"], "itemName": row["itemName"], "count": row["count"], "unitPrice": row["unitPrice"]} for row in data]
        data = data[0]
        # 이전 데이터와 중복되지 않는 경우에만 데이터를 전송
        if data != last_data:
            last_data = data
            message = json.dumps(data, ensure_ascii=False).encode('utf-8')

            # Kafka 토픽에 데이터 전송
            producer.produce("donpa4", key="api_data", value=message)
            # 전송한 메시지 확인
            producer.poll(0)

            #print("API 데이터를 Kafka 토픽에 전송했습니다.")

    except Exception as e:
        print(f"오류 발생: {str(e)}")

    # 5분(300초) 대기
    time.sleep(10)
