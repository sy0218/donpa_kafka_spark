from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

sc = SparkSession.builder \
     .master('yarn') \
     .appName('donpa_spark_stream4') \
     .config("spark.yarn.am.memory", "200m") \
     .config("spark.yarn.am.cores", "1") \
     .getOrCreate()


sc.sparkContext.setLogLevel('INFO')

# read stream
df   = sc.readStream.format('kafka') \
      .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9091,kafka03:9092") \
      .option("subscribe", "donpa4") \
      .option("startingOffsets", "earliest") \
      .load()

# JSON 스키마 정의
schema = StructType([
    StructField("soldDate", TimestampType(), True),
    StructField("itemName", StringType(), True),
    StructField("count", IntegerType(), True),
    StructField("unitPrice", DoubleType(), True)
])

# value 컬럼을 JSON 파싱
df = df.selectExpr("CAST(value AS STRING) as json")
df = df.select(from_json(df.json, schema).alias("data"))

# Define a function to process each batch
def process_batch(batch_df, batch_id):
    if batch_id == 0:
        # Skip processing for batch ID 0
        pass
    else:
        # Your processing logic for other batch IDs
        batch_df.write.option("header", "true") \
                .format("csv") \
                .mode("append") \
                .save("/donpa4_spark_stream")

# Write stream - HDFS using foreachBatch
query2 = df.select("data.*").writeStream \
            .foreachBatch(process_batch) \
            .outputMode("append") \
            .start()


query2.awaitTermination()
