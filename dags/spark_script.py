import sys, json, os
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructType, StringType, StructField

spark = SparkSession \
    .builder \
    .appName("DataProcess") \
    .getOrCreate() 

now = datetime.now()

def bronze_layer(api_data):

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street", StringType(), True)
    ])

    bronze_df = spark.createDataFrame(api_data, schema)
    bronze_df.show()
    bronze_df.write.format('avro').save(f'bronze/bronze_table-{now}')

def silver_layer():
    silver_df = spark.read.format('avro').load(f'bronze/bronze_table-{now}')
    silver_df.write.patitionBy('state').format('parquet').save(f'silver/silver_table-{now}')

def gold_layer():
    gold_df = spark.read.format('parquet').load(f'bronze/silver_table-{now}')
    gold_df = gold_df.withColumn('qtd_breweries', gold_df.groupBy(['state', 'brewery_type'].count()))
    gold_df.write.patitionBy('state').format('parquet').save(f'gold/gold_table-{now}')

if __name__ == "__main__":
    input = sys.argv[1]
    api_data = json.loads(input)
    bronze_layer(api_data)
    silver_layer()
    gold_layer()