# Databricks notebook source
# Kafka configuration
username = dbutils.secrets.get(scope="structured_streaming", key="confluent_api_key")
password = dbutils.secrets.get(scope="structured_streaming", key="confluent_api_secret")
kafka_bootstrap_servers = dbutils.secrets.get(scope="structured_streaming", key="confluent_bootstrap_servers")
kafka_topic = 'topic_car_purchases'

# Kafka Consumer
kafka_config = {
    'subscribe': kafka_topic,
    'kafka.bootstrap.servers': kafka_bootstrap_servers,
    'kafka.security.protocol': 'SASL_SSL',
    'startingOffsets': 'earliest',
    'kafka.sasl.mechanism': 'PLAIN',
    'failOnDataLoss': 'false',
    'kafka.ssl.endpoint.identification.algorithm': 'https',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" password="{password}";'
}

# COMMAND ----------

# Define schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType

movie_schema = StructType([
    StructField("movieId", StringType(), nullable=False),
    StructField("title", StringType(), nullable=False),
    StructField("genres", StringType(), nullable=False),
])

# COMMAND ----------

# read stream from kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_config)
    .load()
)

# COMMAND ----------

# create movie table if not exist with schema
# save in frantzpaul_tech.movielens schema
movie_table_name = "frantzpaul_tech.movielens.movies"
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {movie_table_name} (
  movieId STRING NOT NULL,
  title STRING NOT NULL,
  genres STRING NOT NULL
)
USING DELTA
PARTITIONED BY (genres)
""")

# COMMAND ----------

# write stream to movie table
(
    kafka_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json("value", movie_schema).alias("movie"))
    .select("movie.*")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"/tmp/movielens_big_data/movies/checkpoint")
    .option("mergeSchema", "true")
    .trigger(once=True)
    .table(movie_table_name)
)
