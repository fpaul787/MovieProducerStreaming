# Databricks notebook source
# Kafka configuration
USERNAME = dbutils.secrets.get(scope="movielens", key="confluent_api_key")
PASSWORD = dbutils.secrets.get(scope="movielens", key="confluent_api_secret")
KAFKA_BOOTSTRAP_SERVERS = dbutils.secrets.get(scope="movielens", key="confluent_bootstrap_servers")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType

movie_schema = StructType([
    StructField("movieId", StringType(), nullable=False),
    StructField("title", StringType(), nullable=False),
    StructField("genres", StringType(), nullable=False),
])

rating_schema = StructType([
    StructField("userId", StringType(), nullable=False),
    StructField("movieId", StringType(), nullable=False),
    StructField("rating", DecimalType(2,1), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
])

tag_schema = StructType([
    StructField("userId", StringType(), nullable=False),
    StructField("movieId", StringType(), nullable=False),
    StructField("tag", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
])

link_schema = StructType([
    StructField("movieId", StringType(), nullable=False),
    StructField("imdbId", StringType(), nullable=False),
    StructField("tmdbId", StringType(), nullable=True),
])

# COMMAND ----------

from pyspark.sql.functions import from_json
def ingest_kafka_to_delta(topic_name, schema, table_name, partition_by=None):
    """
    Ingest data from Kafka topic to Delta table
    
    Args:
        topic_name: Kafka topic name
        schema: PySpark StructType schema for the data
        table_name: Fully qualified table name (catalog.schema.table)
        partition_by: Optional column name to partition by
    """
    # Kafka config for this topic
    kafka_config = {
        'subscribe': topic_name,
        'kafka.bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'kafka.security.protocol': 'SASL_SSL',
        'startingOffsets': 'earliest',
        'kafka.sasl.mechanism': 'PLAIN',
        'failOnDataLoss': 'false',
        'kafka.ssl.endpoint.identification.algorithm': 'https',
        'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{USERNAME}" password="{PASSWORD}";'
    }
    
    # Read from Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .options(**kafka_config)
        .load()
    )
    
    # Create table if not exists
    partition_clause = f"PARTITIONED BY ({partition_by})" if partition_by else ""
    schema_ddl = ", ".join([f"{field.name} {field.dataType.simpleString().upper()} {'NOT NULL' if not field.nullable else ''}" 
                            for field in schema.fields])
    
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      {schema_ddl}
    )
    USING DELTA
    {partition_clause}
    """)
    
    # Parse and write stream
    checkpoint_path = f"/dbfs/checkpoints/movielens/{table_name.replace('.', '_')}"
    
    query = (
        kafka_df
        .selectExpr("CAST(value AS STRING)")
        .select(from_json("value", schema).alias("data"))
        .select("data.*")
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .table(table_name)
    )
    
    return query

# COMMAND ----------

# Cell 4: Process all topics
topics_config = [
    {
        "topic": "movies_events_test1",
        "schema": movie_schema,
        "table": "frantzpaul_tech.movielens.movies",
        "partition_by": "genres"
    },
    {
        "topic": "ratings_events_test1",
        "schema": rating_schema,
        "table": "frantzpaul_tech.movielens.ratings",
        "partition_by": None
    },
    {
        "topic": "tags_events_test1",
        "schema": tag_schema,
        "table": "frantzpaul_tech.movielens.tags",
        "partition_by": None
    },
    {
        "topic": "links_events_test1",
        "schema": link_schema,
        "table": "frantzpaul_tech.movielens.links",
        "partition_by": None
    }
]

# COMMAND ----------

queries = []
for config in topics_config:
    print(f"Starting ingestion for {config['topic']} -> {config['table']}")
    query = ingest_kafka_to_delta(
        topic_name=config["topic"],
        schema=config["schema"],
        table_name=config["table"],
        partition_by=config.get("partition_by")
    )
    queries.append(query)
    print(f"✓ Completed {config['topic']}")

print(f"\nAll {len(queries)} topics processed successfully!")

# COMMAND ----------


