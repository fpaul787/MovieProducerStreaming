# Databricks notebook source
# DBTITLE 1,Benchmark explanation
# MAGIC %md
# MAGIC ## Rigorous Join Benchmark
# MAGIC Controls for caching, uses `.count()` for full materialization, runs multiple iterations, and keeps both joins in the same API (PySpark).

# COMMAND ----------

# DBTITLE 1,Rigorous join benchmark
import time
from pyspark.sql.functions import broadcast

movies_df = spark.table(movie_table)
ratings_df = spark.table(ratings_table)

NUM_ITERATIONS = 3

# --- Warm-up: read data into memory once ---
movies_df.count()
ratings_df.count()

# --- Sort-Merge Join (broadcast disabled) ---
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

smj_times = []
for i in range(NUM_ITERATIONS):
    start = time.time()
    (
        ratings_df.join(movies_df, on="movieId", how="inner")
        .select("movieId", "rating", "title")
        .count()  # force full materialization
    )
    smj_times.append(time.time() - start)

# --- Broadcast Join (explicit hint) ---
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

bhj_times = []
for i in range(NUM_ITERATIONS):
    start = time.time()
    (
        ratings_df.join(broadcast(movies_df), on="movieId", how="inner")
        .select("movieId", "rating", "title")
        .count()  # force full materialization
    )
    bhj_times.append(time.time() - start)

print(f"Sort-Merge Join  — times: {[f'{t:.2f}s' for t in smj_times]}, avg: {sum(smj_times)/len(smj_times):.2f}s")
print(f"Broadcast Join   — times: {[f'{t:.2f}s' for t in bhj_times]}, avg: {sum(bhj_times)/len(bhj_times):.2f}s")
print(f"Speedup: {sum(smj_times)/sum(bhj_times):.1f}x")
