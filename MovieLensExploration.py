# Databricks notebook source
movie_table = "frantzpaul_tech.movielens.movies"
ratings_table = "frantzpaul_tech.movielens.ratings"
tags_table = "frantzpaul_tech.movielens.tags"
links_table = "frantzpaul_tech.movielens.links"
ratings_large_table = "frantzpaul_tech.movielens.ratings_large"

# COMMAND ----------

movies = spark.read.table(movie_table)
ratings = spark.read.table(ratings_table)
ratings_large = spark.read.table(ratings_large_table)
tags = spark.read.table(tags_table)
links = spark.read.table(links_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # Movies

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM frantzpaul_tech.movielens.movies;

# COMMAND ----------

# size of movie table
spark.sql(f"ANALYZE TABLE {movie_table} COMPUTE STATISTICS")
spark.sql(f"DESCRIBE EXTENDED {movie_table}").show(truncate=False)

# COMMAND ----------

spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------

display(movies)

# COMMAND ----------

# MAGIC %md
# MAGIC # Ratings

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM frantzpaul_tech.movielens.ratings;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM frantzpaul_tech.movielens.ratings_large;

# COMMAND ----------

display(ratings)

# COMMAND ----------

# MAGIC %md
# MAGIC Lets do a join on movieId

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT movies.movieId, ratings.rating, movies.title
# MAGIC FROM movielens.movies
# MAGIC JOIN movielens.ratings
# MAGIC ON movies.movieId = ratings.movieId;

# COMMAND ----------

# MAGIC %md
# MAGIC Took 55 seconds. We'll figure out how to make quicker.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT movies.movieId, ratings_large.rating, movies.title
# MAGIC FROM movielens.movies
# MAGIC JOIN movielens.ratings_large
# MAGIC ON movies.movieId = ratings_large.movieId;

# COMMAND ----------

# Reset back to default 10MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

# COMMAND ----------

from pyspark.sql.functions import broadcast

movies_df = spark.table(movie_table)
ratings_df = spark.table(ratings_table)

result = ratings_df.join(
    broadcast(movies_df),
    on="movieId",
    how="inner"
).select("movieId", "rating", "title")




# COMMAND ----------

# MAGIC %md
# MAGIC With broadcast join, operation takes 2s.

# COMMAND ----------

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

# COMMAND ----------

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tags

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM frantzpaul_tech.movielens.tags;

# COMMAND ----------

# MAGIC %md
# MAGIC # Links

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM frantzpaul_tech.movielens.links;
