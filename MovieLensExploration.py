# Databricks notebook source
# DBTITLE 1,Define table references
movie_table = "frantzpaul_tech.movielens.movies"
ratings_table = "frantzpaul_tech.movielens.ratings"
tags_table = "frantzpaul_tech.movielens.tags"
links_table = "frantzpaul_tech.movielens.links"
ratings_large_table = "frantzpaul_tech.movielens.ratings_large"

# COMMAND ----------

# DBTITLE 1,Load tables as DataFrames
movies = spark.read.table(movie_table)
ratings = spark.read.table(ratings_table)
ratings_large = spark.read.table(ratings_large_table)
tags = spark.read.table(tags_table)
links = spark.read.table(links_table)

# COMMAND ----------

# DBTITLE 1,Movies section header
# MAGIC %md
# MAGIC    
# MAGIC # Movies

# COMMAND ----------

# DBTITLE 1,Count movies
# MAGIC %sql
# MAGIC     
# MAGIC SELECT COUNT(*) FROM frantzpaul_tech.movielens.movies;

# COMMAND ----------

# DBTITLE 1,Compute movies table statistics
# size of movie table
spark.sql(f"ANALYZE TABLE {movie_table} COMPUTE STATISTICS")
spark.sql(f"DESCRIBE EXTENDED {movie_table}").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Check auto broadcast join threshold
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------

# DBTITLE 1,Display movies table
display(movies)

# COMMAND ----------

# DBTITLE 1,Ratings section header
# MAGIC %md
# MAGIC    
# MAGIC # Ratings

# COMMAND ----------

# DBTITLE 1,Count ratings
# MAGIC %sql
# MAGIC     
# MAGIC SELECT COUNT(*) FROM frantzpaul_tech.movielens.ratings;

# COMMAND ----------

# DBTITLE 1,Count ratings_large
# MAGIC %sql
# MAGIC     
# MAGIC SELECT COUNT(*) FROM frantzpaul_tech.movielens.ratings_large;

# COMMAND ----------

# DBTITLE 1,Display ratings table
display(ratings)

# COMMAND ----------

# DBTITLE 1,Join experiment intro
# MAGIC %md
# MAGIC    
# MAGIC Lets do a join on movieId

# COMMAND ----------

# DBTITLE 1,Sort-merge join movies and ratings
# MAGIC %sql
# MAGIC     
# MAGIC SELECT movies.movieId, ratings.rating, movies.title
# MAGIC FROM movielens.movies
# MAGIC JOIN movielens.ratings
# MAGIC ON movies.movieId = ratings.movieId;

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

# DBTITLE 1,Display broadcast join result
display(result)

# COMMAND ----------

# DBTITLE 1,Tags section header
# MAGIC %md
# MAGIC    
# MAGIC # Tags

# COMMAND ----------

# DBTITLE 1,Count tags
# MAGIC %sql
# MAGIC     
# MAGIC SELECT COUNT(*) FROM frantzpaul_tech.movielens.tags;

# COMMAND ----------

# DBTITLE 1,Links section header
# MAGIC %md
# MAGIC    
# MAGIC # Links

# COMMAND ----------

# DBTITLE 1,Count links
# MAGIC %sql
# MAGIC     
# MAGIC SELECT COUNT(*) FROM frantzpaul_tech.movielens.links;
