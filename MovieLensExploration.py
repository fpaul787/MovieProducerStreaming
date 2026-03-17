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

# DBTITLE 1,Join movies with ratings
# Join movies and ratings on movieId
movies_ratings = movies.join(ratings, on="movieId", how="inner")
print(f"Joined rows: {movies_ratings.count():,}")
display(movies_ratings.limit(20))

# COMMAND ----------

# DBTITLE 1,Rating distribution
from pyspark.sql import functions as F

# Rating value distribution
rating_dist = (
    ratings
    .groupBy("rating")
    .agg(F.count("*").alias("count"))
    .orderBy("rating")
)
display(rating_dist)

# COMMAND ----------

# DBTITLE 1,Average rating and count per genre
from pyspark.sql import functions as F

# Genres are pipe-delimited — explode them into individual rows
genre_stats = (
    movies_ratings
    .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
    .groupBy("genre")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("num_ratings"),
        F.countDistinct("movieId").alias("num_movies")
    )
    .orderBy(F.desc("num_ratings"))
)
display(genre_stats)

# COMMAND ----------

# DBTITLE 1,Top 20 highest-rated movies (min 100 ratings)
from pyspark.sql import functions as F

top_movies = (
    movies_ratings
    .groupBy("movieId", "title")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("num_ratings")
    )
    .filter(F.col("num_ratings") >= 100)
    .orderBy(F.desc("avg_rating"), F.desc("num_ratings"))
    .limit(20)
)
display(top_movies)

# COMMAND ----------

# DBTITLE 1,Explore tags — top 20 most used tags
from pyspark.sql import functions as F

# Most popular tags
top_tags = (
    tags
    .groupBy(F.lower(F.col("tag")).alias("tag_lower"))
    .agg(F.count("*").alias("count"))
    .orderBy(F.desc("count"))
    .limit(20)
)
display(top_tags)

# COMMAND ----------

# DBTITLE 1,Explore links table
from pyspark.sql import functions as F

# Show sample and check for nulls
print("Sample links data:")
display(links.limit(10))
print(f"\nNull tmdbId count: {links.filter(F.col('tmdbId').isNull()).count():,}")

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

# COMMAND ----------


