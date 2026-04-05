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

from pyspark.sql import functions as F

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

# Rating trends over time
ratings_time_df = (
    ratings
        .withColumn("date", F.to_date(F.col("timestamp")))
        .orderBy(F.asc("date"))
        .drop("timestamp")
)

display(ratings_time_df)

# COMMAND ----------

# How have rating shifted year to year
ratings_year_df = (
    ratings_time_df
        .withColumn("year", F.year(F.col("date")))
        .groupBy("year")
        .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
        .orderBy(F.asc("year"))
)
display(ratings_year_df)

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

display(tags)

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

display(links)

# COMMAND ----------

# DBTITLE 1,Explore links table
from pyspark.sql import functions as F

# Show sample and check for nulls
print("Sample links data:")
display(links.limit(10))
print(f"\nNull tmdbId count: {links.filter(F.col('tmdbId').isNull()).count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Movie Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC Rank movies within each genre

# COMMAND ----------

from pyspark.sql.window import Window

# Explode genres and compute avg rating per movie
movies_genre_avg = (
    movies_ratings
    .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
    .groupBy("genre", "movieId", "title")
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"), F.count("*").alias("num_ratings"))
)

# Define window partitioned by genre, ordered by avg_rating desc
genre_window = Window.partitionBy("genre").orderBy(F.desc("avg_rating"))

# Add rank and dense_rank columns
ranked_movies = (
    movies_genre_avg
    .withColumn("rank", F.rank().over(genre_window))
    .withColumn("dense_rank", F.dense_rank().over(genre_window))
)

display(ranked_movies)

# COMMAND ----------

# Decade analysis — The title column includes the release year in parentheses. 
# Extract it with a regex and compare avg ratings by decade.

# COMMAND ----------

# Rating count vs. avg rating — Plot these two dimensions together. 
# You'll likely see an interesting pattern where very popular movies don't always have the highest ratings.

# COMMAND ----------

# MAGIC %md
# MAGIC # Genre Deep Dives

# COMMAND ----------

# Genre combinations — Which multi-genre combinations (e.g., Action|Adventure vs. Drama|Romance) are most common, and do they rate differently?
# Genre trends over time — Did certain genres peak in production in certain decades (e.g., Westerns in the 60s, Sci-Fi in the 2000s)?

# COMMAND ----------

# MAGIC %md
# MAGIC # Tags & Links Enrichment

# COMMAND ----------

# Tags per genre — 
# Join tags back to movies and see which genres generate the most tag activity. 
# Are Drama movies tagged more descriptively than Action movies?
