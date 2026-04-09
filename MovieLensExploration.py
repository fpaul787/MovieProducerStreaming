# Databricks notebook source
# DBTITLE 1,Notebook summary
# MAGIC %md
# MAGIC # MovieLens Exploration
# MAGIC
# MAGIC An EDA notebook on the **MovieLens** dataset (`frantzpaul_tech.movielens`) covering five tables: `movies`, `ratings`, `ratings_large`, `tags`, and `links`.
# MAGIC
# MAGIC **Data setup** — All tables are loaded as DataFrames in cells 2–3 and referenced throughout.
# MAGIC
# MAGIC **Ratings analysis** — The bulk of the notebook. We join movies → ratings, then explore rating distribution, average rating per genre (exploded from pipe-delimited strings), the top 20 highest-rated films (min 100 ratings), and time-series trends (daily and year-over-year).
# MAGIC
# MAGIC **Tags & Links** — Light exploration: top 20 most-used tags (case-insensitive), sample link records, and a null audit on `tmdbId`.
# MAGIC
# MAGIC **Movie Analysis** — Window functions (`RANK` / `DENSE_RANK`) to rank movies within each genre by average rating, plus a decade-level breakdown extracted from title year.
# MAGIC
# MAGIC **Still TODO** — Genre combination frequency & cross-decade production trends; tag activity comparison across genres.

# COMMAND ----------

# DBTITLE 1,Define table references
from pyspark.sql import functions as F

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
# MAGIC SELECT COUNT(*) FROM frantzpaul_tech.movielens.ratings;

# COMMAND ----------

# DBTITLE 1,Count ratings_large
# MAGIC %sql
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

# DBTITLE 1,Rating trends over time
# Rating trends over time
ratings_time_df = (
    ratings
        .withColumn("date", F.to_date(F.col("timestamp")))
        .orderBy(F.asc("date"))
        .drop("timestamp")
)

display(ratings_time_df)

# COMMAND ----------

# DBTITLE 1,Average rating by year
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

# DBTITLE 1,Display tags table
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

# DBTITLE 1,Display links table
display(links)

# COMMAND ----------

# DBTITLE 1,Explore links table
from pyspark.sql import functions as F

# Show sample and check for nulls
print("Sample links data:")
display(links.limit(10))
print(f"\nNull tmdbId count: {links.filter(F.col('tmdbId').isNull()).count():,}")

# COMMAND ----------

# DBTITLE 1,Movie Analysis section header
# MAGIC %md
# MAGIC    
# MAGIC # Movie Analysis

# COMMAND ----------

# DBTITLE 1,Rank movies within each genre intro
# MAGIC %md
# MAGIC    
# MAGIC Rank movies within each genre

# COMMAND ----------

# DBTITLE 1,Rank movies by avg rating within genre
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

# DBTITLE 1,Average rating by decade
# Decade analysis — The title column includes the release year in parentheses. 
# Extract it with a regex and compare avg ratings by decade.
movies_decade = (
    movies_ratings
        .withColumn('year', F.regexp_extract('title', r'\((\d{4})\)', 1))
        .filter(F.col('year') != '')
        .withColumn('decade', (F.col('year').cast('int') / 10).cast('int') * 10)
        .groupBy('decade')
        .agg(F.round(F.avg('rating'), 2).alias('avg_rating'), F.count('*').alias('num_movies'))
        .orderBy(F.desc('avg_rating'))
)

display(movies_decade)

# COMMAND ----------

# DBTITLE 1,Genre Deep Dives section header
# MAGIC %md
# MAGIC    
# MAGIC # Genre Deep Dives

# COMMAND ----------

# DBTITLE 1,Genre combinations and trends TODO
# Genre combinations — Which multi-genre combinations (e.g., Action|Adventure vs. Drama|Romance) are most common, and do they rate differently?
# Genre trends over time — Did certain genres peak in production in certain decades (e.g., Westerns in the 60s, Sci-Fi in the 2000s)?

# COMMAND ----------

# DBTITLE 1,Tags and Links Enrichment section header
# MAGIC %md
# MAGIC    
# MAGIC # Tags & Links Enrichment

# COMMAND ----------

# DBTITLE 1,Tags per genre TODO
# Tags per genre — 
# Join tags back to movies and see which genres generate the most tag activity. 
# Are Drama movies tagged more descriptively than Action movies?
