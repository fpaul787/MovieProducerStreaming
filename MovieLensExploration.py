# Databricks notebook source
movie_table = "frantzpaul_tech.movielens.movies"
ratings_table = "frantzpaul_tech.movielens.ratings"
tags_table = "frantzpaul_tech.movielens.tags"
links_table = "frantzpaul_tech.movielens.links"

# COMMAND ----------

movies = spark.read.table(movie_table)
ratings = spark.read.table(ratings_table)
tags = spark.read.table(tags_table)
links = spark.read.table(links_table)

# COMMAND ----------

display(movies)
display(ratings)
display(tags)
display(links)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   genre,
# MAGIC   COUNT(*) AS amount
# MAGIC FROM (
# MAGIC   SELECT explode(split(genres, '[|]')) AS genre
# MAGIC   FROM frantzpaul_tech.movielens.movies
# MAGIC )
# MAGIC GROUP BY genre
# MAGIC ORDER BY amount DESC;

# COMMAND ----------

from pyspark.sql.functions import split, explode

df_genres = movies.select(
    explode(
        split("genres", "\\|")
    ).alias("genre")
).groupBy("genre").count()

# COMMAND ----------

display(df_genres)

# COMMAND ----------

from pyspark.sql.functions import count
from pyspark.sql.functions import desc
df_tags_by_movie = tags.groupBy("movieId").agg(count("tag").alias("amount")).orderBy(desc("amount"))


display(df_tags_by_movie)


# COMMAND ----------

# MAGIC %md
# MAGIC What is total count of each user for each tag?

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- window function
# MAGIC -- What is total count of each user for each tag?
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------


