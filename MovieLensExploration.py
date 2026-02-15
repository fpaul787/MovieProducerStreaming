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


