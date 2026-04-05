# Databricks notebook source
movie_table = "frantzpaul_tech.movielens.movies"
ratings_table = "frantzpaul_tech.movielens.ratings"
ratings_large_table = "frantzpaul_tech.movielens.ratings_large"

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

import time
from pyspark.sql.functions import broadcast

movies_df = spark.table(movie_table)
ratings_large_df = spark.read.table(ratings_large_table)

NUM_ITERATIONS = 3

# --- Warm-up: read data into memory once ---
movies_df.count()
ratings_large_df.count()

# --- Sort-Merge Join (broadcast disabled) ---
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

smj_times = []
for i in range(NUM_ITERATIONS):
    start = time.time()
    (
        ratings_large_df.join(movies_df, on="movieId", how="inner")
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
        ratings_large_df.join(broadcast(movies_df), on="movieId", how="inner")
        .select("movieId", "rating", "title")
        .count()  # force full materialization
    )
    bhj_times.append(time.time() - start)

print(f"Sort-Merge Join  — times: {[f'{t:.2f}s' for t in smj_times]}, avg: {sum(smj_times)/len(smj_times):.2f}s")
print(f"Broadcast Join   — times: {[f'{t:.2f}s' for t in bhj_times]}, avg: {sum(bhj_times)/len(bhj_times):.2f}s")
print(f"Speedup: {sum(smj_times)/sum(bhj_times):.1f}x")

# COMMAND ----------

# DBTITLE 1,Salting explanation
# MAGIC %md
# MAGIC ## Experiment 1 — Salting for Skewed Joins
# MAGIC If `movieId` is skewed (many ratings concentrated on a few popular movies), SMJ suffers from partition skew — one executor gets far more data than others. **Salting** adds a random suffix to the join key, spreading the load across partitions, then unions the results.

# COMMAND ----------

# DBTITLE 1,Simulate extreme skew — 200x on top 3 movies
import time
import functools
from pyspark.sql.functions import broadcast, col, lit, rand, floor, concat, explode, array

movies_df = spark.table(movie_table)
ratings_large_df = spark.table(ratings_large_table)

# Pick the top-3 most-rated movies and duplicate their ratings 200x
top_movies = (
    ratings_large_df.groupBy("movieId").count()
    .orderBy(col("count").desc())
    .limit(3)
    .select("movieId")
)

skewed_extra = (
    ratings_large_df.join(top_movies, on="movieId", how="inner")
    .withColumn("_copies", explode(array(*[lit(i) for i in range(200)])))
    .drop("_copies")
)

skewed_ratings = ratings_large_df.unionByName(skewed_extra)
skewed_ratings.cache()
skewed_ratings.count()  # materialise cache before benchmarks

print(f"Original rows:  {ratings_large_df.count():,}")
print(f"Skewed rows:    {skewed_ratings.count():,}")

# COMMAND ----------

# DBTITLE 1,Salting experiment — skewed join keys
NUM_ITERATIONS = 3
NUM_SALT_BUCKETS = 10

# Disable AQE skew handling so Spark can't auto-compensate
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", False)

# Step 1: Plain SMJ on skewed data (broadcast disabled)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

skew_smj_times = []
for i in range(NUM_ITERATIONS):
    start = time.time()
    (
        skewed_ratings.join(movies_df, on="movieId", how="inner")
        .select("movieId", "rating", "title")
        .count()
    )
    skew_smj_times.append(time.time() - start)

# Step 2: Salted SMJ
# Add a random salt bucket to the fact table and replicate the dimension
salted_ratings = skewed_ratings.withColumn("salt", floor(rand() * NUM_SALT_BUCKETS).cast("int"))
salted_ratings = salted_ratings.withColumn(
    "salted_key", concat(col("movieId"), lit("_"), col("salt"))
)

# Replicate the dimension table across all salt buckets
movies_salted = (
    movies_df
    .withColumn("salt", explode(array(*[lit(i) for i in range(NUM_SALT_BUCKETS)])))
    .withColumn("salted_key", concat(col("movieId"), lit("_"), col("salt")))
)

salt_smj_times = []
for i in range(NUM_ITERATIONS):
    start = time.time()
    (
        salted_ratings.join(movies_salted, on="salted_key", how="inner")
        .select(salted_ratings.movieId.alias("movieId"), "rating", "title")
        .count()
    )
    salt_smj_times.append(time.time() - start)

# Step 3: BHJ baseline on skewed data
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

skew_bhj_times = []
for i in range(NUM_ITERATIONS):
    start = time.time()
    (
        skewed_ratings.join(broadcast(movies_df), on="movieId", how="inner")
        .select("movieId", "rating", "title")
        .count()
    )
    skew_bhj_times.append(time.time() - start)

# Re-enable AQE skew handling for subsequent experiments
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)

# Clean up cache
skewed_ratings.unpersist()

avg = lambda t: sum(t) / len(t)
print(f"\n{'Strategy':<25} {'Avg Time':>10}  {'Times'}")
print("-" * 70)
print(f"{'SMJ (skewed)':<25} {avg(skew_smj_times):>8.2f}s  {[f'{t:.2f}s' for t in skew_smj_times]}")
print(f"{'SMJ + Salting':<25} {avg(salt_smj_times):>8.2f}s  {[f'{t:.2f}s' for t in salt_smj_times]}")
print(f"{'BHJ (baseline)':<25} {avg(skew_bhj_times):>8.2f}s  {[f'{t:.2f}s' for t in skew_bhj_times]}")
print(f"\nSalting speedup over plain SMJ: {avg(skew_smj_times)/avg(salt_smj_times):.1f}x")

# COMMAND ----------

# DBTITLE 1,Cache experiment explanation
# MAGIC %md
# MAGIC ## Experiment 2 — Cache the Fact Table
# MAGIC How much of SMJ’s cost is **shuffle** vs **I/O** (reading from storage)? By caching the large fact table in memory first, we remove the I/O component, isolating the pure shuffle overhead.

# COMMAND ----------

# DBTITLE 1,Cache experiment — isolating shuffle vs I/O
import time
from pyspark.sql.functions import broadcast
from pyspark import StorageLevel

movies_df = spark.table(movie_table)
ratings_large_df = spark.table(ratings_large_table)

NUM_ITERATIONS = 3

# Baseline: SMJ without cache
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

nocache_times = []
for i in range(NUM_ITERATIONS):
    start = time.time()
    (
        ratings_large_df.join(movies_df, on="movieId", how="inner")
        .select("movieId", "rating", "title")
        .count()
    )
    nocache_times.append(time.time() - start)

# Cache the fact table, then re-run SMJ
ratings_large_df.persist(StorageLevel.MEMORY_AND_DISK)
ratings_large_df.count()  # materialise the cache

cache_times = []
for i in range(NUM_ITERATIONS):
    start = time.time()
    (
        ratings_large_df.join(movies_df, on="movieId", how="inner")
        .select("movieId", "rating", "title")
        .count()
    )
    cache_times.append(time.time() - start)

# Clean up cache
ratings_large_df.unpersist()

avg = lambda t: sum(t) / len(t)
io_pct = (1 - avg(cache_times) / avg(nocache_times)) * 100

print(f"{'Strategy':<25} {'Avg Time':>10}  {'Times'}")
print("-" * 70)
print(f"{'SMJ (no cache)':<25} {avg(nocache_times):>8.2f}s  {[f'{t:.2f}s' for t in nocache_times]}")
print(f"{'SMJ (cached fact)':<25} {avg(cache_times):>8.2f}s  {[f'{t:.2f}s' for t in cache_times]}")
print(f"\nCaching removed ~{io_pct:.0f}% of SMJ time (the I/O portion).")
print(f"Remaining {100-io_pct:.0f}% is shuffle + compute overhead.")

# COMMAND ----------

# DBTITLE 1,Repartition experiment explanation
# MAGIC %md
# MAGIC ## Experiment 3 — Partition Count Impact on SMJ
# MAGIC SMJ performance depends heavily on how data is distributed across partitions. Too few partitions → large tasks that spill to disk. Too many → scheduling overhead and tiny files. This experiment sweeps different `repartition(N)` values to find the sweet spot.

# COMMAND ----------

# DBTITLE 1,Repartition sweep — SMJ with varying partition counts
import time
from pyspark.sql.functions import broadcast

movies_df = spark.table(movie_table)
ratings_large_df = spark.table(ratings_large_table)

NUM_ITERATIONS = 3
PARTITION_COUNTS = [4, 16, 50, 100, 200]

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

results = {}
for n_parts in PARTITION_COUNTS:
    repartitioned = ratings_large_df.repartition(n_parts)
    # Warm up the repartitioned DF
    repartitioned.count()

    times = []
    for i in range(NUM_ITERATIONS):
        start = time.time()
        (
            repartitioned.join(movies_df, on="movieId", how="inner")
            .select("movieId", "rating", "title")
            .count()
        )
        times.append(time.time() - start)
    results[n_parts] = times

print(f"{'Partitions':<12} {'Avg Time':>10}  {'Times'}")
print("-" * 70)
for n_parts, times in results.items():
    avg_t = sum(times) / len(times)
    print(f"{n_parts:<12} {avg_t:>8.2f}s  {[f'{t:.2f}s' for t in times]}")

best = min(results, key=lambda k: sum(results[k]) / len(results[k]))
worst = max(results, key=lambda k: sum(results[k]) / len(results[k]))
print(f"\nBest: {best} partitions | Worst: {worst} partitions")
print(f"Range: {sum(results[worst])/len(results[worst]) - sum(results[best])/len(results[best]):.2f}s difference")

# COMMAND ----------

# DBTITLE 1,Spark UI DAG reading guide
# MAGIC %md
# MAGIC ## Experiment 4 — Reading the Spark UI DAG
# MAGIC
# MAGIC After running the benchmarks above, open the **Spark UI** (cluster → Spark UI → SQL/DataFrame tab) and compare the DAGs.
# MAGIC
# MAGIC ### What to look for
# MAGIC
# MAGIC | Element | Sort-Merge Join (SMJ) | Broadcast Hash Join (BHJ) |
# MAGIC |---|---|---|
# MAGIC | **Exchange** (shuffle) | Present on **both** sides of the join — this is the expensive network I/O where data is repartitioned by join key | **Absent** on the small (dimension) side; only the large table may have an exchange |
# MAGIC | **Sort** | Both inputs are sorted before merging | No sort needed — the dimension is hashed in memory |
# MAGIC | **BroadcastExchange** | Not present | Present — the small table is serialised and sent to every executor |
# MAGIC | **WholeStageCodegen** | May be split across stages due to shuffle boundaries | Often fits in a single codegen stage |
# MAGIC
# MAGIC ### Tips
# MAGIC * Click on a **SQL query ID** in the SQL tab to see the full DAG.
# MAGIC * Hover over **Exchange** nodes to see shuffle bytes read/written — this quantifies the shuffle cost you isolated in Experiment 2.
# MAGIC * In the **Stages** tab, look for tasks with disproportionately long durations — those indicate the **skew** you addressed in Experiment 1.
# MAGIC * Compare **task count** across partition experiments (Experiment 3) — more partitions = more tasks, but each task processes less data.
