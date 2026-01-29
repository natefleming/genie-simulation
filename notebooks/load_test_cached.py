# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Load Test (Cached)
# MAGIC 
# MAGIC This notebook runs load tests against a Genie space **with caching enabled** to measure 
# MAGIC latency improvements from LRU and semantic caching.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC 1. **Configuration**: Ensure `.env` file exists in the parent directory with your settings
# MAGIC 2. **Credentials**: Set up a Databricks secret scope or add credentials to `.env`
# MAGIC 3. **Conversations File**: Run the **export_conversations** notebook to export conversations
# MAGIC 
# MAGIC ## Configuration
# MAGIC 
# MAGIC Most settings are loaded from `.env`. Only runtime parameters use widgets.
# MAGIC 
# MAGIC ## Related Notebooks
# MAGIC 
# MAGIC - **export_conversations**: Export conversations from a Genie space
# MAGIC - **load_test**: Load test without caching (baseline)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime Configuration (Widgets)

# COMMAND ----------

dbutils.widgets.text("user_count", "10", "Number of Concurrent Users")
dbutils.widgets.text("spawn_rate", "2", "User Spawn Rate (per second)")
dbutils.widgets.text("run_time_seconds", "300", "Test Duration (seconds)")

# COMMAND ----------

user_count = int(dbutils.widgets.get("user_count"))
spawn_rate = float(dbutils.widgets.get("spawn_rate"))
run_time_seconds = int(dbutils.widgets.get("run_time_seconds"))

print(f"Runtime Parameters:")
print(f"  Users: {user_count}")
print(f"  Spawn Rate: {spawn_rate}/s")
print(f"  Duration: {run_time_seconds}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration from .env

# COMMAND ----------

from genie_simulation.config import load_config, LoadTestConfig, CacheConfig

# Load environment variables from .env
load_config("../.env")

# Create configs from environment
config = LoadTestConfig.from_env()
cache_config = CacheConfig.from_env(dbutils=dbutils, secret_scope="genie-loadtest")

print("\nConfiguration loaded from .env:")
print("=" * 60)
print("\nGeneral Settings:")
print("-" * 60)
print(f"Space ID: {config.space_id}")
print(f"Conversations File: {config.conversations_file}")
print(f"Wait Time: {config.min_wait}s - {config.max_wait}s")
if config.sample_size:
    print(f"Sample Size: {config.sample_size}")
if config.sample_seed:
    print(f"Sample Seed: {config.sample_seed}")

print("\nCache Settings:")
print("-" * 60)
print(f"Lakebase Instance: {cache_config.lakebase_instance}")
print(f"Warehouse ID: {cache_config.warehouse_id}")
print(f"Cache TTL: {cache_config.cache_ttl}s ({cache_config.cache_ttl // 3600}h)")
print(f"Similarity Threshold: {cache_config.similarity_threshold}")
print(f"LRU Capacity: {cache_config.lru_capacity}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Cached Load Test

# COMMAND ----------

from genie_simulation.notebook_runner import GenieLoadTestRunner

runner = GenieLoadTestRunner(
    conversations_file=config.conversations_file,
    space_id=config.space_id,
    min_wait=config.min_wait,
    max_wait=config.max_wait,
    sample_size=config.sample_size,
    sample_seed=config.sample_seed,
    # Cache settings
    lakebase_client_id=cache_config.client_id,
    lakebase_client_secret=cache_config.client_secret,
    lakebase_instance=cache_config.lakebase_instance,
    warehouse_id=cache_config.warehouse_id,
    cache_ttl=cache_config.cache_ttl,
    similarity_threshold=cache_config.similarity_threshold,
    lru_capacity=cache_config.lru_capacity,
)

print(f"Starting cached load test with {user_count} users for {run_time_seconds}s...")

# COMMAND ----------

results = runner.run(
    user_count=user_count,
    spawn_rate=spawn_rate,
    run_time_seconds=run_time_seconds,
    use_cache=True,
)

print("Cached load test completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary

# COMMAND ----------

display(results.summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cache Metrics

# COMMAND ----------

if results.cache_metrics_df is not None:
    display(results.cache_metrics_df)
else:
    print("Cache metrics not available")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Latency Percentiles

# COMMAND ----------

display(results.percentiles_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Per-Endpoint Breakdown

# COMMAND ----------

if not results.endpoints_df.empty:
    display(results.endpoints_df)
else:
    print("No endpoint data available")
