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

dbutils.widgets.text("conversations_file", "../conversations.yaml", "Conversations File Path")
dbutils.widgets.text("user_count", "10", "Number of Concurrent Users")
dbutils.widgets.text("spawn_rate", "2", "User Spawn Rate (per second)")
dbutils.widgets.text("run_time", "5m", "Test Duration (e.g., 5m, 300s)")

# COMMAND ----------

user_count = int(dbutils.widgets.get("user_count"))
spawn_rate = int(dbutils.widgets.get("spawn_rate"))
run_time = dbutils.widgets.get("run_time")

print(f"Runtime Parameters:")
print(f"  Users: {user_count}")
print(f"  Spawn Rate: {spawn_rate}/s")
print(f"  Duration: {run_time}")

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

# Use widget value if changed from default, otherwise use .env value
widget_conversations_file = dbutils.widgets.get("conversations_file")
conversations_file = (
    widget_conversations_file 
    if widget_conversations_file != "../conversations.yaml" 
    else config.conversations_file
)

print("\nConfiguration:")
print("=" * 60)
print("\nGeneral Settings:")
print("-" * 60)
print(f"Space ID: {config.space_id}")
print(f"Conversations File: {conversations_file}")
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

from genie_simulation.notebook_runner import run_cached_load_test

# Get auth credentials from notebook context to pass to subprocess
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
databricks_host = ctx.apiUrl().get()
databricks_token = ctx.apiToken().get()

results = run_cached_load_test(
    conversations_file=conversations_file,
    space_id=config.space_id,
    lakebase_client_id=cache_config.client_id,
    lakebase_client_secret=cache_config.client_secret,
    lakebase_instance=cache_config.lakebase_instance,
    warehouse_id=cache_config.warehouse_id,
    user_count=user_count,
    spawn_rate=spawn_rate,
    run_time=run_time,
    min_wait=config.min_wait,
    max_wait=config.max_wait,
    sample_size=config.sample_size,
    sample_seed=config.sample_seed,
    cache_ttl=cache_config.cache_ttl,
    similarity_threshold=cache_config.similarity_threshold,
    lru_capacity=cache_config.lru_capacity,
    csv_prefix="genie_cached_loadtest",
    verbose=True,
    databricks_host=databricks_host,
    databricks_token=databricks_token,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results
# MAGIC 
# MAGIC Results have been saved to a timestamped directory. Use the **analyze_results** notebook to 
# MAGIC view detailed analysis and visualizations.
# MAGIC 
# MAGIC Look for request types in the results:
# MAGIC - **GENIE_LRU_HIT**: Served from LRU cache (fastest)
# MAGIC - **GENIE_SEMANTIC_HIT**: Served from semantic cache
# MAGIC - **GENIE_LIVE**: Cache miss, fetched from Genie API

# COMMAND ----------

print(f"Results saved to: {results.results_dir}")
print("\nFiles in results directory:")
print(f"  - stats.csv: Locust summary statistics")
print(f"  - stats_history.csv: Time-series throughput data")
print(f"  - failures.csv: Request failures (if any)")
print(f"  - exceptions.csv: Exceptions (if any)")
print(f"  - detailed_metrics.csv: Per-request detailed metrics")
print("\nTo analyze results, run the analyze_results notebook with:")
print(f'  results_dir = "{results.results_dir}"')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparing Results
# MAGIC 
# MAGIC To compare cached vs non-cached performance:
# MAGIC 
# MAGIC 1. Run the **load_test** notebook (without caching) to get baseline metrics
# MAGIC 2. Run this **load_test_cached** notebook with the same settings
# MAGIC 3. Compare:
# MAGIC    - Average latency reduction
# MAGIC    - P90/P95 latency improvement  
# MAGIC    - Cache hit rates (higher is better)
# MAGIC    - Throughput improvement
