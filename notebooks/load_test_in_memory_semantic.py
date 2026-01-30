# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Load Test (In-Memory Semantic Cache)
# MAGIC 
# MAGIC This notebook runs load tests against a Genie space **with in-memory semantic caching** 
# MAGIC to measure latency improvements from semantic similarity matching without requiring 
# MAGIC PostgreSQL or Databricks Lakebase.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC 1. **Configuration**: Ensure `.env` file exists in the parent directory with your settings
# MAGIC 2. **Conversations File**: Run the **export_conversations** notebook to export conversations
# MAGIC 
# MAGIC ## Configuration
# MAGIC 
# MAGIC Most settings are loaded from `.env`. Only runtime parameters use widgets.
# MAGIC 
# MAGIC ## Key Differences from Standard Cached Version
# MAGIC 
# MAGIC - **No Database Required**: Cache is stored in memory, no PostgreSQL/Lakebase setup needed
# MAGIC - **Simpler Setup**: Only requires warehouse ID, no client credentials
# MAGIC - **Single Instance**: Cache is not shared across processes (perfect for single-node testing)
# MAGIC - **LRU Eviction**: Built-in capacity limits with automatic eviction
# MAGIC 
# MAGIC ## Related Notebooks
# MAGIC 
# MAGIC - **export_conversations**: Export conversations from a Genie space
# MAGIC - **load_test**: Load test without caching (baseline)
# MAGIC - **load_test_cached**: Load test with PostgreSQL semantic cache

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

from genie_simulation.config import load_config, LoadTestConfig, InMemoryCacheConfig

# Load environment variables from .env
load_config("../.env")

# Create configs from environment
config = LoadTestConfig.from_env()
cache_config = InMemoryCacheConfig.from_env(dbutils=dbutils)

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

print("\nIn-Memory Semantic Cache Settings:")
print("-" * 60)
print(f"Warehouse ID: {cache_config.warehouse_id}")
print(f"Cache TTL: {cache_config.cache_ttl}s ({cache_config.cache_ttl // 3600}h)")
print(f"Similarity Threshold: {cache_config.similarity_threshold}")
print(f"Context Similarity Threshold: {cache_config.context_similarity_threshold}")
print(f"Cache Capacity: {cache_config.capacity} entries")
print(f"Context Window Size: {cache_config.context_window_size} turns")
print(f"Embedding Model: {cache_config.embedding_model}")
print(f"LRU Capacity: {cache_config.lru_capacity if cache_config.lru_capacity > 0 else 'disabled'}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run In-Memory Semantic Cached Load Test

# COMMAND ----------

from genie_simulation.notebook_runner import run_in_memory_semantic_load_test

# Get auth credentials from notebook context to pass to subprocess
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
databricks_host = ctx.apiUrl().get()
databricks_token = ctx.apiToken().get()

results = run_in_memory_semantic_load_test(
    conversations_file=conversations_file,
    space_id=config.space_id,
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
    context_similarity_threshold=cache_config.context_similarity_threshold,
    capacity=cache_config.capacity,
    context_window_size=cache_config.context_window_size,
    embedding_model=cache_config.embedding_model,
    lru_capacity=cache_config.lru_capacity,
    csv_prefix="genie_in_memory_semantic_loadtest",
    verbose=True,
    databricks_host=databricks_host,
    databricks_token=databricks_token,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary Statistics
# MAGIC 
# MAGIC Look for request types:
# MAGIC - **GENIE_LRU_HIT**: Served from L1 LRU cache (fastest, ~1-10ms)
# MAGIC - **GENIE_SEMANTIC_HIT**: Served from in-memory semantic cache (~100-500ms)
# MAGIC - **GENIE_LIVE**: Cache miss, fetched from Genie API (~5-30s)

# COMMAND ----------

if not results.stats_df.empty:
    display(results.stats_df)
else:
    print("No stats available")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Failures

# COMMAND ----------

if not results.failures_df.empty:
    display(results.failures_df)
else:
    print("No failures recorded")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exceptions

# COMMAND ----------

if not results.exceptions_df.empty:
    display(results.exceptions_df)
else:
    print("No exceptions recorded")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Response Time History

# COMMAND ----------

if not results.stats_history_df.empty:
    display(results.stats_history_df.tail(20))
else:
    print("No history available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up

# COMMAND ----------

from genie_simulation.notebook_runner import cleanup_csv_files

cleanup_csv_files("genie_in_memory_semantic_loadtest")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparing Results
# MAGIC 
# MAGIC To compare different caching approaches:
# MAGIC 
# MAGIC 1. Run the **load_test** notebook (no caching) to get baseline metrics
# MAGIC 2. Run this **load_test_in_memory_semantic** notebook
# MAGIC 3. Run the **load_test_cached** notebook (PostgreSQL semantic cache)
# MAGIC 4. Compare:
# MAGIC    - Average latency reduction
# MAGIC    - P90/P95 latency improvement  
# MAGIC    - Cache hit rates (higher is better)
# MAGIC    - Throughput improvement
# MAGIC    - Setup complexity (in-memory is simpler than PostgreSQL)
# MAGIC    - Memory usage (in-memory uses more RAM than PostgreSQL)
# MAGIC 
# MAGIC ## Advantages of In-Memory Semantic Cache
# MAGIC 
# MAGIC - **No Database Setup**: No PostgreSQL or Lakebase configuration required
# MAGIC - **Faster Setup**: Just need warehouse ID, no credentials to manage
# MAGIC - **Good for Testing**: Perfect for development and single-node testing
# MAGIC - **Lower Latency**: No network calls to external database
# MAGIC 
# MAGIC ## Limitations
# MAGIC 
# MAGIC - **No Persistence**: Cache is lost when process restarts
# MAGIC - **Single Instance**: Cache is not shared across multiple processes
# MAGIC - **Memory Usage**: Stores all cache entries in RAM
# MAGIC - **Capacity Limits**: Must configure appropriate capacity for available memory
