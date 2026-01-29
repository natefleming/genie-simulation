# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Load Test (Cached)
# MAGIC 
# MAGIC This notebook runs load tests against a Genie space **with caching enabled** to measure 
# MAGIC latency improvements from LRU and semantic caching.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC 1. **Configuration**: Upload a `.env` file to this directory with your settings
# MAGIC 2. **Credentials**: Set up a Databricks secret scope with Lakebase credentials
# MAGIC 3. **Conversations File**: Run the **export_conversations** notebook to export conversations
# MAGIC 4. **Install Package**: The `genie-simulation` package must be installed on the cluster
# MAGIC 
# MAGIC ## Configuration
# MAGIC 
# MAGIC Most settings are loaded from `.env`. Only runtime parameters use widgets.
# MAGIC 
# MAGIC Cache settings loaded from `.env`:
# MAGIC - `GENIE_LAKEBASE_INSTANCE` - Lakebase instance name
# MAGIC - `GENIE_WAREHOUSE_ID` - Warehouse ID
# MAGIC - `GENIE_CACHE_TTL` - Cache TTL in seconds
# MAGIC - `GENIE_SIMILARITY_THRESHOLD` - Semantic similarity threshold
# MAGIC - `GENIE_LRU_CAPACITY` - LRU cache size
# MAGIC 
# MAGIC ## Credentials
# MAGIC 
# MAGIC Lakebase credentials are loaded from:
# MAGIC 1. Databricks secret scope `genie-loadtest` (preferred)
# MAGIC 2. Fallback to `.env` file
# MAGIC 
# MAGIC ## Related Notebooks
# MAGIC 
# MAGIC - **export_conversations**: Export conversations from a Genie space
# MAGIC - **load_test**: Load test without caching (baseline)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime Configuration (Widgets)

# COMMAND ----------

# Only runtime parameters as widgets - the rest comes from .env
dbutils.widgets.text("user_count", "10", "Number of Concurrent Users")
dbutils.widgets.text("spawn_rate", "2", "User Spawn Rate (per second)")
dbutils.widgets.text("run_time_seconds", "300", "Test Duration (seconds)")

# COMMAND ----------

# Get widget values
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

import os
import tempfile
from pathlib import Path

from databricks.sdk import WorkspaceClient
from genie_simulation.config import (
    CacheConfig,
    LoadTestConfig,
    download_workspace_file,
    get_notebook_directory,
)

# Get the notebook directory
notebook_dir = get_notebook_directory(dbutils)
print(f"Notebook directory: {notebook_dir}")

# Initialize workspace client
client = WorkspaceClient()

# Load base configuration from .env
config = LoadTestConfig.from_workspace_env(client, notebook_dir)

# Load cache configuration from .env (with secret scope for credentials)
cache_config = CacheConfig.from_workspace_env(
    client,
    notebook_dir,
    dbutils=dbutils,
    secret_scope="genie-loadtest",
)

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
# MAGIC ## Download Conversations File

# COMMAND ----------

# Create temp directory
temp_dir = tempfile.mkdtemp()

# Download conversations file from workspace
workspace_conversations_path = os.path.join(notebook_dir, config.conversations_file)
local_conversations_path = download_workspace_file(
    client,
    workspace_conversations_path,
    temp_dir,
)

print(f"Downloaded: {workspace_conversations_path}")
print(f"Local path: {local_conversations_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Cached Load Test

# COMMAND ----------

from genie_simulation.notebook_runner import GenieLoadTestRunner

# Create the runner with cache settings
runner = GenieLoadTestRunner(
    conversations_file=local_conversations_path,
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

# Run the cached load test
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
# MAGIC 
# MAGIC Shows cache hit rates for LRU and semantic caches.

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
# MAGIC 
# MAGIC Shows breakdown by request type:
# MAGIC - **GENIE_LRU_HIT**: Served from LRU cache (fastest)
# MAGIC - **GENIE_SEMANTIC_HIT**: Served from semantic cache
# MAGIC - **GENIE_LIVE**: Cache miss, fetched from Genie API

# COMMAND ----------

if not results.endpoints_df.empty:
    display(results.endpoints_df)
else:
    print("No endpoint data available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up

# COMMAND ----------

import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
print("Temporary files cleaned up")

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
