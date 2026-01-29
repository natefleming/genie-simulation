# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Load Test
# MAGIC 
# MAGIC This notebook runs load tests against a Genie space to measure latency and throughput.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC 1. **Conversations File**: Export conversations using `export-conversations --space-id <SPACE_ID>`
# MAGIC 2. **Upload to DBFS**: Upload the conversations.yaml file to DBFS (e.g., `/dbfs/FileStore/genie/conversations.yaml`)
# MAGIC 3. **Install Package**: The `genie-simulation` package must be installed on the cluster
# MAGIC 
# MAGIC ## Configuration
# MAGIC 
# MAGIC Use the widgets above to configure the load test parameters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Create widgets for configuration
dbutils.widgets.text("conversations_file", "/dbfs/FileStore/genie/conversations.yaml", "Conversations File Path")
dbutils.widgets.text("space_id", "", "Space ID (optional, uses file's if empty)")
dbutils.widgets.text("user_count", "10", "Number of Concurrent Users")
dbutils.widgets.text("spawn_rate", "2", "User Spawn Rate (per second)")
dbutils.widgets.text("run_time_seconds", "300", "Test Duration (seconds)")
dbutils.widgets.text("min_wait", "8", "Min Wait Between Messages (seconds)")
dbutils.widgets.text("max_wait", "30", "Max Wait Between Messages (seconds)")
dbutils.widgets.text("sample_size", "", "Sample Size (empty = use all)")
dbutils.widgets.text("sample_seed", "", "Sample Seed (for reproducibility)")
dbutils.widgets.dropdown("use_cache", "false", ["true", "false"], "Use Cache Service")

# Cache-specific widgets (only used when use_cache=true)
dbutils.widgets.text("lakebase_instance", "", "Lakebase Instance")
dbutils.widgets.text("warehouse_id", "", "Warehouse ID")
dbutils.widgets.text("cache_ttl", "86400", "Cache TTL (seconds)")
dbutils.widgets.text("similarity_threshold", "0.85", "Similarity Threshold")
dbutils.widgets.text("lru_capacity", "100", "LRU Cache Capacity")

# COMMAND ----------

# Get widget values
conversations_file = dbutils.widgets.get("conversations_file")
space_id = dbutils.widgets.get("space_id") or None
user_count = int(dbutils.widgets.get("user_count"))
spawn_rate = float(dbutils.widgets.get("spawn_rate"))
run_time_seconds = int(dbutils.widgets.get("run_time_seconds"))
min_wait = float(dbutils.widgets.get("min_wait"))
max_wait = float(dbutils.widgets.get("max_wait"))
sample_size_str = dbutils.widgets.get("sample_size")
sample_size = int(sample_size_str) if sample_size_str else None
sample_seed_str = dbutils.widgets.get("sample_seed")
sample_seed = int(sample_seed_str) if sample_seed_str else None
use_cache = dbutils.widgets.get("use_cache") == "true"

# Cache settings
lakebase_instance = dbutils.widgets.get("lakebase_instance") or None
warehouse_id = dbutils.widgets.get("warehouse_id") or None
cache_ttl = int(dbutils.widgets.get("cache_ttl"))
similarity_threshold = float(dbutils.widgets.get("similarity_threshold"))
lru_capacity = int(dbutils.widgets.get("lru_capacity"))

# Display configuration
print("Load Test Configuration:")
print("-" * 50)
print(f"Conversations File: {conversations_file}")
print(f"Space ID: {space_id or '(from file)'}")
print(f"Users: {user_count}")
print(f"Spawn Rate: {spawn_rate}/s")
print(f"Duration: {run_time_seconds}s")
print(f"Wait Time: {min_wait}s - {max_wait}s")
if sample_size:
    print(f"Sample Size: {sample_size}")
if sample_seed:
    print(f"Sample Seed: {sample_seed}")
print(f"Use Cache: {use_cache}")
if use_cache:
    print(f"  Lakebase Instance: {lakebase_instance}")
    print(f"  Warehouse ID: {warehouse_id}")
    print(f"  Cache TTL: {cache_ttl}s")
    print(f"  Similarity Threshold: {similarity_threshold}")
    print(f"  LRU Capacity: {lru_capacity}")
print("-" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Configuration

# COMMAND ----------

import os
from pathlib import Path

# Validate conversations file exists
conversations_path = Path(conversations_file)
if not conversations_path.exists():
    raise FileNotFoundError(
        f"Conversations file not found: {conversations_file}\n"
        "Please upload your conversations.yaml file to DBFS first."
    )

# For cache mode, check for required secrets
if use_cache:
    # Try to get secrets from Databricks secret scope
    try:
        lakebase_client_id = dbutils.secrets.get(scope="genie-loadtest", key="lakebase-client-id")
        lakebase_client_secret = dbutils.secrets.get(scope="genie-loadtest", key="lakebase-client-secret")
    except Exception:
        # Fall back to environment variables
        lakebase_client_id = os.environ.get("GENIE_LAKEBASE_CLIENT_ID")
        lakebase_client_secret = os.environ.get("GENIE_LAKEBASE_CLIENT_SECRET")
    
    if not lakebase_client_id or not lakebase_client_secret:
        raise ValueError(
            "Cache mode requires Lakebase credentials. Either:\n"
            "1. Create a Databricks secret scope 'genie-loadtest' with keys 'lakebase-client-id' and 'lakebase-client-secret'\n"
            "2. Set environment variables GENIE_LAKEBASE_CLIENT_ID and GENIE_LAKEBASE_CLIENT_SECRET"
        )
    
    if not lakebase_instance:
        raise ValueError("Lakebase Instance is required for cache mode")
    if not warehouse_id:
        raise ValueError("Warehouse ID is required for cache mode")
else:
    lakebase_client_id = None
    lakebase_client_secret = None

print("Configuration validated successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Load Test

# COMMAND ----------

from genie_simulation.notebook_runner import GenieLoadTestRunner

# Create the runner
runner = GenieLoadTestRunner(
    conversations_file=conversations_file,
    space_id=space_id,
    min_wait=min_wait,
    max_wait=max_wait,
    sample_size=sample_size,
    sample_seed=sample_seed,
    # Cache settings
    lakebase_client_id=lakebase_client_id,
    lakebase_client_secret=lakebase_client_secret,
    lakebase_instance=lakebase_instance,
    warehouse_id=warehouse_id,
    cache_ttl=cache_ttl,
    similarity_threshold=similarity_threshold,
    lru_capacity=lru_capacity,
)

print(f"Runner initialized. Starting load test with {user_count} users for {run_time_seconds}s...")

# COMMAND ----------

# Run the load test
results = runner.run(
    user_count=user_count,
    spawn_rate=spawn_rate,
    run_time_seconds=run_time_seconds,
    use_cache=use_cache,
)

print("Load test completed!")

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cache Metrics (if cache mode enabled)

# COMMAND ----------

if results.cache_metrics_df is not None:
    display(results.cache_metrics_df)
else:
    if use_cache:
        print("Cache metrics not available")
    else:
        print("Cache mode was not enabled for this test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Test (Optional)
# MAGIC 
# MAGIC Uncomment and run the cell below to run a quick validation test with a single user.

# COMMAND ----------

# # Quick test - uncomment to run
# quick_results = runner.run_quick_test(
#     user_count=1,
#     messages=3,
#     use_cache=use_cache,
# )
# display(quick_results.summary_df)
