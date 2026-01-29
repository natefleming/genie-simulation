# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Load Test
# MAGIC 
# MAGIC This notebook runs load tests against a Genie space to measure latency and throughput.
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
# MAGIC ## Related Notebooks
# MAGIC 
# MAGIC - **export_conversations**: Export conversations from a Genie space
# MAGIC - **load_test_cached**: Load test with caching service enabled

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

from genie_simulation.config import load_config, LoadTestConfig

# Load environment variables from .env
load_config("../.env")

# Create config from environment
config = LoadTestConfig.from_env()

print("\nConfiguration loaded from .env:")
print("-" * 50)
print(f"Space ID: {config.space_id}")
print(f"Conversations File: {config.conversations_file}")
print(f"Wait Time: {config.min_wait}s - {config.max_wait}s")
if config.sample_size:
    print(f"Sample Size: {config.sample_size}")
if config.sample_seed:
    print(f"Sample Seed: {config.sample_seed}")
print("-" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Load Test

# COMMAND ----------

from genie_simulation.notebook_runner import GenieLoadTestRunner

runner = GenieLoadTestRunner(
    conversations_file=config.conversations_file,
    space_id=config.space_id,
    min_wait=config.min_wait,
    max_wait=config.max_wait,
    sample_size=config.sample_size,
    sample_seed=config.sample_seed,
)

print(f"Starting load test with {user_count} users for {run_time_seconds}s...")

# COMMAND ----------

results = runner.run(
    user_count=user_count,
    spawn_rate=spawn_rate,
    run_time_seconds=run_time_seconds,
    use_cache=False,
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
