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

dbutils.widgets.text("space_id", "", "Genie Space ID (leave blank for .env value)")
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

from genie_simulation.config import load_config, LoadTestConfig

# Load environment variables from .env
load_config("../.env")

# Create config from environment
config = LoadTestConfig.from_env()

# Use widget value if changed from default, otherwise use .env value
widget_conversations_file = dbutils.widgets.get("conversations_file")
conversations_file = (
    widget_conversations_file 
    if widget_conversations_file != "../conversations.yaml" 
    else config.conversations_file
)

# Use widget space_id if provided, otherwise use .env value
widget_space_id = dbutils.widgets.get("space_id")
space_id = widget_space_id if widget_space_id else config.space_id

print("\nConfiguration:")
print("-" * 50)
print(f"Space ID: {space_id}")
print(f"Conversations File: {conversations_file}")
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

from genie_simulation.notebook_runner import run_load_test

# Get auth credentials from notebook context to pass to subprocess
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
databricks_host = ctx.apiUrl().get()
databricks_token = ctx.apiToken().get()

results = run_load_test(
    conversations_file=conversations_file,
    space_id=space_id,
    user_count=user_count,
    spawn_rate=spawn_rate,
    run_time=run_time,
    min_wait=config.min_wait,
    max_wait=config.max_wait,
    sample_size=config.sample_size,
    sample_seed=config.sample_seed,
    csv_prefix="genie_loadtest",
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

