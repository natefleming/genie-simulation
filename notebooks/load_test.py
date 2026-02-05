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

print("\nConfiguration:")
print("-" * 50)
print(f"Space ID: {config.space_id}")
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
    space_id=config.space_id,
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary Statistics

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
    # Show last few rows of history
    display(results.stats_history_df.tail(20))
else:
    print("No history available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recover Results (if notebook crashed)
# MAGIC 
# MAGIC If the notebook crashes after the load test completes but before results are displayed,
# MAGIC you can recover the results by running this cell. The CSV files persist on disk.

# COMMAND ----------

# Uncomment and run this cell to recover results after a crash:

# from genie_simulation.notebook_runner import read_results_from_csv, print_results_summary
# 
# # Option 1: Print formatted summary
# print_results_summary("genie_loadtest")
# 
# # Option 2: Get DataFrames for analysis
# results = read_results_from_csv("genie_loadtest")
# display(results.stats_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Detailed Metrics to Unity Catalog
# MAGIC 
# MAGIC This section loads the detailed per-request metrics CSV and saves them to a Unity Catalog table.

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "", "Unity Catalog Name (optional)")
dbutils.widgets.text("uc_schema", "", "Unity Catalog Schema (optional)")
dbutils.widgets.text("uc_table", "genie_detailed_metrics", "Detailed Metrics Table Name")

# COMMAND ----------

import os
from glob import glob
import pandas as pd

uc_catalog = dbutils.widgets.get("uc_catalog")
uc_schema = dbutils.widgets.get("uc_schema")
uc_table = dbutils.widgets.get("uc_table")

# Find the most recent detailed metrics CSV file
csv_files = sorted(glob("results/genie_detailed_metrics_*.csv"))
if csv_files:
    # Use the most recent file (last in sorted list by timestamp in filename)
    csv_path = csv_files[-1]
    
    # Load the CSV
    detailed_metrics_pdf = pd.read_csv(csv_path)
    print(f"Loaded {len(detailed_metrics_pdf)} records from {csv_path}")
    
    # Display sample
    display(detailed_metrics_pdf.head(10))
    
    # Save to Unity Catalog if configured
    if uc_catalog and uc_schema:
        table_name = f"{uc_catalog}.{uc_schema}.{uc_table}"
        detailed_metrics_df = spark.createDataFrame(detailed_metrics_pdf)
        detailed_metrics_df.write.mode("append").saveAsTable(table_name)
        print(f"\nAppended {len(detailed_metrics_pdf)} records to {table_name}")
    else:
        print("\nSkipping Unity Catalog export (catalog/schema not configured)")
else:
    print("No detailed metrics files found in results/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up

# COMMAND ----------

from genie_simulation.notebook_runner import cleanup_csv_files
from glob import glob
import os

cleanup_csv_files("genie_loadtest")

# Clean up all detailed metrics CSV files
for csv_file in glob("results/genie_detailed_metrics_*.csv"):
    os.remove(csv_file)
    print(f"Removed {csv_file}")
