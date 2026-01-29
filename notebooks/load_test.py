# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Load Test
# MAGIC 
# MAGIC This notebook runs load tests against a Genie space to measure latency and throughput.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC 1. **Configuration**: Upload a `.env` file to this directory with your settings
# MAGIC 2. **Conversations File**: Run the **export_conversations** notebook to export conversations
# MAGIC 3. **Install Package**: The `genie-simulation` package must be installed on the cluster
# MAGIC 
# MAGIC ## Configuration
# MAGIC 
# MAGIC Most settings are loaded from `.env`. Only runtime parameters use widgets:
# MAGIC - **user_count**: Number of concurrent users
# MAGIC - **spawn_rate**: How fast to spawn users
# MAGIC - **run_time_seconds**: Test duration
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
    LoadTestConfig,
    download_workspace_file,
    get_notebook_directory,
)

# Get the notebook directory
notebook_dir = get_notebook_directory(dbutils)
print(f"Notebook directory: {notebook_dir}")

# Initialize workspace client
client = WorkspaceClient()

# Load configuration from .env
config = LoadTestConfig.from_workspace_env(client, notebook_dir)

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
# MAGIC ## Run Load Test

# COMMAND ----------

from genie_simulation.notebook_runner import GenieLoadTestRunner

# Create the runner
runner = GenieLoadTestRunner(
    conversations_file=local_conversations_path,
    space_id=config.space_id,
    min_wait=config.min_wait,
    max_wait=config.max_wait,
    sample_size=config.sample_size,
    sample_seed=config.sample_seed,
)

print(f"Starting load test with {user_count} users for {run_time_seconds}s...")

# COMMAND ----------

# Run the load test
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up

# COMMAND ----------

import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
print("Temporary files cleaned up")
