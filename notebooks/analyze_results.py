# Databricks notebook source
# MAGIC %md
# MAGIC # Load Test Results Analysis
# MAGIC
# MAGIC This notebook analyzes the results of a Genie load test. It loads CSV files from a
# MAGIC results directory and provides summary statistics and visualizations.
# MAGIC
# MAGIC ## Usage
# MAGIC 1. Run a load test to generate results
# MAGIC 2. Enter the results directory path in the widget below
# MAGIC 3. Run all cells to see the analysis

# COMMAND ----------

# MAGIC %pip install pandas matplotlib --quiet --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
from glob import glob
from pathlib import Path

import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# List available results directories
results_base = "results"
available_dirs = sorted(glob(f"{results_base}/*"), key=lambda d: Path(d).stat().st_mtime, reverse=True)

# Show available directories
print("Available results directories (newest first):")
for i, d in enumerate(available_dirs[:10]):
    print(f"  {i+1}. {d}")

# COMMAND ----------

# Widget to select results directory
# Default to most recent if available
default_dir = available_dirs[0] if available_dirs else ""

dbutils.widgets.text("results_dir", default_dir, "Results Directory")

# COMMAND ----------

results_dir = dbutils.widgets.get("results_dir")
print(f"Analyzing results from: {results_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# Load all CSV files from the results directory
stats_path = Path(results_dir) / "stats.csv"
history_path = Path(results_dir) / "stats_history.csv"
failures_path = Path(results_dir) / "failures.csv"
exceptions_path = Path(results_dir) / "exceptions.csv"
detailed_metrics_path = Path(results_dir) / "detailed_metrics.csv"

# Load DataFrames
stats_df = pd.read_csv(stats_path) if stats_path.exists() else pd.DataFrame()
history_df = pd.read_csv(history_path) if history_path.exists() else pd.DataFrame()
failures_df = pd.read_csv(failures_path) if failures_path.exists() else pd.DataFrame()
exceptions_df = pd.read_csv(exceptions_path) if exceptions_path.exists() else pd.DataFrame()
detailed_metrics_df = pd.read_csv(detailed_metrics_path) if detailed_metrics_path.exists() else pd.DataFrame()

print(f"Loaded files:")
print(f"  - stats.csv: {len(stats_df)} rows")
print(f"  - stats_history.csv: {len(history_df)} rows")
print(f"  - failures.csv: {len(failures_df)} rows")
print(f"  - exceptions.csv: {len(exceptions_df)} rows")
print(f"  - detailed_metrics.csv: {len(detailed_metrics_df)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

if not stats_df.empty:
    print("=" * 70)
    print("LOCUST STATS SUMMARY")
    print("=" * 70)
    
    # Get aggregated row
    agg_row = stats_df[stats_df["Name"] == "Aggregated"]
    if agg_row.empty:
        agg_row = stats_df.iloc[[-1]]
    
    for _, row in agg_row.iterrows():
        print(f"\n{'Metric':<35} {'Value':>20}")
        print("-" * 55)
        print(f"{'Total Requests':<35} {row.get('Request Count', 'N/A'):>20}")
        print(f"{'Failed Requests':<35} {row.get('Failure Count', 'N/A'):>20}")
        
        # Calculate failure rate
        total = row.get('Request Count', 0)
        failed = row.get('Failure Count', 0)
        if total > 0:
            fail_rate = (failed / total) * 100
            print(f"{'Failure Rate':<35} {f'{fail_rate:.1f}%':>20}")
        
        print(f"{'Requests/s':<35} {row.get('Requests/s', 'N/A'):>20}")
        print("-" * 55)
        
        # Latency metrics
        print("LATENCY (in seconds):")
        avg_ms = row.get('Average Response Time', 0)
        median_ms = row.get('Median Response Time', 0)
        min_ms = row.get('Min Response Time', 0)
        max_ms = row.get('Max Response Time', 0)
        
        print(f"{'  Average':<35} {avg_ms / 1000:>19.2f}s")
        print(f"{'  Median (P50)':<35} {median_ms / 1000:>19.2f}s")
        print(f"{'  Min':<35} {min_ms / 1000:>19.2f}s")
        print(f"{'  Max':<35} {max_ms / 1000:>19.2f}s")
        
        # Percentiles
        for pct in ['50%', '66%', '75%', '80%', '90%', '95%', '99%']:
            if pct in row and pd.notna(row[pct]):
                print(f"{'  P' + pct.replace('%', ''):<35} {row[pct] / 1000:>19.2f}s")
else:
    print("No stats data available")

# COMMAND ----------

# Display stats table
if not stats_df.empty:
    display(stats_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Metrics Analysis

# COMMAND ----------

if not detailed_metrics_df.empty:
    print("=" * 70)
    print("DETAILED METRICS SUMMARY")
    print("=" * 70)
    
    # Basic stats
    total = len(detailed_metrics_df)
    successful = detailed_metrics_df['success'].sum()
    failed = total - successful
    success_rate = (successful / total) * 100 if total > 0 else 0
    
    print(f"\n{'Metric':<35} {'Value':>20}")
    print("-" * 55)
    print(f"{'Total Requests':<35} {total:>20}")
    print(f"{'Successful':<35} {successful:>20}")
    print(f"{'Failed':<35} {failed:>20}")
    print(f"{'Success Rate':<35} {f'{success_rate:.1f}%':>20}")
    
    # Duration stats
    print("\nDURATION STATISTICS (in seconds):")
    duration_secs = detailed_metrics_df['duration_ms'] / 1000
    print(f"{'  Mean':<35} {duration_secs.mean():>19.2f}s")
    print(f"{'  Median':<35} {duration_secs.median():>19.2f}s")
    print(f"{'  Std Dev':<35} {duration_secs.std():>19.2f}s")
    print(f"{'  Min':<35} {duration_secs.min():>19.2f}s")
    print(f"{'  Max':<35} {duration_secs.max():>19.2f}s")
    print(f"{'  P90':<35} {duration_secs.quantile(0.90):>19.2f}s")
    print(f"{'  P95':<35} {duration_secs.quantile(0.95):>19.2f}s")
    print(f"{'  P99':<35} {duration_secs.quantile(0.99):>19.2f}s")
    
    # Concurrent users
    if 'concurrent_users' in detailed_metrics_df.columns:
        users = detailed_metrics_df['concurrent_users'].iloc[0]
        print(f"\n{'Concurrent Users':<35} {users:>20}")
else:
    print("No detailed metrics data available")

# COMMAND ----------

# Display detailed metrics
if not detailed_metrics_df.empty:
    display(detailed_metrics_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-User Analysis

# COMMAND ----------

if not detailed_metrics_df.empty and 'user' in detailed_metrics_df.columns:
    user_stats = detailed_metrics_df.groupby('user').agg({
        'duration_ms': ['count', 'mean', 'median', 'std'],
        'success': 'sum'
    }).round(2)
    
    user_stats.columns = ['requests', 'avg_duration_ms', 'median_duration_ms', 'std_duration_ms', 'successful']
    user_stats['success_rate'] = (user_stats['successful'] / user_stats['requests'] * 100).round(1)
    
    print("Per-User Statistics:")
    display(user_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Response Time Distribution

# COMMAND ----------

import matplotlib.pyplot as plt

if not detailed_metrics_df.empty:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Histogram
    ax1 = axes[0]
    duration_secs = detailed_metrics_df['duration_ms'] / 1000
    ax1.hist(duration_secs, bins=30, edgecolor='black', alpha=0.7)
    ax1.set_xlabel('Response Time (seconds)')
    ax1.set_ylabel('Frequency')
    ax1.set_title('Response Time Distribution')
    ax1.axvline(duration_secs.median(), color='red', linestyle='--', label=f'Median: {duration_secs.median():.2f}s')
    ax1.axvline(duration_secs.mean(), color='green', linestyle='--', label=f'Mean: {duration_secs.mean():.2f}s')
    ax1.legend()
    
    # Box plot
    ax2 = axes[1]
    ax2.boxplot(duration_secs)
    ax2.set_ylabel('Response Time (seconds)')
    ax2.set_title('Response Time Box Plot')
    
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Throughput Over Time

# COMMAND ----------

if not history_df.empty and 'Timestamp' in history_df.columns:
    fig, ax = plt.subplots(figsize=(14, 5))
    
    # Convert timestamp if needed
    history_df['Timestamp'] = pd.to_datetime(history_df['Timestamp'], unit='s')
    
    # Plot requests per second
    if 'Requests/s' in history_df.columns:
        ax.plot(history_df['Timestamp'], history_df['Requests/s'], label='Requests/s', color='blue')
    
    ax.set_xlabel('Time')
    ax.set_ylabel('Requests per Second')
    ax.set_title('Throughput Over Time')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failures and Exceptions

# COMMAND ----------

if not failures_df.empty:
    print("FAILURES:")
    print("-" * 70)
    display(failures_df)
else:
    print("No failures recorded")

# COMMAND ----------

if not exceptions_df.empty:
    print("EXCEPTIONS:")
    print("-" * 70)
    display(exceptions_df)
else:
    print("No exceptions recorded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export to Unity Catalog

# COMMAND ----------

# Optional: Export detailed metrics to Unity Catalog
# uc_catalog = "your_catalog"
# uc_schema = "your_schema"
# 
# if not detailed_metrics_df.empty:
#     spark_df = spark.createDataFrame(detailed_metrics_df)
#     table_name = f"{uc_catalog}.{uc_schema}.genie_detailed_metrics"
#     spark_df.write.mode("append").saveAsTable(table_name)
#     print(f"Exported to {table_name}")
