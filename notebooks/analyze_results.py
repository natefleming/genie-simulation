# Databricks notebook source
# MAGIC %md
# MAGIC # Load Test Results Analysis
# MAGIC
# MAGIC This notebook analyzes the results of a Genie load test. It loads CSV files from a
# MAGIC results directory and provides summary statistics and visualizations.
# MAGIC
# MAGIC ## Usage
# MAGIC 1. Run a load test to generate results (e.g., `load_test.py`)
# MAGIC 2. Enter the results directory path in the widget below
# MAGIC 3. Run all cells to see the analysis
# MAGIC
# MAGIC ## Analysis Sections
# MAGIC 
# MAGIC | Section | Purpose |
# MAGIC |---------|---------|
# MAGIC | **Summary Statistics** | Overall health metrics: total requests, failures, latency percentiles |
# MAGIC | **Detailed Metrics** | Raw per-request data for drill-down analysis |
# MAGIC | **Per-User Analysis** | Performance comparison across simulated users |
# MAGIC | **Response Time Distribution** | Histogram and box plot showing latency spread |
# MAGIC | **Response Time Over Duration** | Temporal analysis - how latency changes during the test |
# MAGIC | **Prompt Latency Across Executions** | Caching effectiveness - first vs repeated query performance |
# MAGIC | **Performance by Unique Prompt** | Identify slowest/fastest specific queries |
# MAGIC | **Throughput Over Time** | Requests/second sustainability over the test |
# MAGIC | **Failures and Exceptions** | Error analysis and root cause identification |

# COMMAND ----------

# MAGIC %pip install pandas matplotlib python-dotenv --quiet --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
from glob import glob
from importlib.metadata import version
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
# Try multiple paths since CWD varies between local and Databricks environments
load_dotenv("../.env")
load_dotenv(".env")

print(f"dao-ai version: {version('dao-ai')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# List available results directories
# Use relative path which works correctly in Databricks notebooks
results_base = "results"
print(f"Current working directory: {os.getcwd()}")
print(f"Results base: {results_base}")
print(f"Results directory exists: {Path(results_base).exists()}")

# Find all result directories using relative path
available_dirs = []
if Path(results_base).exists():
    available_dirs = sorted(
        glob(f"{results_base}/*"), 
        key=lambda d: Path(d).stat().st_mtime if Path(d).exists() else 0, 
        reverse=True
    )

# Show available directories
print(f"\nFound {len(available_dirs)} results directories")
print(f"Available results directories (newest first):")
if available_dirs:
    for i, d in enumerate(available_dirs[:10]):
        print(f"  {i+1}. {d}")
else:
    print("  (No results directories found)")

# COMMAND ----------

# Widget to select results directory
# Default to most recent if available
default_dir = available_dirs[0] if available_dirs else ""

dbutils.widgets.text("results_dir", default_dir, "Results Directory")

# Widget for SQL warehouse ID (used for enrichment with query.history and access.audit)
default_warehouse_id = os.environ.get("GENIE_WAREHOUSE_ID", "")
dbutils.widgets.text("warehouse_id", default_warehouse_id, "SQL Warehouse ID (for enrichment)")

# Widgets for system table paths (fully-qualified: catalog.schema.table)
# Override when system tables are exposed via views in a different catalog/schema
default_query_history_table = os.environ.get("SYSTEM_QUERY_HISTORY_TABLE", "system.query.history")
default_access_audit_table = os.environ.get("SYSTEM_ACCESS_AUDIT_TABLE", "system.access.audit")
dbutils.widgets.text("query_history_table", default_query_history_table, "Query History Table")
dbutils.widgets.text("access_audit_table", default_access_audit_table, "Access Audit Table")

# COMMAND ----------

results_dir = dbutils.widgets.get("results_dir")
warehouse_id = dbutils.widgets.get("warehouse_id").strip() or os.environ.get("GENIE_WAREHOUSE_ID", "")
query_history_table = dbutils.widgets.get("query_history_table").strip() or os.environ.get("SYSTEM_QUERY_HISTORY_TABLE", "system.query.history")
access_audit_table = dbutils.widgets.get("access_audit_table").strip() or os.environ.get("SYSTEM_ACCESS_AUDIT_TABLE", "system.access.audit")
print(f"Analyzing results from: {results_dir}")
print(f"SQL Warehouse ID: {warehouse_id if warehouse_id else '(not set - needed for SQL metrics enrichment)'}")
print(f"Query History Table: {query_history_table}")
print(f"Access Audit Table: {access_audit_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# Load all CSV files from the results directory
# Handle both local filesystem paths and Databricks workspace paths

def file_exists_dbx(path: str) -> bool:
    """Check if file exists - works with both local and workspace paths."""
    try:
        # First try local filesystem (for driver local paths)
        if Path(path).exists():
            return True
        # Try dbutils for workspace/DBFS paths
        try:
            dbutils.fs.ls(path)
            return True
        except Exception:
            pass
        return False
    except Exception:
        return False

def read_csv_dbx(path: str) -> pd.DataFrame:
    """Read CSV - handles both local and workspace paths."""
    try:
        # Try local path first
        local_path = Path(path)
        if local_path.exists():
            return pd.read_csv(local_path)
        # Try workspace path (for files in /Workspace)
        # Convert to driver-accessible path
        if path.startswith("/Workspace"):
            driver_path = path.replace("/Workspace", "/Workspace")
            if Path(driver_path).exists():
                return pd.read_csv(driver_path)
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return pd.DataFrame()

# Use relative paths as-is (they work correctly in Databricks)
# Only show debug info
print(f"Current working directory: {os.getcwd()}")
print(f"Looking for files in: {results_dir}")
print(f"Directory exists: {Path(results_dir).exists() if results_dir else 'N/A'}")

if results_dir and Path(results_dir).exists():
    print(f"Directory contents:")
    for f in os.listdir(results_dir):
        print(f"  - {f}")

stats_path = Path(results_dir) / "stats.csv"
history_path = Path(results_dir) / "stats_history.csv"
failures_path = Path(results_dir) / "failures.csv"
exceptions_path = Path(results_dir) / "exceptions.csv"
detailed_metrics_path = Path(results_dir) / "detailed_metrics.csv"

# Debug: Show path existence
print(f"\nFile existence check:")
print(f"  - stats.csv: {stats_path} -> exists={stats_path.exists()}")
print(f"  - detailed_metrics.csv: {detailed_metrics_path} -> exists={detailed_metrics_path.exists()}")

# Load DataFrames
stats_df = pd.read_csv(stats_path) if stats_path.exists() else pd.DataFrame()
history_df = pd.read_csv(history_path) if history_path.exists() else pd.DataFrame()
failures_df = pd.read_csv(failures_path) if failures_path.exists() else pd.DataFrame()
exceptions_df = pd.read_csv(exceptions_path) if exceptions_path.exists() else pd.DataFrame()
detailed_metrics_df = pd.read_csv(detailed_metrics_path) if detailed_metrics_path.exists() else pd.DataFrame()

print(f"\nLoaded files:")
print(f"  - stats.csv: {len(stats_df)} rows")
print(f"  - stats_history.csv: {len(history_df)} rows")
print(f"  - failures.csv: {len(failures_df)} rows")
print(f"  - exceptions.csv: {len(exceptions_df)} rows")
print(f"  - detailed_metrics.csv: {len(detailed_metrics_df)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics
# MAGIC 
# MAGIC **What it shows:** High-level aggregated metrics from Locust including total requests, failures, 
# MAGIC throughput (requests/second), and latency percentiles.
# MAGIC 
# MAGIC **Key metrics to watch:**
# MAGIC - **Failure Rate**: Should be <5% for a healthy system. High failure rates indicate stability issues.
# MAGIC - **P50 (Median)**: The "typical" response time. 50% of requests are faster than this.
# MAGIC - **P90/P95/P99**: Tail latencies. P99 shows worst-case performance for 99% of users.
# MAGIC - **Requests/s**: Throughput achieved. Compare against target capacity.
# MAGIC 
# MAGIC **Analysis impact:** Use these metrics as the primary health indicators. If P99 is significantly 
# MAGIC higher than P50, investigate outliers in the detailed analysis sections below.

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
# MAGIC 
# MAGIC **What it shows:** Per-request metrics captured during the load test, including timestamps, 
# MAGIC prompts, SQL generated, and individual response times.
# MAGIC 
# MAGIC **Key columns:**
# MAGIC - **request_started_at / request_completed_at**: Exact timing of each request
# MAGIC - **duration_ms**: How long the request took in milliseconds
# MAGIC - **prompt**: The question sent to Genie
# MAGIC - **sql**: The SQL query generated (if successful)
# MAGIC - **success**: Whether the request completed successfully
# MAGIC - **source_conversation_id / genie_conversation_id**: Links replay source to Genie response
# MAGIC 
# MAGIC **Analysis impact:** This raw data enables drill-down analysis. Use it to identify specific 
# MAGIC failing queries, correlate slow responses with specific prompts, and trace conversation flows.

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
# MAGIC 
# MAGIC **What it shows:** Performance breakdown by simulated user (user_1, user_2, etc.). Each virtual 
# MAGIC user runs independently, replaying conversations concurrently.
# MAGIC 
# MAGIC **Why it matters:**
# MAGIC - Identifies if specific users experienced worse performance (potential thread contention)
# MAGIC - Validates load distribution across virtual users
# MAGIC - Detects if early-spawned users have different performance than late-spawned users
# MAGIC 
# MAGIC **Analysis impact:** If one user shows significantly different metrics, investigate whether 
# MAGIC they were assigned problematic conversations or experienced resource contention. Ideally, 
# MAGIC all users should show similar performance profiles.

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
# MAGIC 
# MAGIC **What it shows:** 
# MAGIC - **Histogram (left)**: Frequency distribution of response times. Shows how responses cluster.
# MAGIC - **Box plot (right)**: Statistical summary showing median, quartiles, and outliers.
# MAGIC 
# MAGIC **How to interpret:**
# MAGIC - **Normal distribution**: Most responses cluster around the mean - system is consistent
# MAGIC - **Bimodal distribution**: Two distinct peaks - indicates cache hits vs misses, or different query types
# MAGIC - **Long right tail**: Many outliers - some queries take much longer than typical
# MAGIC - **Tight box plot**: Consistent performance. Wide box = high variability.
# MAGIC 
# MAGIC **Analysis impact:** A wide spread or bimodal distribution suggests the system behaves 
# MAGIC differently for different queries. Investigate the slow outliers to identify optimization opportunities.

# COMMAND ----------

import matplotlib.pyplot as plt

# Create images directory in results
images_dir = Path(results_dir) / "images"
images_dir.mkdir(parents=True, exist_ok=True)

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
    fig.savefig(images_dir / "response_time_distribution.png", dpi=150, bbox_inches='tight')
    print(f"Saved: {images_dir / 'response_time_distribution.png'}")
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Response Time Over Test Duration
# MAGIC 
# MAGIC **What it shows:** A scatter plot where each dot is a single request, plotted by when it 
# MAGIC started (x-axis) vs how long it took (y-axis). Blue dots are successful, red dots are failures.
# MAGIC The orange line shows the rolling average trend.
# MAGIC 
# MAGIC **What to look for:**
# MAGIC - **Upward trend**: Latency increasing over time - possible memory leak, resource exhaustion, or queue buildup
# MAGIC - **Downward trend**: System warming up - caches filling, connections pooling
# MAGIC - **Flat trend**: Stable system - ideal behavior
# MAGIC - **Spikes/clusters**: Periodic slowdowns - check for external factors (GC, throttling)
# MAGIC - **Red clusters**: Failure patterns - identify when and why failures occur
# MAGIC 
# MAGIC **Analysis impact:** This visualization reveals temporal patterns invisible in aggregate stats. 
# MAGIC If latency degrades over time, the system may not sustain load. If early requests are slow 
# MAGIC but later ones fast, warm-up is working. Use this to determine if test duration was sufficient.

# COMMAND ----------

if not detailed_metrics_df.empty and 'request_started_at' in detailed_metrics_df.columns:
    fig, ax = plt.subplots(figsize=(14, 6))
    
    # Convert timestamp to datetime
    detailed_metrics_df['request_started_at'] = pd.to_datetime(detailed_metrics_df['request_started_at'])
    duration_secs = detailed_metrics_df['duration_ms'] / 1000
    
    # Color by success/failure
    colors = detailed_metrics_df['success'].map({True: 'blue', False: 'red'})
    
    # Scatter plot
    scatter = ax.scatter(
        detailed_metrics_df['request_started_at'], 
        duration_secs,
        alpha=0.6, 
        c=colors,
        s=30
    )
    
    # Add rolling average trend line (window of 10 requests)
    if len(detailed_metrics_df) >= 10:
        sorted_df = detailed_metrics_df.sort_values('request_started_at')
        rolling_avg = sorted_df['duration_ms'].rolling(window=10, min_periods=1).mean() / 1000
        ax.plot(sorted_df['request_started_at'], rolling_avg, color='orange', linewidth=2, 
                label='Rolling Avg (10 requests)', alpha=0.8)
    
    ax.set_xlabel('Request Time')
    ax.set_ylabel('Response Time (seconds)')
    ax.set_title('Response Time Over Test Duration')
    ax.axhline(duration_secs.median(), color='green', linestyle='--', alpha=0.5, 
               label=f'Median: {duration_secs.median():.2f}s')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Add custom legend for success/failure colors
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker='o', color='w', markerfacecolor='blue', markersize=8, label='Success'),
        Line2D([0], [0], marker='o', color='w', markerfacecolor='red', markersize=8, label='Failure'),
    ]
    ax.legend(handles=legend_elements + ax.get_legend_handles_labels()[0], loc='upper right')
    
    plt.tight_layout()
    fig.savefig(images_dir / "response_time_over_duration.png", dpi=150, bbox_inches='tight')
    print(f"Saved: {images_dir / 'response_time_over_duration.png'}")
    plt.show()
    
    # Print summary
    print(f"\nLatency trend analysis:")
    first_half = duration_secs[:len(duration_secs)//2]
    second_half = duration_secs[len(duration_secs)//2:]
    print(f"  First half avg: {first_half.mean():.2f}s")
    print(f"  Second half avg: {second_half.mean():.2f}s")
    diff = second_half.mean() - first_half.mean()
    if abs(diff) > 0.5:
        trend = "increasing" if diff > 0 else "decreasing"
        print(f"  Trend: Latency {trend} by {abs(diff):.2f}s")
    else:
        print(f"  Trend: Stable (diff: {diff:.2f}s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt Latency Across Executions
# MAGIC 
# MAGIC **What it shows:** Line chart tracking how each unique prompt's latency changes across 
# MAGIC repeated executions. X-axis is execution number (1st, 2nd, 3rd time the prompt was run), 
# MAGIC Y-axis is response time. Each line represents a different prompt.
# MAGIC 
# MAGIC **What to look for:**
# MAGIC - **Steep drop after 1st execution**: Strong caching effect - system remembers queries
# MAGIC - **Flat lines**: No caching benefit - each execution takes similar time
# MAGIC - **Increasing latency**: Possible resource degradation or cache eviction
# MAGIC - **High variance**: Inconsistent performance for the same query
# MAGIC 
# MAGIC **Key insight - First vs Subsequent:**
# MAGIC - If first execution is 2-10x slower than subsequent, caching is working effectively
# MAGIC - If no difference, either no caching or cache isn't being hit
# MAGIC 
# MAGIC **Analysis impact:** This directly measures caching effectiveness. A well-tuned system should 
# MAGIC show significant improvement on repeated queries. Use the per-prompt comparison table to 
# MAGIC identify which specific queries benefit most from caching and which don't.

# COMMAND ----------

if not detailed_metrics_df.empty and 'prompt' in detailed_metrics_df.columns:
    # Sort by execution time and assign execution number per prompt
    prompt_executions = detailed_metrics_df.sort_values('request_started_at').copy()
    prompt_executions['execution_num'] = prompt_executions.groupby('prompt').cumcount() + 1
    
    # Get prompts with multiple executions (at least 2)
    execution_counts = prompt_executions.groupby('prompt').size()
    prompts_with_repeats = execution_counts[execution_counts >= 2].index
    
    if len(prompts_with_repeats) > 0:
        # Get top N prompts by execution count for the chart
        top_prompts = execution_counts.nlargest(min(10, len(prompts_with_repeats))).index
        
        # Line chart showing latency across executions
        fig, ax = plt.subplots(figsize=(14, 6))
        
        for prompt in top_prompts:
            data = prompt_executions[prompt_executions['prompt'] == prompt]
            label = prompt[:40] + '...' if len(prompt) > 40 else prompt
            ax.plot(data['execution_num'], data['duration_ms'] / 1000, 
                   marker='o', markersize=4, alpha=0.7, label=label)
        
        ax.set_xlabel('Execution Number')
        ax.set_ylabel('Response Time (seconds)')
        ax.set_title('Prompt Latency Across Repeated Executions')
        ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.set_xticks(range(1, int(prompt_executions['execution_num'].max()) + 1))
        
        plt.tight_layout()
        fig.savefig(images_dir / "prompt_latency_across_executions.png", dpi=150, bbox_inches='tight')
        print(f"Saved: {images_dir / 'prompt_latency_across_executions.png'}")
        plt.show()
        
        # Summary table: first execution vs subsequent executions
        print("\nFirst Execution vs Subsequent Executions:")
        first_exec = prompt_executions[prompt_executions['execution_num'] == 1]
        subsequent_exec = prompt_executions[prompt_executions['execution_num'] > 1]
        
        if len(subsequent_exec) > 0:
            first_avg = first_exec['duration_ms'].mean() / 1000
            subsequent_avg = subsequent_exec['duration_ms'].mean() / 1000
            improvement = ((first_avg - subsequent_avg) / first_avg) * 100 if first_avg > 0 else 0
            
            print(f"  First execution avg: {first_avg:.2f}s ({len(first_exec)} requests)")
            print(f"  Subsequent executions avg: {subsequent_avg:.2f}s ({len(subsequent_exec)} requests)")
            if improvement > 0:
                print(f"  Improvement: {improvement:.1f}% faster on repeat executions")
            else:
                print(f"  Change: {abs(improvement):.1f}% slower on repeat executions")
        
        # Per-prompt comparison table
        prompt_comparison = []
        for prompt in prompts_with_repeats:
            data = prompt_executions[prompt_executions['prompt'] == prompt]
            first = data[data['execution_num'] == 1]['duration_ms'].mean() / 1000
            subsequent = data[data['execution_num'] > 1]['duration_ms'].mean() / 1000
            prompt_comparison.append({
                'prompt': prompt[:60] + '...' if len(prompt) > 60 else prompt,
                'executions': len(data),
                'first_exec_s': round(first, 2),
                'subsequent_avg_s': round(subsequent, 2),
                'change_pct': round(((first - subsequent) / first) * 100 if first > 0 else 0, 1)
            })
        
        comparison_df = pd.DataFrame(prompt_comparison).sort_values('change_pct', ascending=False)
        print("\nPer-Prompt First vs Subsequent Execution Comparison:")
        display(comparison_df)
    else:
        print("No prompts with repeated executions found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance by Unique Prompt
# MAGIC 
# MAGIC **What it shows:** Aggregated performance statistics for each unique prompt/question in the 
# MAGIC test. Shows count, mean, median, and success rate for every distinct query.
# MAGIC 
# MAGIC **Key outputs:**
# MAGIC - **Slowest prompts table**: Top 10 queries with highest average latency
# MAGIC - **Fastest prompts table**: Top 10 queries with lowest average latency  
# MAGIC - **Bar chart**: Visual ranking of the 15 slowest prompts
# MAGIC - **Problematic prompts**: Queries with <90% success rate flagged for investigation
# MAGIC 
# MAGIC **What to look for:**
# MAGIC - **Outlier prompts**: If one prompt is 10x slower, it may need query optimization
# MAGIC - **Low success rate**: Prompts that frequently fail need investigation
# MAGIC - **High count + high latency**: Most impactful optimization targets
# MAGIC 
# MAGIC **Analysis impact:** This identifies specific queries to optimize. Share the slowest prompts 
# MAGIC list with the team responsible for the Genie space to improve SQL generation or add 
# MAGIC instructions. Failing prompts may need prompt engineering or data model changes.

# COMMAND ----------

if not detailed_metrics_df.empty and 'prompt' in detailed_metrics_df.columns:
    # Group by prompt and aggregate stats
    prompt_stats = detailed_metrics_df.groupby('prompt').agg({
        'duration_ms': ['count', 'mean', 'median', 'min', 'max', 'std'],
        'success': 'mean'
    }).round(2)
    
    prompt_stats.columns = ['count', 'mean_ms', 'median_ms', 'min_ms', 'max_ms', 'std_ms', 'success_rate']
    prompt_stats['mean_s'] = (prompt_stats['mean_ms'] / 1000).round(2)
    prompt_stats['median_s'] = (prompt_stats['median_ms'] / 1000).round(2)
    prompt_stats['success_rate'] = (prompt_stats['success_rate'] * 100).round(1)
    
    # Sort by mean latency (slowest first)
    prompt_stats_sorted = prompt_stats.sort_values('mean_ms', ascending=False)
    
    print(f"Performance by Unique Prompt ({len(prompt_stats_sorted)} unique prompts):")
    print("\nSlowest prompts (top 10):")
    display(prompt_stats_sorted[['count', 'mean_s', 'median_s', 'success_rate']].head(10))
    
    print("\nFastest prompts (top 10):")
    display(prompt_stats_sorted[['count', 'mean_s', 'median_s', 'success_rate']].tail(10).iloc[::-1])
    
    # Bar chart of top 10 slowest prompts
    if len(prompt_stats_sorted) > 0:
        fig, ax = plt.subplots(figsize=(14, 6))
        
        top_n = min(15, len(prompt_stats_sorted))
        top_prompts = prompt_stats_sorted.head(top_n)
        
        # Truncate long prompts for display
        labels = [p[:50] + '...' if len(p) > 50 else p for p in top_prompts.index]
        
        bars = ax.barh(range(top_n), top_prompts['mean_s'], alpha=0.7, color='steelblue')
        ax.set_yticks(range(top_n))
        ax.set_yticklabels(labels, fontsize=9)
        ax.set_xlabel('Mean Response Time (seconds)')
        ax.set_title(f'Top {top_n} Slowest Prompts')
        ax.invert_yaxis()  # Slowest at top
        ax.grid(True, alpha=0.3, axis='x')
        
        # Add count labels
        for i, (bar, count) in enumerate(zip(bars, top_prompts['count'].tolist())):
            ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2, 
                   f'n={count}', va='center', fontsize=8)
        
        plt.tight_layout()
        fig.savefig(images_dir / "slowest_prompts.png", dpi=150, bbox_inches='tight')
        print(f"Saved: {images_dir / 'slowest_prompts.png'}")
        plt.show()
    
    # Summary statistics
    print(f"\nPrompt Performance Summary:")
    print(f"  Total unique prompts: {len(prompt_stats_sorted)}")
    print(f"  Avg latency across all prompts: {prompt_stats_sorted['mean_s'].mean():.2f}s")
    print(f"  Slowest prompt: {prompt_stats_sorted['mean_s'].max():.2f}s")
    print(f"  Fastest prompt: {prompt_stats_sorted['mean_s'].min():.2f}s")
    
    # Identify problematic prompts (high latency or low success rate)
    slow_prompts = prompt_stats_sorted[prompt_stats_sorted['mean_s'] > prompt_stats_sorted['mean_s'].quantile(0.9)]
    failing_prompts = prompt_stats_sorted[prompt_stats_sorted['success_rate'] < 90]
    
    if len(slow_prompts) > 0:
        print(f"\n  Prompts in 90th percentile for latency: {len(slow_prompts)}")
    if len(failing_prompts) > 0:
        print(f"  Prompts with <90% success rate: {len(failing_prompts)}")
        display(failing_prompts[['count', 'mean_s', 'success_rate']])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Throughput Over Time
# MAGIC 
# MAGIC **What it shows:** Requests per second (throughput) plotted over the duration of the test.
# MAGIC This comes from Locust's stats_history.csv which samples metrics every few seconds.
# MAGIC 
# MAGIC **What to look for:**
# MAGIC - **Ramp-up period**: Initial increase as virtual users spawn
# MAGIC - **Steady state**: Flat line indicating consistent throughput
# MAGIC - **Degradation**: Declining throughput over time - system can't sustain load
# MAGIC - **Spikes/dips**: Irregular patterns indicating instability
# MAGIC 
# MAGIC **Expected patterns:**
# MAGIC - Healthy system: Ramp up, then flat steady state
# MAGIC - Overloaded system: Ramp up, then gradual decline
# MAGIC - Unstable system: High variance throughout
# MAGIC 
# MAGIC **Analysis impact:** Throughput tells you if the system can handle the target load sustainably. 
# MAGIC If throughput drops over time, the system is overloaded. Compare peak throughput against 
# MAGIC your capacity requirements to determine if scaling is needed.

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
    fig.savefig(images_dir / "throughput_over_time.png", dpi=150, bbox_inches='tight')
    print(f"Saved: {images_dir / 'throughput_over_time.png'}")
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failures and Exceptions
# MAGIC 
# MAGIC **What it shows:** 
# MAGIC - **Failures**: HTTP-level failures or application errors caught during requests
# MAGIC - **Exceptions**: Unexpected Python exceptions that occurred during the test
# MAGIC 
# MAGIC **Common failure types:**
# MAGIC - **Timeout**: Request took too long - increase timeout or investigate slow queries
# MAGIC - **Rate limiting (429)**: Too many requests - reduce concurrency or implement backoff
# MAGIC - **Server errors (5xx)**: Backend issues - check Genie service health
# MAGIC - **Auth errors (401/403)**: Token expired or permissions issue
# MAGIC 
# MAGIC **Analysis impact:** Even a small failure rate can indicate systemic issues. Group failures 
# MAGIC by type to identify root causes. If failures cluster at specific times, correlate with 
# MAGIC the "Response Time Over Test Duration" chart to identify patterns.

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
# MAGIC ---
# MAGIC # Advanced Analysis
# MAGIC
# MAGIC The following sections provide deeper insights into Genie inference performance,
# MAGIC focusing on SQL generation, conversation context, concurrency impact, and error patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conversation Context Analysis
# MAGIC
# MAGIC **What it shows:** How Genie's performance changes across multi-turn conversations. Each conversation
# MAGIC has multiple messages (message_index 0, 1, 2...), and this analysis tracks how latency and success
# MAGIC rate vary by position in the conversation.
# MAGIC
# MAGIC **Why it matters:** Multi-turn conversations are core to Genie's value proposition. Users build context
# MAGIC across multiple questions. Understanding how performance changes with conversation depth is critical for:
# MAGIC - **User Experience**: Later messages shouldn't be significantly slower than early ones
# MAGIC - **Context Window Optimization**: Determine if passing conversation history creates overhead
# MAGIC - **Capacity Planning**: Multi-turn conversations may consume more resources
# MAGIC
# MAGIC **Key insights to look for:**
# MAGIC - **Increasing latency by position**: If 3rd/4th messages are much slower, context window is creating overhead
# MAGIC - **Decreasing success rate**: If later messages fail more, context may be confusing Genie
# MAGIC - **Flat performance**: Ideal - indicates Genie handles context efficiently
# MAGIC - **Conversation length distribution**: Most conversations short? Long conversations common?
# MAGIC
# MAGIC **Analysis impact:** Use this to:
# MAGIC - Set expectations for user experience in multi-turn scenarios
# MAGIC - Optimize context window size (how much history to include)
# MAGIC - Identify if specific conversation patterns (e.g., follow-up questions) perform poorly
# MAGIC - Determine if prompt engineering is needed for multi-turn scenarios

# COMMAND ----------

if not detailed_metrics_df.empty and 'message_index' in detailed_metrics_df.columns:
    print("=" * 70)
    print("CONVERSATION CONTEXT ANALYSIS")
    print("=" * 70)

    # Overall conversation statistics
    total_conversations = detailed_metrics_df['source_conversation_id'].nunique()
    total_messages = len(detailed_metrics_df)
    avg_messages_per_conversation = total_messages / total_conversations if total_conversations > 0 else 0
    max_conversation_length = detailed_metrics_df['message_index'].max() + 1

    print(f"\n{'Metric':<40} {'Value':>20}")
    print("-" * 60)
    print(f"{'Total Conversations':<40} {total_conversations:>20}")
    print(f"{'Total Messages':<40} {total_messages:>20}")
    print(f"{'Avg Messages per Conversation':<40} {avg_messages_per_conversation:>20.2f}")
    print(f"{'Longest Conversation (messages)':<40} {max_conversation_length:>20}")

    # Performance by message position
    print("\n" + "=" * 60)
    print("PERFORMANCE BY MESSAGE POSITION IN CONVERSATION")
    print("=" * 60)

    position_stats = detailed_metrics_df.groupby('message_index').agg({
        'duration_ms': ['count', 'mean', 'median', 'std'],
        'success': 'mean'
    }).round(2)

    position_stats.columns = ['count', 'mean_ms', 'median_ms', 'std_ms', 'success_rate']
    position_stats['mean_s'] = (position_stats['mean_ms'] / 1000).round(2)
    position_stats['median_s'] = (position_stats['median_ms'] / 1000).round(2)
    position_stats['success_rate'] = (position_stats['success_rate'] * 100).round(1)

    print("\nLatency and Success Rate by Message Position:")
    display(position_stats[['count', 'mean_s', 'median_s', 'success_rate']])

    # Visualizations
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    # 1. Average latency by message position (line chart)
    ax1 = axes[0, 0]
    ax1.plot(position_stats.index, position_stats['mean_s'], marker='o', linewidth=2, markersize=8, color='steelblue', label='Mean')
    ax1.plot(position_stats.index, position_stats['median_s'], marker='s', linewidth=2, markersize=6, color='orange', label='Median', linestyle='--')
    ax1.set_xlabel('Message Position in Conversation')
    ax1.set_ylabel('Response Time (seconds)')
    ax1.set_title('Latency by Message Position')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(position_stats.index)

    # 2. Box plot by message position
    ax2 = axes[0, 1]
    positions_to_plot = sorted(detailed_metrics_df['message_index'].unique())[:10]  # Limit to first 10 positions
    data_for_box = [detailed_metrics_df[detailed_metrics_df['message_index'] == pos]['duration_ms'] / 1000
                    for pos in positions_to_plot]
    ax2.boxplot(data_for_box, labels=positions_to_plot)
    ax2.set_xlabel('Message Position in Conversation')
    ax2.set_ylabel('Response Time (seconds)')
    ax2.set_title('Latency Distribution by Message Position')
    ax2.grid(True, alpha=0.3, axis='y')

    # 3. Success rate by message position
    ax3 = axes[1, 0]
    ax3.bar(position_stats.index, position_stats['success_rate'], color='green', alpha=0.7, edgecolor='black')
    ax3.set_xlabel('Message Position in Conversation')
    ax3.set_ylabel('Success Rate (%)')
    ax3.set_title('Success Rate by Message Position')
    ax3.set_ylim([0, 105])
    ax3.axhline(95, color='red', linestyle='--', alpha=0.5, label='95% threshold')
    ax3.legend()
    ax3.grid(True, alpha=0.3, axis='y')
    ax3.set_xticks(position_stats.index)

    # 4. Conversation length distribution
    ax4 = axes[1, 1]
    conversation_lengths = detailed_metrics_df.groupby('source_conversation_id')['message_index'].max() + 1
    ax4.hist(conversation_lengths, bins=range(1, int(conversation_lengths.max()) + 2),
             edgecolor='black', alpha=0.7, color='purple')
    ax4.set_xlabel('Number of Messages in Conversation')
    ax4.set_ylabel('Number of Conversations')
    ax4.set_title('Conversation Length Distribution')
    ax4.axvline(conversation_lengths.median(), color='red', linestyle='--',
                label=f'Median: {conversation_lengths.median():.0f}')
    ax4.legend()
    ax4.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    fig.savefig(images_dir / "conversation_context_analysis.png", dpi=150, bbox_inches='tight')
    print(f"\nSaved: {images_dir / 'conversation_context_analysis.png'}")
    plt.show()

    # Key insights summary
    print("\n" + "=" * 60)
    print("KEY INSIGHTS")
    print("=" * 60)

    # Check for latency degradation across conversation
    if len(position_stats) >= 2:
        first_msg_latency = position_stats.loc[0, 'mean_s']
        last_msg_latency = position_stats.iloc[-1]['mean_s']
        latency_change = ((last_msg_latency - first_msg_latency) / first_msg_latency) * 100

        print(f"\nLatency Trend Across Conversation:")
        print(f"  First message avg: {first_msg_latency:.2f}s")
        print(f"  Last position avg: {last_msg_latency:.2f}s")
        if abs(latency_change) > 20:
            trend = "increases" if latency_change > 0 else "decreases"
            print(f"  ⚠️  Latency {trend} by {abs(latency_change):.1f}% across conversation")
        else:
            print(f"  ✓ Latency is stable across conversation positions (change: {latency_change:.1f}%)")

    # Check for success rate degradation
    first_msg_success = position_stats.loc[0, 'success_rate']
    last_msg_success = position_stats.iloc[-1]['success_rate']
    if last_msg_success < first_msg_success - 5:
        print(f"\n⚠️  Success rate decreases in later messages:")
        print(f"    First: {first_msg_success:.1f}% → Last: {last_msg_success:.1f}%")
    else:
        print(f"\n✓ Success rate is consistent across conversation positions")

    # Conversation length insights
    short_conversations = (conversation_lengths <= 2).sum()
    medium_conversations = ((conversation_lengths > 2) & (conversation_lengths <= 5)).sum()
    long_conversations = (conversation_lengths > 5).sum()

    print(f"\nConversation Length Breakdown:")
    print(f"  Short (1-2 messages): {short_conversations} ({short_conversations/len(conversation_lengths)*100:.1f}%)")
    print(f"  Medium (3-5 messages): {medium_conversations} ({medium_conversations/len(conversation_lengths)*100:.1f}%)")
    print(f"  Long (6+ messages): {long_conversations} ({long_conversations/len(conversation_lengths)*100:.1f}%)")

else:
    print("No message_index data available for conversation analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Complexity Analysis
# MAGIC
# MAGIC **What it shows:** Analysis of the SQL queries generated by Genie, including complexity metrics,
# MAGIC patterns, and correlation between SQL characteristics and response time.
# MAGIC
# MAGIC **Why it matters:** Genie's core capability is generating SQL from natural language. Understanding
# MAGIC SQL generation patterns is critical for:
# MAGIC - **Prompt Engineering**: Identify which prompts produce complex/slow SQL
# MAGIC - **Performance Optimization**: Find SQL patterns that cause bottlenecks
# MAGIC - **Genie Configuration**: Optimize instructions and examples in the Genie space
# MAGIC - **Failure Analysis**: Determine if certain SQL patterns fail more often
# MAGIC
# MAGIC **Key insights to look for:**
# MAGIC - **Complexity vs Latency**: Do longer/more complex queries take longer?
# MAGIC - **Function Call Performance**: Are Genie-generated functions slower than standard SQL?
# MAGIC - **Empty SQL Rate**: What percentage of requests generate no SQL (non-query responses)?
# MAGIC - **Common Patterns**: What SQL operations does Genie use most?
# MAGIC
# MAGIC **Analysis impact:** Use this to:
# MAGIC - Identify prompts that need rewriting to generate simpler SQL
# MAGIC - Optimize Genie space instructions to guide better SQL generation
# MAGIC - Find opportunities to create additional Genie functions for common patterns
# MAGIC - Detect if Genie is generating unnecessarily complex queries
# MAGIC - Guide database indexing and optimization efforts

# COMMAND ----------

import numpy as np

if not detailed_metrics_df.empty and 'sql' in detailed_metrics_df.columns:
    print("=" * 70)
    print("SQL COMPLEXITY ANALYSIS")
    print("=" * 70)

    # Create a copy for SQL analysis
    sql_df = detailed_metrics_df.copy()

    # Calculate SQL metrics
    sql_df['has_sql'] = sql_df['sql'].notna() & (sql_df['sql'] != '')
    sql_df['sql_length'] = sql_df['sql'].fillna('').str.len()
    sql_df['is_function_call'] = sql_df['sql'].fillna('').str.contains(r'genie_\w+\(', regex=True, case=False)

    # Count SQL operations (case-insensitive)
    sql_df['num_joins'] = sql_df['sql'].fillna('').str.upper().str.count(r'\bJOIN\b')
    sql_df['num_group_by'] = sql_df['sql'].fillna('').str.upper().str.count(r'\bGROUP BY\b')
    sql_df['num_order_by'] = sql_df['sql'].fillna('').str.upper().str.count(r'\bORDER BY\b')
    sql_df['num_where'] = sql_df['sql'].fillna('').str.upper().str.count(r'\bWHERE\b')
    sql_df['num_subqueries'] = sql_df['sql'].fillna('').str.count(r'\(SELECT', flags=2)  # Case-insensitive

    # Complexity score (simple heuristic)
    sql_df['complexity_score'] = (
        sql_df['num_joins'] * 2 +
        sql_df['num_group_by'] * 2 +
        sql_df['num_subqueries'] * 3 +
        sql_df['num_where'] * 1 +
        (sql_df['sql_length'] / 100)
    )

    # Categorize complexity
    def categorize_complexity(score):
        if score == 0:
            return 'No SQL'
        elif score < 5:
            return 'Simple'
        elif score < 15:
            return 'Moderate'
        else:
            return 'Complex'

    sql_df['complexity_category'] = sql_df['complexity_score'].apply(categorize_complexity)

    # Overall SQL statistics
    total_requests = len(sql_df)
    requests_with_sql = sql_df['has_sql'].sum()
    function_calls = sql_df['is_function_call'].sum()
    standard_sql = requests_with_sql - function_calls
    no_sql = total_requests - requests_with_sql

    print(f"\n{'Metric':<40} {'Value':>20}")
    print("-" * 60)
    print(f"{'Total Requests':<40} {total_requests:>20}")
    print(f"{'Requests with SQL':<40} {requests_with_sql:>20} ({requests_with_sql/total_requests*100:.1f}%)")
    print(f"{'  - Function Calls':<40} {function_calls:>20} ({function_calls/total_requests*100:.1f}%)")
    print(f"{'  - Standard SQL':<40} {standard_sql:>20} ({standard_sql/total_requests*100:.1f}%)")
    print(f"{'No SQL Generated':<40} {no_sql:>20} ({no_sql/total_requests*100:.1f}%)")

    # SQL complexity statistics (for requests with SQL)
    if requests_with_sql > 0:
        sql_only = sql_df[sql_df['has_sql']]

        print(f"\n{'SQL Complexity Metrics':<40} {'Value':>20}")
        print("-" * 60)
        print(f"{'Avg SQL Length (characters)':<40} {sql_only['sql_length'].mean():>20.0f}")
        print(f"{'Median SQL Length':<40} {sql_only['sql_length'].median():>20.0f}")
        print(f"{'Max SQL Length':<40} {sql_only['sql_length'].max():>20.0f}")
        print(f"{'Avg JOINs per query':<40} {sql_only['num_joins'].mean():>20.2f}")
        print(f"{'Avg GROUP BYs per query':<40} {sql_only['num_group_by'].mean():>20.2f}")
        print(f"{'Queries with subqueries':<40} {(sql_only['num_subqueries'] > 0).sum():>20}")

        # Complexity distribution
        print(f"\n{'Complexity Distribution':<40} {'Count':>15} {'%':>10}")
        print("-" * 65)
        for category in ['No SQL', 'Simple', 'Moderate', 'Complex']:
            count = (sql_df['complexity_category'] == category).sum()
            pct = count / total_requests * 100
            print(f"{category:<40} {count:>15} {pct:>9.1f}%")

        # Performance by SQL type
        print("\n" + "=" * 60)
        print("PERFORMANCE BY SQL TYPE")
        print("=" * 60)

        type_stats = []

        # Function calls
        if function_calls > 0:
            func_data = sql_df[sql_df['is_function_call']]
            type_stats.append({
                'type': 'Function Call',
                'count': len(func_data),
                'mean_s': (func_data['duration_ms'].mean() / 1000),
                'median_s': (func_data['duration_ms'].median() / 1000),
                'success_rate': func_data['success'].mean() * 100
            })

        # Standard SQL
        if standard_sql > 0:
            std_data = sql_df[sql_df['has_sql'] & ~sql_df['is_function_call']]
            type_stats.append({
                'type': 'Standard SQL',
                'count': len(std_data),
                'mean_s': (std_data['duration_ms'].mean() / 1000),
                'median_s': (std_data['duration_ms'].median() / 1000),
                'success_rate': std_data['success'].mean() * 100
            })

        # No SQL
        if no_sql > 0:
            no_sql_data = sql_df[~sql_df['has_sql']]
            type_stats.append({
                'type': 'No SQL',
                'count': len(no_sql_data),
                'mean_s': (no_sql_data['duration_ms'].mean() / 1000),
                'median_s': (no_sql_data['duration_ms'].median() / 1000),
                'success_rate': no_sql_data['success'].mean() * 100
            })

        type_stats_df = pd.DataFrame(type_stats)
        display(type_stats_df)

        # Performance by complexity
        print("\n" + "=" * 60)
        print("PERFORMANCE BY COMPLEXITY LEVEL")
        print("=" * 60)

        complexity_stats = sql_df.groupby('complexity_category').agg({
            'duration_ms': ['count', 'mean', 'median'],
            'success': 'mean'
        }).round(2)

        complexity_stats.columns = ['count', 'mean_ms', 'median_ms', 'success_rate']
        complexity_stats['mean_s'] = (complexity_stats['mean_ms'] / 1000).round(2)
        complexity_stats['median_s'] = (complexity_stats['median_ms'] / 1000).round(2)
        complexity_stats['success_rate'] = (complexity_stats['success_rate'] * 100).round(1)

        # Reorder to logical progression
        category_order = ['No SQL', 'Simple', 'Moderate', 'Complex']
        complexity_stats = complexity_stats.reindex([c for c in category_order if c in complexity_stats.index])

        display(complexity_stats[['count', 'mean_s', 'median_s', 'success_rate']])

        # Visualizations
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))

        # 1. SQL length vs latency scatter plot
        ax1 = axes[0, 0]
        sql_with_data = sql_df[sql_df['has_sql']]
        scatter = ax1.scatter(sql_with_data['sql_length'],
                             sql_with_data['duration_ms'] / 1000,
                             alpha=0.5, s=30)
        ax1.set_xlabel('SQL Length (characters)')
        ax1.set_ylabel('Response Time (seconds)')
        ax1.set_title('SQL Length vs Response Time')
        ax1.grid(True, alpha=0.3)

        # Add trend line
        if len(sql_with_data) > 1:
            z = np.polyfit(sql_with_data['sql_length'], sql_with_data['duration_ms'] / 1000, 1)
            p = np.poly1d(z)
            ax1.plot(sql_with_data['sql_length'].sort_values(),
                    p(sql_with_data['sql_length'].sort_values()),
                    "r--", alpha=0.8, label=f'Trend: y={z[0]:.4f}x+{z[1]:.2f}')
            ax1.legend()

        # 2. Latency by complexity category
        ax2 = axes[0, 1]
        complexity_plot_data = [sql_df[sql_df['complexity_category'] == cat]['duration_ms'] / 1000
                               for cat in category_order if cat in sql_df['complexity_category'].values]
        complexity_labels = [cat for cat in category_order if cat in sql_df['complexity_category'].values]
        ax2.boxplot(complexity_plot_data, labels=complexity_labels)
        ax2.set_xlabel('Complexity Category')
        ax2.set_ylabel('Response Time (seconds)')
        ax2.set_title('Latency by SQL Complexity')
        ax2.grid(True, alpha=0.3, axis='y')
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

        # 3. SQL type distribution pie chart
        ax3 = axes[1, 0]
        type_counts = [function_calls, standard_sql, no_sql]
        type_labels = [f'Function Call\n({function_calls})',
                      f'Standard SQL\n({standard_sql})',
                      f'No SQL\n({no_sql})']
        colors = ['#ff9999', '#66b3ff', '#99ff99']
        ax3.pie([c for c in type_counts if c > 0],
               labels=[l for l, c in zip(type_labels, type_counts) if c > 0],
               colors=[col for col, c in zip(colors, type_counts) if c > 0],
               autopct='%1.1f%%', startangle=90)
        ax3.set_title('SQL Type Distribution')

        # 4. Complexity score distribution
        ax4 = axes[1, 1]
        sql_with_score = sql_df[sql_df['has_sql']]
        if len(sql_with_score) > 0:
            ax4.hist(sql_with_score['complexity_score'], bins=30, edgecolor='black', alpha=0.7, color='coral')
            ax4.set_xlabel('Complexity Score')
            ax4.set_ylabel('Number of Queries')
            ax4.set_title('SQL Complexity Score Distribution')
            ax4.axvline(sql_with_score['complexity_score'].median(), color='red', linestyle='--',
                       label=f'Median: {sql_with_score["complexity_score"].median():.1f}')
            ax4.legend()
            ax4.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()
        fig.savefig(images_dir / "sql_complexity_analysis.png", dpi=150, bbox_inches='tight')
        print(f"\nSaved: {images_dir / 'sql_complexity_analysis.png'}")
        plt.show()

        # Key insights
        print("\n" + "=" * 60)
        print("KEY INSIGHTS")
        print("=" * 60)

        # Correlation between length and latency
        if len(sql_with_data) > 1:
            correlation = sql_with_data['sql_length'].corr(sql_with_data['duration_ms'])
            print(f"\nSQL Length vs Latency Correlation: {correlation:.3f}")
            if correlation > 0.5:
                print("  ⚠️  Strong positive correlation - longer SQL queries are significantly slower")
            elif correlation > 0.3:
                print("  ⚡ Moderate correlation - SQL length has some impact on latency")
            else:
                print("  ✓ Weak correlation - SQL length is not a major factor in latency")

        # Compare function calls vs standard SQL
        if function_calls > 0 and standard_sql > 0:
            func_avg = sql_df[sql_df['is_function_call']]['duration_ms'].mean() / 1000
            std_avg = sql_df[sql_df['has_sql'] & ~sql_df['is_function_call']]['duration_ms'].mean() / 1000
            diff_pct = ((func_avg - std_avg) / std_avg) * 100 if std_avg > 0 else 0

            print(f"\nFunction Calls vs Standard SQL:")
            print(f"  Function calls avg: {func_avg:.2f}s")
            print(f"  Standard SQL avg: {std_avg:.2f}s")
            if abs(diff_pct) > 20:
                comparison = "slower" if diff_pct > 0 else "faster"
                print(f"  ⚠️  Function calls are {abs(diff_pct):.1f}% {comparison} than standard SQL")
            else:
                print(f"  ✓ Performance is similar (diff: {diff_pct:.1f}%)")

        # Complexity impact
        if 'Complex' in complexity_stats.index and 'Simple' in complexity_stats.index:
            complex_avg = complexity_stats.loc['Complex', 'mean_s']
            simple_avg = complexity_stats.loc['Simple', 'mean_s']
            complexity_impact = ((complex_avg - simple_avg) / simple_avg) * 100 if simple_avg > 0 else 0

            print(f"\nComplexity Impact:")
            print(f"  Simple queries avg: {simple_avg:.2f}s")
            print(f"  Complex queries avg: {complex_avg:.2f}s")
            if complexity_impact > 50:
                print(f"  ⚠️  Complex queries are {complexity_impact:.1f}% slower - consider prompt optimization")
            else:
                print(f"  ✓ Complexity impact is manageable ({complexity_impact:.1f}%)")

else:
    print("No SQL data available for analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich Metrics with SQL Execution Data
# MAGIC
# MAGIC **What it does:** Fetches SQL execution metrics (execution time, compilation time, rows produced,
# MAGIC bytes read/written) from `system.query.history` and enriches the detailed_metrics.csv file.
# MAGIC
# MAGIC **Important:** The `system.query.history` table has 15-30 minute ingestion latency. This
# MAGIC enrichment should only be run after waiting for the data to propagate.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - `GENIE_WAREHOUSE_ID` environment variable set (or entered in widget below)
# MAGIC - Wait at least 15-30 minutes after the load test completes

# COMMAND ----------

# Check if enrichment is needed
enrichment_needed = False
if not detailed_metrics_df.empty:
    sql_count = detailed_metrics_df['sql'].notna().sum()
    
    # Check for the new column name first (sql_total_duration_ms), fall back to old name
    if 'sql_total_duration_ms' in detailed_metrics_df.columns:
        enriched_count = detailed_metrics_df['sql_total_duration_ms'].notna().sum()
        if enriched_count == 0 and sql_count > 0:
            enrichment_needed = True
            print(f"⚠️  SQL execution metrics are empty ({enriched_count}/{sql_count} rows enriched)")
            print(f"   Attempting automatic enrichment from {query_history_table}...")
        else:
            print(f"✓ SQL execution metrics present: {enriched_count}/{sql_count} rows ({enriched_count/sql_count*100:.1f}% coverage)")
    elif 'sql_execution_time_ms' in detailed_metrics_df.columns:
        # Legacy column name check
        enriched_count = detailed_metrics_df['sql_execution_time_ms'].notna().sum()
        if enriched_count == 0 and sql_count > 0:
            enrichment_needed = True
            print(f"⚠️  SQL execution metrics are empty ({enriched_count}/{sql_count} rows enriched)")
            print(f"   Attempting automatic enrichment from {query_history_table}...")
        else:
            print(f"✓ SQL execution metrics present: {enriched_count}/{sql_count} rows ({enriched_count/sql_count*100:.1f}% coverage)")
    elif sql_count > 0:
        # No SQL metrics columns at all, need enrichment
        enrichment_needed = True
        print(f"ℹ️  No SQL execution metrics columns found - attempting enrichment...")
    else:
        print("ℹ️  No SQL queries in this test run - enrichment not applicable")
else:
    print("No detailed metrics data available")

# COMMAND ----------

# Automatic enrichment with SQL execution metrics from system.query.history
# 
# This cell will:
# 1. Read the detailed_metrics.csv from the results directory
# 2. Query system.query.history for the time range of the load test
# 3. Match SQL queries and enrich with execution metrics
# 4. Save the enriched file back to the same location
#
# NOTE: system.query.history has 15-30 minute ingestion latency.
# If the load test just completed, enrichment may find no data.

if enrichment_needed and warehouse_id:
    print("=" * 70)
    print("ENRICHING METRICS WITH SQL EXECUTION DATA")
    print("=" * 70)
    
    # Import the enrichment function
    import sys
    sys.path.insert(0, "..")  # Add parent directory to path for genie_simulation module
    
    from genie_simulation.enrich_metrics import enrich_metrics_with_query_history
    
    metrics_path = Path(results_dir) / "detailed_metrics.csv"
    
    try:
        enriched_df = enrich_metrics_with_query_history(
            metrics_csv_path=str(metrics_path),
            warehouse_id=warehouse_id,
            query_history_table=query_history_table,
            access_audit_table=access_audit_table,
        )
        
        # Reload the dataframe
        detailed_metrics_df = enriched_df
        
        print("\n✓ Enrichment complete!")
        
    except Exception as e:
        print(f"❌ Enrichment failed: {e}")
        print("\n   Possible causes:")
        print("   - Invalid warehouse ID")
        print(f"   - Insufficient permissions on {query_history_table}")
        print("   - Load test too recent (data not yet in system tables)")
elif enrichment_needed and not warehouse_id:
    print("⚠️  SQL metrics enrichment skipped - no warehouse ID provided")
    print("   Enter a SQL Warehouse ID in the widget above to enable enrichment")
elif not enrichment_needed:
    print("✓ SQL metrics already enriched or no SQL queries to enrich")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Execution Metrics (from system.query.history)
# MAGIC
# MAGIC **What it shows:** Actual SQL execution time in the warehouse as captured from the Databricks
# MAGIC system.query.history table. This breaks down the end-to-end latency into:
# MAGIC - **SQL Execution Time**: Time spent executing the query in the warehouse
# MAGIC - **SQL Compilation Time**: Time spent planning/compiling the query
# MAGIC - **Overhead Time**: End-to-end latency minus SQL execution (Genie processing, network, etc.)
# MAGIC
# MAGIC **Why it matters:** Understanding where time is spent helps identify optimization opportunities:
# MAGIC - **High SQL execution time**: Optimize queries, add indexes, increase warehouse size
# MAGIC - **High compilation time**: Query complexity issues, consider caching query plans
# MAGIC - **High overhead**: Genie API latency, network issues, or processing bottlenecks
# MAGIC
# MAGIC **Key insights to look for:**
# MAGIC - **SQL execution as % of total**: How much is warehouse vs Genie overhead?
# MAGIC - **Rows produced vs latency**: Does more data = slower queries?
# MAGIC - **Bytes read correlation**: I/O-bound vs compute-bound queries

# COMMAND ----------

# Check for SQL execution metrics columns (new or legacy names)
has_sql_metrics = (
    'sql_total_duration_ms' in detailed_metrics_df.columns or
    'sql_execution_time_ms' in detailed_metrics_df.columns
)

if not detailed_metrics_df.empty and has_sql_metrics:
    # Determine which column to use for filtering
    duration_col = 'sql_total_duration_ms' if 'sql_total_duration_ms' in detailed_metrics_df.columns else 'sql_execution_time_ms'
    
    # Filter to rows with SQL execution metrics
    sql_exec_df = detailed_metrics_df[detailed_metrics_df[duration_col].notna()].copy()
    
    if len(sql_exec_df) > 0:
        print("=" * 70)
        print(f"SQL EXECUTION METRICS (from {query_history_table})")
        print("=" * 70)
        
        # Calculate overhead (end-to-end time minus SQL total time in warehouse)
        sql_total_col = 'sql_total_duration_ms' if 'sql_total_duration_ms' in sql_exec_df.columns else 'sql_execution_time_ms'
        sql_exec_df['overhead_ms'] = sql_exec_df['duration_ms'] - sql_exec_df[sql_total_col]
        sql_exec_df['sql_pct_of_total'] = (sql_exec_df[sql_total_col] / sql_exec_df['duration_ms']) * 100
        
        # Summary statistics
        total_requests = len(sql_exec_df)
        total_with_metrics = sql_exec_df[duration_col].notna().sum()
        
        print(f"\n{'Metric':<40} {'Value':>20}")
        print("-" * 60)
        print(f"{'Requests with SQL metrics':<40} {total_with_metrics:>20}")
        print(f"{'Coverage':<40} {f'{total_with_metrics/len(detailed_metrics_df)*100:.1f}%':>20}")
        
        # Time breakdown - show all available metrics from system.query.history
        print(f"\n{'TIME BREAKDOWN (seconds)':<40}")
        print("-" * 60)
        avg_total = sql_exec_df['duration_ms'].mean() / 1000
        
        # Get SQL warehouse time breakdown
        avg_sql_total = sql_exec_df[sql_total_col].mean() / 1000 if sql_total_col in sql_exec_df.columns else 0
        avg_sql_exec = sql_exec_df['sql_execution_time_ms'].mean() / 1000 if 'sql_execution_time_ms' in sql_exec_df.columns else 0
        avg_compile = sql_exec_df['sql_compilation_time_ms'].mean() / 1000 if 'sql_compilation_time_ms' in sql_exec_df.columns else 0
        avg_queue_wait = sql_exec_df['sql_queue_wait_ms'].mean() / 1000 if 'sql_queue_wait_ms' in sql_exec_df.columns else 0
        avg_compute_wait = sql_exec_df['sql_compute_wait_ms'].mean() / 1000 if 'sql_compute_wait_ms' in sql_exec_df.columns else 0
        avg_result_fetch = sql_exec_df['sql_result_fetch_ms'].mean() / 1000 if 'sql_result_fetch_ms' in sql_exec_df.columns else 0
        avg_overhead = sql_exec_df['overhead_ms'].mean() / 1000
        
        print(f"{'  Total (end-to-end)':<40} {avg_total:>19.2f}s")
        print(f"{'  SQL Total (in warehouse)':<40} {avg_sql_total:>19.2f}s ({avg_sql_total/avg_total*100:.1f}%)")
        print(f"{'    - Execution':<40} {avg_sql_exec:>19.2f}s")
        print(f"{'    - Compilation':<40} {avg_compile:>19.2f}s")
        print(f"{'    - Queue Wait':<40} {avg_queue_wait:>19.2f}s")
        print(f"{'    - Compute Wait':<40} {avg_compute_wait:>19.2f}s")
        print(f"{'    - Result Fetch':<40} {avg_result_fetch:>19.2f}s")
        print(f"{'  Overhead (Genie AI + network)':<40} {avg_overhead:>19.2f}s ({avg_overhead/avg_total*100:.1f}%)")
        
        # I/O metrics
        if 'sql_bytes_read' in sql_exec_df.columns:
            avg_bytes_read = sql_exec_df['sql_bytes_read'].mean()
            avg_rows = sql_exec_df['sql_rows_produced'].mean() if 'sql_rows_produced' in sql_exec_df.columns else 0
            
            print(f"\n{'I/O METRICS':<40}")
            print("-" * 60)
            print(f"{'  Avg Bytes Read':<40} {avg_bytes_read/1024/1024:>18.2f} MB")
            print(f"{'  Avg Rows Produced':<40} {avg_rows:>20.0f}")
        
        # Percentile breakdown using SQL total time
        print(f"\n{'SQL TOTAL TIME PERCENTILES':<40}")
        print("-" * 60)
        sql_total_secs = sql_exec_df[sql_total_col] / 1000
        print(f"{'  P50 (Median)':<40} {sql_total_secs.quantile(0.50):>19.2f}s")
        print(f"{'  P90':<40} {sql_total_secs.quantile(0.90):>19.2f}s")
        print(f"{'  P95':<40} {sql_total_secs.quantile(0.95):>19.2f}s")
        print(f"{'  P99':<40} {sql_total_secs.quantile(0.99):>19.2f}s")
        
        # Visualizations
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        
        # 1. Time breakdown stacked bar - show all components
        ax1 = axes[0, 0]
        categories = ['Execution', 'Compilation', 'Queue Wait', 'Compute Wait', 'Result Fetch', 'AI Overhead']
        values = [avg_sql_exec, avg_compile, avg_queue_wait, avg_compute_wait, avg_result_fetch, avg_overhead]
        colors = ['#2ecc71', '#3498db', '#f39c12', '#9b59b6', '#1abc9c', '#e74c3c']
        ax1.bar(categories, values, color=colors, edgecolor='black', alpha=0.8)
        ax1.set_ylabel('Time (seconds)')
        ax1.set_title('Average Time Breakdown')
        ax1.grid(True, alpha=0.3, axis='y')
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
        for i, v in enumerate(values):
            if v > 0.01:  # Only label non-trivial values
                pct = v / avg_total * 100
                ax1.text(i, v + 0.02, f'{v:.2f}s\n({pct:.0f}%)', ha='center', fontsize=7)
        
        # 2. SQL total time distribution
        ax2 = axes[0, 1]
        ax2.hist(sql_total_secs, bins=30, edgecolor='black', alpha=0.7, color='steelblue')
        ax2.axvline(sql_total_secs.median(), color='red', linestyle='--', 
                   label=f'Median: {sql_total_secs.median():.2f}s')
        ax2.axvline(sql_total_secs.mean(), color='green', linestyle='--', 
                   label=f'Mean: {sql_total_secs.mean():.2f}s')
        ax2.set_xlabel('SQL Total Time in Warehouse (seconds)')
        ax2.set_ylabel('Frequency')
        ax2.set_title('SQL Total Time Distribution')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # 3. SQL total vs end-to-end latency scatter
        ax3 = axes[1, 0]
        ax3.scatter(sql_exec_df[sql_total_col] / 1000, 
                   sql_exec_df['duration_ms'] / 1000, 
                   alpha=0.5, s=30)
        # Add diagonal line (if SQL = total)
        max_val = max(sql_exec_df['duration_ms'].max() / 1000, sql_exec_df[sql_total_col].max() / 1000)
        ax3.plot([0, max_val], [0, max_val], 'r--', alpha=0.5, label='SQL = Total')
        ax3.set_xlabel('SQL Total Time in Warehouse (seconds)')
        ax3.set_ylabel('End-to-End Latency (seconds)')
        ax3.set_title('SQL Time vs Total Latency')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # 4. SQL % of total distribution
        ax4 = axes[1, 1]
        ax4.hist(sql_exec_df['sql_pct_of_total'], bins=20, edgecolor='black', alpha=0.7, color='purple')
        ax4.axvline(sql_exec_df['sql_pct_of_total'].median(), color='red', linestyle='--',
                   label=f'Median: {sql_exec_df["sql_pct_of_total"].median():.1f}%')
        ax4.set_xlabel('SQL Time as % of Total Latency')
        ax4.set_ylabel('Frequency')
        ax4.set_title('SQL Time as Percentage of Total')
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        fig.savefig(images_dir / "sql_execution_metrics.png", dpi=150, bbox_inches='tight')
        print(f"\nSaved: {images_dir / 'sql_execution_metrics.png'}")
        plt.show()
        
        # Key insights
        print("\n" + "=" * 60)
        print("KEY INSIGHTS")
        print("=" * 60)
        
        sql_pct_median = sql_exec_df['sql_pct_of_total'].median()
        if sql_pct_median > 80:
            print(f"\n✓ SQL execution dominates latency ({sql_pct_median:.1f}% of total)")
            print("  Optimization focus: Query performance, warehouse sizing")
        elif sql_pct_median > 50:
            print(f"\n⚡ Mixed latency sources (SQL: {sql_pct_median:.1f}% of total)")
            print("  Both SQL optimization and Genie tuning can help")
        else:
            print(f"\n⚠️  Overhead dominates latency (SQL only {sql_pct_median:.1f}% of total)")
            print("  Focus on Genie AI inference, network, or processing optimization")
        
        # Check for high variance
        sql_cv = sql_total_secs.std() / sql_total_secs.mean() if sql_total_secs.mean() > 0 else 0
        if sql_cv > 1.0:
            print(f"\n⚠️  High SQL time variance (CV: {sql_cv:.2f})")
            print("  Some queries are significantly slower - investigate outliers")
        else:
            print(f"\n✓ SQL times are relatively consistent (CV: {sql_cv:.2f})")
        
        # Additional insights based on wait times
        if avg_queue_wait > 0.5:
            print(f"\n⚠️  Significant queue wait time ({avg_queue_wait:.2f}s avg)")
            print("  Warehouse may be at capacity - consider scaling")
        if avg_compute_wait > 0.5:
            print(f"\n⚠️  Significant compute startup wait ({avg_compute_wait:.2f}s avg)")
            print("  Consider using a serverless warehouse or increasing min clusters")

        # Scan efficiency (rows_read vs rows_produced)
        if 'sql_rows_read' in sql_exec_df.columns:
            avg_rows_read = sql_exec_df['sql_rows_read'].mean()
            avg_rows_produced_val = sql_exec_df['sql_rows_produced'].mean() if 'sql_rows_produced' in sql_exec_df.columns else 0
            if avg_rows_read > 0 and avg_rows_produced_val > 0:
                scan_ratio = avg_rows_read / avg_rows_produced_val
                print(f"\n{'SCAN EFFICIENCY':<40}")
                print("-" * 60)
                print(f"{'  Avg Rows Scanned':<40} {avg_rows_read:>20.0f}")
                print(f"{'  Avg Rows Returned':<40} {avg_rows_produced_val:>20.0f}")
                print(f"{'  Scan Ratio (scanned/returned)':<40} {scan_ratio:>20.1f}x")
                if scan_ratio > 100:
                    print("  ⚠️  High scan ratio - consider adding filters or indexes")
            
    else:
        print("No SQL execution metrics available (column is empty)")
        print(f"Run enrichment after waiting 15-30 minutes for {query_history_table} ingestion")
else:
    print("No SQL execution metrics columns found in detailed_metrics.csv")
    print(f"These metrics are populated by post-processing enrichment from {query_history_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI Overhead Analysis (from system.access.audit)
# MAGIC
# MAGIC **What it shows:** The time Genie spends on AI inference (NL-to-SQL generation) before
# MAGIC executing the first SQL query. This is calculated by correlating:
# MAGIC - `system.access.audit`: When the Genie API received the user message
# MAGIC - `system.query.history`: When the first SQL query started executing
# MAGIC
# MAGIC **AI Overhead = First Query Start - Message Event Time**
# MAGIC
# MAGIC **Why it matters:** This separates "thinking time" from "execution time":
# MAGIC - **High AI overhead**: Genie is slow at generating SQL - may need prompt optimization
# MAGIC - **Low AI overhead**: Genie generates SQL quickly - focus optimization on warehouse

# COMMAND ----------

if not detailed_metrics_df.empty and 'ai_overhead_ms' in detailed_metrics_df.columns:
    ai_df = detailed_metrics_df[detailed_metrics_df['ai_overhead_ms'].notna()].copy()
    
    if len(ai_df) > 0:
        print("=" * 70)
        print(f"AI OVERHEAD ANALYSIS (from {access_audit_table})")
        print("=" * 70)
        
        ai_secs = ai_df['ai_overhead_ms'] / 1000
        
        print(f"\n{'Metric':<40} {'Value':>20}")
        print("-" * 60)
        print(f"{'Requests with AI overhead data':<40} {len(ai_df):>20}")
        print(f"{'Coverage':<40} {f'{len(ai_df)/len(detailed_metrics_df)*100:.1f}%':>20}")
        
        print(f"\n{'AI OVERHEAD (seconds)':<40}")
        print("-" * 60)
        print(f"{'  Mean':<40} {ai_secs.mean():>19.2f}s")
        print(f"{'  Median':<40} {ai_secs.median():>19.2f}s")
        print(f"{'  P90':<40} {ai_secs.quantile(0.90):>19.2f}s")
        print(f"{'  P95':<40} {ai_secs.quantile(0.95):>19.2f}s")
        print(f"{'  P99':<40} {ai_secs.quantile(0.99):>19.2f}s")
        
        # Compare with SQL execution time
        if 'sql_total_duration_ms' in ai_df.columns:
            sql_total = ai_df['sql_total_duration_ms'] / 1000
            total = ai_df['duration_ms'] / 1000
            
            avg_ai = ai_secs.mean()
            avg_sql = sql_total.mean()
            avg_total = total.mean()
            avg_other = avg_total - avg_ai - avg_sql
            
            print(f"\n{'FULL TIME BREAKDOWN (seconds)':<40}")
            print("-" * 60)
            print(f"{'  AI Overhead (NL-to-SQL)':<40} {avg_ai:>19.2f}s ({avg_ai/avg_total*100:.1f}%)")
            print(f"{'  SQL Execution (warehouse)':<40} {avg_sql:>19.2f}s ({avg_sql/avg_total*100:.1f}%)")
            print(f"{'  Other (network, etc.)':<40} {max(0, avg_other):>19.2f}s ({max(0, avg_other)/avg_total*100:.1f}%)")
            print(f"{'  Total (end-to-end)':<40} {avg_total:>19.2f}s")
        
        # Visualization
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        
        ax1 = axes[0]
        ax1.hist(ai_secs, bins=30, edgecolor='black', alpha=0.7, color='coral')
        ax1.axvline(ai_secs.median(), color='red', linestyle='--',
                   label=f'Median: {ai_secs.median():.2f}s')
        ax1.set_xlabel('AI Overhead (seconds)')
        ax1.set_ylabel('Frequency')
        ax1.set_title('AI Overhead Distribution')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Stacked breakdown if we have all components
        ax2 = axes[1]
        if 'sql_total_duration_ms' in ai_df.columns:
            categories = ['AI Overhead', 'SQL Warehouse', 'Other']
            values_breakdown = [avg_ai, avg_sql, max(0, avg_other)]
            colors_breakdown = ['#e74c3c', '#2ecc71', '#95a5a6']
            ax2.bar(categories, values_breakdown, color=colors_breakdown, edgecolor='black', alpha=0.8)
            ax2.set_ylabel('Time (seconds)')
            ax2.set_title('Average End-to-End Breakdown')
            ax2.grid(True, alpha=0.3, axis='y')
            for i, v in enumerate(values_breakdown):
                if v > 0.01:
                    ax2.text(i, v + 0.02, f'{v:.2f}s', ha='center', fontsize=9)
        
        plt.tight_layout()
        fig.savefig(images_dir / "ai_overhead_analysis.png", dpi=150, bbox_inches='tight')
        print(f"\nSaved: {images_dir / 'ai_overhead_analysis.png'}")
        plt.show()
    else:
        print("No AI overhead data available")
        print(f"AI overhead requires {access_audit_table} data (enrichment with warehouse_id)")
else:
    print(f"No AI overhead data in metrics - this is computed from {access_audit_table} during enrichment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bottleneck Classification and Speed Analysis
# MAGIC
# MAGIC **Bottleneck Classification** categorizes each query by its dominant time component:
# MAGIC - **COMPUTE_STARTUP**: Waiting for warehouse compute resources (>50% of total)
# MAGIC - **QUEUE_WAIT**: Waiting in queue at capacity (>30% of total)
# MAGIC - **COMPILATION**: Query planning/compilation time (>40% of total)
# MAGIC - **LARGE_SCAN**: Reading more than 1GB of data
# MAGIC - **SLOW_EXECUTION**: Execution time exceeds 10 seconds
# MAGIC - **NORMAL**: No single bottleneck dominates
# MAGIC
# MAGIC **Speed Categories**: FAST (<5s), MODERATE (5-10s), SLOW (10-30s), CRITICAL (>30s)

# COMMAND ----------

if not detailed_metrics_df.empty and 'sql_bottleneck' in detailed_metrics_df.columns:
    bottleneck_df = detailed_metrics_df[detailed_metrics_df['sql_bottleneck'].notna()].copy()
    
    if len(bottleneck_df) > 0:
        print("=" * 70)
        print("BOTTLENECK CLASSIFICATION AND SPEED ANALYSIS")
        print("=" * 70)
        
        # Bottleneck distribution
        bottleneck_counts = bottleneck_df['sql_bottleneck'].value_counts()
        print(f"\n{'Bottleneck Type':<30} {'Count':>10} {'%':>10}")
        print("-" * 50)
        for bottleneck, count in bottleneck_counts.items():
            pct = count / len(bottleneck_df) * 100
            print(f"{bottleneck:<30} {count:>10} {pct:>9.1f}%")
        
        # Speed category distribution
        if 'sql_speed_category' in bottleneck_df.columns:
            speed_counts = bottleneck_df['sql_speed_category'].value_counts()
            print(f"\n{'Speed Category':<30} {'Count':>10} {'%':>10}")
            print("-" * 50)
            for category in ['FAST', 'MODERATE', 'SLOW', 'CRITICAL']:
                count = speed_counts.get(category, 0)
                pct = count / len(bottleneck_df) * 100
                print(f"{category:<30} {count:>10} {pct:>9.1f}%")
        
        # Visualizations
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        
        # Bottleneck pie chart
        ax1 = axes[0]
        if len(bottleneck_counts) > 0:
            ax1.pie(bottleneck_counts.values, labels=bottleneck_counts.index,
                   autopct='%1.1f%%', startangle=90)
            ax1.set_title(f'Bottleneck Distribution ({len(bottleneck_df)} queries)')
        
        # Speed category bar chart
        ax2 = axes[1]
        if 'sql_speed_category' in bottleneck_df.columns:
            speed_order = ['FAST', 'MODERATE', 'SLOW', 'CRITICAL']
            speed_colors = ['#2ecc71', '#f39c12', '#e67e22', '#e74c3c']
            speed_vals = [speed_counts.get(c, 0) for c in speed_order]
            ax2.bar(speed_order, speed_vals, color=speed_colors, edgecolor='black', alpha=0.8)
            ax2.set_xlabel('Speed Category')
            ax2.set_ylabel('Number of Queries')
            ax2.set_title('Speed Category Distribution')
            ax2.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        fig.savefig(images_dir / "bottleneck_speed_analysis.png", dpi=150, bbox_inches='tight')
        print(f"\nSaved: {images_dir / 'bottleneck_speed_analysis.png'}")
        plt.show()
        
        # Key insights
        print("\n" + "=" * 60)
        print("KEY INSIGHTS")
        print("=" * 60)
        
        if 'NORMAL' in bottleneck_counts.index:
            normal_pct = bottleneck_counts['NORMAL'] / len(bottleneck_df) * 100
            print(f"\n  {normal_pct:.1f}% of queries have no dominant bottleneck (NORMAL)")
        
        problematic = bottleneck_counts.drop('NORMAL', errors='ignore')
        if len(problematic) > 0:
            top_bottleneck = problematic.index[0]
            top_count = problematic.values[0]
            top_pct = top_count / len(bottleneck_df) * 100
            print(f"  Top bottleneck: {top_bottleneck} ({top_pct:.1f}% of queries)")
            
            if top_bottleneck == 'COMPUTE_STARTUP':
                print("  Recommendation: Use serverless warehouse or increase min clusters")
            elif top_bottleneck == 'QUEUE_WAIT':
                print("  Recommendation: Scale warehouse or reduce concurrency")
            elif top_bottleneck == 'COMPILATION':
                print("  Recommendation: Simplify queries or optimize Genie instructions")
            elif top_bottleneck == 'LARGE_SCAN':
                print("  Recommendation: Add filters, optimize partitioning, or add indexes")
            elif top_bottleneck == 'SLOW_EXECUTION':
                print("  Recommendation: Optimize query logic or increase warehouse size")
        
        if 'sql_speed_category' in bottleneck_df.columns:
            critical_count = speed_counts.get('CRITICAL', 0)
            slow_count = speed_counts.get('SLOW', 0)
            if critical_count > 0:
                print(f"\n  ⚠️  {critical_count} queries are CRITICAL (>30s)")
            if slow_count > 0:
                print(f"  ⚡ {slow_count} queries are SLOW (10-30s)")
    else:
        print("No bottleneck data available - run enrichment first")
else:
    print("No bottleneck classification in metrics - this is computed during enrichment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Runtime vs Concurrency
# MAGIC
# MAGIC **What it shows:** How SQL execution metrics from `system.query.history` change as concurrent
# MAGIC users increase within a single test run. This isolates warehouse-level performance from
# MAGIC end-to-end latency to reveal whether the SQL warehouse or the AI/Genie layer is the
# MAGIC bottleneck under load.
# MAGIC
# MAGIC **Why it matters:**
# MAGIC - **Queue Wait Growth**: Rising queue wait times indicate the warehouse is at capacity
# MAGIC - **Execution Time Stability**: If SQL execution time stays flat but E2E grows, the AI layer is the bottleneck
# MAGIC - **Bottleneck Shift Detection**: At low concurrency SQL may dominate; at high concurrency, AI overhead or queue wait may take over
# MAGIC
# MAGIC **Key insights to look for:**
# MAGIC - **SQL P50 degradation**: Does median SQL time grow with users?
# MAGIC - **Queue wait scaling**: Flat = warehouse capacity OK, growing = need to scale warehouse
# MAGIC - **SQL % of total**: Increasing = SQL is bottleneck; decreasing = AI/network is bottleneck

# COMMAND ----------

# Check if SQL execution metrics AND multiple concurrency levels are available
_has_sql_concurrency = (
    not detailed_metrics_df.empty
    and 'sql_total_duration_ms' in detailed_metrics_df.columns
    and detailed_metrics_df['sql_total_duration_ms'].notna().any()
    and 'concurrent_users' in detailed_metrics_df.columns
    and detailed_metrics_df['concurrent_users'].nunique() > 1
)

if _has_sql_concurrency:
    _sql_conc_df = detailed_metrics_df[detailed_metrics_df['sql_total_duration_ms'].notna()].copy()
    _conc_levels = sorted(_sql_conc_df['concurrent_users'].unique())

    # ── Printed table: SQL metrics percentiles by concurrent users ──
    print("=" * 100)
    print("SQL RUNTIME PERCENTILES BY CONCURRENT USERS")
    print("=" * 100)

    _sql_metric_cols = {
        'sql_total_duration_ms': 'SQL Total',
        'sql_execution_time_ms': 'Execution',
        'sql_queue_wait_ms': 'Queue Wait',
        'sql_compilation_time_ms': 'Compilation',
        'sql_compute_wait_ms': 'Compute Wait',
    }
    _available_sql_metric_cols = {k: v for k, v in _sql_metric_cols.items() if k in _sql_conc_df.columns}

    for col_key, col_label in _available_sql_metric_cols.items():
        print(f"\n  {col_label} (seconds)")
        print(f"  {'Users':<10} {'P50':>10} {'P90':>10} {'P95':>10} {'P99':>10} {'Mean':>10}")
        print("  " + "-" * 60)
        for u in _conc_levels:
            vals = _sql_conc_df[_sql_conc_df['concurrent_users'] == u][col_key] / 1000
            print(f"  {u:<10} {vals.quantile(0.50):>10.2f} {vals.quantile(0.90):>10.2f} "
                  f"{vals.quantile(0.95):>10.2f} {vals.quantile(0.99):>10.2f} {vals.mean():>10.2f}")

    # ── Figure: SQL Runtime Scaling (2x2) ──
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))

    # 1. SQL Total Time Percentiles vs Concurrent Users (line chart)
    ax1 = axes[0, 0]
    _sql_pct_data: dict[str, list[float]] = {'P50': [], 'P90': [], 'P95': [], 'P99': []}
    for u in _conc_levels:
        vals = _sql_conc_df[_sql_conc_df['concurrent_users'] == u]['sql_total_duration_ms'] / 1000
        _sql_pct_data['P50'].append(vals.quantile(0.50))
        _sql_pct_data['P90'].append(vals.quantile(0.90))
        _sql_pct_data['P95'].append(vals.quantile(0.95))
        _sql_pct_data['P99'].append(vals.quantile(0.99))

    ax1.plot(_conc_levels, _sql_pct_data['P50'], marker='o', linewidth=2, markersize=8, label='P50', color='blue')
    ax1.plot(_conc_levels, _sql_pct_data['P90'], marker='s', linewidth=2, markersize=6, label='P90', color='orange')
    ax1.plot(_conc_levels, _sql_pct_data['P95'], marker='^', linewidth=2, markersize=6, label='P95', color='red')
    ax1.plot(_conc_levels, _sql_pct_data['P99'], marker='d', linewidth=2, markersize=6, label='P99', color='darkred')
    if len(_conc_levels) > 1:
        z = np.polyfit(_conc_levels, _sql_pct_data['P50'], 1)
        p = np.poly1d(z)
        ax1.plot(_conc_levels, p(_conc_levels), 'b--', alpha=0.5, label=f'P50 Trend: {z[0]:.3f}s/user')
    ax1.set_xlabel('Concurrent Users')
    ax1.set_ylabel('SQL Total Time (seconds)')
    ax1.set_title('SQL Total Time Percentiles vs Concurrent Users')
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(_conc_levels)

    # 2. SQL Time Breakdown Stacked Bar by Concurrency
    ax2 = axes[0, 1]
    _breakdown_cols = {
        'sql_execution_time_ms': 'Execution',
        'sql_compilation_time_ms': 'Compilation',
        'sql_queue_wait_ms': 'Queue Wait',
        'sql_compute_wait_ms': 'Compute Wait',
        'sql_result_fetch_ms': 'Result Fetch',
    }
    _avail_breakdown = {k: v for k, v in _breakdown_cols.items() if k in _sql_conc_df.columns}
    _bottom = np.zeros(len(_conc_levels))
    _stack_colors = ['#2ecc71', '#3498db', '#f39c12', '#9b59b6', '#1abc9c']
    for i, (col_key, col_label) in enumerate(_avail_breakdown.items()):
        means = [_sql_conc_df[_sql_conc_df['concurrent_users'] == u][col_key].mean() / 1000 for u in _conc_levels]
        means_clean = [v if not np.isnan(v) else 0 for v in means]
        ax2.bar([str(u) for u in _conc_levels], means_clean, bottom=_bottom,
                label=col_label, color=_stack_colors[i % len(_stack_colors)], edgecolor='black', alpha=0.8)
        _bottom += np.array(means_clean)
    ax2.set_xlabel('Concurrent Users')
    ax2.set_ylabel('Time (seconds)')
    ax2.set_title('SQL Time Breakdown by Concurrency')
    ax2.legend(loc='upper left', fontsize=8)
    ax2.grid(True, alpha=0.3, axis='y')

    # 3. SQL Queue Wait Scaling (line chart with P50/P90/P99)
    ax3 = axes[1, 0]
    if 'sql_queue_wait_ms' in _sql_conc_df.columns:
        _qw_pct: dict[str, list[float]] = {'P50': [], 'P90': [], 'P99': []}
        for u in _conc_levels:
            vals = _sql_conc_df[_sql_conc_df['concurrent_users'] == u]['sql_queue_wait_ms'] / 1000
            _qw_pct['P50'].append(vals.quantile(0.50))
            _qw_pct['P90'].append(vals.quantile(0.90))
            _qw_pct['P99'].append(vals.quantile(0.99))
        ax3.plot(_conc_levels, _qw_pct['P50'], marker='o', linewidth=2, markersize=8, label='P50', color='blue')
        ax3.plot(_conc_levels, _qw_pct['P90'], marker='s', linewidth=2, markersize=6, label='P90', color='orange')
        ax3.plot(_conc_levels, _qw_pct['P99'], marker='d', linewidth=2, markersize=6, label='P99', color='darkred')
        ax3.set_xlabel('Concurrent Users')
        ax3.set_ylabel('Queue Wait (seconds)')
        ax3.set_title('SQL Queue Wait Scaling')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        ax3.set_xticks(_conc_levels)
    else:
        ax3.text(0.5, 0.5, 'No queue wait data available', ha='center', va='center', transform=ax3.transAxes)
        ax3.set_title('SQL Queue Wait Scaling')

    # 4. SQL % of Total Latency by Concurrency
    ax4 = axes[1, 1]
    _sql_pct_of_total: dict[str, list[float]] = {'Mean': [], 'Median': []}
    for u in _conc_levels:
        user_df = _sql_conc_df[_sql_conc_df['concurrent_users'] == u]
        ratio = (user_df['sql_total_duration_ms'] / user_df['duration_ms']) * 100
        _sql_pct_of_total['Mean'].append(ratio.mean())
        _sql_pct_of_total['Median'].append(ratio.median())
    ax4.plot(_conc_levels, _sql_pct_of_total['Mean'], marker='o', linewidth=2, markersize=8,
             label='Mean', color='purple')
    ax4.plot(_conc_levels, _sql_pct_of_total['Median'], marker='s', linewidth=2, markersize=6,
             label='Median', color='teal')
    ax4.axhline(50, color='gray', linestyle='--', alpha=0.5, label='50% line')
    ax4.set_xlabel('Concurrent Users')
    ax4.set_ylabel('SQL Time as % of Total Latency')
    ax4.set_title('SQL Share of End-to-End Latency by Concurrency')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    ax4.set_xticks(_conc_levels)
    ax4.set_ylim([0, max(max(_sql_pct_of_total['Mean']), max(_sql_pct_of_total['Median'])) * 1.15])

    plt.tight_layout()
    fig.savefig(images_dir / "sql_runtime_vs_concurrency.png", dpi=150, bbox_inches='tight')
    print(f"\nSaved: {images_dir / 'sql_runtime_vs_concurrency.png'}")
    plt.show()

    # ── Key Insights ──
    print("\n" + "=" * 70)
    print("SQL RUNTIME SCALING INSIGHTS")
    print("=" * 70)

    # SQL P50 degradation
    _min_u = _conc_levels[0]
    _max_u = _conc_levels[-1]
    _baseline_sql_p50 = _sql_pct_data['P50'][0]
    _max_load_sql_p50 = _sql_pct_data['P50'][-1]
    _sql_p50_degradation = ((_max_load_sql_p50 - _baseline_sql_p50) / _baseline_sql_p50) * 100 if _baseline_sql_p50 > 0 else 0

    print(f"\nSQL Total Time P50: {_baseline_sql_p50:.2f}s ({_min_u} users) -> {_max_load_sql_p50:.2f}s ({_max_u} users)  ({_sql_p50_degradation:+.1f}%)")
    if _sql_p50_degradation < 25:
        print("  ✓ SQL execution scales well - warehouse handles concurrency efficiently")
    elif _sql_p50_degradation < 75:
        print("  ⚡ Moderate SQL degradation under load - monitor warehouse utilization")
    else:
        print("  ⚠️  Significant SQL degradation - consider scaling warehouse or optimizing queries")

    # Queue wait scaling
    if 'sql_queue_wait_ms' in _sql_conc_df.columns:
        _baseline_qw = _qw_pct['P50'][0]
        _max_qw = _qw_pct['P50'][-1]
        _qw_growth = _max_qw - _baseline_qw
        print(f"\nQueue Wait P50: {_baseline_qw:.2f}s ({_min_u} users) -> {_max_qw:.2f}s ({_max_u} users)  (+{_qw_growth:.2f}s)")
        if _qw_growth < 0.5:
            print("  ✓ Queue wait is flat - warehouse has spare capacity")
        elif _qw_growth < 2.0:
            print("  ⚡ Queue wait is growing - warehouse approaching capacity")
        else:
            print("  ⚠️  Queue wait grows significantly - warehouse is a bottleneck, scale up")

    # Bottleneck shift detection
    _baseline_sql_pct = _sql_pct_of_total['Mean'][0]
    _max_load_sql_pct = _sql_pct_of_total['Mean'][-1]
    _pct_shift = _max_load_sql_pct - _baseline_sql_pct

    print(f"\nSQL Share of Total Latency: {_baseline_sql_pct:.1f}% ({_min_u} users) -> {_max_load_sql_pct:.1f}% ({_max_u} users)")
    if _pct_shift > 10:
        print("  ⚠️  SQL share is INCREASING under load - warehouse is the growing bottleneck")
        print("     Recommendation: Scale warehouse, optimize slow queries, or add indexes")
    elif _pct_shift < -10:
        print("  ⚠️  SQL share is DECREASING under load - AI/Genie overhead is the growing bottleneck")
        print("     Recommendation: Investigate Genie API latency and network overhead")
    else:
        print("  ✓ Bottleneck profile is stable across concurrency levels")

elif not detailed_metrics_df.empty and 'sql_total_duration_ms' in detailed_metrics_df.columns:
    print("SQL Runtime vs Concurrency analysis requires multiple concurrency levels")
    print("Run tests with different user counts to enable this analysis")
else:
    print("No SQL execution metrics available for concurrency analysis")
    print("Run enrichment with a warehouse ID to populate SQL metrics from system.query.history")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concurrency Impact Analysis
# MAGIC
# MAGIC **What it shows:** How Genie's performance changes as the number of concurrent users increases.
# MAGIC This analysis tracks latency percentiles (P50, P90, P99) and throughput as load scales up.
# MAGIC
# MAGIC **Why it matters:** Understanding concurrency impact is critical for:
# MAGIC - **Capacity Planning**: Determine how many users your Genie deployment can handle
# MAGIC - **Resource Allocation**: Decide if you need to scale compute resources
# MAGIC - **SLA Definition**: Set realistic response time expectations under load
# MAGIC - **Cost Optimization**: Find the sweet spot between performance and cost
# MAGIC
# MAGIC **Key insights to look for:**
# MAGIC - **Linear degradation**: Latency increases proportionally with users (manageable, predictable)
# MAGIC - **Exponential degradation**: Latency spikes rapidly beyond certain concurrency (bottleneck)
# MAGIC - **Flat performance**: System handles additional users well (good scaling)
# MAGIC - **Throughput per user**: Does efficiency decrease as you add more users?
# MAGIC
# MAGIC **Analysis impact:** Use this to:
# MAGIC - Determine maximum concurrent users before performance degrades unacceptably
# MAGIC - Identify the "knee" in the performance curve where scaling becomes inefficient
# MAGIC - Justify infrastructure scaling decisions with data
# MAGIC - Set capacity alerts and auto-scaling thresholds
# MAGIC - Calculate the cost per user at different concurrency levels
# MAGIC
# MAGIC ### Visualizations (2 figures, 10 plots total)
# MAGIC
# MAGIC **Figure 1 - Core Metrics:**
# MAGIC 1. **Latency Percentiles Line Chart**: P50/P90/P95/P99 vs concurrent users
# MAGIC 2. **Box Plot by Concurrency**: Distribution spread at each level
# MAGIC 3. **Success Rate vs Users**: Reliability under load
# MAGIC 4. **Degradation Factor**: Normalized performance vs baseline
# MAGIC
# MAGIC **Figure 2 - Extended Analysis:**
# MAGIC 1. **Violin Plot**: Full distribution shape showing bimodal patterns (cache hits vs misses)
# MAGIC 2. **Latency CDF**: Cumulative distribution for SLA analysis (% requests under X seconds)
# MAGIC 3. **Heatmap**: 2D density showing where responses cluster at each concurrency level
# MAGIC 4. **Throughput Efficiency**: Requests per user - reveals diminishing returns
# MAGIC 5. **Tail Latency Amplification**: P99/P50 ratio - shows if worst-case grows faster than typical
# MAGIC 6. **Latency Scatter**: Individual requests colored by concurrency level

# COMMAND ----------

import numpy as np

if not detailed_metrics_df.empty and 'concurrent_users' in detailed_metrics_df.columns:
    print("=" * 70)
    print("CONCURRENCY IMPACT ANALYSIS")
    print("=" * 70)

    # Group by concurrent users and calculate metrics
    concurrency_stats = detailed_metrics_df.groupby('concurrent_users').agg({
        'duration_ms': ['count', 'mean', 'median',
                       lambda x: x.quantile(0.90),
                       lambda x: x.quantile(0.95),
                       lambda x: x.quantile(0.99)],
        'success': 'mean'
    }).round(2)

    concurrency_stats.columns = ['count', 'mean_ms', 'median_ms', 'p90_ms', 'p95_ms', 'p99_ms', 'success_rate']

    # Convert to seconds
    for col in ['mean_ms', 'median_ms', 'p90_ms', 'p95_ms', 'p99_ms']:
        concurrency_stats[col.replace('_ms', '_s')] = (concurrency_stats[col] / 1000).round(2)

    concurrency_stats['success_rate'] = (concurrency_stats['success_rate'] * 100).round(1)

    # Calculate efficiency metrics
    # Note: This is a simplified calculation - actual test duration would be needed for precise throughput
    concurrency_stats['requests_per_user'] = concurrency_stats['count'] / concurrency_stats.index

    print(f"\n{'Concurrent Users':<15} {'Requests':<10} {'Mean (s)':<10} {'P50 (s)':<10} {'P90 (s)':<10} {'P95 (s)':<10} {'P99 (s)':<10} {'Success %':<10}")
    print("-" * 105)
    for idx, row in concurrency_stats.iterrows():
        print(f"{idx:<15} {int(row['count']):<10} {row['mean_s']:<10.2f} {row['median_s']:<10.2f} "
              f"{row['p90_s']:<10.2f} {row['p95_s']:<10.2f} {row['p99_s']:<10.2f} {row['success_rate']:<10.1f}")

    # Visualizations
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    # 1. Latency percentiles vs concurrent users
    ax1 = axes[0, 0]
    ax1.plot(concurrency_stats.index, concurrency_stats['median_s'],
            marker='o', linewidth=2, markersize=8, label='P50 (Median)', color='blue')
    ax1.plot(concurrency_stats.index, concurrency_stats['p90_s'],
            marker='s', linewidth=2, markersize=6, label='P90', color='orange')
    ax1.plot(concurrency_stats.index, concurrency_stats['p95_s'],
            marker='^', linewidth=2, markersize=6, label='P95', color='red')
    ax1.plot(concurrency_stats.index, concurrency_stats['p99_s'],
            marker='d', linewidth=2, markersize=6, label='P99', color='darkred')
    ax1.set_xlabel('Concurrent Users')
    ax1.set_ylabel('Response Time (seconds)')
    ax1.set_title('Latency Percentiles vs Concurrent Users')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # 2. Box plot by concurrent users
    ax2 = axes[0, 1]
    concurrent_user_levels = sorted(detailed_metrics_df['concurrent_users'].unique())
    box_data = [detailed_metrics_df[detailed_metrics_df['concurrent_users'] == level]['duration_ms'] / 1000
                for level in concurrent_user_levels]
    ax2.boxplot(box_data, labels=concurrent_user_levels)
    ax2.set_xlabel('Concurrent Users')
    ax2.set_ylabel('Response Time (seconds)')
    ax2.set_title('Latency Distribution by Concurrent Users')
    ax2.grid(True, alpha=0.3, axis='y')

    # 3. Success rate vs concurrent users
    ax3 = axes[1, 0]
    ax3.plot(concurrency_stats.index, concurrency_stats['success_rate'],
            marker='o', linewidth=2, markersize=8, color='green')
    ax3.set_xlabel('Concurrent Users')
    ax3.set_ylabel('Success Rate (%)')
    ax3.set_title('Success Rate vs Concurrent Users')
    ax3.set_ylim([0, 105])
    ax3.axhline(95, color='red', linestyle='--', alpha=0.5, label='95% threshold')
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    # 4. Degradation factor (normalized to single user baseline)
    ax4 = axes[1, 1]
    if len(concurrency_stats) > 1:
        baseline_latency = concurrency_stats.iloc[0]['median_s']
        degradation_factor = concurrency_stats['median_s'] / baseline_latency

        ax4.plot(concurrency_stats.index, degradation_factor,
                marker='o', linewidth=2, markersize=8, color='purple')
        ax4.axhline(1.0, color='gray', linestyle='--', alpha=0.5, label='Baseline (1x)')
        ax4.set_xlabel('Concurrent Users')
        ax4.set_ylabel('Latency Degradation Factor')
        ax4.set_title('Performance Degradation vs Baseline')
        ax4.legend()
        ax4.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(images_dir / "concurrency_impact_analysis.png", dpi=150, bbox_inches='tight')
    print(f"\nSaved: {images_dir / 'concurrency_impact_analysis.png'}")
    plt.show()

    # Extended Concurrency Visualizations
    print("\n" + "=" * 60)
    print("EXTENDED CONCURRENCY ANALYSIS")
    print("=" * 60)

    fig2, axes2 = plt.subplots(2, 3, figsize=(18, 10))

    # 1. Violin Plot - Full distribution shape at each concurrency level
    ax_violin = axes2[0, 0]
    violin_data = [detailed_metrics_df[detailed_metrics_df['concurrent_users'] == level]['duration_ms'] / 1000
                   for level in concurrent_user_levels]
    parts = ax_violin.violinplot(violin_data, positions=range(len(concurrent_user_levels)), showmedians=True, showextrema=True)
    ax_violin.set_xticks(range(len(concurrent_user_levels)))
    ax_violin.set_xticklabels(concurrent_user_levels)
    ax_violin.set_xlabel('Concurrent Users')
    ax_violin.set_ylabel('Response Time (seconds)')
    ax_violin.set_title('Latency Distribution (Violin Plot)')
    ax_violin.grid(True, alpha=0.3, axis='y')

    # 2. Latency CDF - Cumulative Distribution Function per concurrency level
    ax_cdf = axes2[0, 1]
    for level in concurrent_user_levels:
        level_data = detailed_metrics_df[detailed_metrics_df['concurrent_users'] == level]['duration_ms'] / 1000
        sorted_latencies = np.sort(level_data)
        cdf = np.arange(1, len(sorted_latencies) + 1) / len(sorted_latencies)
        ax_cdf.plot(sorted_latencies, cdf * 100, label=f'{level} users', linewidth=2, alpha=0.8)
    ax_cdf.set_xlabel('Response Time (seconds)')
    ax_cdf.set_ylabel('Cumulative % of Requests')
    ax_cdf.set_title('Latency CDF by Concurrency Level')
    ax_cdf.legend(loc='lower right')
    ax_cdf.grid(True, alpha=0.3)
    ax_cdf.axhline(50, color='gray', linestyle='--', alpha=0.5)
    ax_cdf.axhline(95, color='orange', linestyle='--', alpha=0.5)
    ax_cdf.axhline(99, color='red', linestyle='--', alpha=0.5)

    # 3. Heatmap of Latency Distribution
    ax_heatmap = axes2[0, 2]
    max_latency = detailed_metrics_df['duration_ms'].max() / 1000
    latency_bins = np.linspace(0, min(max_latency, detailed_metrics_df['duration_ms'].quantile(0.99) / 1000), 20)
    heatmap_data = []
    for level in concurrent_user_levels:
        level_data = detailed_metrics_df[detailed_metrics_df['concurrent_users'] == level]['duration_ms'] / 1000
        hist, _ = np.histogram(level_data, bins=latency_bins, density=True)
        heatmap_data.append(hist)
    heatmap_array = np.array(heatmap_data).T
    im = ax_heatmap.imshow(heatmap_array, aspect='auto', cmap='YlOrRd', origin='lower',
                           extent=[0, len(concurrent_user_levels), latency_bins[0], latency_bins[-1]])
    ax_heatmap.set_xticks(np.arange(len(concurrent_user_levels)) + 0.5)
    ax_heatmap.set_xticklabels(concurrent_user_levels)
    ax_heatmap.set_xlabel('Concurrent Users')
    ax_heatmap.set_ylabel('Response Time (seconds)')
    ax_heatmap.set_title('Latency Density Heatmap')
    plt.colorbar(im, ax=ax_heatmap, label='Density')

    # 4. Throughput Efficiency Curve (requests per user)
    ax_efficiency = axes2[1, 0]
    efficiency = concurrency_stats['count'] / concurrency_stats.index
    ax_efficiency.plot(concurrency_stats.index, efficiency, marker='o', linewidth=2, markersize=8, color='teal')
    ax_efficiency.set_xlabel('Concurrent Users')
    ax_efficiency.set_ylabel('Requests per User')
    ax_efficiency.set_title('Throughput Efficiency (Requests/User)')
    ax_efficiency.grid(True, alpha=0.3)
    if len(efficiency) > 1:
        baseline_efficiency = efficiency.iloc[0]
        for i, (users, eff) in enumerate(zip(concurrency_stats.index, efficiency)):
            pct = (eff / baseline_efficiency) * 100
            ax_efficiency.annotate(f'{pct:.0f}%', (users, eff), textcoords="offset points",
                                  xytext=(0, 10), ha='center', fontsize=8)

    # 5. Tail Latency Amplification (P99/P50 ratio)
    ax_tail = axes2[1, 1]
    tail_ratio = concurrency_stats['p99_s'] / concurrency_stats['median_s']
    ax_tail.plot(concurrency_stats.index, tail_ratio, marker='s', linewidth=2, markersize=8, color='darkred')
    ax_tail.axhline(2.0, color='orange', linestyle='--', alpha=0.5, label='2x threshold')
    ax_tail.axhline(3.0, color='red', linestyle='--', alpha=0.5, label='3x threshold')
    ax_tail.set_xlabel('Concurrent Users')
    ax_tail.set_ylabel('P99/P50 Ratio')
    ax_tail.set_title('Tail Latency Amplification')
    ax_tail.legend()
    ax_tail.grid(True, alpha=0.3)

    # 6. Latency Scatter by Concurrency (colored by user count)
    ax_scatter = axes2[1, 2]
    scatter = ax_scatter.scatter(
        detailed_metrics_df['concurrent_users'],
        detailed_metrics_df['duration_ms'] / 1000,
        c=detailed_metrics_df['concurrent_users'],
        cmap='viridis',
        alpha=0.5,
        s=20
    )
    ax_scatter.set_xlabel('Concurrent Users')
    ax_scatter.set_ylabel('Response Time (seconds)')
    ax_scatter.set_title('Individual Request Latencies')
    ax_scatter.grid(True, alpha=0.3)
    plt.colorbar(scatter, ax=ax_scatter, label='Concurrent Users')

    plt.tight_layout()
    fig2.savefig(images_dir / "concurrency_impact_extended.png", dpi=150, bbox_inches='tight')
    print(f"Saved: {images_dir / 'concurrency_impact_extended.png'}")
    plt.show()

    # Extended insights
    print("\nExtended Concurrency Insights:")
    if len(concurrency_stats) > 1:
        min_tail_ratio = tail_ratio.min()
        max_tail_ratio = tail_ratio.max()
        print(f"  Tail Latency Ratio (P99/P50): {min_tail_ratio:.2f}x → {max_tail_ratio:.2f}x")
        if max_tail_ratio > 3:
            print("    ⚠️  High tail latency amplification - investigate outliers")
        elif max_tail_ratio > 2:
            print("    ⚡ Moderate tail latency - some requests are significantly slower")
        else:
            print("    ✓ Tail latencies are well-controlled")

        efficiency_drop = ((efficiency.iloc[-1] / efficiency.iloc[0]) - 1) * 100
        print(f"  Throughput Efficiency Change: {efficiency_drop:+.1f}%")
        if efficiency_drop < -30:
            print("    ⚠️  Significant efficiency loss with more users")
        else:
            print("    ✓ Efficiency remains reasonable under load")

    # Key insights
    print("\n" + "=" * 60)
    print("KEY INSIGHTS")
    print("=" * 60)

    if len(concurrency_stats) > 1:
        # Calculate degradation from lowest to highest concurrency
        min_users = concurrency_stats.index.min()
        max_users = concurrency_stats.index.max()

        baseline_p50 = concurrency_stats.loc[min_users, 'median_s']
        max_load_p50 = concurrency_stats.loc[max_users, 'median_s']

        baseline_p99 = concurrency_stats.loc[min_users, 'p99_s']
        max_load_p99 = concurrency_stats.loc[max_users, 'p99_s']

        p50_degradation = ((max_load_p50 - baseline_p50) / baseline_p50) * 100 if baseline_p50 > 0 else 0
        p99_degradation = ((max_load_p99 - baseline_p99) / baseline_p99) * 100 if baseline_p99 > 0 else 0

        print(f"\nPerformance Degradation ({min_users} → {max_users} users):")
        print(f"  P50 (Median): {baseline_p50:.2f}s → {max_load_p50:.2f}s ({p50_degradation:+.1f}%)")
        print(f"  P99: {baseline_p99:.2f}s → {max_load_p99:.2f}s ({p99_degradation:+.1f}%)")

        if p50_degradation > 100:
            print(f"\n  ⚠️  CRITICAL: Median latency more than doubles under load")
            print(f"      System may not be suitable for {max_users} concurrent users")
        elif p50_degradation > 50:
            print(f"\n  ⚠️  WARNING: Significant performance degradation under load")
            print(f"      Consider scaling compute resources")
        elif p50_degradation > 25:
            print(f"\n  ⚡ MODERATE: Noticeable performance impact")
            print(f"      Performance is acceptable but monitor closely")
        else:
            print(f"\n  ✓ GOOD: System scales well with additional users")

        # Per-user degradation rate
        per_user_degradation = p50_degradation / (max_users - min_users) if max_users > min_users else 0
        print(f"\n  Degradation per additional user: ~{per_user_degradation:.1f}%")

        # Success rate impact
        baseline_success = concurrency_stats.loc[min_users, 'success_rate']
        max_load_success = concurrency_stats.loc[max_users, 'success_rate']
        success_change = max_load_success - baseline_success

        if success_change < -5:
            print(f"\n  ⚠️  Success rate drops under load: {baseline_success:.1f}% → {max_load_success:.1f}%")
        else:
            print(f"\n  ✓ Success rate remains stable: {baseline_success:.1f}% → {max_load_success:.1f}%")

        # Calculate theoretical maximum capacity
        # Assuming acceptable degradation is 2x baseline latency
        if p50_degradation > 0 and max_users > min_users:
            acceptable_degradation_pct = 100  # 2x baseline = 100% increase
            estimated_max_users = min_users + (acceptable_degradation_pct / per_user_degradation) if per_user_degradation > 0 else max_users

            print(f"\n  Estimated capacity at 2x baseline latency: ~{int(estimated_max_users)} concurrent users")

    else:
        print("\nInsufficient concurrency data points for degradation analysis")
        print("Run tests with multiple concurrency levels (e.g., 1, 5, 10, 20 users)")

else:
    print("No concurrent_users data available for analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Deep Dive
# MAGIC
# MAGIC **What it shows:** Comprehensive analysis of errors and failures, including categorization,
# MAGIC patterns, temporal distribution, and identification of problematic prompts.
# MAGIC
# MAGIC **Why it matters:** Even small error rates can indicate systemic issues. Understanding error patterns is critical for:
# MAGIC - **System Reliability**: Identify and fix root causes of failures
# MAGIC - **User Experience**: Errors frustrate users and reduce trust in Genie
# MAGIC - **Prompt Engineering**: Find prompts that consistently fail and need improvement
# MAGIC - **Infrastructure Issues**: Detect timeouts, rate limiting, or resource constraints
# MAGIC
# MAGIC **Key insights to look for:**
# MAGIC - **Error clustering**: Do errors happen at specific times or under specific conditions?
# MAGIC - **Prompt-specific failures**: Are certain queries failing consistently?
# MAGIC - **Error types**: What's causing failures (timeout, auth, invalid SQL, etc.)?
# MAGIC - **Correlation with load**: Do errors increase with concurrent users?
# MAGIC
# MAGIC **Analysis impact:** Use this to:
# MAGIC - Prioritize bug fixes based on error frequency and impact
# MAGIC - Identify prompts that need rewriting or additional Genie instructions
# MAGIC - Detect infrastructure issues (timeouts, memory, rate limits)
# MAGIC - Set up monitoring and alerting for specific error patterns
# MAGIC - Improve Genie space configuration to handle edge cases

# COMMAND ----------

if not detailed_metrics_df.empty:
    print("=" * 70)
    print("ERROR DEEP DIVE")
    print("=" * 70)

    # Overall error statistics
    total_requests = len(detailed_metrics_df)
    failed_requests = (~detailed_metrics_df['success']).sum()
    error_rate = (failed_requests / total_requests) * 100 if total_requests > 0 else 0

    print(f"\n{'Metric':<40} {'Value':>20}")
    print("-" * 60)
    print(f"{'Total Requests':<40} {total_requests:>20}")
    print(f"{'Failed Requests':<40} {failed_requests:>20}")
    print(f"{'Error Rate':<40} {f'{error_rate:.2f}%':>20}")

    if failed_requests > 0:
        errors_df = detailed_metrics_df[~detailed_metrics_df['success']].copy()

        # Error categorization
        print("\n" + "=" * 60)
        print("ERROR CATEGORIZATION")
        print("=" * 60)

        # Extract error types from error messages
        def categorize_error(error_msg):
            if pd.isna(error_msg) or error_msg == '':
                return 'Unknown Error'
            error_lower = str(error_msg).lower()

            if 'timeout' in error_lower or 'timed out' in error_lower:
                return 'Timeout'
            elif 'rate limit' in error_lower or '429' in error_lower:
                return 'Rate Limit'
            elif 'auth' in error_lower or '401' in error_lower or '403' in error_lower:
                return 'Authentication/Authorization'
            elif '500' in error_lower or '502' in error_lower or '503' in error_lower:
                return 'Server Error (5xx)'
            elif '400' in error_lower or 'bad request' in error_lower:
                return 'Bad Request (4xx)'
            elif 'sql' in error_lower or 'syntax' in error_lower:
                return 'SQL Generation Error'
            elif 'connection' in error_lower or 'network' in error_lower:
                return 'Connection Error'
            else:
                return 'Other Error'

        errors_df['error_category'] = errors_df['error'].apply(categorize_error)

        error_counts = errors_df['error_category'].value_counts()
        print(f"\n{'Error Type':<35} {'Count':<10} {'% of Errors':<15} {'% of Total':<15}")
        print("-" * 75)
        for error_type, count in error_counts.items():
            pct_of_errors = (count / failed_requests) * 100
            pct_of_total = (count / total_requests) * 100
            print(f"{error_type:<35} {count:<10} {pct_of_errors:>13.1f}% {pct_of_total:>14.2f}%")

        # Temporal error analysis
        print("\n" + "=" * 60)
        print("TEMPORAL ERROR DISTRIBUTION")
        print("=" * 60)

        if 'request_started_at' in errors_df.columns:
            errors_df['request_started_at'] = pd.to_datetime(errors_df['request_started_at'])
            detailed_metrics_df['request_started_at'] = pd.to_datetime(detailed_metrics_df['request_started_at'])

            # Calculate error rate over time (using 1-minute windows)
            detailed_metrics_df['time_window'] = detailed_metrics_df['request_started_at'].dt.floor('1min')
            errors_df['time_window'] = errors_df['request_started_at'].dt.floor('1min')

            error_rate_over_time = detailed_metrics_df.groupby('time_window').agg({
                'success': ['count', lambda x: (~x).sum()]
            })
            error_rate_over_time.columns = ['total', 'failures']
            error_rate_over_time['error_rate_pct'] = (error_rate_over_time['failures'] / error_rate_over_time['total']) * 100

            print(f"\nError rate by time window:")
            print(f"  Mean error rate: {error_rate_over_time['error_rate_pct'].mean():.2f}%")
            print(f"  Max error rate: {error_rate_over_time['error_rate_pct'].max():.2f}%")
            print(f"  Windows with errors: {(error_rate_over_time['failures'] > 0).sum()} / {len(error_rate_over_time)}")

        # Errors by prompt
        print("\n" + "=" * 60)
        print("ERRORS BY PROMPT")
        print("=" * 60)

        if 'prompt' in errors_df.columns:
            prompt_errors = errors_df.groupby('prompt').size().sort_values(ascending=False)

            # Calculate failure rate per prompt
            prompt_stats = detailed_metrics_df.groupby('prompt').agg({
                'success': ['count', lambda x: (~x).sum(), 'mean']
            })
            prompt_stats.columns = ['total_requests', 'failures', 'success_rate']
            prompt_stats['failure_rate_pct'] = ((1 - prompt_stats['success_rate']) * 100).round(1)
            prompt_stats = prompt_stats[prompt_stats['failures'] > 0].sort_values('failures', ascending=False)

            print(f"\nTop 10 prompts with most failures:")
            display(prompt_stats[['total_requests', 'failures', 'failure_rate_pct']].head(10))

            # Identify prompts with 100% failure rate
            always_failing = prompt_stats[prompt_stats['success_rate'] == 0]
            if len(always_failing) > 0:
                print(f"\n⚠️  {len(always_failing)} prompt(s) have 100% failure rate:")
                display(always_failing[['total_requests', 'failures']])

        # Errors by user
        if 'user' in errors_df.columns:
            print("\n" + "=" * 60)
            print("ERRORS BY USER")
            print("=" * 60)

            user_errors = detailed_metrics_df.groupby('user').agg({
                'success': ['count', lambda x: (~x).sum(), 'mean']
            })
            user_errors.columns = ['total_requests', 'failures', 'success_rate']
            user_errors['failure_rate_pct'] = ((1 - user_errors['success_rate']) * 100).round(1)
            user_errors = user_errors[user_errors['failures'] > 0].sort_values('failures', ascending=False)

            if len(user_errors) > 0:
                print(f"\nUsers with errors:")
                display(user_errors)

        # Errors by concurrency
        if 'concurrent_users' in errors_df.columns:
            print("\n" + "=" * 60)
            print("ERRORS BY CONCURRENCY LEVEL")
            print("=" * 60)

            concurrency_errors = detailed_metrics_df.groupby('concurrent_users').agg({
                'success': ['count', lambda x: (~x).sum(), 'mean']
            })
            concurrency_errors.columns = ['total_requests', 'failures', 'success_rate']
            concurrency_errors['failure_rate_pct'] = ((1 - concurrency_errors['success_rate']) * 100).round(1)

            display(concurrency_errors)

        # Visualizations
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))

        # 1. Error type distribution (pie chart)
        ax1 = axes[0, 0]
        if len(error_counts) > 0:
            colors = plt.cm.Set3(range(len(error_counts)))
            ax1.pie(error_counts.values, labels=error_counts.index, autopct='%1.1f%%',
                   startangle=90, colors=colors)
            ax1.set_title(f'Error Type Distribution ({failed_requests} total errors)')

        # 2. Errors over time
        ax2 = axes[0, 1]
        if 'request_started_at' in errors_df.columns and len(errors_df) > 0:
            # Plot all requests with errors highlighted
            all_times = detailed_metrics_df['request_started_at']
            all_success = detailed_metrics_df['success']

            success_times = all_times[all_success]
            error_times = all_times[~all_success]

            # Create timeline scatter
            ax2.scatter(success_times, [1]*len(success_times), alpha=0.3, s=20, c='green', label='Success')
            ax2.scatter(error_times, [1]*len(error_times), alpha=0.8, s=50, c='red', marker='x', label='Error')
            ax2.set_xlabel('Time')
            ax2.set_yticks([])
            ax2.set_title('Error Timeline')
            ax2.legend()
            ax2.grid(True, alpha=0.3, axis='x')

        # 3. Error rate by concurrent users
        ax3 = axes[1, 0]
        if 'concurrent_users' in errors_df.columns and len(concurrency_errors) > 0:
            ax3.bar(concurrency_errors.index, concurrency_errors['failure_rate_pct'],
                   alpha=0.7, color='red', edgecolor='black')
            ax3.set_xlabel('Concurrent Users')
            ax3.set_ylabel('Failure Rate (%)')
            ax3.set_title('Failure Rate by Concurrency Level')
            ax3.axhline(5, color='orange', linestyle='--', alpha=0.5, label='5% threshold')
            ax3.legend()
            ax3.grid(True, alpha=0.3, axis='y')

        # 4. Top failing prompts
        ax4 = axes[1, 1]
        if 'prompt' in errors_df.columns and len(prompt_stats) > 0:
            top_failing = prompt_stats.head(10)
            labels = [p[:40] + '...' if len(p) > 40 else p for p in top_failing.index]

            bars = ax4.barh(range(len(top_failing)), top_failing['failures'],
                           alpha=0.7, color='darkred', edgecolor='black')
            ax4.set_yticks(range(len(top_failing)))
            ax4.set_yticklabels(labels, fontsize=8)
            ax4.set_xlabel('Number of Failures')
            ax4.set_title('Top 10 Failing Prompts')
            ax4.invert_yaxis()
            ax4.grid(True, alpha=0.3, axis='x')

            # Add failure rate labels
            for i, (bar, rate) in enumerate(zip(bars, top_failing['failure_rate_pct'])):
                ax4.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                        f'{rate:.1f}%', va='center', fontsize=7)

        plt.tight_layout()
        fig.savefig(images_dir / "error_deep_dive.png", dpi=150, bbox_inches='tight')
        print(f"\nSaved: {images_dir / 'error_deep_dive.png'}")
        plt.show()

        # Key insights
        print("\n" + "=" * 60)
        print("KEY INSIGHTS & RECOMMENDATIONS")
        print("=" * 60)

        print(f"\nOverall Error Rate: {error_rate:.2f}%")
        if error_rate > 10:
            print("  🚨 CRITICAL: >10% error rate indicates serious issues")
            print("     Action: Immediate investigation required")
        elif error_rate > 5:
            print("  ⚠️  WARNING: >5% error rate is concerning")
            print("     Action: Review error patterns and optimize prompts")
        elif error_rate > 1:
            print("  ⚡ MODERATE: 1-5% error rate is acceptable but should be improved")
            print("     Action: Monitor and optimize problematic prompts")
        else:
            print("  ✓ GOOD: <1% error rate is healthy")

        # Most common error type
        most_common_error = error_counts.index[0]
        most_common_count = error_counts.values[0]
        most_common_pct = (most_common_count / failed_requests) * 100

        print(f"\nMost Common Error: {most_common_error} ({most_common_count} occurrences, {most_common_pct:.1f}% of errors)")

        # Recommendations by error type
        recommendations = {
            'Timeout': '- Increase timeout settings\n- Optimize slow SQL queries\n- Check warehouse size/compute resources',
            'Rate Limit': '- Reduce request concurrency\n- Implement backoff/retry logic\n- Check Genie API rate limits',
            'Authentication/Authorization': '- Verify Databricks credentials\n- Check token expiration\n- Review workspace permissions',
            'Server Error (5xx)': '- Check Genie service health\n- Review Databricks workspace status\n- Contact Databricks support if persistent',
            'SQL Generation Error': '- Review failing prompts\n- Add examples to Genie space instructions\n- Simplify complex prompts',
            'Connection Error': '- Check network connectivity\n- Verify workspace URL\n- Review firewall/proxy settings'
        }

        if most_common_error in recommendations:
            print(f"\nRecommendations for {most_common_error}:")
            print(recommendations[most_common_error])

        # Prompt-specific recommendations
        if 'prompt' in errors_df.columns and len(always_failing) > 0:
            print(f"\n⚠️  Action Required: {len(always_failing)} prompt(s) always fail")
            print("   These prompts need immediate attention:")
            print("   - Review prompt phrasing")
            print("   - Add relevant examples to Genie space")
            print("   - Check if data exists for these queries")

        # Concurrency correlation
        if 'concurrent_users' in errors_df.columns and len(concurrency_errors) > 1:
            error_rate_increase = concurrency_errors['failure_rate_pct'].iloc[-1] - concurrency_errors['failure_rate_pct'].iloc[0]
            if error_rate_increase > 3:
                print(f"\n⚠️  Error rate increases significantly with load (+{error_rate_increase:.1f}%)")
                print("   System may not be ready for production concurrency")
                print("   Consider: scaling compute, optimizing queries, reducing concurrency")

    else:
        print("\n✅ No errors recorded in this test run!")
        print("   This indicates excellent system stability and prompt quality.")

else:
    print("No detailed metrics data available for error analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export to Unity Catalog
# MAGIC
# MAGIC **Optional:** Export the detailed metrics to a Unity Catalog table for long-term storage,
# MAGIC cross-run comparison, and analysis with SQL or BI tools.
# MAGIC
# MAGIC Uncomment the code below and set your catalog/schema to enable export.

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
