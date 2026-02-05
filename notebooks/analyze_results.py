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
# MAGIC ## Response Time Over Test Duration
# MAGIC 
# MAGIC Scatter plot showing when each request ran and how long it took. This reveals:
# MAGIC - Whether latency increases/decreases as the test progresses
# MAGIC - Clustering of slow requests
# MAGIC - System warm-up effects

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
# MAGIC ## Performance by Message Position
# MAGIC 
# MAGIC Analyze how message position in a conversation affects latency.
# MAGIC First messages (conversation start) typically take longer than subsequent messages.

# COMMAND ----------

if not detailed_metrics_df.empty and 'message_index' in detailed_metrics_df.columns:
    # Group by message position
    position_stats = detailed_metrics_df.groupby('message_index').agg({
        'duration_ms': ['count', 'mean', 'median', 'std', 'min', 'max'],
        'success': 'mean'
    }).round(2)
    
    position_stats.columns = ['count', 'mean_ms', 'median_ms', 'std_ms', 'min_ms', 'max_ms', 'success_rate']
    position_stats['mean_s'] = (position_stats['mean_ms'] / 1000).round(2)
    position_stats['median_s'] = (position_stats['median_ms'] / 1000).round(2)
    position_stats['success_rate'] = (position_stats['success_rate'] * 100).round(1)
    
    print("Performance by Message Position in Conversation:")
    display(position_stats[['count', 'mean_s', 'median_s', 'success_rate']])
    
    # Bar chart
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Mean response time by position
    ax1 = axes[0]
    positions = position_stats.index.tolist()
    means = position_stats['mean_s'].tolist()
    stds = (position_stats['std_ms'] / 1000).tolist()
    
    bars = ax1.bar(positions, means, yerr=stds, capsize=5, alpha=0.7, color='steelblue')
    ax1.set_xlabel('Message Position in Conversation')
    ax1.set_ylabel('Mean Response Time (seconds)')
    ax1.set_title('Response Time by Message Position')
    ax1.set_xticks(positions)
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Add count labels on bars
    for bar, count in zip(bars, position_stats['count'].tolist()):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                f'n={count}', ha='center', va='bottom', fontsize=9)
    
    # Box plot by position
    ax2 = axes[1]
    position_data = [detailed_metrics_df[detailed_metrics_df['message_index'] == i]['duration_ms'] / 1000 
                     for i in sorted(detailed_metrics_df['message_index'].unique())]
    ax2.boxplot(position_data, labels=sorted(detailed_metrics_df['message_index'].unique()))
    ax2.set_xlabel('Message Position in Conversation')
    ax2.set_ylabel('Response Time (seconds)')
    ax2.set_title('Response Time Distribution by Position')
    ax2.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.show()
    
    # Summary insights
    if len(position_stats) > 1:
        first_msg_time = position_stats.loc[0, 'mean_s'] if 0 in position_stats.index else position_stats.iloc[0]['mean_s']
        other_msg_time = position_stats[position_stats.index > 0]['mean_s'].mean() if len(position_stats) > 1 else first_msg_time
        print(f"\nInsight: First message avg: {first_msg_time:.2f}s, Subsequent messages avg: {other_msg_time:.2f}s")
        if first_msg_time > other_msg_time * 1.2:
            print("  -> First messages are significantly slower (expected due to conversation initialization)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance by Unique Prompt
# MAGIC 
# MAGIC Breakdown of performance by distinct prompts/questions. Identifies which queries are slowest.

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
