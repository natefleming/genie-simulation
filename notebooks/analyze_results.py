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
