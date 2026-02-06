# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-Run Comparison Analysis
# MAGIC 
# MAGIC This notebook combines results from multiple load test runs to analyze how latency scales with 
# MAGIC concurrent users and identify correlations between variables.
# MAGIC 
# MAGIC ## Usage
# MAGIC 1. Run multiple load tests with different user counts (e.g., 5, 10, 20 users)
# MAGIC 2. Enter the results directory paths below (comma-separated)
# MAGIC 3. Run all cells to see the combined analysis
# MAGIC 
# MAGIC ## Output
# MAGIC All visualizations are saved to a timestamped directory under `results/comparisons/`.

# COMMAND ----------

# MAGIC %pip install pandas matplotlib seaborn scipy python-dotenv --quiet --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
from datetime import datetime
from glob import glob
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from dotenv import load_dotenv
from scipy import stats

# Load environment variables from .env file
# Try multiple paths since CWD varies between local and Databricks environments
load_dotenv("../.env")
load_dotenv(".env")

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

# Find all genie_* directories using relative path
available_dirs = []
if Path(results_base).exists():
    available_dirs = sorted(
        glob(f"{results_base}/genie_*"), 
        key=lambda d: Path(d).stat().st_mtime if Path(d).exists() else 0, 
        reverse=True
    )

print(f"Found {len(available_dirs)} results directories")

def extract_metadata_from_dirname(dir_name: str) -> dict:
    """Extract metadata (space_id, users) from results directory name."""
    parts = dir_name.split("_")
    user_part = [p for p in parts if "users" in p]
    user_info = user_part[0] if user_part else "unknown"
    
    # Extract space_id - it's typically the second component after the prefix
    # Format: genie_loadtest_<space_id>_<users>users_<duration>_<timestamp>
    space_id = "unknown"
    if len(parts) >= 3:
        # Check if the third part looks like a space_id (not "users" or a duration)
        potential_space_id = parts[2]
        if not any(x in potential_space_id.lower() for x in ["users", "s", "m", "h"]) or len(potential_space_id) > 10:
            space_id = potential_space_id
    
    return {"space_id": space_id, "users": user_info}

print("\nAvailable results directories (newest first):")
print("-" * 80)
if available_dirs:
    for i, d in enumerate(available_dirs[:20]):
        dir_name = os.path.basename(d)
        metadata = extract_metadata_from_dirname(dir_name)
        try:
            mtime = datetime.fromtimestamp(Path(d).stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        except Exception:
            mtime = "unknown"
        print(f"  {i+1:2}. {dir_name}")
        print(f"      Space: {metadata['space_id']}, Users: {metadata['users']}, Modified: {mtime}")
else:
    print("  (No results directories found)")
    print(f"\n  Searched in: {results_base}")
    print(f"\n  Looking for directories matching: genie_*")

# COMMAND ----------

# Widget for selecting multiple results directories (comma-separated)
# Default to all available directories
default_dirs = ", ".join(available_dirs)

dbutils.widgets.text("results_dirs", default_dirs, "Results Directories (comma-separated, results/ prefix optional)")
dbutils.widgets.text("space_id_filter", "", "Filter by Space ID (leave blank for all)")

# Widget for SQL warehouse ID (used for enrichment with query.history and access.audit)
default_warehouse_id = os.environ.get("GENIE_WAREHOUSE_ID", "")
dbutils.widgets.text("warehouse_id", default_warehouse_id, "SQL Warehouse ID (for enrichment)")

# Widgets for system table paths (fully-qualified: catalog.schema.table)
default_query_history_table = os.environ.get("SYSTEM_QUERY_HISTORY_TABLE", "system.query.history")
default_access_audit_table = os.environ.get("SYSTEM_ACCESS_AUDIT_TABLE", "system.access.audit")
dbutils.widgets.text("query_history_table", default_query_history_table, "Query History Table")
dbutils.widgets.text("access_audit_table", default_access_audit_table, "Access Audit Table")

# COMMAND ----------

# Parse selected directories (comma-separated)
results_dirs_text = dbutils.widgets.get("results_dirs")
raw_dirs = [d.strip() for d in results_dirs_text.strip().split(",") if d.strip()]

# Normalize paths - prepend results/ if not already present
results_dirs = []
for d in raw_dirs:
    # Skip empty entries
    if not d:
        continue
    # If absolute path, use as-is
    if os.path.isabs(d):
        results_dirs.append(d)
    # If already has results/ prefix, use as-is
    elif d.startswith("results/"):
        results_dirs.append(d)
    else:
        # Prepend results/ for bare directory names
        results_dirs.append(f"results/{d}")

# Debug output
print(f"Current working directory: {os.getcwd()}")
print(f"Selected {len(results_dirs)} directories for comparison:")
for d in results_dirs:
    exists = "✓" if Path(d).exists() else "✗ NOT FOUND"
    print(f"  {exists} {d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Combine Data

# COMMAND ----------

# Get space_id filter value
space_id_filter = dbutils.widgets.get("space_id_filter").strip()
if space_id_filter:
    print(f"Filtering by space_id: {space_id_filter}")
else:
    print("No space_id filter applied - loading all runs")

# Load detailed metrics from each directory
dfs = []
run_metadata = []

print("\nLoading data from directories:")
print("-" * 60)

for dir_path in results_dirs:
    metrics_path = Path(dir_path) / "detailed_metrics.csv"
    print(f"\nChecking: {dir_path}")
    print(f"  Directory exists: {os.path.isdir(dir_path)}")
    print(f"  Metrics file: {metrics_path}")
    print(f"  Metrics exists: {metrics_path.exists()}")
    
    # List directory contents if it exists
    if os.path.isdir(dir_path):
        contents = os.listdir(dir_path)
        print(f"  Contents: {contents[:5]}{'...' if len(contents) > 5 else ''}")
    
    if metrics_path.exists():
        df = pd.read_csv(metrics_path)
        
        # Extract run info from directory name or data
        run_id = os.path.basename(dir_path)
        concurrent_users = df['concurrent_users'].iloc[0] if 'concurrent_users' in df.columns else None
        
        # Extract space_id from data or directory name
        if 'space_id' in df.columns:
            space_id = df['space_id'].iloc[0]
        else:
            # Fallback: extract from directory name
            dir_metadata = extract_metadata_from_dirname(run_id)
            space_id = dir_metadata['space_id']
            df['space_id'] = space_id
        
        # If run_id column doesn't exist, add it
        if 'run_id' not in df.columns:
            df['run_id'] = run_id
        
        dfs.append(df)
        
        run_metadata.append({
            'run_id': run_id,
            'directory': dir_path,
            'space_id': space_id,
            'concurrent_users': concurrent_users,
            'total_requests': len(df),
            'success_rate': df['success'].mean() * 100 if 'success' in df.columns else None,
            'mean_duration_s': df['duration_ms'].mean() / 1000 if 'duration_ms' in df.columns else None,
        })
        
        print(f"✓ Loaded {len(df)} records from {run_id} (space: {space_id})")
    else:
        print(f"✗ No detailed_metrics.csv found in {dir_path}")

if not dfs:
    raise ValueError("No data loaded. Please check the selected directories.")

# Combine all data
combined_df = pd.concat(dfs, ignore_index=True)
metadata_df = pd.DataFrame(run_metadata)

# Apply space_id filter if provided
if space_id_filter:
    original_count = len(combined_df)
    combined_df = combined_df[combined_df['space_id'] == space_id_filter]
    metadata_df = metadata_df[metadata_df['space_id'] == space_id_filter]
    filtered_count = len(combined_df)
    print(f"\nFiltered from {original_count} to {filtered_count} records (space_id: {space_id_filter})")
    
    if len(combined_df) == 0:
        available_spaces = metadata_df['space_id'].unique() if not metadata_df.empty else []
        raise ValueError(f"No data for space_id '{space_id_filter}'. Available: {list(available_spaces)}")

print(f"\nTotal records: {len(combined_df)}")
print(f"Unique runs: {combined_df['run_id'].nunique()}")
print(f"Space IDs: {sorted(combined_df['space_id'].unique())}")
print(f"Concurrent user levels: {sorted(combined_df['concurrent_users'].unique())}")

# COMMAND ----------

# Enrich each directory's metrics if needed
warehouse_id = dbutils.widgets.get("warehouse_id").strip() or os.environ.get("GENIE_WAREHOUSE_ID", "")
query_history_table = dbutils.widgets.get("query_history_table").strip() or os.environ.get("SYSTEM_QUERY_HISTORY_TABLE", "system.query.history")
access_audit_table = dbutils.widgets.get("access_audit_table").strip() or os.environ.get("SYSTEM_ACCESS_AUDIT_TABLE", "system.access.audit")

if warehouse_id:
    import sys
    sys.path.insert(0, "..")  # Add parent directory to path for genie_simulation module
    from genie_simulation.enrich_metrics import enrich_results_directory

    any_enriched = False
    for dir_path in results_dirs:
        metrics_path = Path(dir_path) / "detailed_metrics.csv"
        if not metrics_path.exists():
            continue
        check_df = pd.read_csv(metrics_path)
        sql_count = check_df['sql'].notna().sum() if 'sql' in check_df.columns else 0
        enriched_count = check_df['sql_total_duration_ms'].notna().sum() if 'sql_total_duration_ms' in check_df.columns else 0
        if sql_count > 0 and enriched_count == 0:
            print(f"Enriching {dir_path}...")
            try:
                enrich_results_directory(
                    results_dir=dir_path,
                    warehouse_id=warehouse_id,
                    query_history_table=query_history_table,
                    access_audit_table=access_audit_table,
                )
                any_enriched = True
            except Exception as e:
                print(f"  Enrichment failed for {dir_path}: {e}")
    
    if any_enriched:
        # Reload combined data
        dfs = []
        for dir_path in results_dirs:
            metrics_path = Path(dir_path) / "detailed_metrics.csv"
            if metrics_path.exists():
                df = pd.read_csv(metrics_path)
                if 'run_id' not in df.columns:
                    df['run_id'] = os.path.basename(dir_path)
                dfs.append(df)
        combined_df = pd.concat(dfs, ignore_index=True)
        if space_id_filter:
            combined_df = combined_df[combined_df['space_id'] == space_id_filter]
        print(f"\nReloaded {len(combined_df)} records after enrichment")
else:
    print("No warehouse ID provided - skipping enrichment")

# COMMAND ----------

# Display run metadata summary
print("=" * 80)
print("RUN METADATA SUMMARY")
print("=" * 80)
display(metadata_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Output Directory

# COMMAND ----------

# Create timestamped output directory
comparison_dir = Path("results/comparisons") / f"compare_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
images_dir = comparison_dir / "images"
images_dir.mkdir(parents=True, exist_ok=True)

print(f"Output directory: {comparison_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Run Statistics

# COMMAND ----------

# Aggregate statistics by concurrent users (across all runs)
cross_run_stats = combined_df.groupby('concurrent_users').agg({
    'duration_ms': ['count', 'mean', 'median', 'std',
                    lambda x: x.quantile(0.90),
                    lambda x: x.quantile(0.95),
                    lambda x: x.quantile(0.99)],
    'success': 'mean'
}).round(2)

cross_run_stats.columns = ['count', 'mean_ms', 'median_ms', 'std_ms', 'p90_ms', 'p95_ms', 'p99_ms', 'success_rate']

# Convert to seconds
for col in ['mean_ms', 'median_ms', 'std_ms', 'p90_ms', 'p95_ms', 'p99_ms']:
    cross_run_stats[col.replace('_ms', '_s')] = (cross_run_stats[col] / 1000).round(2)

cross_run_stats['success_rate'] = (cross_run_stats['success_rate'] * 100).round(1)

print("=" * 80)
print("CROSS-RUN STATISTICS BY CONCURRENT USERS")
print("=" * 80)
display(cross_run_stats[['count', 'mean_s', 'median_s', 'p90_s', 'p95_s', 'p99_s', 'success_rate']])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Correlation Analysis
# MAGIC 
# MAGIC **What it shows:** Relationships between concurrent users, latency, and other variables.
# MAGIC 
# MAGIC **Key correlations to look for:**
# MAGIC - **concurrent_users vs duration_ms**: How much does latency increase with users?
# MAGIC - **message_index vs duration_ms**: Do later messages in a conversation take longer?
# MAGIC - **success vs concurrent_users**: Does reliability decrease under load?

# COMMAND ----------

# Select numeric columns for correlation analysis
numeric_cols = ['concurrent_users', 'duration_ms', 'message_index', 'response_size']
available_cols = [c for c in numeric_cols if c in combined_df.columns]

# Add success as numeric
if 'success' in combined_df.columns:
    combined_df['success_numeric'] = combined_df['success'].astype(int)
    available_cols.append('success_numeric')

# Calculate correlation matrix
correlation_matrix = combined_df[available_cols].corr()

print("=" * 80)
print("CORRELATION MATRIX")
print("=" * 80)
print("\nPearson Correlation Coefficients:")
display(correlation_matrix.round(3))

# Statistical significance
print("\nKey Correlations with P-values:")
if 'concurrent_users' in available_cols and 'duration_ms' in available_cols:
    corr, pval = stats.pearsonr(combined_df['concurrent_users'], combined_df['duration_ms'])
    print(f"  concurrent_users vs duration_ms: r={corr:.3f}, p={pval:.2e}")
    
if 'message_index' in available_cols and 'duration_ms' in available_cols:
    corr, pval = stats.pearsonr(combined_df['message_index'], combined_df['duration_ms'])
    print(f"  message_index vs duration_ms: r={corr:.3f}, p={pval:.2e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations
# MAGIC 
# MAGIC All visualizations are saved to the output directory.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Figure 1: Latency Percentiles vs Concurrent Users
# MAGIC 
# MAGIC **Primary scaling curve** - Shows how P50, P90, P95, P99 latencies change with concurrent users.

# COMMAND ----------

fig, ax = plt.subplots(figsize=(12, 6))

user_levels = sorted(combined_df['concurrent_users'].unique())

ax.plot(cross_run_stats.index, cross_run_stats['median_s'], 
        marker='o', linewidth=2, markersize=8, label='P50 (Median)', color='blue')
ax.plot(cross_run_stats.index, cross_run_stats['p90_s'], 
        marker='s', linewidth=2, markersize=6, label='P90', color='orange')
ax.plot(cross_run_stats.index, cross_run_stats['p95_s'], 
        marker='^', linewidth=2, markersize=6, label='P95', color='red')
ax.plot(cross_run_stats.index, cross_run_stats['p99_s'], 
        marker='d', linewidth=2, markersize=6, label='P99', color='darkred')

# Add trend line for P50
z = np.polyfit(cross_run_stats.index, cross_run_stats['median_s'], 1)
p = np.poly1d(z)
ax.plot(cross_run_stats.index, p(cross_run_stats.index), 
        'b--', alpha=0.5, label=f'P50 Trend: {z[0]:.3f}s/user')

ax.set_xlabel('Concurrent Users', fontsize=12)
ax.set_ylabel('Response Time (seconds)', fontsize=12)
ax.set_title('Latency Percentiles vs Concurrent Users (Combined Runs)', fontsize=14)
ax.legend(loc='upper left')
ax.grid(True, alpha=0.3)
ax.set_xticks(user_levels)

plt.tight_layout()
fig.savefig(images_dir / "01_latency_percentiles_vs_users.png", dpi=150, bbox_inches='tight')
print(f"Saved: {images_dir / '01_latency_percentiles_vs_users.png'}")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Figure 2: Latency Distribution (Violin Plot)
# MAGIC 
# MAGIC **Full distribution shape** - Reveals bimodal patterns, outliers, and distribution width at each level.

# COMMAND ----------

fig, ax = plt.subplots(figsize=(14, 6))

violin_data = [combined_df[combined_df['concurrent_users'] == level]['duration_ms'] / 1000 
               for level in user_levels]

parts = ax.violinplot(violin_data, positions=range(len(user_levels)), showmedians=True, showextrema=True)

# Color the violins
for pc in parts['bodies']:
    pc.set_facecolor('steelblue')
    pc.set_alpha(0.7)

ax.set_xticks(range(len(user_levels)))
ax.set_xticklabels(user_levels)
ax.set_xlabel('Concurrent Users', fontsize=12)
ax.set_ylabel('Response Time (seconds)', fontsize=12)
ax.set_title('Latency Distribution by Concurrent Users', fontsize=14)
ax.grid(True, alpha=0.3, axis='y')

plt.tight_layout()
fig.savefig(images_dir / "02_latency_distribution_violin.png", dpi=150, bbox_inches='tight')
print(f"Saved: {images_dir / '02_latency_distribution_violin.png'}")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Figure 3: Success Rate vs Concurrent Users
# MAGIC 
# MAGIC **Reliability under load** - Shows if success rate degrades as users increase.

# COMMAND ----------

fig, ax = plt.subplots(figsize=(10, 5))

ax.plot(cross_run_stats.index, cross_run_stats['success_rate'], 
        marker='o', linewidth=2, markersize=10, color='green')

ax.axhline(99, color='gold', linestyle='--', alpha=0.7, label='99% threshold')
ax.axhline(95, color='red', linestyle='--', alpha=0.7, label='95% threshold')

ax.set_xlabel('Concurrent Users', fontsize=12)
ax.set_ylabel('Success Rate (%)', fontsize=12)
ax.set_title('Success Rate vs Concurrent Users', fontsize=14)
ax.set_ylim([max(0, cross_run_stats['success_rate'].min() - 5), 102])
ax.legend()
ax.grid(True, alpha=0.3)
ax.set_xticks(user_levels)

plt.tight_layout()
fig.savefig(images_dir / "03_success_rate_vs_users.png", dpi=150, bbox_inches='tight')
print(f"Saved: {images_dir / '03_success_rate_vs_users.png'}")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Figure 4: Throughput vs Concurrent Users
# MAGIC 
# MAGIC **Capacity analysis** - Shows requests processed at each concurrency level.

# COMMAND ----------

fig, ax = plt.subplots(figsize=(10, 5))

ax.bar(range(len(user_levels)), cross_run_stats['count'], 
       color='teal', alpha=0.7, edgecolor='black')

ax.set_xticks(range(len(user_levels)))
ax.set_xticklabels(user_levels)
ax.set_xlabel('Concurrent Users', fontsize=12)
ax.set_ylabel('Total Requests', fontsize=12)
ax.set_title('Request Volume by Concurrent Users', fontsize=14)
ax.grid(True, alpha=0.3, axis='y')

# Add count labels
for i, count in enumerate(cross_run_stats['count']):
    ax.text(i, count + max(cross_run_stats['count']) * 0.02, f'{int(count)}', 
            ha='center', fontsize=10)

plt.tight_layout()
fig.savefig(images_dir / "04_throughput_vs_users.png", dpi=150, bbox_inches='tight')
print(f"Saved: {images_dir / '04_throughput_vs_users.png'}")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Figure 5: Correlation Heatmap
# MAGIC 
# MAGIC **Variable relationships** - Visual correlation matrix showing how variables relate.

# COMMAND ----------

fig, ax = plt.subplots(figsize=(10, 8))

# Create heatmap
mask = np.triu(np.ones_like(correlation_matrix, dtype=bool), k=1)
sns.heatmap(correlation_matrix, annot=True, cmap='RdBu_r', center=0, 
            fmt='.2f', square=True, ax=ax, mask=mask,
            cbar_kws={'label': 'Correlation Coefficient'})

ax.set_title('Correlation Heatmap', fontsize=14)

plt.tight_layout()
fig.savefig(images_dir / "05_correlation_heatmap.png", dpi=150, bbox_inches='tight')
print(f"Saved: {images_dir / '05_correlation_heatmap.png'}")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Figure 6: Scaling Efficiency
# MAGIC 
# MAGIC **Diminishing returns analysis** - Shows requests per user and efficiency degradation.

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Requests per user
ax1 = axes[0]
requests_per_user = cross_run_stats['count'] / cross_run_stats.index
ax1.plot(cross_run_stats.index, requests_per_user, 
         marker='o', linewidth=2, markersize=8, color='purple')
ax1.set_xlabel('Concurrent Users', fontsize=12)
ax1.set_ylabel('Requests per User', fontsize=12)
ax1.set_title('Throughput Efficiency', fontsize=14)
ax1.grid(True, alpha=0.3)
ax1.set_xticks(user_levels)

# Efficiency percentage vs baseline
ax2 = axes[1]
if len(requests_per_user) > 0:
    baseline = requests_per_user.iloc[0]
    efficiency_pct = (requests_per_user / baseline) * 100
    ax2.plot(cross_run_stats.index, efficiency_pct, 
             marker='s', linewidth=2, markersize=8, color='darkorange')
    ax2.axhline(100, color='gray', linestyle='--', alpha=0.5, label='Baseline (100%)')
    ax2.axhline(80, color='red', linestyle='--', alpha=0.5, label='80% threshold')
    ax2.set_xlabel('Concurrent Users', fontsize=12)
    ax2.set_ylabel('Efficiency vs Baseline (%)', fontsize=12)
    ax2.set_title('Scaling Efficiency', fontsize=14)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(user_levels)

plt.tight_layout()
fig.savefig(images_dir / "06_scaling_efficiency.png", dpi=150, bbox_inches='tight')
print(f"Saved: {images_dir / '06_scaling_efficiency.png'}")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Figure 7: Tail Latency Analysis
# MAGIC 
# MAGIC **Worst-case performance** - Shows P99/P50 ratio and absolute tail penalty.

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# P99/P50 ratio
ax1 = axes[0]
tail_ratio = cross_run_stats['p99_s'] / cross_run_stats['median_s']
ax1.plot(cross_run_stats.index, tail_ratio, 
         marker='o', linewidth=2, markersize=8, color='darkred')
ax1.axhline(2.0, color='orange', linestyle='--', alpha=0.7, label='2x threshold')
ax1.axhline(3.0, color='red', linestyle='--', alpha=0.7, label='3x threshold')
ax1.set_xlabel('Concurrent Users', fontsize=12)
ax1.set_ylabel('P99/P50 Ratio', fontsize=12)
ax1.set_title('Tail Latency Amplification', fontsize=14)
ax1.legend()
ax1.grid(True, alpha=0.3)
ax1.set_xticks(user_levels)

# Absolute tail penalty (P99 - P50)
ax2 = axes[1]
tail_penalty = cross_run_stats['p99_s'] - cross_run_stats['median_s']
ax2.bar(range(len(user_levels)), tail_penalty, 
        color='crimson', alpha=0.7, edgecolor='black')
ax2.set_xticks(range(len(user_levels)))
ax2.set_xticklabels(user_levels)
ax2.set_xlabel('Concurrent Users', fontsize=12)
ax2.set_ylabel('P99 - P50 (seconds)', fontsize=12)
ax2.set_title('Absolute Tail Latency Penalty', fontsize=14)
ax2.grid(True, alpha=0.3, axis='y')

plt.tight_layout()
fig.savefig(images_dir / "07_tail_latency_analysis.png", dpi=150, bbox_inches='tight')
print(f"Saved: {images_dir / '07_tail_latency_analysis.png'}")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Figure 8: Per-Run Comparison
# MAGIC 
# MAGIC **Individual run analysis** - Side-by-side comparison of each input run.

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# Mean latency by run
ax1 = axes[0, 0]
run_stats = combined_df.groupby('run_id').agg({
    'duration_ms': 'mean',
    'success': 'mean',
    'concurrent_users': 'first'
}).sort_values('concurrent_users')
run_stats['duration_s'] = run_stats['duration_ms'] / 1000

bars = ax1.barh(range(len(run_stats)), run_stats['duration_s'], 
                color='steelblue', alpha=0.7, edgecolor='black')
ax1.set_yticks(range(len(run_stats)))
ax1.set_yticklabels([f"{r[:30]}..." if len(r) > 30 else r for r in run_stats.index], fontsize=8)
ax1.set_xlabel('Mean Response Time (seconds)', fontsize=10)
ax1.set_title('Mean Latency by Run', fontsize=12)
ax1.grid(True, alpha=0.3, axis='x')

# Success rate by run
ax2 = axes[0, 1]
success_rates = run_stats['success'] * 100
colors = ['green' if s >= 95 else 'orange' if s >= 90 else 'red' for s in success_rates]
ax2.barh(range(len(run_stats)), success_rates, color=colors, alpha=0.7, edgecolor='black')
ax2.set_yticks(range(len(run_stats)))
ax2.set_yticklabels([f"{r[:30]}..." if len(r) > 30 else r for r in run_stats.index], fontsize=8)
ax2.set_xlabel('Success Rate (%)', fontsize=10)
ax2.set_title('Success Rate by Run', fontsize=12)
ax2.axvline(95, color='red', linestyle='--', alpha=0.5)
ax2.set_xlim([0, 105])
ax2.grid(True, alpha=0.3, axis='x')

# Request count by run
ax3 = axes[1, 0]
request_counts = combined_df.groupby('run_id').size().reindex(run_stats.index)
ax3.barh(range(len(run_stats)), request_counts, color='teal', alpha=0.7, edgecolor='black')
ax3.set_yticks(range(len(run_stats)))
ax3.set_yticklabels([f"{r[:30]}..." if len(r) > 30 else r for r in run_stats.index], fontsize=8)
ax3.set_xlabel('Total Requests', fontsize=10)
ax3.set_title('Request Volume by Run', fontsize=12)
ax3.grid(True, alpha=0.3, axis='x')

# Latency box plot by run
ax4 = axes[1, 1]
run_order = run_stats.index.tolist()
box_data = [combined_df[combined_df['run_id'] == run]['duration_ms'] / 1000 for run in run_order]
ax4.boxplot(box_data, vert=False)
ax4.set_yticklabels([f"{r[:25]}..." if len(r) > 25 else r for r in run_order], fontsize=8)
ax4.set_xlabel('Response Time (seconds)', fontsize=10)
ax4.set_title('Latency Distribution by Run', fontsize=12)
ax4.grid(True, alpha=0.3, axis='x')

plt.tight_layout()
fig.savefig(images_dir / "08_per_run_comparison.png", dpi=150, bbox_inches='tight')
print(f"Saved: {images_dir / '08_per_run_comparison.png'}")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Recommendations

# COMMAND ----------

print("=" * 80)
print("SUMMARY AND RECOMMENDATIONS")
print("=" * 80)

# Scaling analysis
min_users = cross_run_stats.index.min()
max_users = cross_run_stats.index.max()
baseline_p50 = cross_run_stats.loc[min_users, 'median_s']
max_load_p50 = cross_run_stats.loc[max_users, 'median_s']
p50_degradation = ((max_load_p50 - baseline_p50) / baseline_p50) * 100 if baseline_p50 > 0 else 0

print(f"\n📊 SCALING CHARACTERISTICS ({min_users} → {max_users} users):")
print(f"   P50 Latency: {baseline_p50:.2f}s → {max_load_p50:.2f}s ({p50_degradation:+.1f}%)")

per_user_increase = (max_load_p50 - baseline_p50) / (max_users - min_users) if max_users > min_users else 0
print(f"   Per-user latency increase: ~{per_user_increase:.3f}s/user")

# Scaling type assessment
if p50_degradation < 25:
    print(f"   ✓ EXCELLENT: Near-linear scaling, system handles load well")
elif p50_degradation < 50:
    print(f"   ⚡ GOOD: Sub-linear scaling, acceptable performance degradation")
elif p50_degradation < 100:
    print(f"   ⚠️ MODERATE: Noticeable performance degradation under load")
else:
    print(f"   🚨 POOR: Super-linear scaling, system struggles with concurrency")

# Success rate analysis
baseline_success = cross_run_stats.loc[min_users, 'success_rate']
max_load_success = cross_run_stats.loc[max_users, 'success_rate']
success_change = max_load_success - baseline_success

print(f"\n📈 RELIABILITY:")
print(f"   Success Rate: {baseline_success:.1f}% → {max_load_success:.1f}% ({success_change:+.1f}%)")
if max_load_success >= 99:
    print(f"   ✓ EXCELLENT: >99% success rate maintained")
elif max_load_success >= 95:
    print(f"   ⚡ GOOD: >95% success rate")
elif max_load_success >= 90:
    print(f"   ⚠️ MODERATE: Success rate dropping, investigate failures")
else:
    print(f"   🚨 POOR: <90% success rate, critical reliability issues")

# Tail latency analysis
max_tail_ratio = tail_ratio.max()
print(f"\n📉 TAIL LATENCY:")
print(f"   Max P99/P50 ratio: {max_tail_ratio:.2f}x")
if max_tail_ratio < 2:
    print(f"   ✓ EXCELLENT: Tail latencies well controlled")
elif max_tail_ratio < 3:
    print(f"   ⚡ GOOD: Moderate tail latency amplification")
else:
    print(f"   ⚠️ WARNING: High tail latency - investigate outliers")

# Capacity estimation
print(f"\n🎯 CAPACITY ESTIMATE:")
# Find the point where latency exceeds 2x baseline
acceptable_latency = baseline_p50 * 2
users_below_threshold = cross_run_stats[cross_run_stats['median_s'] <= acceptable_latency].index
if len(users_below_threshold) > 0:
    max_recommended = users_below_threshold.max()
    print(f"   Recommended max users (2x baseline latency): ~{max_recommended}")
else:
    print(f"   ⚠️ All tested concurrency levels exceed 2x baseline latency")

# Correlation insights
print(f"\n🔗 CORRELATION INSIGHTS:")
if 'concurrent_users' in available_cols and 'duration_ms' in available_cols:
    corr = correlation_matrix.loc['concurrent_users', 'duration_ms']
    if abs(corr) > 0.7:
        print(f"   Strong correlation between users and latency (r={corr:.3f})")
    elif abs(corr) > 0.4:
        print(f"   Moderate correlation between users and latency (r={corr:.3f})")
    else:
        print(f"   Weak correlation between users and latency (r={corr:.3f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Execution Metrics Comparison
# MAGIC
# MAGIC **What it shows:** How SQL execution metrics from `system.query.history` change across
# MAGIC concurrency levels. Includes time breakdown, AI overhead, and bottleneck analysis.
# MAGIC
# MAGIC **Data sources:**
# MAGIC - `system.query.history`: SQL execution, compilation, queue, compute wait times
# MAGIC - `system.access.audit`: AI overhead (NL-to-SQL inference time)

# COMMAND ----------

# Check if SQL execution metrics are available
has_sql_metrics = 'sql_total_duration_ms' in combined_df.columns and combined_df['sql_total_duration_ms'].notna().any()

if has_sql_metrics:
    sql_df = combined_df[combined_df['sql_total_duration_ms'].notna()].copy()
    
    print("=" * 80)
    print("SQL EXECUTION METRICS COMPARISON ACROSS CONCURRENCY LEVELS")
    print("=" * 80)
    
    # SQL time breakdown by concurrency level
    sql_cols = {
        'sql_execution_time_ms': 'Execution',
        'sql_compilation_time_ms': 'Compilation',
        'sql_queue_wait_ms': 'Queue Wait',
        'sql_compute_wait_ms': 'Compute Wait',
        'sql_result_fetch_ms': 'Result Fetch',
    }
    
    available_sql_cols = {k: v for k, v in sql_cols.items() if k in sql_df.columns}
    
    print(f"\n{'Users':<10}", end="")
    for label in available_sql_cols.values():
        print(f" {label:>14}", end="")
    if 'ai_overhead_ms' in sql_df.columns:
        print(f" {'AI Overhead':>14}", end="")
    print(f" {'SQL Total':>14} {'E2E Total':>14}")
    print("-" * (10 + 14 * (len(available_sql_cols) + 2 + (1 if 'ai_overhead_ms' in sql_df.columns else 0))))
    
    for users in sorted(sql_df['concurrent_users'].unique()):
        user_df = sql_df[sql_df['concurrent_users'] == users]
        print(f"{users:<10}", end="")
        for col in available_sql_cols.keys():
            avg_val = user_df[col].mean() / 1000 if col in user_df.columns else 0
            print(f" {avg_val:>13.2f}s", end="")
        if 'ai_overhead_ms' in sql_df.columns:
            avg_ai = user_df['ai_overhead_ms'].mean() / 1000 if user_df['ai_overhead_ms'].notna().any() else 0
            print(f" {avg_ai:>13.2f}s", end="")
        avg_sql_total = user_df['sql_total_duration_ms'].mean() / 1000
        avg_e2e = user_df['duration_ms'].mean() / 1000
        print(f" {avg_sql_total:>13.2f}s {avg_e2e:>13.2f}s")
    
    # Visualization: SQL time breakdown stacked by concurrency
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    user_levels = sorted(sql_df['concurrent_users'].unique())
    
    # 1. Stacked bar: Time breakdown per concurrency level
    ax1 = axes[0, 0]
    breakdown_data = {}
    for col, label in available_sql_cols.items():
        breakdown_data[label] = [sql_df[sql_df['concurrent_users'] == u][col].mean() / 1000 for u in user_levels]
    if 'ai_overhead_ms' in sql_df.columns:
        breakdown_data['AI Overhead'] = [
            sql_df[(sql_df['concurrent_users'] == u) & sql_df['ai_overhead_ms'].notna()]['ai_overhead_ms'].mean() / 1000
            if sql_df[(sql_df['concurrent_users'] == u) & sql_df['ai_overhead_ms'].notna()].shape[0] > 0 else 0
            for u in user_levels
        ]
    
    bottom = np.zeros(len(user_levels))
    colors_stacked = ['#2ecc71', '#3498db', '#f39c12', '#9b59b6', '#1abc9c', '#e74c3c']
    for i, (label, vals) in enumerate(breakdown_data.items()):
        vals_clean = [v if not np.isnan(v) else 0 for v in vals]
        ax1.bar([str(u) for u in user_levels], vals_clean, bottom=bottom,
               label=label, color=colors_stacked[i % len(colors_stacked)], edgecolor='black', alpha=0.8)
        bottom += np.array(vals_clean)
    ax1.set_xlabel('Concurrent Users')
    ax1.set_ylabel('Time (seconds)')
    ax1.set_title('Average Time Breakdown by Concurrency')
    ax1.legend(loc='upper left', fontsize=8)
    ax1.grid(True, alpha=0.3, axis='y')
    
    # 2. SQL total time percentiles across concurrency
    ax2 = axes[0, 1]
    percentiles_data = {
        'P50': [], 'P90': [], 'P95': [], 'P99': []
    }
    for u in user_levels:
        user_sql = sql_df[sql_df['concurrent_users'] == u]['sql_total_duration_ms'] / 1000
        percentiles_data['P50'].append(user_sql.quantile(0.50))
        percentiles_data['P90'].append(user_sql.quantile(0.90))
        percentiles_data['P95'].append(user_sql.quantile(0.95))
        percentiles_data['P99'].append(user_sql.quantile(0.99))
    
    x = range(len(user_levels))
    width = 0.2
    for i, (label, vals) in enumerate(percentiles_data.items()):
        ax2.bar([xi + i * width for xi in x], vals, width, label=label, alpha=0.8)
    ax2.set_xlabel('Concurrent Users')
    ax2.set_ylabel('SQL Total Time (seconds)')
    ax2.set_title('SQL Total Time Percentiles by Concurrency')
    ax2.set_xticks([xi + 1.5 * width for xi in x])
    ax2.set_xticklabels([str(u) for u in user_levels])
    ax2.legend()
    ax2.grid(True, alpha=0.3, axis='y')
    
    # 3. Bottleneck distribution across concurrency
    ax3 = axes[1, 0]
    if 'sql_bottleneck' in sql_df.columns and sql_df['sql_bottleneck'].notna().any():
        bottleneck_data = {}
        for u in user_levels:
            user_bottlenecks = sql_df[sql_df['concurrent_users'] == u]['sql_bottleneck'].value_counts(normalize=True) * 100
            bottleneck_data[str(u)] = user_bottlenecks
        
        bottleneck_df_chart = pd.DataFrame(bottleneck_data).fillna(0)
        bottleneck_df_chart.T.plot(kind='bar', stacked=True, ax=ax3, edgecolor='black', alpha=0.8)
        ax3.set_xlabel('Concurrent Users')
        ax3.set_ylabel('Percentage')
        ax3.set_title('Bottleneck Distribution by Concurrency')
        ax3.legend(loc='upper left', fontsize=7, ncol=2)
        ax3.set_xticklabels(ax3.get_xticklabels(), rotation=0)
        ax3.grid(True, alpha=0.3, axis='y')
    else:
        ax3.text(0.5, 0.5, 'No bottleneck data available', ha='center', va='center', transform=ax3.transAxes)
        ax3.set_title('Bottleneck Distribution by Concurrency')
    
    # 4. Speed category distribution across concurrency
    ax4 = axes[1, 1]
    if 'sql_speed_category' in sql_df.columns and sql_df['sql_speed_category'].notna().any():
        speed_data = {}
        speed_order = ['FAST', 'MODERATE', 'SLOW', 'CRITICAL']
        speed_colors_chart = {'FAST': '#2ecc71', 'MODERATE': '#f39c12', 'SLOW': '#e67e22', 'CRITICAL': '#e74c3c'}
        
        for u in user_levels:
            user_speeds = sql_df[sql_df['concurrent_users'] == u]['sql_speed_category'].value_counts(normalize=True) * 100
            speed_data[str(u)] = user_speeds
        
        speed_df_chart = pd.DataFrame(speed_data).reindex(speed_order).fillna(0)
        speed_df_chart.T.plot(kind='bar', stacked=True, ax=ax4,
                             color=[speed_colors_chart[s] for s in speed_order if s in speed_df_chart.index],
                             edgecolor='black', alpha=0.8)
        ax4.set_xlabel('Concurrent Users')
        ax4.set_ylabel('Percentage')
        ax4.set_title('Speed Category Distribution by Concurrency')
        ax4.legend(loc='upper left', fontsize=8)
        ax4.set_xticklabels(ax4.get_xticklabels(), rotation=0)
        ax4.grid(True, alpha=0.3, axis='y')
    else:
        ax4.text(0.5, 0.5, 'No speed category data available', ha='center', va='center', transform=ax4.transAxes)
        ax4.set_title('Speed Category Distribution by Concurrency')
    
    plt.tight_layout()
    fig.savefig(comparison_dir / "sql_metrics_comparison.png", dpi=150, bbox_inches='tight')
    print(f"\nSaved: {comparison_dir / 'sql_metrics_comparison.png'}")
    plt.show()
    
    # AI overhead comparison
    if 'ai_overhead_ms' in sql_df.columns and sql_df['ai_overhead_ms'].notna().any():
        print(f"\n{'AI OVERHEAD BY CONCURRENCY (seconds)':<40}")
        print("-" * 60)
        for users in user_levels:
            user_df = sql_df[(sql_df['concurrent_users'] == users) & sql_df['ai_overhead_ms'].notna()]
            if len(user_df) > 0:
                ai_mean = user_df['ai_overhead_ms'].mean() / 1000
                ai_p50 = user_df['ai_overhead_ms'].median() / 1000
                ai_p90 = user_df['ai_overhead_ms'].quantile(0.90) / 1000
                print(f"  {users:>3} users:  mean={ai_mean:.2f}s  median={ai_p50:.2f}s  P90={ai_p90:.2f}s  ({len(user_df)} samples)")
    
    print(f"\n✓ SQL execution metrics comparison complete")
else:
    print("No SQL execution metrics available for comparison")
    print("Run enrichment with a warehouse ID to populate SQL metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Data

# COMMAND ----------

# Save combined metrics
combined_csv_path = comparison_dir / "combined_metrics.csv"
combined_df.to_csv(combined_csv_path, index=False)
print(f"Saved combined metrics: {combined_csv_path}")

# Save cross-run statistics
stats_csv_path = comparison_dir / "comparison_summary.csv"
cross_run_stats.to_csv(stats_csv_path)
print(f"Saved comparison summary: {stats_csv_path}")

# Save run metadata
metadata_csv_path = comparison_dir / "run_metadata.csv"
metadata_df.to_csv(metadata_csv_path, index=False)
print(f"Saved run metadata: {metadata_csv_path}")

print(f"\n✓ All outputs saved to: {comparison_dir}")

# COMMAND ----------

# List all output files
print("\nOutput files:")
for f in sorted(comparison_dir.rglob("*")):
    if f.is_file():
        print(f"  {f.relative_to(comparison_dir)}")
