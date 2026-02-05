# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-Run Comparison Analysis
# MAGIC 
# MAGIC This notebook combines results from multiple load test runs to analyze how latency scales with 
# MAGIC concurrent users and identify correlations between variables.
# MAGIC 
# MAGIC ## Usage
# MAGIC 1. Run multiple load tests with different user counts (e.g., 5, 10, 20 users)
# MAGIC 2. Enter the results directory paths below (one per line)
# MAGIC 3. Run all cells to see the combined analysis
# MAGIC 
# MAGIC ## Output
# MAGIC All visualizations are saved to a timestamped directory under `results/comparisons/`.

# COMMAND ----------

# MAGIC %pip install pandas matplotlib seaborn scipy --quiet --upgrade

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
from scipy import stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# List available results directories
results_base = "results"
available_dirs = sorted(glob(f"{results_base}/genie_*"), key=lambda d: Path(d).stat().st_mtime, reverse=True)

print("Available results directories (newest first):")
print("-" * 80)
for i, d in enumerate(available_dirs[:20]):
    dir_name = os.path.basename(d)
    # Try to extract user count from directory name
    parts = dir_name.split("_")
    user_part = [p for p in parts if "users" in p]
    user_info = user_part[0] if user_part else "unknown"
    mtime = datetime.fromtimestamp(Path(d).stat().st_mtime).strftime("%Y-%m-%d %H:%M")
    print(f"  {i+1:2}. {dir_name}")
    print(f"      Users: {user_info}, Modified: {mtime}")

# COMMAND ----------

# Widget for selecting multiple results directories (one per line)
default_dirs = "\n".join(available_dirs[:3]) if len(available_dirs) >= 3 else "\n".join(available_dirs)

dbutils.widgets.multiline("results_dirs", default_dirs, "Results Directories (one per line)")

# COMMAND ----------

# Parse selected directories
results_dirs_text = dbutils.widgets.get("results_dirs")
results_dirs = [d.strip() for d in results_dirs_text.strip().split("\n") if d.strip()]

print(f"Selected {len(results_dirs)} directories for comparison:")
for d in results_dirs:
    exists = "✓" if Path(d).exists() else "✗ NOT FOUND"
    print(f"  {exists} {d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Combine Data

# COMMAND ----------

# Load detailed metrics from each directory
dfs = []
run_metadata = []

for dir_path in results_dirs:
    metrics_path = Path(dir_path) / "detailed_metrics.csv"
    if metrics_path.exists():
        df = pd.read_csv(metrics_path)
        
        # Extract run info from directory name or data
        run_id = os.path.basename(dir_path)
        concurrent_users = df['concurrent_users'].iloc[0] if 'concurrent_users' in df.columns else None
        
        # If run_id column doesn't exist, add it
        if 'run_id' not in df.columns:
            df['run_id'] = run_id
        
        dfs.append(df)
        
        run_metadata.append({
            'run_id': run_id,
            'directory': dir_path,
            'concurrent_users': concurrent_users,
            'total_requests': len(df),
            'success_rate': df['success'].mean() * 100 if 'success' in df.columns else None,
            'mean_duration_s': df['duration_ms'].mean() / 1000 if 'duration_ms' in df.columns else None,
        })
        
        print(f"✓ Loaded {len(df)} records from {run_id}")
    else:
        print(f"✗ No detailed_metrics.csv found in {dir_path}")

if not dfs:
    raise ValueError("No data loaded. Please check the selected directories.")

# Combine all data
combined_df = pd.concat(dfs, ignore_index=True)
metadata_df = pd.DataFrame(run_metadata)

print(f"\nTotal records: {len(combined_df)}")
print(f"Unique runs: {combined_df['run_id'].nunique()}")
print(f"Concurrent user levels: {sorted(combined_df['concurrent_users'].unique())}")

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
