"""
Subprocess-based Locust runner for Databricks notebooks.

This module provides a way to run Genie load tests from within a Databricks notebook
by executing Locust via subprocess. This avoids gevent monkey-patching issues.

Usage:
    from genie_simulation.notebook_runner import run_load_test, LoadTestResults
    
    results = run_load_test(
        conversations_file="../conversations.yaml",
        space_id="your-space-id",
        user_count=10,
        spawn_rate=2,
        run_time="5m",
    )
    
    display(results.stats_df)
"""

from __future__ import annotations

import os
import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


def generate_results_dir(base_prefix: str, space_id: str, user_count: int, run_time: str) -> str:
    """
    Generate a results directory path with config and timestamp.
    
    Args:
        base_prefix: Base prefix for the directory (e.g., "genie_loadtest")
        space_id: Genie space ID
        user_count: Number of concurrent users in the simulation
        run_time: Duration of the test (e.g., "60s", "5m")
    
    Returns:
        Directory path like "results/genie_loadtest_01JCQK9M123_5users_60s_20260204_153045"
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"results/{base_prefix}_{space_id}_{user_count}users_{run_time}_{timestamp}"


@dataclass
class LoadTestResults:
    """Container for load test results from Locust CSV output."""
    
    stats_df: pd.DataFrame
    stats_history_df: pd.DataFrame
    failures_df: pd.DataFrame
    exceptions_df: pd.DataFrame
    results_dir: str
    
    def display_summary(self) -> None:
        """Print a summary of the results."""
        if self.stats_df.empty:
            print("No results available")
            return
        
        # Get the aggregated row (usually the last one with "Aggregated" name)
        agg_row = self.stats_df[self.stats_df["Name"] == "Aggregated"]
        if agg_row.empty:
            agg_row = self.stats_df.iloc[[-1]]
        
        print("\n" + "=" * 60)
        print("LOAD TEST SUMMARY")
        print("=" * 60)
        for _, row in agg_row.iterrows():
            print(f"Total Requests: {row.get('Request Count', 'N/A')}")
            print(f"Failure Count: {row.get('Failure Count', 'N/A')}")
            print(f"Median Response Time: {row.get('Median Response Time', 'N/A')} ms")
            print(f"Average Response Time: {row.get('Average Response Time', 'N/A')} ms")
            print(f"Min Response Time: {row.get('Min Response Time', 'N/A')} ms")
            print(f"Max Response Time: {row.get('Max Response Time', 'N/A')} ms")
            print(f"Requests/s: {row.get('Requests/s', 'N/A')}")
        print("=" * 60)


def run_load_test(
    conversations_file: str | Path,
    space_id: str,
    user_count: int = 10,
    spawn_rate: int = 2,
    run_time: str = "5m",
    min_wait: float = 8.0,
    max_wait: float = 30.0,
    sample_size: int | None = None,
    sample_seed: int | None = None,
    csv_prefix: str = "genie_loadtest",
    locustfile: str | None = None,
    verbose: bool = True,
    databricks_host: str | None = None,
    databricks_token: str | None = None,
) -> LoadTestResults:
    """
    Run a Genie load test using Locust via subprocess.
    
    Args:
        conversations_file: Path to the conversations YAML file
        space_id: Genie space ID
        user_count: Number of concurrent simulated users
        spawn_rate: Users to spawn per second
        run_time: Duration of the test (e.g., "5m", "300s", "1h")
        min_wait: Minimum wait time between messages in seconds
        max_wait: Maximum wait time between messages in seconds
        sample_size: Number of conversations to sample (None = use all)
        sample_seed: Random seed for reproducible sampling
        csv_prefix: Prefix for output CSV files
        locustfile: Path to the locustfile (default: genie_locustfile.py in same dir)
        verbose: If True, print Locust output in real-time
        databricks_host: Databricks workspace URL (for subprocess auth)
        databricks_token: Databricks auth token (for subprocess auth)
    
    Returns:
        LoadTestResults containing DataFrames with test metrics
    """
    # Resolve paths
    conversations_path = Path(conversations_file).resolve()
    
    if locustfile is None:
        # Default to genie_locustfile.py in the notebooks directory
        locustfile = str(Path(__file__).parent.parent / "notebooks" / "genie_locustfile.py")
    
    locustfile_path = Path(locustfile).resolve()
    
    if not conversations_path.exists():
        raise FileNotFoundError(f"Conversations file not found: {conversations_path}")
    
    if not locustfile_path.exists():
        raise FileNotFoundError(f"Locustfile not found: {locustfile_path}")
    
    # Set environment variables for the locustfile
    env = os.environ.copy()
    env["GENIE_SPACE_ID"] = space_id
    env["GENIE_CONVERSATIONS_FILE"] = str(conversations_path)
    env["GENIE_MIN_WAIT"] = str(min_wait)
    env["GENIE_MAX_WAIT"] = str(max_wait)
    
    if sample_size is not None:
        env["GENIE_SAMPLE_SIZE"] = str(sample_size)
    if sample_seed is not None:
        env["GENIE_SAMPLE_SEED"] = str(sample_seed)
    
    # Pass Databricks credentials for subprocess authentication
    if databricks_host:
        env["DATABRICKS_HOST"] = databricks_host
    if databricks_token:
        env["DATABRICKS_TOKEN"] = databricks_token
    
    # Pass simulation config for dynamic filename generation
    env["GENIE_USER_COUNT"] = str(user_count)
    env["GENIE_RUN_TIME"] = run_time
    
    # Generate results directory with config suffix
    results_dir = generate_results_dir(csv_prefix, space_id, user_count, run_time)
    
    # Pass results directory to locustfile for detailed metrics
    env["GENIE_RESULTS_DIR"] = results_dir
    
    # Create the results directory
    os.makedirs(results_dir, exist_ok=True)
    
    # Build locust command (Locust will create files like locust_stats.csv)
    cmd = (
        f"locust "
        f"--host=https://localhost "
        f"--users={user_count} "
        f"--spawn-rate={spawn_rate} "
        f"--run-time={run_time} "
        f"--headless "
        f"--only-summary "
        f"--locustfile={locustfile_path} "
        f"--csv={results_dir}/locust"
    )
    
    print(f"Starting Locust load test...")
    print(f"  Users: {user_count}")
    print(f"  Spawn Rate: {spawn_rate}/s")
    print(f"  Duration: {run_time}")
    print(f"  Conversations: {conversations_path}")
    print(f"  Results: {results_dir}")
    print("-" * 60)
    
    # Run locust via subprocess
    process = subprocess.Popen(
        shlex.split(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        env=env,
    )
    
    # Stream output
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        if line:
            line = line.strip()
            if "CPU" in line and "overload" in line.lower():
                print(f"WARNING: {line}")
            elif verbose:
                print(line)
    
    return_code = process.wait()
    
    print("-" * 60)
    if return_code == 0:
        print("Load test completed successfully")
    else:
        print(f"Load test finished with return code: {return_code}")
    
    # Rename Locust files to generic names
    _rename_locust_files(results_dir)
    
    # Read CSV results
    return _read_csv_results(results_dir)


def _rename_locust_files(results_dir: str) -> None:
    """Rename Locust output files to generic names."""
    renames = [
        ("locust_stats.csv", "stats.csv"),
        ("locust_stats_history.csv", "stats_history.csv"),
        ("locust_failures.csv", "failures.csv"),
        ("locust_exceptions.csv", "exceptions.csv"),
    ]
    
    for old_name, new_name in renames:
        old_path = Path(results_dir) / old_name
        new_path = Path(results_dir) / new_name
        if old_path.exists():
            old_path.rename(new_path)


def _read_csv_results(results_dir: str) -> LoadTestResults:
    """Read Locust CSV output files from a results directory."""
    stats_file = f"{results_dir}/stats.csv"
    history_file = f"{results_dir}/stats_history.csv"
    failures_file = f"{results_dir}/failures.csv"
    exceptions_file = f"{results_dir}/exceptions.csv"
    
    stats_df = pd.DataFrame()
    stats_history_df = pd.DataFrame()
    failures_df = pd.DataFrame()
    exceptions_df = pd.DataFrame()
    
    if Path(stats_file).exists():
        stats_df = pd.read_csv(stats_file)
    else:
        print(f"Warning: Stats file not found: {stats_file}")
    
    if Path(history_file).exists():
        stats_history_df = pd.read_csv(history_file)
    
    if Path(failures_file).exists():
        failures_df = pd.read_csv(failures_file)
    
    if Path(exceptions_file).exists():
        exceptions_df = pd.read_csv(exceptions_file)
    
    return LoadTestResults(
        stats_df=stats_df,
        stats_history_df=stats_history_df,
        failures_df=failures_df,
        exceptions_df=exceptions_df,
        results_dir=results_dir,
    )


def run_cached_load_test(
    conversations_file: str | Path,
    space_id: str,
    lakebase_client_id: str,
    lakebase_client_secret: str,
    lakebase_instance: str,
    warehouse_id: str,
    user_count: int = 10,
    spawn_rate: int = 2,
    run_time: str = "5m",
    min_wait: float = 8.0,
    max_wait: float = 30.0,
    sample_size: int | None = None,
    sample_seed: int | None = None,
    cache_ttl: int = 86400,
    similarity_threshold: float = 0.85,
    lru_capacity: int = 100,
    csv_prefix: str = "genie_cached_loadtest",
    locustfile: str | None = None,
    verbose: bool = True,
    databricks_host: str | None = None,
    databricks_token: str | None = None,
) -> LoadTestResults:
    """
    Run a cached Genie load test using Locust via subprocess.
    
    Args:
        conversations_file: Path to the conversations YAML file
        space_id: Genie space ID
        lakebase_client_id: Lakebase client ID
        lakebase_client_secret: Lakebase client secret
        lakebase_instance: Lakebase instance name
        warehouse_id: Warehouse ID
        user_count: Number of concurrent simulated users
        spawn_rate: Users to spawn per second
        run_time: Duration of the test (e.g., "5m", "300s", "1h")
        min_wait: Minimum wait time between messages in seconds
        max_wait: Maximum wait time between messages in seconds
        sample_size: Number of conversations to sample (None = use all)
        sample_seed: Random seed for reproducible sampling
        cache_ttl: Cache TTL in seconds
        similarity_threshold: Semantic similarity threshold
        lru_capacity: LRU cache capacity
        csv_prefix: Prefix for output CSV files
        locustfile: Path to the locustfile (default: genie_locustfile_cached.py)
        verbose: If True, print Locust output in real-time
        databricks_host: Databricks workspace URL (for subprocess auth)
        databricks_token: Databricks auth token (for subprocess auth)
    
    Returns:
        LoadTestResults containing DataFrames with test metrics
    """
    # Resolve paths
    conversations_path = Path(conversations_file).resolve()
    
    if locustfile is None:
        locustfile = str(Path(__file__).parent.parent / "notebooks" / "genie_locustfile_cached.py")
    
    locustfile_path = Path(locustfile).resolve()
    
    if not conversations_path.exists():
        raise FileNotFoundError(f"Conversations file not found: {conversations_path}")
    
    if not locustfile_path.exists():
        raise FileNotFoundError(f"Locustfile not found: {locustfile_path}")
    
    # Set environment variables for the locustfile
    env = os.environ.copy()
    env["GENIE_SPACE_ID"] = space_id
    env["GENIE_CONVERSATIONS_FILE"] = str(conversations_path)
    env["GENIE_MIN_WAIT"] = str(min_wait)
    env["GENIE_MAX_WAIT"] = str(max_wait)
    env["GENIE_LAKEBASE_CLIENT_ID"] = lakebase_client_id
    env["GENIE_LAKEBASE_CLIENT_SECRET"] = lakebase_client_secret
    env["GENIE_LAKEBASE_INSTANCE"] = lakebase_instance
    env["GENIE_WAREHOUSE_ID"] = warehouse_id
    env["GENIE_CACHE_TTL"] = str(cache_ttl)
    env["GENIE_SIMILARITY_THRESHOLD"] = str(similarity_threshold)
    env["GENIE_LRU_CAPACITY"] = str(lru_capacity)
    
    if sample_size is not None:
        env["GENIE_SAMPLE_SIZE"] = str(sample_size)
    if sample_seed is not None:
        env["GENIE_SAMPLE_SEED"] = str(sample_seed)
    
    # Pass Databricks credentials for subprocess authentication
    if databricks_host:
        env["DATABRICKS_HOST"] = databricks_host
    if databricks_token:
        env["DATABRICKS_TOKEN"] = databricks_token
    
    # Pass simulation config for dynamic filename generation
    env["GENIE_USER_COUNT"] = str(user_count)
    env["GENIE_RUN_TIME"] = run_time
    
    # Generate results directory with config suffix
    results_dir = generate_results_dir(csv_prefix, space_id, user_count, run_time)
    
    # Pass results directory to locustfile for detailed metrics
    env["GENIE_RESULTS_DIR"] = results_dir
    
    # Create the results directory
    os.makedirs(results_dir, exist_ok=True)
    
    # Build locust command (Locust will create files like locust_stats.csv)
    cmd = (
        f"locust "
        f"--host=https://localhost "
        f"--users={user_count} "
        f"--spawn-rate={spawn_rate} "
        f"--run-time={run_time} "
        f"--headless "
        f"--only-summary "
        f"--locustfile={locustfile_path} "
        f"--csv={results_dir}/locust"
    )
    
    print(f"Starting Cached Locust load test...")
    print(f"  Users: {user_count}")
    print(f"  Spawn Rate: {spawn_rate}/s")
    print(f"  Duration: {run_time}")
    print(f"  Conversations: {conversations_path}")
    print(f"  Cache: LRU={lru_capacity}, TTL={cache_ttl}s, Threshold={similarity_threshold}")
    print(f"  Results: {results_dir}")
    print("-" * 60)
    
    # Run locust via subprocess
    process = subprocess.Popen(
        shlex.split(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        env=env,
    )
    
    # Stream output
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        if line:
            line = line.strip()
            if "CPU" in line and "overload" in line.lower():
                print(f"WARNING: {line}")
            elif verbose:
                print(line)
    
    return_code = process.wait()
    
    print("-" * 60)
    if return_code == 0:
        print("Cached load test completed successfully")
    else:
        print(f"Cached load test finished with return code: {return_code}")
    
    # Rename Locust files to generic names
    _rename_locust_files(results_dir)
    
    # Read CSV results
    return _read_csv_results(results_dir)


def run_in_memory_semantic_load_test(
    conversations_file: str | Path,
    space_id: str,
    warehouse_id: str,
    user_count: int = 10,
    spawn_rate: int = 2,
    run_time: str = "5m",
    min_wait: float = 8.0,
    max_wait: float = 30.0,
    sample_size: int | None = None,
    sample_seed: int | None = None,
    cache_ttl: int = 86400,
    similarity_threshold: float = 0.85,
    context_similarity_threshold: float = 0.80,
    capacity: int = 1000,
    context_window_size: int = 3,
    embedding_model: str = "databricks-gte-large-en",
    lru_capacity: int = 100,
    csv_prefix: str = "genie_in_memory_semantic_loadtest",
    locustfile: str | None = None,
    verbose: bool = True,
    databricks_host: str | None = None,
    databricks_token: str | None = None,
) -> LoadTestResults:
    """
    Run an in-memory semantic cached Genie load test using Locust via subprocess.
    
    Unlike run_cached_load_test, this does not require PostgreSQL or Lakebase
    credentials - all cache storage is in memory.
    
    Args:
        conversations_file: Path to the conversations YAML file
        space_id: Genie space ID
        warehouse_id: Warehouse ID for SQL execution
        user_count: Number of concurrent simulated users
        spawn_rate: Users to spawn per second
        run_time: Duration of the test (e.g., "5m", "300s", "1h")
        min_wait: Minimum wait time between messages in seconds
        max_wait: Maximum wait time between messages in seconds
        sample_size: Number of conversations to sample (None = use all)
        sample_seed: Random seed for reproducible sampling
        cache_ttl: Cache TTL in seconds
        similarity_threshold: Question similarity threshold
        context_similarity_threshold: Context similarity threshold
        capacity: Maximum cache entries (LRU eviction when full)
        context_window_size: Number of previous conversation turns to include
        embedding_model: Embedding model name
        lru_capacity: Optional L1 LRU cache capacity (0 to disable)
        csv_prefix: Prefix for output CSV files
        locustfile: Path to the locustfile (default: genie_locustfile_in_memory_semantic.py)
        verbose: If True, print Locust output in real-time
        databricks_host: Databricks workspace URL (for subprocess auth)
        databricks_token: Databricks auth token (for subprocess auth)
    
    Returns:
        LoadTestResults containing DataFrames with test metrics
    """
    # Resolve paths
    conversations_path = Path(conversations_file).resolve()
    
    if locustfile is None:
        locustfile = str(Path(__file__).parent.parent / "notebooks" / "genie_locustfile_in_memory_semantic.py")
    
    locustfile_path = Path(locustfile).resolve()
    
    if not conversations_path.exists():
        raise FileNotFoundError(f"Conversations file not found: {conversations_path}")
    
    if not locustfile_path.exists():
        raise FileNotFoundError(f"Locustfile not found: {locustfile_path}")
    
    # Set environment variables for the locustfile
    env = os.environ.copy()
    env["GENIE_SPACE_ID"] = space_id
    env["GENIE_CONVERSATIONS_FILE"] = str(conversations_path)
    env["GENIE_MIN_WAIT"] = str(min_wait)
    env["GENIE_MAX_WAIT"] = str(max_wait)
    env["GENIE_WAREHOUSE_ID"] = warehouse_id
    env["GENIE_CACHE_TTL"] = str(cache_ttl)
    env["GENIE_SIMILARITY_THRESHOLD"] = str(similarity_threshold)
    env["GENIE_CONTEXT_SIMILARITY_THRESHOLD"] = str(context_similarity_threshold)
    env["GENIE_CACHE_CAPACITY"] = str(capacity)
    env["GENIE_CONTEXT_WINDOW_SIZE"] = str(context_window_size)
    env["GENIE_EMBEDDING_MODEL"] = embedding_model
    env["GENIE_LRU_CAPACITY"] = str(lru_capacity)
    
    if sample_size is not None:
        env["GENIE_SAMPLE_SIZE"] = str(sample_size)
    if sample_seed is not None:
        env["GENIE_SAMPLE_SEED"] = str(sample_seed)
    
    # Pass Databricks credentials for subprocess authentication
    if databricks_host:
        env["DATABRICKS_HOST"] = databricks_host
    if databricks_token:
        env["DATABRICKS_TOKEN"] = databricks_token
    
    # Pass simulation config for dynamic filename generation
    env["GENIE_USER_COUNT"] = str(user_count)
    env["GENIE_RUN_TIME"] = run_time
    
    # Generate results directory with config suffix
    results_dir = generate_results_dir(csv_prefix, space_id, user_count, run_time)
    
    # Pass results directory to locustfile for detailed metrics
    env["GENIE_RESULTS_DIR"] = results_dir
    
    # Create the results directory
    os.makedirs(results_dir, exist_ok=True)
    
    # Build locust command (Locust will create files like locust_stats.csv)
    cmd = (
        f"locust "
        f"--host=https://localhost "
        f"--users={user_count} "
        f"--spawn-rate={spawn_rate} "
        f"--run-time={run_time} "
        f"--headless "
        f"--only-summary "
        f"--locustfile={locustfile_path} "
        f"--csv={results_dir}/locust"
    )
    
    print(f"Starting In-Memory Semantic Cached Locust load test...")
    print(f"  Users: {user_count}")
    print(f"  Spawn Rate: {spawn_rate}/s")
    print(f"  Duration: {run_time}")
    print(f"  Conversations: {conversations_path}")
    print(f"  Cache: Capacity={capacity}, TTL={cache_ttl}s, Similarity={similarity_threshold}")
    print(f"  Context: Window={context_window_size}, Similarity={context_similarity_threshold}")
    print(f"  LRU: {lru_capacity if lru_capacity > 0 else 'disabled'}")
    print(f"  Results: {results_dir}")
    print("-" * 60)
    
    # Run locust via subprocess
    process = subprocess.Popen(
        shlex.split(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        env=env,
    )
    
    # Stream output
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        if line:
            line = line.strip()
            if "CPU" in line and "overload" in line.lower():
                print(f"WARNING: {line}")
            elif verbose:
                print(line)
    
    return_code = process.wait()
    
    print("-" * 60)
    if return_code == 0:
        print("In-memory semantic cached load test completed successfully")
    else:
        print(f"In-memory semantic cached load test finished with return code: {return_code}")
    
    # Rename Locust files to generic names
    _rename_locust_files(results_dir)
    
    # Read CSV results
    return _read_csv_results(results_dir)


def cleanup_results(base_prefix: str = "genie_loadtest") -> None:
    """
    Remove results directories generated by load tests.
    
    Uses glob patterns to find and remove all matching directories, including
    those with config suffixes (e.g., results/genie_loadtest_5users_60s_*).
    
    Args:
        base_prefix: Base prefix for the directories (e.g., "genie_loadtest")
    """
    import shutil
    from glob import glob
    
    pattern = f"results/{base_prefix}_*"
    
    for dir_path in glob(pattern):
        if Path(dir_path).is_dir():
            shutil.rmtree(dir_path)
            print(f"Removed directory: {dir_path}")


def read_results_from_dir(results_dir: str) -> LoadTestResults:
    """
    Read load test results directly from a results directory.
    
    Use this function to recover results if the notebook crashes after the
    load test completes but before results are displayed. The CSV files
    persist on disk even if the Python kernel crashes.
    
    Args:
        results_dir: Path to the results directory (e.g., "results/genie_loadtest_5users_60s_20260204_153045")
    
    Returns:
        LoadTestResults containing DataFrames with test metrics
    
    Example:
        # If notebook crashed, run this in a new cell to recover results:
        from genie_simulation.notebook_runner import read_results_from_dir
        
        results = read_results_from_dir("results/genie_loadtest_5users_60s_20260204_153045")
        display(results.stats_df)
        results.display_summary()
    """
    return _read_csv_results(results_dir)


def list_results_dirs(base_prefix: str = "genie_loadtest") -> list[str]:
    """
    List all results directories matching a prefix.
    
    Args:
        base_prefix: Base prefix for the directories (e.g., "genie_loadtest")
    
    Returns:
        List of matching directory paths, sorted by modification time (newest first)
    """
    from glob import glob
    
    pattern = f"results/{base_prefix}_*"
    dirs = [d for d in glob(pattern) if Path(d).is_dir()]
    # Sort by modification time, newest first
    dirs.sort(key=lambda d: Path(d).stat().st_mtime, reverse=True)
    return dirs


def print_results_summary(results_dir: str) -> None:
    """
    Print a formatted summary of load test results from a results directory.
    
    This is a convenience function that reads CSV files and prints a
    comprehensive summary including latency percentiles and per-endpoint
    breakdown.
    
    Args:
        results_dir: Path to the results directory
    
    Example:
        # Quick recovery after crash:
        from genie_simulation.notebook_runner import print_results_summary, list_results_dirs
        
        # List available results directories
        dirs = list_results_dirs()
        print(dirs)
        
        # Print summary for the most recent one
        print_results_summary(dirs[0])
    """
    results = _read_csv_results(results_dir)
    
    if results.stats_df.empty:
        print(f"No results found in '{results_dir}'")
        print(f"Expected file: {results_dir}/stats.csv")
        return
    
    print("=" * 70)
    print("LOAD TEST RESULTS (from CSV)")
    print("=" * 70)
    
    # Get aggregated stats
    agg_row = results.stats_df[results.stats_df["Name"] == "Aggregated"]
    if agg_row.empty:
        agg_row = results.stats_df.iloc[[-1]]
    
    for _, row in agg_row.iterrows():
        print(f"\n{'Metric':<35} {'Value':>20}")
        print("-" * 70)
        print(f"{'Total Requests':<35} {row.get('Request Count', 'N/A'):>20}")
        print(f"{'Failed Requests':<35} {row.get('Failure Count', 'N/A'):>20}")
        
        # Calculate failure rate
        total = row.get('Request Count', 0)
        failed = row.get('Failure Count', 0)
        if total > 0:
            fail_rate = (failed / total) * 100
            print(f"{'Failure Rate':<35} {f'{fail_rate:.1f}%':>20}")
        
        print(f"{'Requests/s':<35} {row.get('Requests/s', 'N/A'):>20}")
        print("-" * 70)
        
        # Latency metrics (convert from ms to seconds for readability)
        print("LATENCY (in seconds):")
        avg_ms = row.get('Average Response Time', 0)
        median_ms = row.get('Median Response Time', 0)
        min_ms = row.get('Min Response Time', 0)
        max_ms = row.get('Max Response Time', 0)
        
        print(f"{'  Average':<35} {avg_ms / 1000:>19.2f}s")
        print(f"{'  Median (P50)':<35} {median_ms / 1000:>19.2f}s")
        print(f"{'  Min':<35} {min_ms / 1000:>19.2f}s")
        print(f"{'  Max':<35} {max_ms / 1000:>19.2f}s")
        
        # Percentiles if available
        for pct in ['50%', '66%', '75%', '80%', '90%', '95%', '99%']:
            col = pct
            if col in row and pd.notna(row[col]):
                print(f"{'  P' + pct.replace('%', ''):<35} {row[col] / 1000:>19.2f}s")
    
    print("-" * 70)
    
    # Per-endpoint breakdown
    print("\nPER-ENDPOINT BREAKDOWN:")
    print("-" * 80)
    print(f"{'Endpoint':<25} {'Reqs':>8} {'Fails':>8} {'Avg(s)':>10} {'Med(s)':>10} {'P90(s)':>10}")
    print("-" * 80)
    
    for _, row in results.stats_df.iterrows():
        name = row.get('Name', '')
        if name == 'Aggregated':
            continue
        
        # Truncate long names
        if len(name) > 24:
            name = name[:21] + "..."
        
        reqs = row.get('Request Count', 0)
        fails = row.get('Failure Count', 0)
        avg = row.get('Average Response Time', 0) / 1000
        med = row.get('Median Response Time', 0) / 1000
        p90 = row.get('90%', 0) / 1000 if '90%' in row and pd.notna(row.get('90%')) else 0
        
        print(f"{name:<25} {reqs:>8} {fails:>8} {avg:>9.2f}s {med:>9.2f}s {p90:>9.2f}s")
    
    print("-" * 80)
    
    # Failures summary
    if not results.failures_df.empty:
        print(f"\nFAILURES ({len(results.failures_df)} unique):")
        print("-" * 70)
        for _, row in results.failures_df.iterrows():
            print(f"  - {row.get('Name', 'Unknown')}: {row.get('Error', 'Unknown error')}")
    
    # Exceptions summary
    if not results.exceptions_df.empty:
        print(f"\nEXCEPTIONS ({len(results.exceptions_df)} unique):")
        print("-" * 70)
        for _, row in results.exceptions_df.iterrows():
            print(f"  - Count: {row.get('Count', 'N/A')}, Message: {row.get('Message', 'Unknown')[:100]}")
    
    print("\n" + "=" * 70)
