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
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class LoadTestResults:
    """Container for load test results from Locust CSV output."""
    
    stats_df: pd.DataFrame
    stats_history_df: pd.DataFrame
    failures_df: pd.DataFrame
    exceptions_df: pd.DataFrame
    csv_prefix: str
    
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
    
    # Build locust command
    cmd = (
        f"locust "
        f"--host=https://localhost "
        f"--users={user_count} "
        f"--spawn-rate={spawn_rate} "
        f"--run-time={run_time} "
        f"--headless "
        f"--only-summary "
        f"--locustfile={locustfile_path} "
        f"--csv={csv_prefix}"
    )
    
    print(f"Starting Locust load test...")
    print(f"  Users: {user_count}")
    print(f"  Spawn Rate: {spawn_rate}/s")
    print(f"  Duration: {run_time}")
    print(f"  Conversations: {conversations_path}")
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
    
    # Read CSV results
    return _read_csv_results(csv_prefix)


def _read_csv_results(csv_prefix: str) -> LoadTestResults:
    """Read Locust CSV output files into DataFrames."""
    stats_file = f"{csv_prefix}_stats.csv"
    history_file = f"{csv_prefix}_stats_history.csv"
    failures_file = f"{csv_prefix}_failures.csv"
    exceptions_file = f"{csv_prefix}_exceptions.csv"
    
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
        csv_prefix=csv_prefix,
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
    
    # Build locust command
    cmd = (
        f"locust "
        f"--host=https://localhost "
        f"--users={user_count} "
        f"--spawn-rate={spawn_rate} "
        f"--run-time={run_time} "
        f"--headless "
        f"--only-summary "
        f"--locustfile={locustfile_path} "
        f"--csv={csv_prefix}"
    )
    
    print(f"Starting Cached Locust load test...")
    print(f"  Users: {user_count}")
    print(f"  Spawn Rate: {spawn_rate}/s")
    print(f"  Duration: {run_time}")
    print(f"  Conversations: {conversations_path}")
    print(f"  Cache: LRU={lru_capacity}, TTL={cache_ttl}s, Threshold={similarity_threshold}")
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
    
    # Read CSV results
    return _read_csv_results(csv_prefix)


def cleanup_csv_files(csv_prefix: str) -> None:
    """Remove CSV files generated by the load test."""
    patterns = [
        f"{csv_prefix}_stats.csv",
        f"{csv_prefix}_stats_history.csv",
        f"{csv_prefix}_failures.csv",
        f"{csv_prefix}_exceptions.csv",
    ]
    
    for pattern in patterns:
        path = Path(pattern)
        if path.exists():
            path.unlink()
            print(f"Removed: {pattern}")
