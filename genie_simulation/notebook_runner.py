"""
Programmatic Locust runner for Databricks notebooks.

This module provides a way to run Genie load tests from within a Databricks notebook
using Locust's programmatic API instead of the CLI.

Usage:
    from genie_simulation.notebook_runner import GenieLoadTestRunner
    
    runner = GenieLoadTestRunner(
        conversations_file="/dbfs/path/to/conversations.yaml",
        space_id="your-space-id",  # optional, uses file's space_id if not provided
    )
    
    results = runner.run(
        user_count=10,
        spawn_rate=2.0,
        run_time_seconds=300,
        use_cache=False,
    )
    
    # Display results in notebook
    display(results.summary_df)
    display(results.percentiles_df)
"""

from __future__ import annotations

# Monkey-patch before any other imports to avoid gevent/SSL conflicts
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="gevent")

from gevent import monkey
monkey.patch_all(thread=False, select=False)

import logging
import math
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import gevent
import pandas as pd
import yaml
from locust import User, between, events
from locust.env import Environment
from locust.log import setup_logging
from locust.runners import LocalRunner
from loguru import logger

# Suppress noisy third-party library logs
logging.getLogger("databricks.sdk").setLevel(logging.WARNING)
logging.getLogger("mlflow").setLevel(logging.ERROR)
logging.getLogger("mlflow.tracing").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


@dataclass
class CacheMetrics:
    """Thread-safe cache metrics tracker."""
    
    lru_hits: int = 0
    semantic_hits: int = 0
    misses: int = 0
    _lock: Any = field(default_factory=lambda: threading.Lock())
    
    def record_hit(self, cache_type: str) -> None:
        """Record a cache hit."""
        with self._lock:
            if cache_type == "lru":
                self.lru_hits += 1
            elif cache_type == "semantic":
                self.semantic_hits += 1
    
    def record_miss(self) -> None:
        """Record a cache miss."""
        with self._lock:
            self.misses += 1
    
    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self.lru_hits = 0
            self.semantic_hits = 0
            self.misses = 0
    
    @property
    def total_requests(self) -> int:
        """Total number of requests."""
        return self.lru_hits + self.semantic_hits + self.misses
    
    @property
    def total_hits(self) -> int:
        """Total number of cache hits."""
        return self.lru_hits + self.semantic_hits
    
    @property
    def hit_rate(self) -> float:
        """Overall cache hit rate."""
        total = self.total_requests
        return (self.total_hits / total * 100) if total > 0 else 0.0
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        total = self.total_requests
        return {
            "total_requests": total,
            "lru_hits": self.lru_hits,
            "lru_hit_rate": (self.lru_hits / total * 100) if total > 0 else 0.0,
            "semantic_hits": self.semantic_hits,
            "semantic_hit_rate": (self.semantic_hits / total * 100) if total > 0 else 0.0,
            "misses": self.misses,
            "miss_rate": (self.misses / total * 100) if total > 0 else 0.0,
            "total_hits": self.total_hits,
            "hit_rate": self.hit_rate,
        }
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert to a display-friendly DataFrame."""
        data = self.to_dict()
        rows = [
            {"Metric": "Total Requests", "Value": data["total_requests"], "Rate": ""},
            {"Metric": "LRU Cache Hits", "Value": data["lru_hits"], "Rate": f"{data['lru_hit_rate']:.1f}%"},
            {"Metric": "Semantic Cache Hits", "Value": data["semantic_hits"], "Rate": f"{data['semantic_hit_rate']:.1f}%"},
            {"Metric": "Cache Misses", "Value": data["misses"], "Rate": f"{data['miss_rate']:.1f}%"},
            {"Metric": "Total Hits", "Value": data["total_hits"], "Rate": f"{data['hit_rate']:.1f}%"},
        ]
        return pd.DataFrame(rows)


@dataclass
class LoadTestResults:
    """Container for load test results."""
    
    summary_df: pd.DataFrame
    percentiles_df: pd.DataFrame
    endpoints_df: pd.DataFrame
    cache_metrics_df: pd.DataFrame | None = None
    raw_stats: dict[str, Any] = field(default_factory=dict)
    
    def display_all(self) -> None:
        """Print all results (for non-Databricks environments)."""
        print("\n" + "=" * 70)
        print("LOAD TEST RESULTS")
        print("=" * 70)
        
        print("\n--- Summary ---")
        print(self.summary_df.to_string(index=False))
        
        print("\n--- Latency Percentiles (seconds) ---")
        print(self.percentiles_df.to_string(index=False))
        
        print("\n--- Per-Endpoint Breakdown ---")
        print(self.endpoints_df.to_string(index=False))
        
        if self.cache_metrics_df is not None:
            print("\n--- Cache Metrics ---")
            print(self.cache_metrics_df.to_string(index=False))


def _calculate_robust_stats(stats_entry: Any) -> dict[str, float]:
    """Calculate robust statistics that are less sensitive to outliers."""
    if stats_entry.num_requests == 0:
        return {
            "trimmed_mean": 0.0,
            "std_dev": 0.0,
            "iqr": 0.0,
            "p10": 0.0,
            "p25": 0.0,
            "p75": 0.0,
            "p90": 0.0,
            "p95": 0.0,
            "p99": 0.0,
        }
    
    p10 = stats_entry.get_response_time_percentile(0.10)
    p25 = stats_entry.get_response_time_percentile(0.25)
    p75 = stats_entry.get_response_time_percentile(0.75)
    p90 = stats_entry.get_response_time_percentile(0.90)
    p95 = stats_entry.get_response_time_percentile(0.95)
    p99 = stats_entry.get_response_time_percentile(0.99)
    
    iqr = p75 - p25
    
    response_times: dict[int, int] = stats_entry.response_times
    
    all_times: list[float] = []
    trimmed_times: list[float] = []
    
    for resp_time, count in response_times.items():
        for _ in range(count):
            all_times.append(float(resp_time))
            if p10 <= resp_time <= p90:
                trimmed_times.append(float(resp_time))
    
    trimmed_mean = sum(trimmed_times) / len(trimmed_times) if trimmed_times else 0.0
    
    if len(all_times) > 1:
        mean = sum(all_times) / len(all_times)
        variance = sum((x - mean) ** 2 for x in all_times) / len(all_times)
        std_dev = math.sqrt(variance)
    else:
        std_dev = 0.0
    
    return {
        "trimmed_mean": trimmed_mean,
        "std_dev": std_dev,
        "iqr": iqr,
        "p10": p10,
        "p25": p25,
        "p75": p75,
        "p90": p90,
        "p95": p95,
        "p99": p99,
    }


def _stats_to_dataframes(
    environment: Environment,
    cache_metrics: CacheMetrics | None = None,
) -> LoadTestResults:
    """Convert Locust stats to pandas DataFrames."""
    stats = environment.stats.total
    robust = _calculate_robust_stats(stats)
    
    # Summary DataFrame
    summary_data = [
        {"Metric": "Total Requests", "Value": stats.num_requests},
        {"Metric": "Failed Requests", "Value": stats.num_failures},
        {"Metric": "Failure Rate", "Value": f"{stats.fail_ratio * 100:.1f}%"},
        {"Metric": "Throughput (req/s)", "Value": f"{stats.total_rps:.3f}"},
        {"Metric": "Avg Latency (s)", "Value": f"{stats.avg_response_time / 1000:.2f}"},
        {"Metric": "Trimmed Mean (s)", "Value": f"{robust['trimmed_mean'] / 1000:.2f}"},
        {"Metric": "Median (s)", "Value": f"{stats.median_response_time / 1000:.2f}"},
        {"Metric": "Std Deviation (s)", "Value": f"{robust['std_dev'] / 1000:.2f}"},
    ]
    summary_df = pd.DataFrame(summary_data)
    
    # Percentiles DataFrame
    percentiles_data = [
        {"Percentile": "P10", "Latency (s)": f"{robust['p10'] / 1000:.2f}"},
        {"Percentile": "P25", "Latency (s)": f"{robust['p25'] / 1000:.2f}"},
        {"Percentile": "P50 (Median)", "Latency (s)": f"{stats.median_response_time / 1000:.2f}"},
        {"Percentile": "P75", "Latency (s)": f"{robust['p75'] / 1000:.2f}"},
        {"Percentile": "P90", "Latency (s)": f"{robust['p90'] / 1000:.2f}"},
        {"Percentile": "P95", "Latency (s)": f"{robust['p95'] / 1000:.2f}"},
        {"Percentile": "P99", "Latency (s)": f"{robust['p99'] / 1000:.2f}"},
        {"Percentile": "Min", "Latency (s)": f"{stats.min_response_time / 1000:.2f}"},
        {"Percentile": "Max", "Latency (s)": f"{stats.max_response_time / 1000:.2f}"},
    ]
    percentiles_df = pd.DataFrame(percentiles_data)
    
    # Per-endpoint breakdown
    endpoints_data = []
    for name, entry in sorted(environment.stats.entries.items()):
        if entry.num_requests > 0:
            endpoint_name = name[1] if isinstance(name, tuple) else str(name)
            request_type = name[0] if isinstance(name, tuple) else "GENIE"
            entry_robust = _calculate_robust_stats(entry)
            endpoints_data.append({
                "Type": request_type,
                "Endpoint": endpoint_name[:30],
                "Requests": entry.num_requests,
                "Failures": entry.num_failures,
                "Median (s)": f"{entry.median_response_time / 1000:.1f}",
                "Avg (s)": f"{entry.avg_response_time / 1000:.1f}",
                "P90 (s)": f"{entry_robust['p90'] / 1000:.1f}",
            })
    endpoints_df = pd.DataFrame(endpoints_data) if endpoints_data else pd.DataFrame()
    
    # Cache metrics DataFrame
    cache_metrics_df = None
    if cache_metrics is not None and cache_metrics.total_requests > 0:
        cache_metrics_df = cache_metrics.to_dataframe()
    
    return LoadTestResults(
        summary_df=summary_df,
        percentiles_df=percentiles_df,
        endpoints_df=endpoints_df,
        cache_metrics_df=cache_metrics_df,
        raw_stats={
            "total_requests": stats.num_requests,
            "total_failures": stats.num_failures,
            "avg_response_time_ms": stats.avg_response_time,
            "median_response_time_ms": stats.median_response_time,
            "total_rps": stats.total_rps,
        },
    )


class GenieLoadTestRunner:
    """
    Programmatic runner for Genie load tests.
    
    This class wraps Locust to run load tests from within a Databricks notebook
    or any Python environment without needing the Locust CLI.
    
    Example:
        runner = GenieLoadTestRunner(
            conversations_file="/dbfs/path/to/conversations.yaml",
        )
        
        results = runner.run(
            user_count=10,
            spawn_rate=2.0,
            run_time_seconds=300,
        )
        
        # In Databricks notebook:
        display(results.summary_df)
    """
    
    def __init__(
        self,
        conversations_file: str | Path,
        space_id: str | None = None,
        min_wait: float = 8.0,
        max_wait: float = 30.0,
        sample_size: int | None = None,
        sample_seed: int | None = None,
        # Cache-specific settings
        lakebase_client_id: str | None = None,
        lakebase_client_secret: str | None = None,
        lakebase_instance: str | None = None,
        warehouse_id: str | None = None,
        cache_ttl: int = 86400,
        similarity_threshold: float = 0.85,
        lru_capacity: int = 100,
    ) -> None:
        """
        Initialize the load test runner.
        
        Args:
            conversations_file: Path to the conversations YAML file
            space_id: Genie space ID (uses file's space_id if not provided)
            min_wait: Minimum wait time between messages in seconds
            max_wait: Maximum wait time between messages in seconds
            sample_size: Number of conversations to sample (None = use all)
            sample_seed: Random seed for reproducible sampling
            lakebase_client_id: Lakebase client ID for cache service
            lakebase_client_secret: Lakebase client secret for cache service
            lakebase_instance: Lakebase instance name
            warehouse_id: Warehouse ID for cache operations
            cache_ttl: Cache TTL in seconds
            similarity_threshold: Semantic similarity threshold
            lru_capacity: LRU cache capacity
        """
        self.conversations_file = Path(conversations_file)
        self.space_id = space_id
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.sample_size = sample_size
        self.sample_seed = sample_seed
        
        # Cache settings
        self.lakebase_client_id = lakebase_client_id
        self.lakebase_client_secret = lakebase_client_secret
        self.lakebase_instance = lakebase_instance
        self.warehouse_id = warehouse_id
        self.cache_ttl = cache_ttl
        self.similarity_threshold = similarity_threshold
        self.lru_capacity = lru_capacity
        
        # Load and validate conversations
        self._conversations_data = self._load_conversations()
        
        # Cache metrics tracker (shared across users)
        self._cache_metrics = CacheMetrics()
    
    def _load_conversations(self) -> dict[str, Any]:
        """Load and validate the conversations file."""
        if not self.conversations_file.exists():
            raise FileNotFoundError(
                f"Conversations file not found: {self.conversations_file}"
            )
        
        with open(self.conversations_file) as f:
            data = yaml.safe_load(f)
        
        if not data.get("conversations"):
            raise ValueError("No conversations found in the file")
        
        # Use provided space_id or fall back to file's space_id
        if not self.space_id:
            self.space_id = data.get("space_id", "")
        
        if not self.space_id:
            raise ValueError(
                "No space_id provided and none found in conversations file"
            )
        
        logger.info(
            f"Loaded {data.get('total_conversations', len(data['conversations']))} "
            f"conversations with {data.get('total_messages', 0)} messages"
        )
        
        return data
    
    def _set_environment_variables(self, use_cache: bool) -> None:
        """Set environment variables for the load test."""
        os.environ["GENIE_CONVERSATIONS_FILE"] = str(self.conversations_file.absolute())
        os.environ["GENIE_MIN_WAIT"] = str(self.min_wait)
        os.environ["GENIE_MAX_WAIT"] = str(self.max_wait)
        os.environ["GENIE_SPACE_ID"] = self.space_id or ""
        
        if self.sample_size is not None:
            os.environ["GENIE_SAMPLE_SIZE"] = str(self.sample_size)
        if self.sample_seed is not None:
            os.environ["GENIE_SAMPLE_SEED"] = str(self.sample_seed)
        
        if use_cache:
            if self.lakebase_client_id:
                os.environ["GENIE_LAKEBASE_CLIENT_ID"] = self.lakebase_client_id
            if self.lakebase_client_secret:
                os.environ["GENIE_LAKEBASE_CLIENT_SECRET"] = self.lakebase_client_secret
            if self.lakebase_instance:
                os.environ["GENIE_LAKEBASE_INSTANCE"] = self.lakebase_instance
            if self.warehouse_id:
                os.environ["GENIE_WAREHOUSE_ID"] = self.warehouse_id
            os.environ["GENIE_CACHE_TTL"] = str(self.cache_ttl)
            os.environ["GENIE_SIMILARITY_THRESHOLD"] = str(self.similarity_threshold)
            os.environ["GENIE_LRU_CAPACITY"] = str(self.lru_capacity)
    
    def _get_user_class(self, use_cache: bool) -> type[User]:
        """
        Get the appropriate User class for the test.
        
        This imports the User class dynamically to ensure environment variables
        are set before the module is loaded.
        """
        if use_cache:
            # Import from cached locustfile
            from locustfile_cached import CachedGenieLoadTestUser
            return CachedGenieLoadTestUser
        else:
            # Import from standard locustfile
            from locustfile import GenieLoadTestUser
            return GenieLoadTestUser
    
    def run(
        self,
        user_count: int = 10,
        spawn_rate: float = 1.0,
        run_time_seconds: int = 300,
        use_cache: bool = False,
    ) -> LoadTestResults:
        """
        Run the load test.
        
        Args:
            user_count: Number of concurrent simulated users
            spawn_rate: Users to spawn per second
            run_time_seconds: Total test duration in seconds
            use_cache: If True, use the cached Genie service
        
        Returns:
            LoadTestResults containing DataFrames with test metrics
        """
        # Reset cache metrics
        self._cache_metrics.reset()
        
        # Set environment variables before importing User classes
        self._set_environment_variables(use_cache)
        
        # Get the appropriate User class
        user_class = self._get_user_class(use_cache)
        
        logger.info("=" * 70)
        logger.info(f"Starting {'Cached ' if use_cache else ''}Genie Load Test")
        logger.info("=" * 70)
        logger.info(f"Space ID: {self.space_id}")
        logger.info(f"Users: {user_count}, Spawn Rate: {spawn_rate}/s")
        logger.info(f"Duration: {run_time_seconds}s")
        logger.info(f"Wait Time Range: {self.min_wait}s - {self.max_wait}s")
        if use_cache:
            logger.info(f"Cache: LRU capacity={self.lru_capacity}, TTL={self.cache_ttl}s")
        logger.info("=" * 70)
        
        # Create Locust environment
        environment = Environment(user_classes=[user_class])
        
        # Create runner
        runner = environment.create_local_runner()
        
        # Start the test
        runner.start(user_count=user_count, spawn_rate=spawn_rate)
        
        # Run for the specified duration
        try:
            gevent.sleep(run_time_seconds)
        except KeyboardInterrupt:
            logger.info("Test interrupted by user")
        
        # Stop the test
        runner.stop()
        runner.quit()
        
        logger.info("=" * 70)
        logger.info("Load Test Complete")
        logger.info("=" * 70)
        
        # Get cache metrics if using cache
        cache_metrics = None
        if use_cache:
            try:
                from locustfile_cached import CACHE_METRICS
                cache_metrics = CacheMetrics(
                    lru_hits=CACHE_METRICS.lru_hits,
                    semantic_hits=CACHE_METRICS.semantic_hits,
                    misses=CACHE_METRICS.misses,
                )
            except ImportError:
                pass
        
        # Convert stats to DataFrames
        return _stats_to_dataframes(environment, cache_metrics)
    
    def run_quick_test(
        self,
        user_count: int = 1,
        messages: int = 5,
        use_cache: bool = False,
    ) -> LoadTestResults:
        """
        Run a quick test with a small number of messages.
        
        This is useful for validating the setup before running a full load test.
        
        Args:
            user_count: Number of concurrent users
            messages: Approximate number of messages to send per user
            use_cache: If True, use the cached Genie service
        
        Returns:
            LoadTestResults containing DataFrames with test metrics
        """
        # Estimate run time based on wait times and message count
        avg_wait = (self.min_wait + self.max_wait) / 2
        estimated_duration = int(messages * avg_wait * 1.5)  # Add buffer
        
        logger.info(f"Running quick test: {messages} messages, ~{estimated_duration}s duration")
        
        return self.run(
            user_count=user_count,
            spawn_rate=user_count,  # Spawn all users immediately
            run_time_seconds=estimated_duration,
            use_cache=use_cache,
        )
