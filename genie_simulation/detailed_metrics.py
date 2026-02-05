"""
Detailed metrics collector for Genie load tests.

This module provides thread-safe collection of detailed per-request metrics
that can be exported to CSV for analysis and loading into Unity Catalog.
"""

import csv
import os
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd


@dataclass
class RequestMetric:
    """A single request metric record."""
    
    run_id: str  # Unique run identifier (results directory name)
    space_id: str  # Genie space ID
    request_started_at: datetime
    request_completed_at: datetime
    duration_ms: float
    concurrent_users: int  # Number of concurrent users in the simulation
    user: str  # Simulated user ID (e.g., "user_1", "user_2")
    prompt: str
    source_conversation_id: Optional[str]  # From replay file (conversations.yaml)
    source_message_id: Optional[str]       # From replay file (conversations.yaml)
    genie_conversation_id: Optional[str]   # Returned by Genie API
    genie_message_id: Optional[str]        # Returned by Genie API
    message_index: int
    sql: Optional[str]
    response_size: int
    success: bool
    error: Optional[str]
    # SQL execution metrics from system.query.history
    sql_statement_id: Optional[str] = None  # Statement ID from query history
    sql_execution_time_ms: Optional[float] = None  # Time spent executing SQL in warehouse
    sql_compilation_time_ms: Optional[float] = None  # Time spent compiling/planning SQL
    sql_rows_produced: Optional[int] = None  # Number of rows returned by query
    sql_bytes_read: Optional[int] = None  # Bytes read during execution
    sql_bytes_written: Optional[int] = None  # Bytes written during execution


class DetailedMetricsCollector:
    """
    Thread-safe collector for detailed request metrics.
    
    This collector accumulates metrics from all Locust users during a load test
    and can export them to CSV or DataFrame for analysis.
    
    Usage:
        collector = DetailedMetricsCollector()
        
        # In each request handler:
        started_at = datetime.now()
        # ... perform request ...
        completed_at = datetime.now()
        
        collector.record(RequestMetric(
            run_id="genie_loadtest_ABC123_5users_60s_20260205",  # Results directory name
            space_id="01JCQK9M1234567890AB",        # Genie space ID
            request_started_at=started_at,
            request_completed_at=completed_at,
            duration_ms=1500.5,
            concurrent_users=5,                     # Number of concurrent users
            user="user_1",
            prompt="What are total sales?",
            source_conversation_id="src_conv_123",  # From replay file
            source_message_id="src_msg_456",        # From replay file
            genie_conversation_id="genie_conv_789", # From Genie API
            genie_message_id="genie_msg_012",       # From Genie API
            message_index=0,
            sql="SELECT SUM(sales) FROM orders",
            response_size=1024,
            success=True,
            error=None,
        ))
        
        # At test end:
        collector.to_csv("detailed_metrics.csv")
    """
    
    def __init__(self) -> None:
        self._metrics: list[RequestMetric] = []
        self._lock = threading.Lock()
    
    def record(self, metric: RequestMetric) -> None:
        """
        Record a single request metric (thread-safe).
        
        Args:
            metric: The RequestMetric to record.
        """
        with self._lock:
            self._metrics.append(metric)
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert collected metrics to a pandas DataFrame.
        
        Returns:
            DataFrame with all collected metrics.
        """
        with self._lock:
            if not self._metrics:
                return pd.DataFrame()
            
            records = [asdict(m) for m in self._metrics]
            return pd.DataFrame(records)
    
    def to_csv(self, path: str) -> int:
        """
        Write collected metrics to a CSV file.
        
        Args:
            path: Path to the output CSV file.
            
        Returns:
            Number of records written.
        """
        df = self.to_dataframe()
        if df.empty:
            # Write empty file with headers
            with open(path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "run_id", "space_id", "request_started_at", "request_completed_at", "duration_ms",
                    "concurrent_users", "user", "prompt", "source_conversation_id",
                    "source_message_id", "genie_conversation_id", "genie_message_id",
                    "message_index", "sql", "response_size", "success", "error",
                    "sql_statement_id", "sql_execution_time_ms", "sql_compilation_time_ms",
                    "sql_rows_produced", "sql_bytes_read", "sql_bytes_written"
                ])
            return 0
        
        df.to_csv(path, index=False)
        return len(df)
    
    def clear(self) -> None:
        """Clear all collected metrics."""
        with self._lock:
            self._metrics.clear()
    
    def count(self) -> int:
        """Return the number of collected metrics."""
        with self._lock:
            return len(self._metrics)
    
    def get_summary(self) -> dict:
        """
        Get a summary of collected metrics.
        
        Returns:
            Dictionary with summary statistics.
        """
        with self._lock:
            if not self._metrics:
                return {
                    "total_requests": 0,
                    "successful_requests": 0,
                    "failed_requests": 0,
                    "success_rate": 0.0,
                    "avg_execution_time_ms": 0.0,
                    "unique_users": 0,
                    "unique_conversations": 0,
                }
            
            total = len(self._metrics)
            successful = sum(1 for m in self._metrics if m.success)
            failed = total - successful
            avg_time = sum(m.duration_ms for m in self._metrics) / total
            unique_users = len(set(m.user for m in self._metrics))
            unique_convs = len(set(m.genie_conversation_id for m in self._metrics if m.genie_conversation_id))
            
            return {
                "total_requests": total,
                "successful_requests": successful,
                "failed_requests": failed,
                "success_rate": (successful / total) * 100 if total > 0 else 0.0,
                "avg_execution_time_ms": avg_time,
                "unique_users": unique_users,
                "unique_conversations": unique_convs,
            }


# Global instance for use across Locust users
DETAILED_METRICS = DetailedMetricsCollector()
