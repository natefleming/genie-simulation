"""
Query history utilities for fetching SQL execution metrics from Databricks system tables.

This module provides functions to query system.query.history and correlate
SQL execution metrics with load test requests using the SQL text or conversation ID.

Usage:
    from genie_simulation.query_history import QueryHistoryClient, SQLExecutionMetrics
    
    # Initialize client with SQL warehouse connection
    client = QueryHistoryClient(warehouse_id="your-warehouse-id")
    
    # Fetch metrics for a specific SQL query
    metrics = client.get_metrics_for_sql(
        sql_text="SELECT * FROM sales",
        start_time=datetime(2026, 2, 5, 10, 0, 0),
        end_time=datetime(2026, 2, 5, 11, 0, 0),
    )
    
    if metrics:
        print(f"Execution time: {metrics.execution_time_ms}ms")
"""

import hashlib
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


@dataclass
class SQLExecutionMetrics:
    """
    Metrics from system.query.history for a SQL execution.
    
    Field mapping from system.query.history columns:
        - statement_id: Unique query identifier
        - total_duration_ms: Total time in warehouse (total_duration_ms)
        - execution_time_ms: Time executing query (execution_duration_ms)
        - compilation_time_ms: Time compiling/planning (compilation_duration_ms)
        - queue_wait_ms: Time waiting in queue (waiting_at_capacity_duration_ms)
        - compute_wait_ms: Time waiting for compute (waiting_for_compute_duration_ms)
        - result_fetch_ms: Time fetching results (result_fetch_duration_ms)
        - rows_produced: Number of rows returned (produced_rows)
        - bytes_read: Bytes read from storage (read_bytes)
        - rows_read: Rows scanned from storage (read_rows)
        - execution_status: Query status (execution_status)
        - error_message: Error details if failed (error_message)
        - warehouse_id: Which warehouse ran query (compute.warehouse_id)
        - genie_space_id: Genie space that originated query (query_source.genie_space_id)
        - genie_conversation_id: Conversation ID (query_source.genie_conversation_id)
    """
    
    statement_id: str
    total_duration_ms: float
    execution_time_ms: float
    compilation_time_ms: float
    queue_wait_ms: float
    compute_wait_ms: float
    result_fetch_ms: float
    rows_produced: int
    bytes_read: int
    start_time: datetime
    end_time: datetime
    execution_status: str
    rows_read: int = 0
    error_message: Optional[str] = None
    warehouse_id: Optional[str] = None
    genie_space_id: Optional[str] = None
    genie_conversation_id: Optional[str] = None


class QueryHistoryClient:
    """
    Client for querying Databricks system.query.history table.
    
    This client fetches SQL execution metrics that can be correlated with
    load test requests to understand actual warehouse execution time vs
    end-to-end latency.
    """
    
    def __init__(
        self, 
        warehouse_id: str,
        system_catalog: str = "system",
        lookback_minutes: int = 30,
    ) -> None:
        """
        Initialize the query history client.
        
        Args:
            warehouse_id: The SQL warehouse ID to use for querying system tables.
            system_catalog: The catalog containing system tables (default: "system").
            lookback_minutes: How far back to search for matching queries (default: 30).
        """
        self.warehouse_id = warehouse_id
        self.system_catalog = system_catalog
        self.lookback_minutes = lookback_minutes
        self._client = WorkspaceClient()
        self._lock = threading.Lock()
        
        # Cache to avoid duplicate queries
        self._cache: dict[str, SQLExecutionMetrics] = {}
    
    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL for matching by removing whitespace variations."""
        if not sql:
            return ""
        # Remove extra whitespace and normalize
        normalized = " ".join(sql.split())
        return normalized.strip()
    
    def _sql_hash(self, sql: str) -> str:
        """Create a hash of normalized SQL for caching."""
        normalized = self._normalize_sql(sql)
        return hashlib.md5(normalized.encode()).hexdigest()[:16]
    
    def get_metrics_for_sql(
        self,
        sql_text: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[SQLExecutionMetrics]:
        """
        Fetch execution metrics for a SQL query from system.query.history.
        
        Searches for a matching query by comparing the SQL text within the
        specified time window.
        
        Args:
            sql_text: The SQL query text to search for.
            start_time: Start of the time window to search (default: lookback_minutes ago).
            end_time: End of the time window to search (default: now).
            
        Returns:
            SQLExecutionMetrics if found, None otherwise.
        """
        if not sql_text:
            return None
        
        # Check cache first
        cache_key = self._sql_hash(sql_text)
        with self._lock:
            if cache_key in self._cache:
                return self._cache[cache_key]
        
        # Set default time window
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(minutes=self.lookback_minutes)
        
        # Format timestamps for SQL
        start_ts = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_ts = end_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Escape special characters for the LIKE comparison
        escaped_sql = sql_text.replace("'", "''")
        # Escape SQL LIKE wildcards to avoid incorrect matches
        escaped_sql = escaped_sql.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        # Take first 200 chars for matching to avoid issues with very long queries
        sql_prefix = escaped_sql[:200] if len(escaped_sql) > 200 else escaped_sql
        
        # Query system.query.history
        # Note: We use LIKE with the first part of the query to find matches
        # Column mapping from system.query.history:
        #   - total_duration_ms: Total time in warehouse
        #   - execution_duration_ms: Time executing (not execution_time_ms)
        #   - compilation_duration_ms: Time planning (not compilation_time_ms)
        #   - waiting_at_capacity_duration_ms: Queue wait time
        #   - waiting_for_compute_duration_ms: Compute startup wait
        #   - result_fetch_duration_ms: Result fetch time
        #   - produced_rows: Rows returned (not rows_produced)
        #   - read_bytes: Bytes read
        #   - execution_status: Query status (not status)
        query = f"""
        SELECT
            statement_id,
            COALESCE(total_duration_ms, 0) as total_duration_ms,
            COALESCE(execution_duration_ms, 0) as execution_duration_ms,
            COALESCE(compilation_duration_ms, 0) as compilation_duration_ms,
            COALESCE(waiting_at_capacity_duration_ms, 0) as queue_wait_ms,
            COALESCE(waiting_for_compute_duration_ms, 0) as compute_wait_ms,
            COALESCE(result_fetch_duration_ms, 0) as result_fetch_ms,
            COALESCE(produced_rows, 0) as rows_produced,
            COALESCE(read_bytes, 0) as bytes_read,
            COALESCE(read_rows, 0) as rows_read,
            start_time,
            end_time,
            execution_status,
            error_message,
            compute.warehouse_id as warehouse_id,
            query_source.genie_space_id as genie_space_id,
            query_source.genie_conversation_id as genie_conversation_id
        FROM {self.system_catalog}.query.history
        WHERE start_time >= '{start_ts}'
          AND start_time <= '{end_ts}'
          AND statement_text LIKE '{sql_prefix}%'
        ORDER BY start_time DESC
        LIMIT 1
        """
        
        try:
            # Execute using the SDK
            response = self._client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=query,
                wait_timeout="30s",
            )
            
            if response.status and response.status.state == StatementState.SUCCEEDED:
                if response.result and response.result.data_array:
                    row = response.result.data_array[0]
                    metrics = SQLExecutionMetrics(
                        statement_id=str(row[0]) if row[0] else "",
                        total_duration_ms=float(row[1]) if row[1] else 0.0,
                        execution_time_ms=float(row[2]) if row[2] else 0.0,
                        compilation_time_ms=float(row[3]) if row[3] else 0.0,
                        queue_wait_ms=float(row[4]) if row[4] else 0.0,
                        compute_wait_ms=float(row[5]) if row[5] else 0.0,
                        result_fetch_ms=float(row[6]) if row[6] else 0.0,
                        rows_produced=int(row[7]) if row[7] else 0,
                        bytes_read=int(row[8]) if row[8] else 0,
                        rows_read=int(row[9]) if row[9] else 0,
                        start_time=datetime.fromisoformat(str(row[10])) if row[10] else start_time,
                        end_time=datetime.fromisoformat(str(row[11])) if row[11] else end_time,
                        execution_status=str(row[12]) if row[12] else "UNKNOWN",
                        error_message=str(row[13]) if row[13] else None,
                        warehouse_id=str(row[14]) if row[14] else None,
                        genie_space_id=str(row[15]) if row[15] else None,
                        genie_conversation_id=str(row[16]) if row[16] else None,
                    )
                    
                    # Cache the result
                    with self._lock:
                        self._cache[cache_key] = metrics
                    
                    return metrics
                    
        except Exception as e:
            # Log but don't fail - metrics are optional enhancement
            import logging
            logging.warning(f"Failed to fetch query history metrics: {e}")
        
        return None
    
    def batch_get_metrics(
        self,
        sql_queries: list[tuple[str, datetime, datetime]],
    ) -> dict[str, Optional[SQLExecutionMetrics]]:
        """
        Fetch metrics for multiple SQL queries in a batch.
        
        This is more efficient than calling get_metrics_for_sql for each query
        as it uses a single query to fetch all metrics.
        
        Args:
            sql_queries: List of (sql_text, start_time, end_time) tuples.
            
        Returns:
            Dictionary mapping SQL hash to metrics (or None if not found).
        """
        results: dict[str, Optional[SQLExecutionMetrics]] = {}
        
        # For now, just call individual queries
        # A more efficient implementation could batch these
        for sql_text, start_time, end_time in sql_queries:
            cache_key = self._sql_hash(sql_text)
            results[cache_key] = self.get_metrics_for_sql(sql_text, start_time, end_time)
        
        return results
    
    def clear_cache(self) -> None:
        """Clear the metrics cache."""
        with self._lock:
            self._cache.clear()
    
    def get_all_queries_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> list[SQLExecutionMetrics]:
        """
        Fetch all query history entries for a time range.
        
        This is useful for post-processing enrichment where we want to fetch
        all queries at once and then match them with load test metrics.
        
        Args:
            start_time: Start of the time range.
            end_time: End of the time range.
            
        Returns:
            List of SQLExecutionMetrics for all queries in the time range.
        """
        # Format timestamps for SQL
        start_ts = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_ts = end_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Query for all Genie-originated queries in time range
        # Uses query_source.genie_space_id to filter to Genie queries only
        query = f"""
        SELECT
            statement_id,
            statement_text,
            COALESCE(total_duration_ms, 0) as total_duration_ms,
            COALESCE(execution_duration_ms, 0) as execution_duration_ms,
            COALESCE(compilation_duration_ms, 0) as compilation_duration_ms,
            COALESCE(waiting_at_capacity_duration_ms, 0) as queue_wait_ms,
            COALESCE(waiting_for_compute_duration_ms, 0) as compute_wait_ms,
            COALESCE(result_fetch_duration_ms, 0) as result_fetch_ms,
            COALESCE(produced_rows, 0) as rows_produced,
            COALESCE(read_bytes, 0) as bytes_read,
            COALESCE(read_rows, 0) as rows_read,
            start_time,
            end_time,
            execution_status,
            error_message,
            compute.warehouse_id as warehouse_id,
            query_source.genie_space_id as genie_space_id,
            query_source.genie_conversation_id as genie_conversation_id
        FROM {self.system_catalog}.query.history
        WHERE start_time >= '{start_ts}'
          AND start_time <= '{end_ts}'
          AND query_source.genie_space_id IS NOT NULL
        ORDER BY start_time
        """
        
        results: list[SQLExecutionMetrics] = []
        
        try:
            response = self._client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=query,
                wait_timeout="120s",
            )
            
            if response.status and response.status.state == StatementState.SUCCEEDED:
                if response.result and response.result.data_array:
                    for row in response.result.data_array:
                        metrics = SQLExecutionMetrics(
                            statement_id=str(row[0]) if row[0] else "",
                            total_duration_ms=float(row[2]) if row[2] else 0.0,
                            execution_time_ms=float(row[3]) if row[3] else 0.0,
                            compilation_time_ms=float(row[4]) if row[4] else 0.0,
                            queue_wait_ms=float(row[5]) if row[5] else 0.0,
                            compute_wait_ms=float(row[6]) if row[6] else 0.0,
                            result_fetch_ms=float(row[7]) if row[7] else 0.0,
                            rows_produced=int(row[8]) if row[8] else 0,
                            bytes_read=int(row[9]) if row[9] else 0,
                            rows_read=int(row[10]) if row[10] else 0,
                            start_time=datetime.fromisoformat(str(row[11])) if row[11] else start_time,
                            end_time=datetime.fromisoformat(str(row[12])) if row[12] else end_time,
                            execution_status=str(row[13]) if row[13] else "UNKNOWN",
                            error_message=str(row[14]) if row[14] else None,
                            warehouse_id=str(row[15]) if row[15] else None,
                            genie_space_id=str(row[16]) if row[16] else None,
                            genie_conversation_id=str(row[17]) if row[17] else None,
                        )
                        results.append(metrics)
                        
                        # Also cache by SQL text if we have it
                        if row[1]:
                            cache_key = self._sql_hash(str(row[1]))
                            with self._lock:
                                self._cache[cache_key] = metrics
                        
        except Exception as e:
            import logging
            logging.warning(f"Failed to fetch query history for timerange: {e}")
        
        return results


# Global instance for use across the application
_query_history_client: Optional[QueryHistoryClient] = None
_client_lock = threading.Lock()


def get_query_history_client() -> Optional[QueryHistoryClient]:
    """
    Get the global QueryHistoryClient instance.
    
    Initializes the client on first call using environment variables:
    - GENIE_WAREHOUSE_ID: Required warehouse ID
    - SYSTEM_CATALOG: Optional catalog name (default: "system")
    
    Returns:
        QueryHistoryClient instance, or None if warehouse ID not configured.
    """
    global _query_history_client
    
    with _client_lock:
        if _query_history_client is None:
            warehouse_id = os.environ.get("GENIE_WAREHOUSE_ID", "")
            if warehouse_id:
                system_catalog = os.environ.get("SYSTEM_CATALOG", "system")
                _query_history_client = QueryHistoryClient(
                    warehouse_id=warehouse_id,
                    system_catalog=system_catalog,
                )
        
        return _query_history_client
