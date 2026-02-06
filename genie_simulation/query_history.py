"""
Query history utilities for fetching SQL execution metrics from Databricks system tables.

This module provides a client to query system.query.history by statement_id,
which is captured at runtime from GenieResponse.statement_id.

Usage:
    from genie_simulation.query_history import QueryHistoryClient, SQLExecutionMetrics
    
    client = QueryHistoryClient(warehouse_id="your-warehouse-id")
    
    metrics = client.get_metrics_by_statement_id("01f0-stmt-abc123")
    if metrics:
        print(f"Execution time: {metrics.execution_time_ms}ms")
"""

import os
import threading
from dataclasses import dataclass
from datetime import datetime
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


class QueryHistoryClient:
    """
    Client for querying Databricks system.query.history table by statement_id.
    
    Fetches SQL execution metrics using the statement_id captured at runtime
    from GenieResponse. This provides deterministic matching without fuzzy
    SQL text or time-based heuristics.
    """
    
    def __init__(
        self, 
        warehouse_id: str,
        query_history_table: str = "system.query.history",
    ) -> None:
        """
        Initialize the query history client.
        
        Args:
            warehouse_id: The SQL warehouse ID to use for querying system tables.
            query_history_table: Fully-qualified query history table path
                (default: "system.query.history").
        """
        self.warehouse_id = warehouse_id
        self.query_history_table = query_history_table
        self._client = WorkspaceClient()
        self._lock = threading.Lock()
        self._cache: dict[str, SQLExecutionMetrics] = {}
    
    def get_metrics_by_statement_id(
        self,
        statement_id: str,
    ) -> Optional[SQLExecutionMetrics]:
        """
        Fetch execution metrics by statement_id.
        
        Args:
            statement_id: The SQL statement_id from the Statement Execution API.
            
        Returns:
            SQLExecutionMetrics if found, None otherwise.
        """
        if not statement_id:
            return None
        
        # Check cache first
        with self._lock:
            if statement_id in self._cache:
                return self._cache[statement_id]
        
        # Escape single quotes
        escaped_id = statement_id.replace("'", "''")
        
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
            error_message
        FROM {self.query_history_table}
        WHERE statement_id = '{escaped_id}'
        LIMIT 1
        """
        
        try:
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
                        start_time=datetime.fromisoformat(str(row[10])) if row[10] else datetime.now(),
                        end_time=datetime.fromisoformat(str(row[11])) if row[11] else datetime.now(),
                        execution_status=str(row[12]) if row[12] else "UNKNOWN",
                        error_message=str(row[13]) if row[13] else None,
                    )
                    
                    with self._lock:
                        self._cache[statement_id] = metrics
                    
                    return metrics
            elif response.status:
                error_msg = (
                    response.status.error.message
                    if response.status.error
                    else f"state={response.status.state}"
                )
                import logging
                logging.warning(f"Query history lookup failed: {error_msg}")
                    
        except Exception as e:
            import logging
            logging.warning(f"Failed to fetch query history by statement_id: {e}")
        
        return None

    def clear_cache(self) -> None:
        """Clear the metrics cache."""
        with self._lock:
            self._cache.clear()


# Global instance for use across the application
_query_history_client: Optional[QueryHistoryClient] = None
_client_lock = threading.Lock()


def get_query_history_client() -> Optional[QueryHistoryClient]:
    """
    Get the global QueryHistoryClient instance.
    
    Initializes the client on first call using environment variables:
    - GENIE_WAREHOUSE_ID: Required warehouse ID
    - SYSTEM_QUERY_HISTORY_TABLE: Fully-qualified table path (default: system.query.history)
    
    Returns:
        QueryHistoryClient instance, or None if warehouse ID not configured.
    """
    global _query_history_client
    
    with _client_lock:
        if _query_history_client is None:
            warehouse_id = os.environ.get("GENIE_WAREHOUSE_ID", "")
            if warehouse_id:
                query_history_table = os.environ.get(
                    "SYSTEM_QUERY_HISTORY_TABLE", "system.query.history"
                )
                _query_history_client = QueryHistoryClient(
                    warehouse_id=warehouse_id,
                    query_history_table=query_history_table,
                )
        
        return _query_history_client
