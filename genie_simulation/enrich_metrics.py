"""
Enrich detailed metrics with SQL execution data from system.query.history.

This module provides post-processing functionality to add SQL execution metrics
(execution time, compilation time, rows produced, bytes read/written) to the
detailed_metrics.csv file after a load test completes.

The enrichment must be run after the system.query.history table has been updated
with the query data, which typically takes 15-30 minutes after the queries execute.

Usage:
    from genie_simulation.enrich_metrics import enrich_metrics_with_query_history
    
    # Run after load test and waiting for system table ingestion
    enriched_df = enrich_metrics_with_query_history(
        metrics_csv_path="results/genie_loadtest_.../detailed_metrics.csv",
        warehouse_id="your-warehouse-id",
    )
"""

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def normalize_sql(sql: str) -> str:
    """Normalize SQL for matching by removing whitespace variations."""
    if not sql or pd.isna(sql):
        return ""
    # Remove extra whitespace and normalize
    normalized = " ".join(str(sql).split())
    return normalized.strip().lower()


def get_query_history_for_timerange(
    warehouse_id: str,
    start_time: datetime,
    end_time: datetime,
    system_catalog: str = "system",
    client: Optional[WorkspaceClient] = None,
) -> pd.DataFrame:
    """
    Fetch all query history entries for a time range.
    
    Args:
        warehouse_id: SQL warehouse ID to use for querying.
        start_time: Start of the time range.
        end_time: End of the time range.
        system_catalog: Catalog containing system tables (default: "system").
        client: Optional WorkspaceClient instance.
        
    Returns:
        DataFrame with query history entries.
    """
    if client is None:
        client = WorkspaceClient()
    
    # Format timestamps
    start_ts = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = end_time.strftime("%Y-%m-%d %H:%M:%S")
    
    # Query system.query.history for SQL execution metrics
    # Column mapping (system.query.history -> our field names):
    #   - statement_id -> sql_statement_id
    #   - total_duration_ms -> sql_total_duration_ms (total time in warehouse)
    #   - execution_duration_ms -> sql_execution_time_ms (time executing)
    #   - compilation_duration_ms -> sql_compilation_time_ms (time planning)
    #   - waiting_at_capacity_duration_ms -> sql_queue_wait_ms (waiting in queue)
    #   - waiting_for_compute_duration_ms -> sql_compute_wait_ms (waiting for startup)
    #   - result_fetch_duration_ms -> sql_result_fetch_ms (fetching results)
    #   - produced_rows -> sql_rows_produced
    #   - read_bytes -> sql_bytes_read
    #   - execution_status -> (used for filtering)
    #   - query_source.genie_space_id -> (used for filtering to Genie queries)
    query = f"""
    SELECT
        statement_id,
        statement_text,
        start_time,
        end_time,
        COALESCE(total_duration_ms, 0) as total_duration_ms,
        COALESCE(execution_duration_ms, 0) as execution_duration_ms,
        COALESCE(compilation_duration_ms, 0) as compilation_duration_ms,
        COALESCE(waiting_at_capacity_duration_ms, 0) as queue_wait_ms,
        COALESCE(waiting_for_compute_duration_ms, 0) as compute_wait_ms,
        COALESCE(result_fetch_duration_ms, 0) as result_fetch_ms,
        COALESCE(produced_rows, 0) as rows_produced,
        COALESCE(read_bytes, 0) as bytes_read,
        execution_status,
        query_source.genie_space_id as genie_space_id
    FROM {system_catalog}.query.history
    WHERE start_time >= '{start_ts}'
      AND start_time <= '{end_ts}'
      AND query_source.genie_space_id IS NOT NULL
    ORDER BY start_time
    """
    
    try:
        response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout="120s",
        )
        
        if response.status and response.status.state == StatementState.SUCCEEDED:
            if response.result and response.result.data_array:
                columns = [
                    "statement_id", "statement_text", "start_time", "end_time",
                    "total_duration_ms", "execution_duration_ms", "compilation_duration_ms",
                    "queue_wait_ms", "compute_wait_ms", "result_fetch_ms",
                    "rows_produced", "bytes_read", "execution_status", "genie_space_id"
                ]
                df = pd.DataFrame(response.result.data_array, columns=columns)
                
                # Convert numeric types
                numeric_cols = [
                    "total_duration_ms", "execution_duration_ms", "compilation_duration_ms",
                    "queue_wait_ms", "compute_wait_ms", "result_fetch_ms"
                ]
                for col in numeric_cols:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
                
                df["rows_produced"] = pd.to_numeric(df["rows_produced"], errors="coerce").fillna(0).astype(int)
                df["bytes_read"] = pd.to_numeric(df["bytes_read"], errors="coerce").fillna(0).astype(int)
                
                # Normalize SQL for matching
                df["sql_normalized"] = df["statement_text"].apply(normalize_sql)
                
                return df
                
        return pd.DataFrame()
        
    except Exception as e:
        print(f"Error fetching query history: {e}")
        return pd.DataFrame()


def match_queries(
    metrics_df: pd.DataFrame,
    history_df: pd.DataFrame,
    time_tolerance_seconds: int = 300,
) -> pd.DataFrame:
    """
    Match metrics rows with query history entries.
    
    Uses SQL text prefix matching and timestamp proximity to find the best match.
    
    Args:
        metrics_df: DataFrame with detailed metrics.
        history_df: DataFrame with query history.
        time_tolerance_seconds: Maximum time difference for matching (default: 5 min).
        
    Returns:
        Metrics DataFrame with SQL execution columns populated.
    """
    if history_df.empty:
        print("No query history data to match")
        return metrics_df
    
    # Normalize SQL in metrics
    metrics_df = metrics_df.copy()
    metrics_df["sql_normalized"] = metrics_df["sql"].apply(normalize_sql)
    
    # Convert timestamps
    metrics_df["request_started_at"] = pd.to_datetime(metrics_df["request_started_at"])
    history_df["start_time"] = pd.to_datetime(history_df["start_time"])
    
    matched_count = 0
    
    for idx, row in metrics_df.iterrows():
        if not row["sql_normalized"]:
            continue
        
        # Find potential matches by SQL prefix (first 100 chars normalized)
        sql_prefix = row["sql_normalized"][:100]
        
        # Filter history by SQL prefix match
        candidates = history_df[
            history_df["sql_normalized"].str.startswith(sql_prefix, na=False)
        ]
        
        if candidates.empty:
            # Try a more lenient match - look for any overlap
            candidates = history_df[
                history_df["sql_normalized"].str.contains(
                    sql_prefix[:50].replace("(", r"\(").replace(")", r"\)"), 
                    regex=True, 
                    na=False
                )
            ]
        
        if candidates.empty:
            continue
        
        # Filter by time proximity
        request_time = row["request_started_at"]
        time_diffs = abs((candidates["start_time"] - request_time).dt.total_seconds())
        candidates = candidates[time_diffs <= time_tolerance_seconds]
        
        if candidates.empty:
            continue
        
        # Take the closest match by time
        time_diffs = abs((candidates["start_time"] - request_time).dt.total_seconds())
        best_idx = time_diffs.idxmin()
        match = candidates.loc[best_idx]
        
        # Update the metrics row with all SQL execution metrics
        # These map directly from system.query.history columns
        metrics_df.at[idx, "sql_statement_id"] = match["statement_id"]
        metrics_df.at[idx, "sql_total_duration_ms"] = match["total_duration_ms"]
        metrics_df.at[idx, "sql_execution_time_ms"] = match["execution_duration_ms"]
        metrics_df.at[idx, "sql_compilation_time_ms"] = match["compilation_duration_ms"]
        metrics_df.at[idx, "sql_queue_wait_ms"] = match["queue_wait_ms"]
        metrics_df.at[idx, "sql_compute_wait_ms"] = match["compute_wait_ms"]
        metrics_df.at[idx, "sql_result_fetch_ms"] = match["result_fetch_ms"]
        metrics_df.at[idx, "sql_rows_produced"] = match["rows_produced"]
        metrics_df.at[idx, "sql_bytes_read"] = match["bytes_read"]
        matched_count += 1
    
    # Clean up temp column
    metrics_df.drop(columns=["sql_normalized"], inplace=True)
    
    print(f"Matched {matched_count} of {len(metrics_df)} requests with query history")
    return metrics_df


def enrich_metrics_with_query_history(
    metrics_csv_path: str,
    warehouse_id: Optional[str] = None,
    system_catalog: Optional[str] = None,
    output_path: Optional[str] = None,
    time_buffer_minutes: int = 30,
) -> pd.DataFrame:
    """
    Enrich detailed_metrics.csv with SQL execution metrics from system.query.history.
    
    This function should be run after the load test completes AND after waiting
    for the system.query.history table to be updated (typically 15-30 minutes).
    
    Args:
        metrics_csv_path: Path to the detailed_metrics.csv file.
        warehouse_id: SQL warehouse ID (default: from GENIE_WAREHOUSE_ID env var).
        system_catalog: System catalog name (default: from SYSTEM_CATALOG env var or "system").
        output_path: Optional output path (default: overwrites input file).
        time_buffer_minutes: Extra time buffer around test window (default: 30).
        
    Returns:
        Enriched DataFrame with SQL execution metrics.
    """
    # Get configuration
    if warehouse_id is None:
        warehouse_id = os.environ.get("GENIE_WAREHOUSE_ID", "")
    if not warehouse_id:
        raise ValueError("warehouse_id is required (or set GENIE_WAREHOUSE_ID env var)")
    
    if system_catalog is None:
        system_catalog = os.environ.get("SYSTEM_CATALOG", "system")
    
    if output_path is None:
        output_path = metrics_csv_path
    
    # Load metrics
    print(f"Loading metrics from: {metrics_csv_path}")
    metrics_df = pd.read_csv(metrics_csv_path)
    print(f"  Loaded {len(metrics_df)} rows")
    
    # Check if already enriched (look for any of the SQL execution columns)
    if "sql_total_duration_ms" in metrics_df.columns:
        enriched_count = metrics_df["sql_total_duration_ms"].notna().sum()
        if enriched_count > 0:
            print(f"  Already has {enriched_count} enriched rows")
            return metrics_df
    
    # Get time range
    metrics_df["request_started_at"] = pd.to_datetime(metrics_df["request_started_at"])
    metrics_df["request_completed_at"] = pd.to_datetime(metrics_df["request_completed_at"])
    
    start_time = metrics_df["request_started_at"].min() - timedelta(minutes=time_buffer_minutes)
    end_time = metrics_df["request_completed_at"].max() + timedelta(minutes=time_buffer_minutes)
    
    print(f"  Time range: {start_time} to {end_time}")
    
    # Count rows with SQL
    sql_count = metrics_df["sql"].notna().sum()
    print(f"  Rows with SQL: {sql_count}")
    
    if sql_count == 0:
        print("  No SQL queries to enrich")
        return metrics_df
    
    # Fetch query history
    print(f"Fetching query history from {system_catalog}.query.history...")
    history_df = get_query_history_for_timerange(
        warehouse_id=warehouse_id,
        start_time=start_time,
        end_time=end_time,
        system_catalog=system_catalog,
    )
    print(f"  Found {len(history_df)} query history entries")
    
    if history_df.empty:
        print("  No query history found. The data may not be available yet.")
        print("  System tables typically have 15-30 minute ingestion delay.")
        return metrics_df
    
    # Match and enrich
    print("Matching queries...")
    enriched_df = match_queries(metrics_df, history_df)
    
    # Save
    print(f"Saving enriched metrics to: {output_path}")
    enriched_df.to_csv(output_path, index=False)
    
    # Summary
    enriched_count = enriched_df["sql_total_duration_ms"].notna().sum()
    print(f"\nEnrichment complete:")
    print(f"  Total rows: {len(enriched_df)}")
    print(f"  Enriched rows: {enriched_count}")
    print(f"  Enrichment rate: {enriched_count/sql_count*100:.1f}% of SQL queries")
    
    # Show a sample of enriched columns
    if enriched_count > 0:
        print(f"\nSQL Execution Metrics (from system.query.history):")
        print(f"  - sql_total_duration_ms: Total time in warehouse")
        print(f"  - sql_execution_time_ms: Time executing query")
        print(f"  - sql_compilation_time_ms: Time compiling/planning")
        print(f"  - sql_queue_wait_ms: Time waiting in queue")
        print(f"  - sql_compute_wait_ms: Time waiting for compute")
        print(f"  - sql_result_fetch_ms: Time fetching results")
        print(f"  - sql_rows_produced: Number of rows returned")
        print(f"  - sql_bytes_read: Bytes read from storage")
    
    return enriched_df


def enrich_results_directory(
    results_dir: str,
    warehouse_id: Optional[str] = None,
    system_catalog: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """
    Enrich the detailed_metrics.csv file in a results directory.
    
    Convenience function that handles finding the metrics file.
    
    Args:
        results_dir: Path to the results directory.
        warehouse_id: SQL warehouse ID (optional, uses env var).
        system_catalog: System catalog name (optional, uses env var).
        
    Returns:
        Enriched DataFrame, or None if file not found.
    """
    metrics_path = Path(results_dir) / "detailed_metrics.csv"
    
    if not metrics_path.exists():
        print(f"No detailed_metrics.csv found in {results_dir}")
        return None
    
    return enrich_metrics_with_query_history(
        metrics_csv_path=str(metrics_path),
        warehouse_id=warehouse_id,
        system_catalog=system_catalog,
    )
