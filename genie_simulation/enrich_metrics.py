"""
Enrich detailed metrics with SQL execution data from system tables.

This module provides post-processing functionality to add:
1. SQL execution metrics from system.query.history (execution time, compilation, etc.)
2. AI overhead from system.access.audit (time from message to first SQL query)
3. Bottleneck classification and speed categorization

The enrichment must be run after the system tables have been updated
with the data, which typically takes 15-30 minutes after the queries execute.

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


def classify_bottleneck(row: pd.Series) -> str:
    """
    Classify the dominant bottleneck for a query based on time breakdown.
    
    Uses the following heuristics from the genie-audit-streamlit reference:
    - COMPUTE_STARTUP: compute_wait > 50% of total
    - QUEUE_WAIT: queue_wait > 30% of total
    - COMPILATION: compilation > 40% of total
    - LARGE_SCAN: bytes_read > 1GB
    - SLOW_EXECUTION: execution > 10s
    - NORMAL: none of the above
    
    Args:
        row: A pandas Series with SQL execution metric columns.
        
    Returns:
        Bottleneck classification string.
    """
    total = row.get("sql_total_duration_ms", 0) or 0
    if total <= 0:
        return "UNKNOWN"
    
    compute_wait = row.get("sql_compute_wait_ms", 0) or 0
    queue_wait = row.get("sql_queue_wait_ms", 0) or 0
    compilation = row.get("sql_compilation_time_ms", 0) or 0
    execution = row.get("sql_execution_time_ms", 0) or 0
    bytes_read = row.get("sql_bytes_read", 0) or 0
    
    if compute_wait > total * 0.5:
        return "COMPUTE_STARTUP"
    elif queue_wait > total * 0.3:
        return "QUEUE_WAIT"
    elif compilation > total * 0.4:
        return "COMPILATION"
    elif bytes_read > 1_073_741_824:  # 1 GB
        return "LARGE_SCAN"
    elif execution > 10_000:  # 10 seconds
        return "SLOW_EXECUTION"
    else:
        return "NORMAL"


def classify_speed(duration_ms: float) -> str:
    """
    Classify query speed into buckets.
    
    Args:
        duration_ms: Total duration in milliseconds.
        
    Returns:
        Speed category string.
    """
    if pd.isna(duration_ms) or duration_ms <= 0:
        return "UNKNOWN"
    
    duration_s = duration_ms / 1000.0
    if duration_s < 5:
        return "FAST"
    elif duration_s < 10:
        return "MODERATE"
    elif duration_s < 30:
        return "SLOW"
    else:
        return "CRITICAL"


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
    #   - read_rows -> sql_rows_read (rows scanned, not returned)
    #   - error_message -> sql_error_message (reason for failure)
    #   - compute.warehouse_id -> sql_warehouse_id (which warehouse ran the query)
    #   - query_source.genie_space_id -> (used for filtering to Genie queries)
    #   - query_source.genie_conversation_id -> (used for matching)
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
        COALESCE(read_rows, 0) as rows_read,
        execution_status,
        error_message,
        compute.warehouse_id as warehouse_id,
        query_source.genie_space_id as genie_space_id,
        query_source.genie_conversation_id as genie_conversation_id
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
            wait_timeout="50s",
        )
        
        if response.status and response.status.state == StatementState.SUCCEEDED:
            if response.result and response.result.data_array:
                columns = [
                    "statement_id", "statement_text", "start_time", "end_time",
                    "total_duration_ms", "execution_duration_ms", "compilation_duration_ms",
                    "queue_wait_ms", "compute_wait_ms", "result_fetch_ms",
                    "rows_produced", "bytes_read", "rows_read",
                    "execution_status", "error_message", "warehouse_id",
                    "genie_space_id", "genie_conversation_id"
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
                df["rows_read"] = pd.to_numeric(df["rows_read"], errors="coerce").fillna(0).astype(int)
                
                # Normalize SQL for matching
                df["sql_normalized"] = df["statement_text"].apply(normalize_sql)
                
                return df
                
        return pd.DataFrame()
        
    except Exception as e:
        print(f"Error fetching query history: {e}")
        return pd.DataFrame()


def get_audit_events_for_timerange(
    warehouse_id: str,
    start_time: datetime,
    end_time: datetime,
    space_id: str,
    system_catalog: str = "system",
    client: Optional[WorkspaceClient] = None,
) -> pd.DataFrame:
    """
    Fetch Genie audit events from system.access.audit for AI overhead calculation.
    
    The audit log captures when Genie API received a message. By comparing this
    with the query start_time from system.query.history, we can calculate the
    AI overhead (NL-to-SQL inference time).
    
    Args:
        warehouse_id: SQL warehouse ID to use for querying.
        start_time: Start of the time range.
        end_time: End of the time range.
        space_id: Genie space ID to filter events.
        system_catalog: Catalog containing system tables (default: "system").
        client: Optional WorkspaceClient instance.
        
    Returns:
        DataFrame with audit event entries.
    """
    if client is None:
        client = WorkspaceClient()
    
    # Format timestamps for date filter (audit uses event_date for partition pruning)
    start_date = start_time.strftime("%Y-%m-%d")
    end_date = (end_time + timedelta(days=1)).strftime("%Y-%m-%d")
    start_ts = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = end_time.strftime("%Y-%m-%d %H:%M:%S")
    
    # Query system.access.audit for Genie message events
    # These events record when the Genie API received a user message
    query = f"""
    SELECT
        event_time as message_time,
        request_params.conversation_id as conversation_id,
        request_params.message_id as message_id,
        user_identity.email as user_email,
        action_name
    FROM {system_catalog}.access.audit
    WHERE service_name = 'aibiGenie'
      AND event_date >= '{start_date}'
      AND event_date <= '{end_date}'
      AND event_time >= '{start_ts}'
      AND event_time <= '{end_ts}'
      AND action_name IN (
          'genieStartConversationMessage',
          'genieContinueConversationMessage',
          'genieCreateConversationMessage',
          'createConversationMessage',
          'regenerateConversationMessage'
      )
      AND request_params.space_id = '{space_id}'
    ORDER BY event_time
    """
    
    try:
        response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout="50s",
        )
        
        if response.status and response.status.state == StatementState.SUCCEEDED:
            if response.result and response.result.data_array:
                columns = [
                    "message_time", "conversation_id", "message_id",
                    "user_email", "action_name"
                ]
                df = pd.DataFrame(response.result.data_array, columns=columns)
                df["message_time"] = pd.to_datetime(df["message_time"])
                return df
                
        return pd.DataFrame()
        
    except Exception as e:
        print(f"Error fetching audit events: {e}")
        return pd.DataFrame()


def match_queries(
    metrics_df: pd.DataFrame,
    history_df: pd.DataFrame,
    audit_df: Optional[pd.DataFrame] = None,
    time_tolerance_seconds: int = 300,
) -> pd.DataFrame:
    """
    Match metrics rows with query history entries.
    
    Uses a two-tier matching strategy:
    1. Primary: Match by genie_conversation_id (most reliable)
    2. Fallback: Match by SQL text prefix + timestamp proximity
    
    Optionally computes AI overhead from audit events.
    
    Args:
        metrics_df: DataFrame with detailed metrics.
        history_df: DataFrame with query history.
        audit_df: Optional DataFrame with audit events for AI overhead.
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
    
    # Check if conversation ID matching is possible
    has_conv_id = (
        "genie_conversation_id" in metrics_df.columns
        and "genie_conversation_id" in history_df.columns
    )
    
    matched_count = 0
    conv_id_matched = 0
    sql_text_matched = 0
    
    for idx, row in metrics_df.iterrows():
        if not row["sql_normalized"]:
            continue
        
        match = None
        
        # Strategy 1: Match by genie_conversation_id (most reliable)
        if has_conv_id and pd.notna(row.get("genie_conversation_id")):
            conv_id = row["genie_conversation_id"]
            candidates = history_df[
                history_df["genie_conversation_id"] == conv_id
            ]
            
            if not candidates.empty:
                # If multiple queries for this conversation, match by time proximity
                request_time = row["request_started_at"]
                time_diffs = abs((candidates["start_time"] - request_time).dt.total_seconds())
                candidates = candidates[time_diffs <= time_tolerance_seconds]
                
                if not candidates.empty:
                    best_idx = time_diffs[candidates.index].idxmin()
                    match = candidates.loc[best_idx]
                    conv_id_matched += 1
        
        # Strategy 2: Fallback to SQL text prefix matching
        if match is None:
            sql_prefix = row["sql_normalized"][:100]
            
            candidates = history_df[
                history_df["sql_normalized"].str.startswith(sql_prefix, na=False)
            ]
            
            if candidates.empty:
                # Try a more lenient match
                candidates = history_df[
                    history_df["sql_normalized"].str.contains(
                        sql_prefix[:50].replace("(", r"\(").replace(")", r"\)"),
                        regex=True,
                        na=False
                    )
                ]
            
            if not candidates.empty:
                # Filter by time proximity
                request_time = row["request_started_at"]
                time_diffs = abs((candidates["start_time"] - request_time).dt.total_seconds())
                candidates = candidates[time_diffs <= time_tolerance_seconds]
                
                if not candidates.empty:
                    best_idx = time_diffs.idxmin()
                    match = candidates.loc[best_idx]
                    sql_text_matched += 1
        
        if match is None:
            continue
        
        # Update the metrics row with all SQL execution metrics
        metrics_df.at[idx, "sql_statement_id"] = match["statement_id"]
        metrics_df.at[idx, "sql_total_duration_ms"] = match["total_duration_ms"]
        metrics_df.at[idx, "sql_execution_time_ms"] = match["execution_duration_ms"]
        metrics_df.at[idx, "sql_compilation_time_ms"] = match["compilation_duration_ms"]
        metrics_df.at[idx, "sql_queue_wait_ms"] = match["queue_wait_ms"]
        metrics_df.at[idx, "sql_compute_wait_ms"] = match["compute_wait_ms"]
        metrics_df.at[idx, "sql_result_fetch_ms"] = match["result_fetch_ms"]
        metrics_df.at[idx, "sql_rows_produced"] = match["rows_produced"]
        metrics_df.at[idx, "sql_bytes_read"] = match["bytes_read"]
        metrics_df.at[idx, "sql_rows_read"] = match.get("rows_read", 0)
        metrics_df.at[idx, "sql_error_message"] = match.get("error_message")
        metrics_df.at[idx, "sql_warehouse_id"] = match.get("warehouse_id")
        matched_count += 1
    
    # Compute AI overhead from audit events if available
    if audit_df is not None and not audit_df.empty:
        _compute_ai_overhead(metrics_df, history_df, audit_df)
    
    # Add bottleneck classification and speed category for matched rows
    enriched_mask = metrics_df["sql_total_duration_ms"].notna()
    if enriched_mask.any():
        metrics_df.loc[enriched_mask, "sql_bottleneck"] = metrics_df[enriched_mask].apply(
            classify_bottleneck, axis=1
        )
        metrics_df.loc[enriched_mask, "sql_speed_category"] = metrics_df[enriched_mask][
            "sql_total_duration_ms"
        ].apply(classify_speed)
    
    # Clean up temp column
    metrics_df.drop(columns=["sql_normalized"], inplace=True, errors="ignore")
    
    print(f"Matched {matched_count} of {len(metrics_df)} requests with query history")
    if has_conv_id:
        print(f"  - {conv_id_matched} matched by conversation ID")
        print(f"  - {sql_text_matched} matched by SQL text (fallback)")
    return metrics_df


def _compute_ai_overhead(
    metrics_df: pd.DataFrame,
    history_df: pd.DataFrame,
    audit_df: pd.DataFrame,
) -> None:
    """
    Compute AI overhead by correlating audit events with query history.
    
    AI overhead = time from message event (audit) to first query start (query history).
    This represents NL-to-SQL inference time.
    
    Modifies metrics_df in place.
    
    Args:
        metrics_df: DataFrame with detailed metrics (modified in place).
        history_df: DataFrame with query history.
        audit_df: DataFrame with audit events.
    """
    if audit_df.empty or history_df.empty:
        return
    
    # Build a lookup: conversation_id -> earliest query start_time
    if "genie_conversation_id" not in history_df.columns:
        return
    
    # For each audit message event, find the first query that followed it
    # in the same conversation
    overhead_count = 0
    
    for idx, row in metrics_df.iterrows():
        conv_id = row.get("genie_conversation_id")
        if pd.isna(conv_id) or not conv_id:
            continue
        
        # Find matching audit event for this conversation
        audit_matches = audit_df[audit_df["conversation_id"] == conv_id]
        if audit_matches.empty:
            continue
        
        # Find the audit event closest to (but before) the request start time
        request_time = row["request_started_at"]
        audit_before = audit_matches[audit_matches["message_time"] <= request_time]
        
        if audit_before.empty:
            # Try using the closest audit event within tolerance
            time_diffs = abs((audit_matches["message_time"] - request_time).dt.total_seconds())
            close_events = audit_matches[time_diffs <= 60]
            if close_events.empty:
                continue
            best_audit = close_events.loc[time_diffs[close_events.index].idxmin()]
        else:
            # Take the most recent audit event before the request
            best_audit = audit_before.loc[audit_before["message_time"].idxmax()]
        
        message_time = best_audit["message_time"]
        
        # Find the first query for this conversation after the message
        conv_queries = history_df[
            (history_df["genie_conversation_id"] == conv_id)
            & (history_df["start_time"] >= message_time)
        ]
        
        if conv_queries.empty:
            continue
        
        first_query_time = conv_queries["start_time"].min()
        
        # AI overhead = time from message to first query start
        ai_overhead_ms = (first_query_time - message_time).total_seconds() * 1000
        
        # Sanity check: AI overhead should be positive and reasonable (< 5 minutes)
        if 0 <= ai_overhead_ms <= 300_000:
            metrics_df.at[idx, "ai_overhead_ms"] = ai_overhead_ms
            overhead_count += 1
    
    print(f"  Computed AI overhead for {overhead_count} requests")


def enrich_metrics_with_query_history(
    metrics_csv_path: str,
    warehouse_id: Optional[str] = None,
    system_catalog: Optional[str] = None,
    output_path: Optional[str] = None,
    time_buffer_minutes: int = 30,
) -> pd.DataFrame:
    """
    Enrich detailed_metrics.csv with SQL execution metrics and AI overhead.
    
    This function:
    1. Queries system.query.history for SQL execution metrics
    2. Queries system.access.audit for AI overhead (message -> query timing)
    3. Adds bottleneck classification and speed categorization
    
    Should be run after the load test completes AND after waiting for the
    system tables to be updated (typically 15-30 minutes).
    
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
    
    # Fetch query history from system.query.history
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
    
    # Fetch audit events from system.access.audit for AI overhead
    audit_df = pd.DataFrame()
    space_id = metrics_df["space_id"].iloc[0] if "space_id" in metrics_df.columns else None
    if space_id:
        print(f"Fetching audit events from {system_catalog}.access.audit...")
        try:
            audit_df = get_audit_events_for_timerange(
                warehouse_id=warehouse_id,
                start_time=start_time,
                end_time=end_time,
                space_id=space_id,
                system_catalog=system_catalog,
            )
            print(f"  Found {len(audit_df)} audit events")
        except Exception as e:
            print(f"  Warning: Could not fetch audit events: {e}")
            print("  AI overhead will not be calculated.")
    else:
        print("  No space_id found in metrics - skipping audit event lookup")
    
    # Match and enrich
    print("Matching queries...")
    enriched_df = match_queries(metrics_df, history_df, audit_df=audit_df)
    
    # Save
    print(f"Saving enriched metrics to: {output_path}")
    enriched_df.to_csv(output_path, index=False)
    
    # Summary
    enriched_count = enriched_df["sql_total_duration_ms"].notna().sum()
    print(f"\nEnrichment complete:")
    print(f"  Total rows: {len(enriched_df)}")
    print(f"  Enriched rows: {enriched_count}")
    print(f"  Enrichment rate: {enriched_count/sql_count*100:.1f}% of SQL queries")
    
    if enriched_count > 0:
        # Show data source summary
        print(f"\nData Sources:")
        print(f"  {system_catalog}.query.history -> SQL execution metrics:")
        print(f"    sql_total_duration_ms, sql_execution_time_ms, sql_compilation_time_ms")
        print(f"    sql_queue_wait_ms, sql_compute_wait_ms, sql_result_fetch_ms")
        print(f"    sql_rows_produced, sql_bytes_read, sql_rows_read")
        print(f"    sql_error_message, sql_warehouse_id")
        
        # AI overhead summary
        if "ai_overhead_ms" in enriched_df.columns:
            overhead_count = enriched_df["ai_overhead_ms"].notna().sum()
            if overhead_count > 0:
                avg_overhead = enriched_df["ai_overhead_ms"].mean()
                print(f"  {system_catalog}.access.audit -> AI overhead:")
                print(f"    ai_overhead_ms: {overhead_count} rows, avg {avg_overhead:.0f}ms")
        
        # Bottleneck summary
        if "sql_bottleneck" in enriched_df.columns:
            bottleneck_counts = enriched_df["sql_bottleneck"].value_counts()
            print(f"\n  Bottleneck Distribution:")
            for bottleneck, count in bottleneck_counts.items():
                if pd.notna(bottleneck):
                    print(f"    {bottleneck}: {count}")
    
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
