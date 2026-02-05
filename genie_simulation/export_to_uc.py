"""
Export load test results to Unity Catalog.

This module provides functions to export detailed metrics to Unity Catalog
when running in a Databricks notebook environment.
"""

import os
from glob import glob
from pathlib import Path

from loguru import logger


def export_to_unity_catalog_if_available() -> None:
    """
    Export detailed metrics to Unity Catalog if running in Databricks notebook.
    
    This function checks if a Spark session is available (indicating a Databricks
    notebook environment) and exports the detailed metrics CSV to a UC table.
    
    In CLI mode (no Spark), this function silently returns.
    """
    # Check if Spark is available
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            logger.debug("No active Spark session - skipping UC export")
            return
    except ImportError:
        logger.debug("PySpark not available - skipping UC export")
        return
    
    # Get UC configuration from environment
    uc_catalog = os.environ.get("MLFLOW_UC_CATALOG", "")
    uc_schema = os.environ.get("MLFLOW_UC_SCHEMA", "")
    
    if not uc_catalog or not uc_schema:
        logger.debug("UC catalog/schema not configured - skipping UC export")
        return
    
    # Find the most recent detailed metrics CSV file
    csv_files = sorted(glob("results/genie_detailed_metrics_*.csv"))
    if not csv_files:
        logger.debug("No detailed metrics CSV files found - skipping UC export")
        return
    
    # Use the most recent file (last in sorted list by timestamp in filename)
    csv_path = Path(csv_files[-1])
    logger.info(f"Found metrics file: {csv_path}")
    
    try:
        import pandas as pd
        
        df = pd.read_csv(csv_path)
        if not df.empty:
            table_name = f"{uc_catalog}.{uc_schema}.genie_detailed_metrics"
            spark_df = spark.createDataFrame(df)
            spark_df.write.mode("append").saveAsTable(table_name)
            logger.info(f"Exported {len(df)} records to {table_name}")
    except Exception as e:
        logger.warning(f"Failed to export to UC: {e}")
