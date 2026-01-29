"""
Configuration loader for Genie load tests.

This module provides a centralized way to load configuration from .env files.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def load_config(env_path: str | Path = "../.env") -> None:
    """
    Load environment variables from .env file.
    
    Args:
        env_path: Path to .env file (default: ../.env for notebooks in subdirectory)
    """
    load_dotenv(env_path)


@dataclass
class LoadTestConfig:
    """Configuration for Genie load tests."""
    
    space_id: str
    conversations_file: str = "conversations.yaml"
    min_wait: float = 8.0
    max_wait: float = 30.0
    sample_size: int | None = None
    sample_seed: int | None = None
    
    @classmethod
    def from_env(cls) -> "LoadTestConfig":
        """
        Create configuration from environment variables.
        
        Call load_config() first to load from .env file.
        
        Returns:
            LoadTestConfig instance
        """
        space_id = os.environ.get("GENIE_SPACE_ID", "")
        if not space_id:
            raise ValueError("GENIE_SPACE_ID is required in .env file")
        
        sample_size_str = os.environ.get("GENIE_SAMPLE_SIZE", "")
        sample_size = int(sample_size_str) if sample_size_str and sample_size_str.isdigit() else None
        
        sample_seed_str = os.environ.get("GENIE_SAMPLE_SEED", "")
        sample_seed = int(sample_seed_str) if sample_seed_str and sample_seed_str.isdigit() else None
        
        return cls(
            space_id=space_id,
            conversations_file=os.environ.get("GENIE_CONVERSATIONS_FILE", "conversations.yaml"),
            min_wait=float(os.environ.get("GENIE_MIN_WAIT", "8")),
            max_wait=float(os.environ.get("GENIE_MAX_WAIT", "30")),
            sample_size=sample_size,
            sample_seed=sample_seed,
        )


@dataclass
class CacheConfig:
    """Configuration for cache service."""
    
    lakebase_instance: str
    warehouse_id: str
    cache_ttl: int = 86400
    similarity_threshold: float = 0.85
    lru_capacity: int = 100
    client_id: str | None = None
    client_secret: str | None = None
    
    @classmethod
    def from_env(cls, dbutils: any = None, secret_scope: str = "genie-loadtest") -> "CacheConfig":
        """
        Create cache configuration from environment variables.
        
        Credentials are loaded from:
        1. Databricks secret scope (if dbutils provided)
        2. Fallback to environment variables
        
        Args:
            dbutils: Databricks dbutils for secret access (optional)
            secret_scope: Name of the secret scope for credentials
        
        Returns:
            CacheConfig instance
        """
        lakebase_instance = os.environ.get("GENIE_LAKEBASE_INSTANCE", "")
        if not lakebase_instance:
            raise ValueError("GENIE_LAKEBASE_INSTANCE is required in .env file")
        
        warehouse_id = os.environ.get("GENIE_WAREHOUSE_ID", "")
        if not warehouse_id:
            raise ValueError("GENIE_WAREHOUSE_ID is required in .env file")
        
        # Get credentials from secret scope or environment
        client_id = None
        client_secret = None
        
        if dbutils is not None:
            try:
                client_id = dbutils.secrets.get(scope=secret_scope, key="lakebase-client-id")
                client_secret = dbutils.secrets.get(scope=secret_scope, key="lakebase-client-secret")
            except Exception:
                pass
        
        # Fallback to environment variables
        if not client_id:
            client_id = os.environ.get("GENIE_LAKEBASE_CLIENT_ID")
        if not client_secret:
            client_secret = os.environ.get("GENIE_LAKEBASE_CLIENT_SECRET")
        
        if not client_id or not client_secret:
            raise ValueError(
                "Lakebase credentials not found.\n\n"
                f"Option 1: Create a Databricks secret scope '{secret_scope}' with:\n"
                "  - Key: lakebase-client-id\n"
                "  - Key: lakebase-client-secret\n\n"
                "Option 2: Set in .env file:\n"
                "  - GENIE_LAKEBASE_CLIENT_ID\n"
                "  - GENIE_LAKEBASE_CLIENT_SECRET"
            )
        
        return cls(
            lakebase_instance=lakebase_instance,
            warehouse_id=warehouse_id,
            cache_ttl=int(os.environ.get("GENIE_CACHE_TTL", "86400")),
            similarity_threshold=float(os.environ.get("GENIE_SIMILARITY_THRESHOLD", "0.85")),
            lru_capacity=int(os.environ.get("GENIE_LRU_CAPACITY", "100")),
            client_id=client_id,
            client_secret=client_secret,
        )
