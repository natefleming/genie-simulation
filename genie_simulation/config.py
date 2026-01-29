"""
Configuration loader for Genie load tests.

This module provides a centralized way to load configuration from .env files,
supporting both local development and Databricks notebook environments.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from typing import Any

from dotenv import dotenv_values


@dataclass
class LoadTestConfig:
    """Configuration for Genie load tests."""
    
    # Required settings
    space_id: str
    
    # Conversations file (relative to notebook directory)
    conversations_file: str = "conversations.yaml"
    
    # Wait time between messages
    min_wait: float = 8.0
    max_wait: float = 30.0
    
    # Sampling configuration
    sample_size: int | None = None
    sample_seed: int | None = None
    
    @classmethod
    def from_env(cls, env_vars: dict[str, str | None]) -> "LoadTestConfig":
        """
        Create configuration from environment variables dictionary.
        
        Args:
            env_vars: Dictionary of environment variables (from dotenv_values or os.environ)
        
        Returns:
            LoadTestConfig instance
        """
        space_id = env_vars.get("GENIE_SPACE_ID", "")
        if not space_id:
            raise ValueError("GENIE_SPACE_ID is required in .env file")
        
        sample_size_str = env_vars.get("GENIE_SAMPLE_SIZE", "")
        sample_size = int(sample_size_str) if sample_size_str and sample_size_str.isdigit() else None
        
        sample_seed_str = env_vars.get("GENIE_SAMPLE_SEED", "")
        sample_seed = int(sample_seed_str) if sample_seed_str and sample_seed_str.isdigit() else None
        
        return cls(
            space_id=space_id,
            conversations_file=env_vars.get("GENIE_CONVERSATIONS_FILE", "conversations.yaml"),
            min_wait=float(env_vars.get("GENIE_MIN_WAIT", "8")),
            max_wait=float(env_vars.get("GENIE_MAX_WAIT", "30")),
            sample_size=sample_size,
            sample_seed=sample_seed,
        )
    
    @classmethod
    def from_env_file(cls, env_file_path: str | Path) -> "LoadTestConfig":
        """
        Load configuration from a local .env file.
        
        Args:
            env_file_path: Path to the .env file
        
        Returns:
            LoadTestConfig instance
        """
        env_vars = dotenv_values(env_file_path)
        return cls.from_env(env_vars)
    
    @classmethod
    def from_workspace_env(
        cls,
        workspace_client: Any,
        notebook_dir: str,
        env_filename: str = ".env",
    ) -> "LoadTestConfig":
        """
        Load configuration from a .env file in a Databricks workspace directory.
        
        Args:
            workspace_client: Databricks WorkspaceClient instance
            notebook_dir: Workspace path to the notebook directory
            env_filename: Name of the .env file (default: ".env")
        
        Returns:
            LoadTestConfig instance
        """
        workspace_env_path = os.path.join(notebook_dir, env_filename)
        
        try:
            with workspace_client.workspace.download(workspace_env_path) as f:
                content = f.read().decode("utf-8")
        except Exception as e:
            raise FileNotFoundError(
                f"Could not find .env file at: {workspace_env_path}\n"
                f"Please upload your .env file to the same directory as the notebook.\n"
                f"Error: {e}"
            )
        
        # Parse the .env content
        env_vars = dotenv_values(stream=StringIO(content))
        return cls.from_env(env_vars)


@dataclass
class CacheConfig:
    """Configuration for cache service."""
    
    # Required for cache
    lakebase_instance: str
    warehouse_id: str
    
    # Optional with defaults
    cache_ttl: int = 86400  # 24 hours
    similarity_threshold: float = 0.85
    lru_capacity: int = 100
    
    # Credentials (loaded separately from secrets)
    client_id: str | None = None
    client_secret: str | None = None
    
    @classmethod
    def from_env(cls, env_vars: dict[str, str | None]) -> "CacheConfig":
        """
        Create cache configuration from environment variables dictionary.
        
        Args:
            env_vars: Dictionary of environment variables
        
        Returns:
            CacheConfig instance
        """
        lakebase_instance = env_vars.get("GENIE_LAKEBASE_INSTANCE", "")
        if not lakebase_instance:
            raise ValueError("GENIE_LAKEBASE_INSTANCE is required in .env file for cached load test")
        
        warehouse_id = env_vars.get("GENIE_WAREHOUSE_ID", "")
        if not warehouse_id:
            raise ValueError("GENIE_WAREHOUSE_ID is required in .env file for cached load test")
        
        return cls(
            lakebase_instance=lakebase_instance,
            warehouse_id=warehouse_id,
            cache_ttl=int(env_vars.get("GENIE_CACHE_TTL", "86400")),
            similarity_threshold=float(env_vars.get("GENIE_SIMILARITY_THRESHOLD", "0.85")),
            lru_capacity=int(env_vars.get("GENIE_LRU_CAPACITY", "100")),
            client_id=env_vars.get("GENIE_LAKEBASE_CLIENT_ID"),
            client_secret=env_vars.get("GENIE_LAKEBASE_CLIENT_SECRET"),
        )
    
    @classmethod
    def from_workspace_env(
        cls,
        workspace_client: Any,
        notebook_dir: str,
        env_filename: str = ".env",
        dbutils: Any = None,
        secret_scope: str = "genie-loadtest",
    ) -> "CacheConfig":
        """
        Load cache configuration from a .env file in a Databricks workspace directory.
        
        Credentials are loaded from:
        1. Databricks secret scope (if dbutils provided)
        2. Fallback to .env file
        
        Args:
            workspace_client: Databricks WorkspaceClient instance
            notebook_dir: Workspace path to the notebook directory
            env_filename: Name of the .env file (default: ".env")
            dbutils: Databricks dbutils for secret access (optional)
            secret_scope: Name of the secret scope for credentials
        
        Returns:
            CacheConfig instance
        """
        workspace_env_path = os.path.join(notebook_dir, env_filename)
        
        try:
            with workspace_client.workspace.download(workspace_env_path) as f:
                content = f.read().decode("utf-8")
        except Exception as e:
            raise FileNotFoundError(
                f"Could not find .env file at: {workspace_env_path}\n"
                f"Please upload your .env file to the same directory as the notebook.\n"
                f"Error: {e}"
            )
        
        # Parse the .env content
        env_vars = dotenv_values(stream=StringIO(content))
        config = cls.from_env(env_vars)
        
        # Try to load credentials from secret scope
        if dbutils is not None:
            try:
                config.client_id = dbutils.secrets.get(scope=secret_scope, key="lakebase-client-id")
                config.client_secret = dbutils.secrets.get(scope=secret_scope, key="lakebase-client-secret")
            except Exception:
                # Fall back to .env values (already set in from_env)
                pass
        
        # Validate credentials
        if not config.client_id or not config.client_secret:
            raise ValueError(
                "Lakebase credentials not found.\n\n"
                f"Option 1: Create a Databricks secret scope '{secret_scope}' with:\n"
                "  - Key: lakebase-client-id\n"
                "  - Key: lakebase-client-secret\n\n"
                "Option 2: Set in .env file:\n"
                "  - GENIE_LAKEBASE_CLIENT_ID\n"
                "  - GENIE_LAKEBASE_CLIENT_SECRET"
            )
        
        return config


def get_notebook_directory(dbutils: Any) -> str:
    """
    Get the workspace directory containing the current notebook.
    
    Args:
        dbutils: Databricks dbutils
    
    Returns:
        Workspace path to the notebook's directory
    """
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    return os.path.dirname(notebook_path)


def download_workspace_file(
    workspace_client: Any,
    workspace_path: str,
    local_dir: str | None = None,
) -> Path:
    """
    Download a file from Databricks workspace to local temp directory.
    
    Args:
        workspace_client: Databricks WorkspaceClient instance
        workspace_path: Workspace path to the file
        local_dir: Local directory to save to (creates temp dir if None)
    
    Returns:
        Path to the downloaded local file
    """
    if local_dir is None:
        local_dir = tempfile.mkdtemp()
    
    filename = os.path.basename(workspace_path)
    local_path = Path(local_dir) / filename
    
    try:
        with workspace_client.workspace.download(workspace_path) as f:
            content = f.read()
        
        with open(local_path, "wb") as f:
            f.write(content)
        
        return local_path
    except Exception as e:
        raise FileNotFoundError(
            f"Could not download file: {workspace_path}\n"
            f"Error: {e}"
        )
