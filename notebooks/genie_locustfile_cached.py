"""
Standalone Locust file for cached Genie load testing in Databricks notebooks.

This file is designed to be run via the Locust CLI (subprocess) from notebooks.
All configuration is read from environment variables set by the notebook.

Environment Variables:
    GENIE_SPACE_ID: Genie space ID (required)
    GENIE_CONVERSATIONS_FILE: Path to conversations YAML file (required)
    GENIE_MIN_WAIT: Minimum wait time between messages in seconds (default: 8)
    GENIE_MAX_WAIT: Maximum wait time between messages in seconds (default: 30)
    GENIE_SAMPLE_SIZE: Number of conversations to sample (default: all)
    GENIE_SAMPLE_SEED: Random seed for reproducible sampling (default: None)
    
    Cache Configuration:
    GENIE_LAKEBASE_CLIENT_ID: Lakebase client ID (required)
    GENIE_LAKEBASE_CLIENT_SECRET: Lakebase client secret (required)
    GENIE_LAKEBASE_INSTANCE: Lakebase instance name (required)
    GENIE_WAREHOUSE_ID: Warehouse ID (required)
    GENIE_CACHE_TTL: Cache TTL in seconds (default: 86400)
    GENIE_SIMILARITY_THRESHOLD: Semantic similarity threshold (default: 0.85)
    GENIE_LRU_CAPACITY: LRU cache capacity (default: 100)
    DATABRICKS_HOST: Databricks workspace URL (required, passed from notebook)
    DATABRICKS_TOKEN: Databricks auth token (required, passed from notebook)
"""

import logging
import os
import random
import sys
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

# Add parent directory to path for genie_simulation imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from genie_simulation.detailed_metrics import (
    DETAILED_METRICS,
    RequestMetric,
)
from genie_simulation.export_to_uc import export_to_unity_catalog_if_available

from dao_ai.config import (
    DatabaseModel,
    GenieLRUCacheParametersModel,
    GenieContextAwareCacheParametersModel,
    WarehouseModel,
)
from dao_ai.genie import GenieService, GenieServiceBase
from dao_ai.genie.cache import CacheResult, LRUCacheService, PostgresContextAwareGenieService
from databricks.sdk import WorkspaceClient
from dao_ai.genie import Genie, GenieResponse
from locust import User, between, events, task
from loguru import logger

# Configure loguru for clean key=value output
logger.remove()
logger.add(
    sys.stdout,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
)

# Suppress noisy third-party library logs
logging.getLogger("databricks.sdk").setLevel(logging.WARNING)
logging.getLogger("mlflow").setLevel(logging.ERROR)
logging.getLogger("mlflow.tracing").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def load_conversations(file_path: str | Path) -> dict[str, Any]:
    """Load conversations from YAML file."""
    with open(file_path) as f:
        return yaml.safe_load(f)


def sample_conversations(
    conversations: list[dict[str, Any]],
    sample_size: int | None,
    seed: int | None = None,
) -> list[dict[str, Any]]:
    """Sample a subset of conversations for reuse during the simulation."""
    if not conversations:
        return []
    
    if sample_size is None or sample_size >= len(conversations):
        return conversations
    
    rng = random.Random(seed)
    return rng.sample(conversations, sample_size)


# Load configuration from environment variables
CONVERSATIONS_FILE = os.environ.get("GENIE_CONVERSATIONS_FILE", "conversations.yaml")
MIN_WAIT = float(os.environ.get("GENIE_MIN_WAIT", "8"))
MAX_WAIT = float(os.environ.get("GENIE_MAX_WAIT", "30"))

_sample_size_str = os.environ.get("GENIE_SAMPLE_SIZE", "")
SAMPLE_SIZE: int | None = int(_sample_size_str) if _sample_size_str.isdigit() else None

_sample_seed_str = os.environ.get("GENIE_SAMPLE_SEED", "")
SAMPLE_SEED: int | None = int(_sample_seed_str) if _sample_seed_str.isdigit() else None

# Cache configuration
SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")
LAKEBASE_CLIENT_ID = os.environ.get("GENIE_LAKEBASE_CLIENT_ID", "")
LAKEBASE_CLIENT_SECRET = os.environ.get("GENIE_LAKEBASE_CLIENT_SECRET", "")
LAKEBASE_INSTANCE = os.environ.get("GENIE_LAKEBASE_INSTANCE", "")
WAREHOUSE_ID = os.environ.get("GENIE_WAREHOUSE_ID", "")
CACHE_TTL = int(os.environ.get("GENIE_CACHE_TTL", "86400"))
SIMILARITY_THRESHOLD = float(os.environ.get("GENIE_SIMILARITY_THRESHOLD", "0.85"))
LRU_CAPACITY = int(os.environ.get("GENIE_LRU_CAPACITY", "100"))

# Databricks auth (passed from notebook via environment variables)
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

# Load conversations at module level
try:
    _raw_data = load_conversations(CONVERSATIONS_FILE)
    logger.info(
        f"conversations_loaded total={_raw_data['total_conversations']} "
        f"messages={_raw_data['total_messages']}"
    )

    _all_conversations = _raw_data.get("conversations", [])
    _sampled_conversations = sample_conversations(_all_conversations, SAMPLE_SIZE, SAMPLE_SEED)

    if SAMPLE_SIZE and SAMPLE_SIZE < len(_all_conversations):
        logger.info(f"conversations_sampled count={len(_sampled_conversations)}")

    CONVERSATIONS_DATA: dict[str, Any] = {
        "space_id": SPACE_ID or _raw_data.get("space_id", ""),
        "total_conversations": len(_sampled_conversations),
        "total_messages": sum(len(c.get("messages", [])) for c in _sampled_conversations),
        "conversations": _sampled_conversations,
    }
except FileNotFoundError:
    logger.warning(f"conversations_not_found file={CONVERSATIONS_FILE}")
    CONVERSATIONS_DATA = {"space_id": SPACE_ID, "conversations": []}


@dataclass
class CacheMetrics:
    """Thread-safe cache metrics tracker."""
    
    lru_hits: int = 0
    semantic_hits: int = 0
    misses: int = 0
    _lock: Any = field(default_factory=lambda: threading.Lock())
    
    def record_hit(self, cache_type: str) -> None:
        with self._lock:
            if cache_type == "lru":
                self.lru_hits += 1
            elif cache_type == "semantic":
                self.semantic_hits += 1
    
    def record_miss(self) -> None:
        with self._lock:
            self.misses += 1
    
    @property
    def total_requests(self) -> int:
        return self.lru_hits + self.semantic_hits + self.misses
    
    def get_summary(self) -> dict[str, Any]:
        total = self.total_requests
        return {
            "total_requests": total,
            "lru_hits": self.lru_hits,
            "lru_hit_rate": (self.lru_hits / total * 100) if total > 0 else 0.0,
            "semantic_hits": self.semantic_hits,
            "semantic_hit_rate": (self.semantic_hits / total * 100) if total > 0 else 0.0,
            "misses": self.misses,
            "miss_rate": (self.misses / total * 100) if total > 0 else 0.0,
        }


# Global cache metrics
CACHE_METRICS = CacheMetrics()


class CachedGenieLoadTestUser(User):
    """Simulates a user interacting with Genie through the caching service."""

    wait_time = between(MIN_WAIT, MAX_WAIT)
    _user_counter: int = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.genie_service: GenieServiceBase | None = None
        self.space_id: str = CONVERSATIONS_DATA.get("space_id", "")
        self.conversations: list[dict[str, Any]] = CONVERSATIONS_DATA.get("conversations", [])
        
        CachedGenieLoadTestUser._user_counter += 1
        self.user_id: int = CachedGenieLoadTestUser._user_counter

    def on_start(self) -> None:
        """Initialize the cached Genie service."""
        if not self.space_id:
            raise ValueError("No space_id found. Set GENIE_SPACE_ID environment variable.")

        if not self.conversations:
            raise ValueError("No conversations found. Check GENIE_CONVERSATIONS_FILE.")

        # Validate cache configuration
        if not LAKEBASE_CLIENT_ID or not LAKEBASE_CLIENT_SECRET:
            raise ValueError("Lakebase credentials not found.")
        if not LAKEBASE_INSTANCE:
            raise ValueError("GENIE_LAKEBASE_INSTANCE is required.")
        if not WAREHOUSE_ID:
            raise ValueError("GENIE_WAREHOUSE_ID is required.")

        if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
            raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")

        logger.info(f"user_init user_id={self.user_id} space_id={self.space_id}")
        
        # Create base Genie service with explicit credentials
        client = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
        genie = Genie(space_id=self.space_id, client=client)
        genie_service: GenieServiceBase = GenieService(genie=genie)
        
        # Configure database connection for semantic cache
        database = DatabaseModel(
            instance_name=LAKEBASE_INSTANCE,
            client_id=LAKEBASE_CLIENT_ID,
            client_secret=LAKEBASE_CLIENT_SECRET,
        )
        
        warehouse = WarehouseModel(warehouse_id=WAREHOUSE_ID)
        
        # Wrap with context-aware semantic cache (PostgreSQL backend)
        semantic_cache_params = GenieContextAwareCacheParametersModel(
            database=database,
            warehouse=warehouse,
            time_to_live_seconds=CACHE_TTL,
            similarity_threshold=SIMILARITY_THRESHOLD,
        )
        
        genie_service = PostgresContextAwareGenieService(
            impl=genie_service,
            parameters=semantic_cache_params,
        ).initialize()
        
        # Wrap with LRU cache
        lru_cache_params = GenieLRUCacheParametersModel(
            warehouse=warehouse,
            capacity=LRU_CAPACITY,
            time_to_live_seconds=CACHE_TTL,
        )
        
        self.genie_service = LRUCacheService(
            impl=genie_service,
            parameters=lru_cache_params,
        )
        
        logger.info(f"user_ready user_id={self.user_id} cache=enabled")

    @task
    def replay_conversation(self) -> None:
        """Replay a random conversation through the cache service."""
        if not self.conversations or not self.genie_service:
            return

        conversation = random.choice(self.conversations)
        messages = conversation.get("messages", [])

        if not messages:
            return

        conversation_id: str = conversation.get("id", "unknown")
        logger.info(
            f"conversation_start user_id={self.user_id} conversation_id={conversation_id} "
            f"messages={len(messages)}"
        )

        for i, msg in enumerate(messages):
            content: str = msg.get("content", "")
            if not content:
                continue

            request_started_at: datetime = datetime.now()
            start_time: float = time.time()
            exception: Exception | None = None
            response_length: int = 0
            cache_status: str = "miss"
            request_type: str = "GENIE_LIVE"

            try:
                result: CacheResult = self.genie_service.ask_question(content)
                
                response: GenieResponse = result.response
                response_length = len(str(response)) if response else 0
                
                if result.cache_hit:
                    served_by = (result.served_by or "").lower()
                    
                    if "lru" in served_by:
                        cache_status = "hit:lru"
                        CACHE_METRICS.record_hit("lru")
                        request_type = "GENIE_LRU_HIT"
                    elif "semantic" in served_by:
                        cache_status = "hit:semantic"
                        CACHE_METRICS.record_hit("semantic")
                        request_type = "GENIE_SEMANTIC_HIT"
                    else:
                        cache_status = "hit"
                        CACHE_METRICS.record_hit("semantic")
                        request_type = "GENIE_CACHED"
                else:
                    cache_status = "miss"
                    CACHE_METRICS.record_miss()
                    request_type = "GENIE_LIVE"

            except Exception as e:
                exception = e
                response = None
                CACHE_METRICS.record_miss()
                logger.error(f"request_error user_id={self.user_id} error={e}")

            request_completed_at: datetime = datetime.now()
            duration_ms = (time.time() - start_time) * 1000

            # Record detailed metrics
            DETAILED_METRICS.record(RequestMetric(
                request_started_at=request_started_at,
                request_completed_at=request_completed_at,
                duration_ms=duration_ms,
                concurrent_users=int(os.environ.get("GENIE_USER_COUNT", "1")),
                user=f"user_{self.user_id}",
                prompt=content,
                source_conversation_id=conversation.get("id"),
                source_message_id=msg.get("message_id"),
                genie_conversation_id=response.conversation_id if response else None,
                genie_message_id=response.message_id if response else None,
                message_index=i,
                sql=response.query if response else None,
                response_size=response_length,
                success=exception is None,
                error=str(exception) if exception else None,
            ))

            events.request.fire(
                request_type=request_type,
                name=f"message_{i+1}_of_{len(messages)}",
                response_time=duration_ms,
                response_length=response_length,
                exception=exception,
            )

            if i < len(messages) - 1:
                time.sleep(random.uniform(MIN_WAIT / 2, MAX_WAIT / 2))


# Event handlers for logging
@events.request.add_listener
def on_request(
    request_type: str,
    name: str,
    response_time: float,
    response_length: int,
    exception: Exception | None,
    **kwargs: Any,
) -> None:
    """Log each request with key=value format."""
    status = "failure" if exception else "success"
    logger.info(
        f"request type={request_type} name={name} status={status} "
        f"response_time_ms={response_time:.0f} response_length={response_length}"
    )


@events.test_start.add_listener
def on_test_start(environment: Any, **kwargs: Any) -> None:
    """Log test configuration at start."""
    logger.info(
        f"test_start space_id={CONVERSATIONS_DATA.get('space_id', 'N/A')} "
        f"conversations={CONVERSATIONS_DATA.get('total_conversations', 0)} "
        f"messages={CONVERSATIONS_DATA.get('total_messages', 0)} "
        f"wait_min={MIN_WAIT}s wait_max={MAX_WAIT}s"
    )
    logger.info(
        f"cache_config lakebase_instance={LAKEBASE_INSTANCE} warehouse_id={WAREHOUSE_ID} "
        f"cache_ttl={CACHE_TTL}s similarity_threshold={SIMILARITY_THRESHOLD} "
        f"lru_capacity={LRU_CAPACITY}"
    )


@events.test_stop.add_listener
def on_test_stop(environment: Any, **kwargs: Any) -> None:
    """Log summary with cache metrics at test end."""
    cache_summary = CACHE_METRICS.get_summary()
    logger.info(
        f"test_complete total_requests={cache_summary['total_requests']} "
        f"lru_hits={cache_summary['lru_hits']} lru_hit_rate={cache_summary['lru_hit_rate']:.1f}% "
        f"semantic_hits={cache_summary['semantic_hits']} semantic_hit_rate={cache_summary['semantic_hit_rate']:.1f}% "
        f"misses={cache_summary['misses']} miss_rate={cache_summary['miss_rate']:.1f}%"
    )
    
    # Write detailed metrics to CSV in results directory
    results_dir = os.environ.get("GENIE_RESULTS_DIR", "results")
    os.makedirs(results_dir, exist_ok=True)
    csv_path = f"{results_dir}/detailed_metrics.csv"
    records_written = DETAILED_METRICS.to_csv(csv_path)
    if records_written > 0:
        logger.info(f"Detailed metrics written to {csv_path} ({records_written} records)")
        summary = DETAILED_METRICS.get_summary()
        logger.info(
            f"Metrics summary: {summary['successful_requests']}/{summary['total_requests']} successful "
            f"({summary['success_rate']:.1f}%), avg {summary['avg_execution_time_ms']:.0f}ms"
        )
    
    # Export to Unity Catalog if running in Databricks notebook
    export_to_unity_catalog_if_available()
