"""
Locust load test for Genie chatbot with caching service.

This load test simulates real-world usage of Genie through the dao-ai caching
service (SemanticCacheService + LRUCacheService), tracking both latency and
cache hit/miss metrics.

Usage:
    # Run with web UI
    locust -f locustfile_cached.py

    # Run headless with specific parameters
    locust -f locustfile_cached.py --headless -u 10 -r 2 -t 5m

Environment Variables:
    # Common configuration
    GENIE_CONVERSATIONS_FILE: Path to the YAML conversations file (default: conversations.yaml)
    GENIE_MIN_WAIT: Minimum wait time between messages in seconds (default: 8)
    GENIE_MAX_WAIT: Maximum wait time between messages in seconds (default: 30)
    GENIE_SAMPLE_SIZE: Number of conversations to sample and reuse (default: all)
    GENIE_SAMPLE_SEED: Random seed for reproducible sampling (default: None)
    
    # Cache service configuration
    GENIE_SPACE_ID: Genie space ID (required)
    GENIE_LAKEBASE_CLIENT_ID: Lakebase client ID for cache service
    GENIE_LAKEBASE_CLIENT_SECRET: Lakebase client secret for cache service
    GENIE_LAKEBASE_INSTANCE: Lakebase instance name
    GENIE_WAREHOUSE_ID: Warehouse ID for cache operations
    GENIE_CACHE_TTL: Cache TTL in seconds (default: 86400)
    GENIE_SIMILARITY_THRESHOLD: Semantic similarity threshold (default: 0.85)
    GENIE_LRU_CAPACITY: LRU cache capacity (default: 100)
"""

import logging
import math
import os
import random
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dao_ai.config import (
    DatabaseModel,
    GenieLRUCacheParametersModel,
    GenieContextAwareCacheParametersModel,
    WarehouseModel,
)
from dao_ai.genie import GenieService, GenieServiceBase
from dao_ai.genie.cache import CacheResult, LRUCacheService, PostgresContextAwareGenieService
from databricks_ai_bridge.genie import Genie, GenieResponse
from dotenv import load_dotenv
from locust import User, between, events, task
from loguru import logger

# Load environment variables from .env file if present
load_dotenv()


def format_extra(record: dict[str, Any]) -> str:
    """Format extra fields as key=value pairs."""
    extra: dict[str, Any] = record["extra"]
    if not extra:
        return ""

    formatted_pairs: list[str] = []
    for key, value in extra.items():
        if isinstance(value, str):
            formatted_pairs.append(f"{key}={value}")
        elif isinstance(value, (list, tuple)):
            formatted_pairs.append(f"{key}={','.join(str(v) for v in value)}")
        else:
            formatted_pairs.append(f"{key}={value}")

    return " | ".join(formatted_pairs)


# Configure loguru for stdout with structured attributes
logger.remove()
logger.add(
    sys.stdout,
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<level>{message}</level>"
        "{extra}"
    ),
    level="INFO",
    colorize=True,
)

# Add custom formatter for extra fields
logger.configure(
    patcher=lambda record: record.update(
        extra=" | " + format_extra(record) if record["extra"] else ""
    )
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
    """
    Sample a subset of conversations for reuse during the simulation.
    
    Args:
        conversations: Full list of conversations
        sample_size: Number of conversations to sample (None = use all)
        seed: Random seed for reproducible sampling
    
    Returns:
        Sampled list of conversations
    """
    if not conversations:
        return []
    
    if sample_size is None or sample_size >= len(conversations):
        return conversations
    
    # Use a separate random instance to not affect other randomness
    rng = random.Random(seed)
    sampled = rng.sample(conversations, sample_size)
    
    return sampled


# =============================================================================
# Configuration from environment variables
# =============================================================================

CONVERSATIONS_FILE = os.environ.get("GENIE_CONVERSATIONS_FILE", "conversations.yaml")
MIN_WAIT = float(os.environ.get("GENIE_MIN_WAIT", "8"))
MAX_WAIT = float(os.environ.get("GENIE_MAX_WAIT", "30"))

# Sample size configuration
_sample_size_str = os.environ.get("GENIE_SAMPLE_SIZE", "")
SAMPLE_SIZE: int | None = int(_sample_size_str) if _sample_size_str.isdigit() else None

_sample_seed_str = os.environ.get("GENIE_SAMPLE_SEED", "")
SAMPLE_SEED: int | None = int(_sample_seed_str) if _sample_seed_str.isdigit() else None

# Cache service configuration
SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")
GENIE_LAKEBASE_CLIENT_ID = os.environ.get("GENIE_LAKEBASE_CLIENT_ID", "")
GENIE_LAKEBASE_CLIENT_SECRET = os.environ.get("GENIE_LAKEBASE_CLIENT_SECRET", "")
LAKEBASE_INSTANCE = os.environ.get("GENIE_LAKEBASE_INSTANCE", "")
WAREHOUSE_ID = os.environ.get("GENIE_WAREHOUSE_ID", "")
CACHE_TTL = int(os.environ.get("GENIE_CACHE_TTL", "86400"))
SIMILARITY_THRESHOLD = float(os.environ.get("GENIE_SIMILARITY_THRESHOLD", "0.85"))
LRU_CAPACITY = int(os.environ.get("GENIE_LRU_CAPACITY", "100"))

# Load conversations at module level to avoid repeated file reads
try:
    _raw_data = load_conversations(CONVERSATIONS_FILE)
    logger.info(
        "Loaded conversations",
        conversations=_raw_data["total_conversations"],
        messages=_raw_data["total_messages"],
        file=CONVERSATIONS_FILE,
    )

    # Sample conversations if configured
    _all_conversations = _raw_data.get("conversations", [])
    _sampled_conversations = sample_conversations(_all_conversations, SAMPLE_SIZE, SAMPLE_SEED)

    if SAMPLE_SIZE and SAMPLE_SIZE < len(_all_conversations):
        logger.info("Sampled conversations for reuse", count=len(_sampled_conversations))
        if SAMPLE_SEED is not None:
            logger.info("Using random seed for reproducible sampling", seed=SAMPLE_SEED)

    # Build the final data structure with sampled conversations
    CONVERSATIONS_DATA: dict[str, Any] = {
        "space_id": _raw_data.get("space_id", "") or SPACE_ID,
        "total_conversations": len(_sampled_conversations),
        "total_messages": sum(len(c.get("messages", [])) for c in _sampled_conversations),
        "conversations": _sampled_conversations,
    }
except FileNotFoundError:
    logger.warning(
        "Conversations file not found. Please run export-conversations first.",
        file=CONVERSATIONS_FILE,
    )
    CONVERSATIONS_DATA = {"space_id": SPACE_ID, "conversations": []}


# =============================================================================
# Cache Metrics Tracking
# =============================================================================

@dataclass
class CacheMetrics:
    """Thread-safe cache metrics tracker."""
    
    lru_hits: int = 0
    semantic_hits: int = 0
    misses: int = 0
    _lock: Any = field(default_factory=lambda: __import__("threading").Lock())
    
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
    
    def get_summary(self) -> dict[str, Any]:
        """Get a summary of cache metrics."""
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


# Global cache metrics instance
CACHE_METRICS = CacheMetrics()


# =============================================================================
# Load Test User Classes
# =============================================================================

class CachedGenieLoadTestUser(User):
    """
    Simulates a user interacting with Genie through the caching service.
    
    Each user will:
    1. Initialize the Genie service with LRU and Semantic cache layers
    2. Pick a random conversation from the exported data
    3. Replay each message in the conversation sequentially
    4. Track cache hit/miss metrics for each request
    """

    # Wait time between tasks (conversations)
    wait_time = between(MIN_WAIT, MAX_WAIT)

    # Class-level counter for unique user IDs
    _user_counter: int = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.genie_service: GenieServiceBase | None = None
        self.space_id: str = CONVERSATIONS_DATA.get("space_id", "")
        self.conversations: list[dict[str, Any]] = CONVERSATIONS_DATA.get("conversations", [])
        
        # Assign unique user ID
        CachedGenieLoadTestUser._user_counter += 1
        self.user_id: int = CachedGenieLoadTestUser._user_counter

    def on_start(self) -> None:
        """Initialize the cached Genie service when a simulated user starts."""
        if not self.space_id:
            raise ValueError(
                "No space_id found. Set GENIE_SPACE_ID environment variable "
                "or ensure it's in the conversations YAML file."
            )

        if not self.conversations:
            raise ValueError(
                "No conversations found in the data. "
                "Please run export-conversations first."
            )

        # Validate required cache configuration
        missing_config = []
        if not GENIE_LAKEBASE_CLIENT_ID:
            missing_config.append("GENIE_LAKEBASE_CLIENT_ID")
        if not GENIE_LAKEBASE_CLIENT_SECRET:
            missing_config.append("GENIE_LAKEBASE_CLIENT_SECRET")
        if not LAKEBASE_INSTANCE:
            missing_config.append("GENIE_LAKEBASE_INSTANCE")
        if not WAREHOUSE_ID:
            missing_config.append("GENIE_WAREHOUSE_ID")
        
        if missing_config:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_config)}. "
                "Please set these in your .env file or environment."
            )

        logger.info("Initializing cached Genie service", user=self.user_id, space_id=self.space_id)
        
        # Create base Genie service
        genie = Genie(space_id=self.space_id)
        genie_service: GenieServiceBase = GenieService(genie=genie)
        
        # Configure database connection for semantic cache
        database = DatabaseModel(
            instance_name=LAKEBASE_INSTANCE,
            client_id=GENIE_LAKEBASE_CLIENT_ID,
            client_secret=GENIE_LAKEBASE_CLIENT_SECRET,
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
        
        logger.info(
            "Cached Genie service initialized",
            user=self.user_id,
            lru_capacity=LRU_CAPACITY,
            ttl_s=CACHE_TTL,
            similarity=SIMILARITY_THRESHOLD,
        )

    @task
    def replay_conversation(self) -> None:
        """
        Replay a random conversation from the exported data through the cache service.
        
        This task picks a random conversation and replays all its messages
        sequentially, tracking cache hits and misses.
        """
        if not self.conversations or not self.genie_service:
            return

        # Pick a random conversation to replay
        conversation = random.choice(self.conversations)
        messages = conversation.get("messages", [])

        if not messages:
            return

        conversation_id: str = conversation.get("id", "unknown")
        conversation_title: str = conversation.get("title", "Unknown")
        
        # Truncate title for logging
        title_preview: str = conversation_title[:50] + "..." if len(conversation_title) > 50 else conversation_title
        logger.info(
            "Replaying conversation",
            user=self.user_id,
            conversation_id=conversation_id,
            title=title_preview,
            messages=len(messages),
        )

        wait_time_before: float = 0.0
        
        for i, msg in enumerate(messages):
            content: str = msg.get("content", "")
            if not content:
                continue

            # Truncate content for logging
            content_preview: str = content[:80] + "..." if len(content) > 80 else content
            
            # Log with wait time info
            if i == 0:
                logger.info(
                    "Sending message",
                    user=self.user_id,
                    msg_num=f"{i+1}/{len(messages)}",
                    content=content_preview,
                )
            else:
                logger.info(
                    "Sending message",
                    user=self.user_id,
                    msg_num=f"{i+1}/{len(messages)}",
                    wait_s=round(wait_time_before, 1),
                    content=content_preview,
                )

            # Time the request
            start_time: float = time.time()
            exception: Exception | None = None
            response_length: int = 0
            cache_status: str = "miss"
            request_type: str = "GENIE_LIVE"

            try:
                # Send the question through the cache service
                result: CacheResult = self.genie_service.ask_question(content)
                
                response: GenieResponse = result.response
                response_length = len(str(response)) if response else 0
                response_time_secs: float = time.time() - start_time
                
                # Determine cache status from result
                # CacheResult has cache_hit (bool) and served_by (str | None)
                # served_by contains class names like "LRUCacheService" or "SemanticCacheService"
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
                
                logger.info(
                    "Response received",
                    user=self.user_id,
                    bytes=response_length,
                    time_s=round(response_time_secs, 2),
                    cache=cache_status,
                )

            except Exception as e:
                exception = e
                CACHE_METRICS.record_miss()
                logger.error("Request failed", user=self.user_id, error=str(e))

            # Calculate response time in milliseconds
            response_time = (time.time() - start_time) * 1000

            # Fire the request event for Locust to track
            events.request.fire(
                request_type=request_type,
                name=f"message_{i+1}_of_{len(messages)}",
                response_time=response_time,
                response_length=response_length,
                exception=exception,
                context={
                    "conversation": conversation_title,
                    "message_index": i,
                    "cache_status": cache_status,
                },
            )

            # Add think time between messages within a conversation
            if i < len(messages) - 1:
                wait_time_before = random.uniform(MIN_WAIT / 2, MAX_WAIT / 2)
                time.sleep(wait_time_before)


class CachedGenieSequentialUser(User):
    """
    A simpler user that sends individual messages through the cache service.
    
    Useful for testing cache behavior with standalone questions.
    """

    wait_time = between(MIN_WAIT, MAX_WAIT)
    
    _user_counter: int = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.genie_service: GenieServiceBase | None = None
        self.space_id: str = CONVERSATIONS_DATA.get("space_id", "")
        self.all_messages: list[str] = []
        self.last_message_time: float | None = None
        self.message_count: int = 0
        
        CachedGenieSequentialUser._user_counter += 1
        self.user_id: int = CachedGenieSequentialUser._user_counter

    def on_start(self) -> None:
        """Initialize and collect all messages."""
        if not self.space_id:
            raise ValueError("No space_id found. Set GENIE_SPACE_ID environment variable.")

        # Validate required cache configuration
        missing_config = []
        if not GENIE_LAKEBASE_CLIENT_ID:
            missing_config.append("GENIE_LAKEBASE_CLIENT_ID")
        if not GENIE_LAKEBASE_CLIENT_SECRET:
            missing_config.append("GENIE_LAKEBASE_CLIENT_SECRET")
        if not LAKEBASE_INSTANCE:
            missing_config.append("GENIE_LAKEBASE_INSTANCE")
        if not WAREHOUSE_ID:
            missing_config.append("GENIE_WAREHOUSE_ID")
        
        if missing_config:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_config)}."
            )

        logger.info("Initializing cached Genie service", seq_user=self.user_id, space_id=self.space_id)
        
        # Create and wrap Genie service (same as CachedGenieLoadTestUser)
        genie = Genie(space_id=self.space_id)
        genie_service: GenieServiceBase = GenieService(genie=genie)
        
        database = DatabaseModel(
            instance_name=LAKEBASE_INSTANCE,
            client_id=GENIE_LAKEBASE_CLIENT_ID,
            client_secret=GENIE_LAKEBASE_CLIENT_SECRET,
        )
        
        warehouse = WarehouseModel(warehouse_id=WAREHOUSE_ID)
        
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
        
        lru_cache_params = GenieLRUCacheParametersModel(
            warehouse=warehouse,
            capacity=LRU_CAPACITY,
            time_to_live_seconds=CACHE_TTL,
        )
        
        self.genie_service = LRUCacheService(
            impl=genie_service,
            parameters=lru_cache_params,
        )

        # Flatten all messages into a single list
        for conv in CONVERSATIONS_DATA.get("conversations", []):
            for msg in conv.get("messages", []):
                content: str = msg.get("content", "")
                if content:
                    self.all_messages.append(content)

        logger.info("Collected messages for random selection", seq_user=self.user_id, count=len(self.all_messages))

    @task
    def send_random_message(self) -> None:
        """Send a random message through the cache service."""
        if not self.all_messages or not self.genie_service:
            return

        self.message_count += 1
        content: str = random.choice(self.all_messages)
        
        content_preview: str = content[:80] + "..." if len(content) > 80 else content
        
        current_time: float = time.time()
        if self.last_message_time is None:
            logger.info(
                "Sending message",
                seq_user=self.user_id,
                msg_num=self.message_count,
                content=content_preview,
            )
        else:
            wait_time: float = current_time - self.last_message_time
            logger.info(
                "Sending message",
                seq_user=self.user_id,
                msg_num=self.message_count,
                wait_s=round(wait_time, 1),
                content=content_preview,
            )

        start_time: float = time.time()
        exception: Exception | None = None
        response_length: int = 0
        cache_status: str = "miss"
        request_type: str = "GENIE_LIVE"

        try:
            result: CacheResult = self.genie_service.ask_question(content)
            
            response: GenieResponse = result.response
            response_length = len(str(response)) if response else 0
            response_time_secs: float = time.time() - start_time
            
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
            
            logger.info(
                "Response received",
                seq_user=self.user_id,
                bytes=response_length,
                time_s=round(response_time_secs, 2),
                cache=cache_status,
            )
        except Exception as e:
            exception = e
            CACHE_METRICS.record_miss()
            logger.error("Request failed", seq_user=self.user_id, error=str(e))

        self.last_message_time = time.time()
        response_time: float = (time.time() - start_time) * 1000

        events.request.fire(
            request_type=request_type,
            name="random_message",
            response_time=response_time,
            response_length=response_length,
            exception=exception,
            context={"cache_status": cache_status},
        )


# =============================================================================
# Event Handlers
# =============================================================================

@events.test_start.add_listener
def on_test_start(environment: Any, **kwargs: Any) -> None:
    """Log test configuration at start."""
    logger.info("=" * 70)
    logger.info("Cached Genie Load Test Starting")
    logger.info("=" * 70)
    logger.info(
        "Test configuration",
        space_id=CONVERSATIONS_DATA.get("space_id", "N/A"),
        conversations=CONVERSATIONS_DATA.get("total_conversations", 0),
        messages=CONVERSATIONS_DATA.get("total_messages", 0),
        wait_range=f"{MIN_WAIT}s-{MAX_WAIT}s",
    )
    logger.info(
        "Cache configuration",
        lakebase_instance=LAKEBASE_INSTANCE,
        warehouse_id=WAREHOUSE_ID,
        cache_ttl_s=CACHE_TTL,
        similarity_threshold=SIMILARITY_THRESHOLD,
        lru_capacity=LRU_CAPACITY,
    )
    logger.info("=" * 70)


def calculate_robust_stats(stats_entry: Any) -> dict[str, float]:
    """
    Calculate robust statistics that are less sensitive to outliers.
    """
    if stats_entry.num_requests == 0:
        return {
            "trimmed_mean": 0.0,
            "std_dev": 0.0,
            "iqr": 0.0,
            "p10": 0.0,
            "p25": 0.0,
            "p75": 0.0,
            "p90": 0.0,
        }
    
    p10 = stats_entry.get_response_time_percentile(0.10)
    p25 = stats_entry.get_response_time_percentile(0.25)
    p75 = stats_entry.get_response_time_percentile(0.75)
    p90 = stats_entry.get_response_time_percentile(0.90)
    
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
    }


@events.test_stop.add_listener
def on_test_stop(environment: Any, **kwargs: Any) -> None:
    """Log summary at test end with cache metrics."""
    logger.info("=" * 70)
    logger.success("Cached Genie Load Test Complete")
    logger.info("=" * 70)
    
    # Print cache metrics
    cache_summary = CACHE_METRICS.get_summary()
    logger.info("")
    logger.info("CACHE METRICS:")
    logger.info("-" * 70)
    logger.info(f"{'Metric':<35} {'Value':>15} {'Rate':>12}")
    logger.info("-" * 70)
    logger.info(f"{'Total Requests':<35} {cache_summary['total_requests']:>15}")
    logger.info(
        f"{'LRU Cache Hits':<35} {cache_summary['lru_hits']:>15} "
        f"{cache_summary['lru_hit_rate']:>11.1f}%"
    )
    logger.info(
        f"{'Semantic Cache Hits':<35} {cache_summary['semantic_hits']:>15} "
        f"{cache_summary['semantic_hit_rate']:>11.1f}%"
    )
    logger.info(
        f"{'Cache Misses (Live)':<35} {cache_summary['misses']:>15} "
        f"{cache_summary['miss_rate']:>11.1f}%"
    )
    logger.info("-" * 70)
    logger.info(
        f"{'Overall Hit Rate':<35} {cache_summary['total_hits']:>15} "
        f"{cache_summary['hit_rate']:>11.1f}%"
    )
    logger.info("-" * 70)
    
    # Print standard stats
    if environment.stats.total.num_requests > 0:
        stats = environment.stats.total
        robust = calculate_robust_stats(stats)
        
        logger.info("")
        logger.info("LATENCY SUMMARY (times in seconds):")
        logger.info("-" * 70)
        logger.info(f"{'Metric':<30} {'Value':>15}")
        logger.info("-" * 70)
        logger.info(f"{'Total Requests':<30} {stats.num_requests:>15}")
        logger.info(f"{'Failed Requests':<30} {stats.num_failures:>15}")
        logger.info(f"{'Failure Rate':<30} {stats.fail_ratio * 100:>14.1f}%")
        logger.info(f"{'Throughput (req/s)':<30} {stats.total_rps:>15.3f}")
        logger.info("-" * 70)
        logger.info("LATENCY METRICS:")
        logger.info(f"{'  Avg (raw)':<30} {stats.avg_response_time / 1000:>14.2f}s")
        logger.info(f"{'  Trimmed Mean (P10-P90)':<30} {robust['trimmed_mean'] / 1000:>14.2f}s")
        logger.info(f"{'  Median (P50)':<30} {stats.median_response_time / 1000:>14.2f}s")
        logger.info(f"{'  Std Deviation':<30} {robust['std_dev'] / 1000:>14.2f}s")
        logger.info("-" * 70)
        logger.info("PERCENTILE DISTRIBUTION:")
        logger.info(f"{'  P10':<30} {robust['p10'] / 1000:>14.2f}s")
        logger.info(f"{'  P25':<30} {robust['p25'] / 1000:>14.2f}s")
        logger.info(f"{'  P50 (Median)':<30} {stats.median_response_time / 1000:>14.2f}s")
        logger.info(f"{'  P75':<30} {robust['p75'] / 1000:>14.2f}s")
        logger.info(f"{'  P90':<30} {robust['p90'] / 1000:>14.2f}s")
        logger.info(f"{'  P95':<30} {stats.get_response_time_percentile(0.95) / 1000:>14.2f}s")
        logger.info(f"{'  P99':<30} {stats.get_response_time_percentile(0.99) / 1000:>14.2f}s")
        logger.info("-" * 70)
        
        # Per-request-type breakdown
        logger.info("")
        logger.info("PER-REQUEST-TYPE BREAKDOWN (times in seconds):")
        logger.info("-" * 80)
        logger.info(f"{'Request Type':<22} {'Reqs':>5} {'TrimMean':>9} {'Med':>8} {'P90':>8}")
        logger.info("-" * 80)
        
        for name, entry in sorted(environment.stats.entries.items()):
            if entry.num_requests > 0:
                endpoint_name = name[0] if isinstance(name, tuple) else str(name)
                if len(endpoint_name) > 21:
                    endpoint_name = endpoint_name[:18] + "..."
                
                entry_robust = calculate_robust_stats(entry)
                logger.info(
                    f"{endpoint_name:<22} "
                    f"{entry.num_requests:>5} "
                    f"{entry_robust['trimmed_mean'] / 1000:>8.1f}s "
                    f"{entry.median_response_time / 1000:>7.1f}s "
                    f"{entry_robust['p90'] / 1000:>7.1f}s"
                )
        
        logger.info("-" * 80)
