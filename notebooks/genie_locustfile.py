"""
Standalone Locust file for Genie load testing in Databricks notebooks.

This file is designed to be run via the Locust CLI (subprocess) from notebooks.
All configuration is read from environment variables set by the notebook.

Environment Variables:
    GENIE_SPACE_ID: Genie space ID (required)
    GENIE_CONVERSATIONS_FILE: Path to conversations YAML file (required)
    GENIE_MIN_WAIT: Minimum wait time between messages in seconds (default: 8)
    GENIE_MAX_WAIT: Maximum wait time between messages in seconds (default: 30)
    GENIE_SAMPLE_SIZE: Number of conversations to sample (default: all)
    GENIE_SAMPLE_SEED: Random seed for reproducible sampling (default: None)
    DATABRICKS_HOST: Databricks workspace URL (required, passed from notebook)
    DATABRICKS_TOKEN: Databricks auth token (required, passed from notebook)
"""

import logging
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient

# Add parent directory to path for genie_simulation imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from genie_simulation.detailed_metrics import (
    DETAILED_METRICS,
    RequestMetric,
)
from genie_simulation.export_to_uc import export_to_unity_catalog_if_available
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
        "space_id": os.environ.get("GENIE_SPACE_ID") or _raw_data.get("space_id", ""),
        "total_conversations": len(_sampled_conversations),
        "total_messages": sum(len(c.get("messages", [])) for c in _sampled_conversations),
        "conversations": _sampled_conversations,
    }
except FileNotFoundError:
    logger.warning(f"conversations_not_found file={CONVERSATIONS_FILE}")
    CONVERSATIONS_DATA = {"space_id": os.environ.get("GENIE_SPACE_ID", ""), "conversations": []}


class GenieLoadTestUser(User):
    """Simulates a user interacting with Genie by replaying exported conversations."""

    wait_time = between(MIN_WAIT, MAX_WAIT)
    _user_counter: int = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.genie: Genie | None = None
        self.client: WorkspaceClient | None = None
        self.space_id: str = CONVERSATIONS_DATA.get("space_id", "")
        self.conversations: list[dict[str, Any]] = CONVERSATIONS_DATA.get("conversations", [])
        
        GenieLoadTestUser._user_counter += 1
        self.user_id: int = GenieLoadTestUser._user_counter

    def on_start(self) -> None:
        """Initialize the Genie client when a simulated user starts."""
        if not self.space_id:
            raise ValueError("No space_id found. Set GENIE_SPACE_ID environment variable.")

        if not self.conversations:
            raise ValueError("No conversations found. Check GENIE_CONVERSATIONS_FILE.")

        if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
            raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")

        logger.info(f"user_init user_id={self.user_id} space_id={self.space_id}")
        self.client = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
        self.genie = Genie(
            space_id=self.space_id,
            client=self.client,
        )

    @task
    def replay_conversation(self) -> None:
        """Replay a random conversation from the exported data."""
        if not self.conversations or not self.genie:
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

        # Track the active conversation ID for this session (starts as None)
        active_conversation_id: str | None = None

        for i, msg in enumerate(messages):
            content: str = msg.get("content", "")
            if not content:
                continue

            request_started_at: datetime = datetime.now()
            start_time: float = time.time()
            exception: Exception | None = None
            response_length: int = 0

            try:
                # Use None for first message, then use returned conversation_id
                response: GenieResponse = self.genie.ask_question(content, conversation_id=active_conversation_id)
                response_length = len(str(response)) if response else 0
                
                # Capture conversation_id for subsequent messages in this conversation
                if response and response.conversation_id:
                    active_conversation_id = response.conversation_id
            except Exception as e:
                exception = e
                response = None
                logger.error(f"request_error user_id={self.user_id} error={e}")

            request_completed_at: datetime = datetime.now()
            duration_ms = (time.time() - start_time) * 1000  # Convert to ms

            # Record detailed metrics
            run_id = os.path.basename(os.environ.get("GENIE_RESULTS_DIR", "unknown_run"))
            space_id = os.environ.get("GENIE_SPACE_ID", "unknown_space")
            DETAILED_METRICS.record(RequestMetric(
                run_id=run_id,
                space_id=space_id,
                request_started_at=request_started_at,
                request_completed_at=request_completed_at,
                duration_ms=duration_ms,
                concurrent_users=int(os.environ.get("GENIE_USER_COUNT", "1")),
                user=f"user_{self.user_id}",
                prompt=content,
                source_conversation_id=conversation.get("id"),
                source_message_id=msg.get("message_id"),
                genie_conversation_id=getattr(response, "conversation_id", None) if response else None,
                genie_message_id=response.message_id if response else None,
                message_index=i,
                sql=getattr(response, "query", None) if response else None,
                response_size=response_length,
                success=exception is None,
                error=str(exception) if exception else None,
            ))

            events.request.fire(
                request_type="GENIE",
                name=f"message_{i+1}_of_{len(messages)}",
                response_time=duration_ms,
                response_length=response_length,
                exception=exception,
            )

            # Think time between messages within a conversation
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


@events.test_stop.add_listener
def on_test_stop(environment: Any, **kwargs: Any) -> None:
    """Log summary at test end."""
    logger.info("test_complete")
    
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
