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
import time
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks_ai_bridge.genie import Genie, GenieResponse
from locust import User, between, events, task

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
    print(f"Loaded {_raw_data['total_conversations']} conversations "
          f"with {_raw_data['total_messages']} messages")

    _all_conversations = _raw_data.get("conversations", [])
    _sampled_conversations = sample_conversations(_all_conversations, SAMPLE_SIZE, SAMPLE_SEED)

    if SAMPLE_SIZE and SAMPLE_SIZE < len(_all_conversations):
        print(f"Sampled {len(_sampled_conversations)} conversations")

    CONVERSATIONS_DATA: dict[str, Any] = {
        "space_id": os.environ.get("GENIE_SPACE_ID") or _raw_data.get("space_id", ""),
        "total_conversations": len(_sampled_conversations),
        "total_messages": sum(len(c.get("messages", [])) for c in _sampled_conversations),
        "conversations": _sampled_conversations,
    }
except FileNotFoundError:
    print(f"Conversations file '{CONVERSATIONS_FILE}' not found.")
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

        print(f"[User {self.user_id}] Initializing Genie for space: {self.space_id}")
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
        print(f"[User {self.user_id}] Replaying conversation: {conversation_id} ({len(messages)} messages)")

        for i, msg in enumerate(messages):
            content: str = msg.get("content", "")
            if not content:
                continue

            start_time: float = time.time()
            exception: Exception | None = None
            response_length: int = 0

            try:
                response: GenieResponse = self.genie.ask_question(content)
                response_length = len(str(response)) if response else 0
            except Exception as e:
                exception = e
                print(f"[User {self.user_id}] Request failed: {e}")

            response_time = (time.time() - start_time) * 1000  # Convert to ms

            events.request.fire(
                request_type="GENIE",
                name=f"message_{i+1}_of_{len(messages)}",
                response_time=response_time,
                response_length=response_length,
                exception=exception,
            )

            # Think time between messages within a conversation
            if i < len(messages) - 1:
                time.sleep(random.uniform(MIN_WAIT / 2, MAX_WAIT / 2))


# Event handlers for test start/stop logging
@events.test_start.add_listener
def on_test_start(environment: Any, **kwargs: Any) -> None:
    """Log test configuration at start."""
    print("=" * 60)
    print("Genie Load Test Starting")
    print("=" * 60)
    print(f"Space ID: {CONVERSATIONS_DATA.get('space_id', 'N/A')}")
    print(f"Conversations: {CONVERSATIONS_DATA.get('total_conversations', 0)}")
    print(f"Total Messages: {CONVERSATIONS_DATA.get('total_messages', 0)}")
    print(f"Wait Time Range: {MIN_WAIT}s - {MAX_WAIT}s")
    print("=" * 60)


@events.test_stop.add_listener
def on_test_stop(environment: Any, **kwargs: Any) -> None:
    """Log summary at test end."""
    print("=" * 60)
    print("Genie Load Test Complete")
    print("=" * 60)
