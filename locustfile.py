"""
Locust load test for Genie chatbot conversation replay.

This load test simulates real-world usage of Genie by replaying exported conversations
with realistic think times between messages.

Usage:
    # Run with web UI
    locust -f locustfile.py

    # Run headless with specific parameters
    locust -f locustfile.py --headless -u 10 -r 2 -t 5m

Environment Variables:
    GENIE_CONVERSATIONS_FILE: Path to the YAML conversations file (default: conversations.yaml)
    GENIE_MIN_WAIT: Minimum wait time between messages in seconds (default: 8)
    GENIE_MAX_WAIT: Maximum wait time between messages in seconds (default: 30)
    GENIE_SAMPLE_SIZE: Number of conversations to sample and reuse (default: all)
    GENIE_SAMPLE_SEED: Random seed for reproducible sampling (default: None)
"""

import logging
import math
import os
import random
import sys
import time
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks_ai_bridge.genie import Genie, GenieResponse
from locust import User, between, events, task
from loguru import logger

# Configure loguru for stdout only
logger.remove()
logger.add(
    sys.stdout,
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<level>{message}</level>"
    ),
    level="INFO",
    colorize=True,
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


# Global conversations data - loaded once at module level
CONVERSATIONS_FILE = os.environ.get("GENIE_CONVERSATIONS_FILE", "conversations.yaml")
MIN_WAIT = float(os.environ.get("GENIE_MIN_WAIT", "8"))
MAX_WAIT = float(os.environ.get("GENIE_MAX_WAIT", "30"))

# Sample size configuration
_sample_size_str = os.environ.get("GENIE_SAMPLE_SIZE", "")
SAMPLE_SIZE: int | None = int(_sample_size_str) if _sample_size_str.isdigit() else None

_sample_seed_str = os.environ.get("GENIE_SAMPLE_SEED", "")
SAMPLE_SEED: int | None = int(_sample_seed_str) if _sample_seed_str.isdigit() else None

# Load conversations at module level to avoid repeated file reads
try:
    _raw_data = load_conversations(CONVERSATIONS_FILE)
    logger.info(
        f"Loaded {_raw_data['total_conversations']} conversations "
        f"with {_raw_data['total_messages']} messages from {CONVERSATIONS_FILE}"
    )

    # Sample conversations if configured
    _all_conversations = _raw_data.get("conversations", [])
    _sampled_conversations = sample_conversations(_all_conversations, SAMPLE_SIZE, SAMPLE_SEED)

    if SAMPLE_SIZE and SAMPLE_SIZE < len(_all_conversations):
        logger.info(f"Sampled {len(_sampled_conversations)} conversations for reuse during simulation")
        if SAMPLE_SEED is not None:
            logger.info(f"Using random seed {SAMPLE_SEED} for reproducible sampling")

    # Build the final data structure with sampled conversations
    CONVERSATIONS_DATA: dict[str, Any] = {
        "space_id": _raw_data.get("space_id", ""),
        "total_conversations": len(_sampled_conversations),
        "total_messages": sum(len(c.get("messages", [])) for c in _sampled_conversations),
        "conversations": _sampled_conversations,
    }
except FileNotFoundError:
    logger.warning(
        f"Conversations file '{CONVERSATIONS_FILE}' not found. "
        "Please run export-conversations first or set GENIE_CONVERSATIONS_FILE."
    )
    CONVERSATIONS_DATA = {"space_id": "", "conversations": []}


class GenieLoadTestUser(User):
    """
    Simulates a user interacting with Genie by replaying exported conversations.
    
    Each user will:
    1. Pick a random conversation from the exported data
    2. Replay each message in the conversation sequentially
    3. Wait a realistic amount of time between messages (simulating think time)
    4. Start a new conversation for each replay (not reusing conversation IDs)
    """

    # Wait time between tasks (conversations)
    wait_time = between(MIN_WAIT, MAX_WAIT)

    # Class-level counter for unique user IDs
    _user_counter: int = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.genie: Genie | None = None
        self.client: WorkspaceClient | None = None
        self.space_id: str = CONVERSATIONS_DATA.get("space_id", "")
        self.conversations: list[dict[str, Any]] = CONVERSATIONS_DATA.get("conversations", [])
        
        # Assign unique user ID
        GenieLoadTestUser._user_counter += 1
        self.user_id: int = GenieLoadTestUser._user_counter

    def on_start(self) -> None:
        """Initialize the Genie client when a simulated user starts."""
        if not self.space_id:
            raise ValueError("No space_id found in conversations data. "
                           "Please check your conversations YAML file.")

        if not self.conversations:
            raise ValueError("No conversations found in the data. "
                           "Please run export-conversations first.")

        logger.info(f"[User {self.user_id}] Initializing Genie for space: {self.space_id}")
        self.client = WorkspaceClient()
        self.genie = Genie(
            space_id=self.space_id,
            client=self.client,
        )
        logger.info(f"[User {self.user_id}] Genie initialized successfully")

    @task
    def replay_conversation(self) -> None:
        """
        Replay a random conversation from the exported data.
        
        This task picks a random conversation and replays all its messages
        sequentially, simulating a real user's interaction pattern.
        """
        if not self.conversations or not self.genie:
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
        logger.info(f"[User {self.user_id}] Replaying conversation: {conversation_id} - \"{title_preview}\" ({len(messages)} messages)")

        wait_time_before: float = 0.0  # Track wait time before each message
        
        for i, msg in enumerate(messages):
            content: str = msg.get("content", "")
            if not content:
                continue

            # Truncate content for logging
            content_preview: str = content[:80] + "..." if len(content) > 80 else content
            
            # Log with wait time info
            if i == 0:
                logger.info(f"[User {self.user_id}]   Sending message {i+1}/{len(messages)}: \"{content_preview}\"")
            else:
                logger.info(f"[User {self.user_id}]   Sending message {i+1}/{len(messages)}: after {wait_time_before:.1f}s \"{content_preview}\"")

            # Time the request
            start_time: float = time.time()
            exception: Exception | None = None
            response_length: int = 0

            try:
                # Send the question to Genie
                response: GenieResponse = self.genie.ask_question(content)
                response_length = len(str(response)) if response else 0
                response_time_secs: float = time.time() - start_time
                logger.info(f"[User {self.user_id}]   Response received: {response_length} bytes in {response_time_secs:.2f}s")

            except Exception as e:
                exception = e
                logger.error(f"[User {self.user_id}]   Request failed: {e}")

            # Calculate response time
            response_time = (time.time() - start_time) * 1000  # Convert to ms

            # Fire the request event for Locust to track
            events.request.fire(
                request_type="GENIE",
                name=f"message_{i+1}_of_{len(messages)}",
                response_time=response_time,
                response_length=response_length,
                exception=exception,
                context={
                    "conversation": conversation_title,
                    "message_index": i,
                },
            )

            # Add think time between messages within a conversation
            # (shorter than between conversations)
            if i < len(messages) - 1:  # Don't wait after the last message
                wait_time_before = random.uniform(MIN_WAIT / 2, MAX_WAIT / 2)
                time.sleep(wait_time_before)


class GenieSequentialUser(User):
    """
    A simpler user that sends individual messages without conversation context.
    
    This is useful for testing Genie's response to standalone questions
    without the overhead of maintaining conversation state.
    """

    wait_time = between(MIN_WAIT, MAX_WAIT)
    
    # Class-level counter for unique user IDs
    _user_counter: int = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.genie: Genie | None = None
        self.client: WorkspaceClient | None = None
        self.space_id: str = CONVERSATIONS_DATA.get("space_id", "")
        self.all_messages: list[str] = []
        self.last_message_time: float | None = None  # Track when last message was sent
        self.message_count: int = 0  # Track message count for this user
        
        # Assign unique user ID
        GenieSequentialUser._user_counter += 1
        self.user_id: int = GenieSequentialUser._user_counter

    def on_start(self) -> None:
        """Initialize and collect all messages."""
        if not self.space_id:
            raise ValueError("No space_id found in conversations data.")

        logger.info(f"[SeqUser {self.user_id}] Initializing Genie for space: {self.space_id}")
        self.client = WorkspaceClient()
        self.genie = Genie(
            space_id=self.space_id,
            client=self.client,
        )

        # Flatten all messages into a single list for random selection
        for conv in CONVERSATIONS_DATA.get("conversations", []):
            for msg in conv.get("messages", []):
                content: str = msg.get("content", "")
                if content:
                    self.all_messages.append(content)

        logger.info(f"[SeqUser {self.user_id}] Collected {len(self.all_messages)} messages for random selection")

    @task
    def send_random_message(self) -> None:
        """Send a random message from the pool of exported messages."""
        if not self.all_messages or not self.genie:
            return

        self.message_count += 1
        content: str = random.choice(self.all_messages)
        
        # Truncate content for logging
        content_preview: str = content[:80] + "..." if len(content) > 80 else content
        
        # Calculate wait time since last message
        current_time: float = time.time()
        if self.last_message_time is None:
            logger.info(f"[SeqUser {self.user_id}] Sending message #{self.message_count}: \"{content_preview}\"")
        else:
            wait_time: float = current_time - self.last_message_time
            logger.info(f"[SeqUser {self.user_id}] Sending message #{self.message_count}: after {wait_time:.1f}s \"{content_preview}\"")

        start_time: float = time.time()
        exception: Exception | None = None
        response_length: int = 0

        try:
            # Send the question to Genie
            response: GenieResponse = self.genie.ask_question(content)
            response_length = len(str(response)) if response else 0
            response_time_secs: float = time.time() - start_time
            logger.info(f"[SeqUser {self.user_id}]   Response received: {response_length} bytes in {response_time_secs:.2f}s")
        except Exception as e:
            exception = e
            logger.error(f"[SeqUser {self.user_id}]   Request failed: {e}")

        # Update last message time (after response received)
        self.last_message_time = time.time()

        response_time: float = (time.time() - start_time) * 1000

        events.request.fire(
            request_type="GENIE",
            name="random_message",
            response_time=response_time,
            response_length=response_length,
            exception=exception,
            context=None,
        )


# Custom event handlers for additional metrics
@events.test_start.add_listener
def on_test_start(environment: Any, **kwargs: Any) -> None:
    """Log test configuration at start."""
    logger.info("=" * 60)
    logger.info("Genie Load Test Starting")
    logger.info("=" * 60)
    logger.info(f"Space ID: {CONVERSATIONS_DATA.get('space_id', 'N/A')}")
    logger.info(f"Conversations: {CONVERSATIONS_DATA.get('total_conversations', 0)}")
    logger.info(f"Total Messages: {CONVERSATIONS_DATA.get('total_messages', 0)}")
    logger.info(f"Wait Time Range: {MIN_WAIT}s - {MAX_WAIT}s")
    logger.info("=" * 60)


def calculate_robust_stats(stats_entry: Any) -> dict[str, float]:
    """
    Calculate robust statistics that are less sensitive to outliers.
    
    Returns dict with:
        - trimmed_mean: Mean of values between P10 and P90
        - std_dev: Standard deviation
        - iqr: Interquartile range (P75 - P25)
        - p10, p25, p75, p90: Percentile values
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
    
    # Get percentiles
    p10 = stats_entry.get_response_time_percentile(0.10)
    p25 = stats_entry.get_response_time_percentile(0.25)
    p75 = stats_entry.get_response_time_percentile(0.75)
    p90 = stats_entry.get_response_time_percentile(0.90)
    
    # Calculate IQR
    iqr = p75 - p25
    
    # Calculate trimmed mean (P10-P90) and standard deviation from response_times dict
    # response_times is a dict mapping response_time -> count
    response_times: dict[int, int] = stats_entry.response_times
    
    # Build list of all response times for calculations
    all_times: list[float] = []
    trimmed_times: list[float] = []
    
    for resp_time, count in response_times.items():
        for _ in range(count):
            all_times.append(float(resp_time))
            # Only include times within P10-P90 range for trimmed mean
            if p10 <= resp_time <= p90:
                trimmed_times.append(float(resp_time))
    
    # Calculate trimmed mean
    trimmed_mean = sum(trimmed_times) / len(trimmed_times) if trimmed_times else 0.0
    
    # Calculate standard deviation
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
    """Log summary at test end with times in seconds."""
    logger.info("=" * 70)
    logger.success("Genie Load Test Complete")
    logger.info("=" * 70)
    
    # Print stats in seconds
    if environment.stats.total.num_requests > 0:
        stats = environment.stats.total
        robust = calculate_robust_stats(stats)
        
        logger.info("")
        logger.info("SUMMARY (times in seconds):")
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
        logger.info(f"{'  Trimmed Mean (P10-P90)':<30} {robust['trimmed_mean'] / 1000:>14.2f}s  <- More representative")
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
        logger.info(f"{'  IQR (P75-P25)':<30} {robust['iqr'] / 1000:>14.2f}s")
        logger.info("-" * 70)
        logger.info("OUTLIER BOUNDARIES:")
        logger.info(f"{'  Min':<30} {stats.min_response_time / 1000:>14.2f}s")
        logger.info(f"{'  Max':<30} {stats.max_response_time / 1000:>14.2f}s")
        logger.info("-" * 70)
        
        # Per-endpoint breakdown with trimmed mean
        logger.info("")
        logger.info("PER-ENDPOINT BREAKDOWN (times in seconds):")
        logger.info("-" * 80)
        logger.info(f"{'Endpoint':<22} {'Reqs':>5} {'TrimMean':>9} {'Med':>8} {'StdDev':>8} {'P90':>8}")
        logger.info("-" * 80)
        
        for name, entry in sorted(environment.stats.entries.items()):
            if entry.num_requests > 0:
                endpoint_name = name[1] if isinstance(name, tuple) else str(name)
                # Truncate long names
                if len(endpoint_name) > 21:
                    endpoint_name = endpoint_name[:18] + "..."
                
                entry_robust = calculate_robust_stats(entry)
                logger.info(
                    f"{endpoint_name:<22} "
                    f"{entry.num_requests:>5} "
                    f"{entry_robust['trimmed_mean'] / 1000:>8.1f}s "
                    f"{entry.median_response_time / 1000:>7.1f}s "
                    f"{entry_robust['std_dev'] / 1000:>7.1f}s "
                    f"{entry_robust['p90'] / 1000:>7.1f}s"
                )
        
        logger.info("-" * 80)
