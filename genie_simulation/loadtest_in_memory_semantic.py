"""
CLI wrapper to run in-memory semantic cached Genie load tests with Locust.

This provides a convenient way to run the in-memory semantic cached load test without 
having to remember all the locust command-line arguments. Unlike the standard cached
version, this does not require PostgreSQL or Lakebase - all cache storage is in memory.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

# Load environment variables from .env file if present
load_dotenv()

# Configure logging on import
from genie_simulation.logging_config import configure_logging

configure_logging()


def find_locustfile() -> Path:
    """Find the locustfile_in_memory_semantic.py in the package or current directory."""
    # Check current directory first
    cwd_locustfile = Path.cwd() / "locustfile_in_memory_semantic.py"
    if cwd_locustfile.exists():
        return cwd_locustfile

    # Check package directory
    package_dir = Path(__file__).parent.parent
    package_locustfile = package_dir / "locustfile_in_memory_semantic.py"
    if package_locustfile.exists():
        return package_locustfile

    raise FileNotFoundError(
        "Could not find locustfile_in_memory_semantic.py. Please run this command from the "
        "genie-simulation directory or ensure locustfile_in_memory_semantic.py exists."
    )


def main() -> int:
    """Run the in-memory semantic cached Genie load test using Locust."""
    parser = argparse.ArgumentParser(
        description="Run in-memory semantic cached Genie load tests using Locust.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with web UI (opens http://localhost:8089)
  genie-loadtest-in-memory-semantic

  # Run headless with 10 users, spawning 2/sec, for 5 minutes
  genie-loadtest-in-memory-semantic --headless -u 10 -r 2 -t 5m

  # Use a custom conversations file
  genie-loadtest-in-memory-semantic --conversations my_conversations.yaml

  # Run with custom cache settings
  genie-loadtest-in-memory-semantic --cache-ttl 3600 --similarity-threshold 0.9 --capacity 2000

  # Sample 10 conversations and reuse them throughout the test
  genie-loadtest-in-memory-semantic --sample-size 10

Environment Variables (can also be set in .env file):
  GENIE_WAREHOUSE_ID       Warehouse ID for cache operations
  GENIE_SPACE_ID           Genie space ID (optional, can be from conversations file)
        """,
    )

    # Common arguments
    parser.add_argument(
        "-c", "--conversations",
        type=Path,
        default=Path("conversations.yaml"),
        help="Path to the conversations YAML file (default: conversations.yaml).",
    )
    parser.add_argument(
        "--min-wait",
        type=float,
        default=8.0,
        help="Minimum wait time between messages in seconds (default: 8).",
    )
    parser.add_argument(
        "--max-wait",
        type=float,
        default=30.0,
        help="Maximum wait time between messages in seconds (default: 30).",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Number of conversations to sample and reuse (default: use all).",
    )
    parser.add_argument(
        "--sample-seed",
        type=int,
        default=None,
        help="Random seed for reproducible conversation sampling.",
    )

    # In-memory semantic cache service arguments
    parser.add_argument(
        "--space-id",
        type=str,
        default=None,
        help="Genie space ID (default: from conversations file or GENIE_SPACE_ID env).",
    )
    parser.add_argument(
        "--warehouse-id",
        type=str,
        default=None,
        help="Warehouse ID for cache operations (default: GENIE_WAREHOUSE_ID env).",
    )
    parser.add_argument(
        "--cache-ttl",
        type=int,
        default=86400,
        help="Cache TTL in seconds (default: 86400 = 24 hours).",
    )
    parser.add_argument(
        "--similarity-threshold",
        type=float,
        default=0.85,
        help="Question similarity threshold for cache hits (default: 0.85).",
    )
    parser.add_argument(
        "--context-similarity-threshold",
        type=float,
        default=0.80,
        help="Context similarity threshold for cache hits (default: 0.80).",
    )
    parser.add_argument(
        "--capacity",
        type=int,
        default=1000,
        help="Maximum number of cache entries (default: 1000).",
    )
    parser.add_argument(
        "--context-window-size",
        type=int,
        default=3,
        help="Number of previous conversation turns to include (default: 3).",
    )
    parser.add_argument(
        "--embedding-model",
        type=str,
        default="databricks-gte-large-en",
        help="Embedding model name (default: databricks-gte-large-en).",
    )
    parser.add_argument(
        "--lru-capacity",
        type=int,
        default=100,
        help="LRU cache capacity (default: 100).",
    )

    # Locust arguments
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run in headless mode (no web UI).",
    )
    parser.add_argument(
        "-u", "--users",
        type=int,
        default=1,
        help="Number of concurrent users (default: 1). Used with --headless.",
    )
    parser.add_argument(
        "-r", "--spawn-rate",
        type=float,
        default=1.0,
        help="User spawn rate per second (default: 1). Used with --headless.",
    )
    parser.add_argument(
        "-t", "--run-time",
        type=str,
        default="10m",
        help="Run time, e.g. 30s, 5m, 1h (default: 10m). Used with --headless.",
    )
    parser.add_argument(
        "-H", "--host",
        type=str,
        default="",
        help="Host URL (not used for Genie, but required by Locust).",
    )
    parser.add_argument(
        "--web-port",
        type=int,
        default=8089,
        help="Port for Locust web UI (default: 8089).",
    )

    args = parser.parse_args()

    # Find locustfile
    try:
        locustfile = find_locustfile()
        logger.debug(f"Found locustfile at: {locustfile}")
    except FileNotFoundError as e:
        logger.error(f"Locustfile not found: {e}")
        return 1

    # Check conversations file exists
    if not args.conversations.exists():
        logger.error(f"Conversations file not found: {args.conversations}")
        logger.info("Run 'export-conversations --space-id <SPACE_ID>' first to export conversations.")
        return 1

    # Set environment variables for the load test
    env = os.environ.copy()
    env["GENIE_CONVERSATIONS_FILE"] = str(args.conversations.absolute())
    env["GENIE_MIN_WAIT"] = str(args.min_wait)
    env["GENIE_MAX_WAIT"] = str(args.max_wait)
    
    if args.sample_size is not None:
        env["GENIE_SAMPLE_SIZE"] = str(args.sample_size)
    if args.sample_seed is not None:
        env["GENIE_SAMPLE_SEED"] = str(args.sample_seed)

    # In-memory semantic cache configuration (CLI args override env vars)
    if args.space_id:
        env["GENIE_SPACE_ID"] = args.space_id
    if args.warehouse_id:
        env["GENIE_WAREHOUSE_ID"] = args.warehouse_id
    
    env["GENIE_CACHE_TTL"] = str(args.cache_ttl)
    env["GENIE_SIMILARITY_THRESHOLD"] = str(args.similarity_threshold)
    env["GENIE_CONTEXT_SIMILARITY_THRESHOLD"] = str(args.context_similarity_threshold)
    env["GENIE_CACHE_CAPACITY"] = str(args.capacity)
    env["GENIE_CONTEXT_WINDOW_SIZE"] = str(args.context_window_size)
    env["GENIE_EMBEDDING_MODEL"] = args.embedding_model
    env["GENIE_LRU_CAPACITY"] = str(args.lru_capacity)

    # Validate required cache configuration
    missing_config = []
    if not env.get("GENIE_WAREHOUSE_ID"):
        missing_config.append("GENIE_WAREHOUSE_ID (or --warehouse-id)")
    
    if missing_config:
        logger.error("Missing required cache configuration:")
        for config in missing_config:
            logger.error(f"  - {config}")
        logger.info("Set these in your .env file or provide via command-line arguments.")
        return 1

    # Build locust command
    cmd = [
        "locust",
        "-f", str(locustfile),
        "--host", args.host or "https://localhost",
    ]

    if args.headless:
        cmd.extend([
            "--headless",
            "-u", str(args.users),
            "-r", str(args.spawn_rate),
            "-t", args.run_time,
        ])
    else:
        cmd.extend(["--web-port", str(args.web_port)])

    logger.info("Starting In-Memory Semantic Cached Genie load test...")
    logger.info(f"Conversations file: {args.conversations}")
    logger.info(f"Wait time range: {args.min_wait}s - {args.max_wait}s")
    if args.sample_size is not None:
        logger.info(f"Sample size: {args.sample_size} conversations")
        if args.sample_seed is not None:
            logger.info(f"Sample seed: {args.sample_seed}")
    logger.info("-" * 50)
    logger.info("Cache Configuration:")
    logger.info(f"  Warehouse ID: {env.get('GENIE_WAREHOUSE_ID')}")
    logger.info(f"  Cache TTL: {args.cache_ttl}s")
    logger.info(f"  Similarity Threshold: {args.similarity_threshold}")
    logger.info(f"  Context Similarity Threshold: {args.context_similarity_threshold}")
    logger.info(f"  Cache Capacity: {args.capacity}")
    logger.info(f"  Context Window Size: {args.context_window_size}")
    logger.info(f"  Embedding Model: {args.embedding_model}")
    logger.info(f"  LRU Capacity: {args.lru_capacity}")
    if not args.headless:
        logger.info(f"Web UI: http://localhost:{args.web_port}")

    # Run locust
    try:
        result = subprocess.run(cmd, env=env)
        return result.returncode
    except KeyboardInterrupt:
        logger.info("Load test interrupted by user")
        return 0
    except FileNotFoundError:
        logger.error("Locust not found. Make sure it's installed: pip install locust")
        return 1


if __name__ == "__main__":
    sys.exit(main())
