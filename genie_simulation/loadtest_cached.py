"""
CLI wrapper to run cached Genie load tests with Locust.

This provides a convenient way to run the cached load test without having to remember
all the locust command-line arguments.
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
    """Find the locustfile_cached.py in the package or current directory."""
    # Check current directory first
    cwd_locustfile = Path.cwd() / "locustfile_cached.py"
    if cwd_locustfile.exists():
        return cwd_locustfile

    # Check package directory
    package_dir = Path(__file__).parent.parent
    package_locustfile = package_dir / "locustfile_cached.py"
    if package_locustfile.exists():
        return package_locustfile

    raise FileNotFoundError(
        "Could not find locustfile_cached.py. Please run this command from the "
        "genie-simulation directory or ensure locustfile_cached.py exists."
    )


def main() -> int:
    """Run the cached Genie load test using Locust."""
    parser = argparse.ArgumentParser(
        description="Run cached Genie load tests using Locust.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with web UI (opens http://localhost:8089)
  genie-loadtest-cached

  # Run headless with 10 users, spawning 2/sec, for 5 minutes
  genie-loadtest-cached --headless -u 10 -r 2 -t 5m

  # Use a custom conversations file
  genie-loadtest-cached --conversations my_conversations.yaml

  # Run with custom cache settings
  genie-loadtest-cached --cache-ttl 3600 --similarity-threshold 0.9

  # Sample 10 conversations and reuse them throughout the test
  genie-loadtest-cached --sample-size 10

Environment Variables (can also be set in .env file):
  GENIE_LAKEBASE_CLIENT_ID       Lakebase client ID
  GENIE_LAKEBASE_CLIENT_SECRET   Lakebase client secret
  GENIE_LAKEBASE_INSTANCE  Lakebase instance name
  GENIE_WAREHOUSE_ID       Warehouse ID for cache operations
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

    # Cache service arguments
    parser.add_argument(
        "--space-id",
        type=str,
        default=None,
        help="Genie space ID (default: from conversations file or GENIE_SPACE_ID env).",
    )
    parser.add_argument(
        "--client-id",
        type=str,
        default=None,
        help="Lakebase client ID (default: GENIE_LAKEBASE_CLIENT_ID env).",
    )
    parser.add_argument(
        "--client-secret",
        type=str,
        default=None,
        help="Lakebase client secret (default: GENIE_LAKEBASE_CLIENT_SECRET env).",
    )
    parser.add_argument(
        "--lakebase-instance",
        type=str,
        default=None,
        help="Lakebase instance name (default: GENIE_LAKEBASE_INSTANCE env).",
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
        help="Semantic similarity threshold for cache hits (default: 0.85).",
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

    # Cache service configuration (CLI args override env vars)
    if args.space_id:
        env["GENIE_SPACE_ID"] = args.space_id
    if args.client_id:
        env["GENIE_LAKEBASE_CLIENT_ID"] = args.client_id
    if args.client_secret:
        env["GENIE_LAKEBASE_CLIENT_SECRET"] = args.client_secret
    if args.lakebase_instance:
        env["GENIE_LAKEBASE_INSTANCE"] = args.lakebase_instance
    if args.warehouse_id:
        env["GENIE_WAREHOUSE_ID"] = args.warehouse_id
    
    env["GENIE_CACHE_TTL"] = str(args.cache_ttl)
    env["GENIE_SIMILARITY_THRESHOLD"] = str(args.similarity_threshold)
    env["GENIE_LRU_CAPACITY"] = str(args.lru_capacity)

    # Validate required cache configuration
    missing_config = []
    if not env.get("GENIE_LAKEBASE_CLIENT_ID"):
        missing_config.append("GENIE_LAKEBASE_CLIENT_ID (or --client-id)")
    if not env.get("GENIE_LAKEBASE_CLIENT_SECRET"):
        missing_config.append("GENIE_LAKEBASE_CLIENT_SECRET (or --client-secret)")
    if not env.get("GENIE_LAKEBASE_INSTANCE"):
        missing_config.append("GENIE_LAKEBASE_INSTANCE (or --lakebase-instance)")
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

    logger.info("Starting Cached Genie load test...")
    logger.info(f"Conversations file: {args.conversations}")
    logger.info(f"Wait time range: {args.min_wait}s - {args.max_wait}s")
    if args.sample_size is not None:
        logger.info(f"Sample size: {args.sample_size} conversations")
        if args.sample_seed is not None:
            logger.info(f"Sample seed: {args.sample_seed}")
    logger.info("-" * 50)
    logger.info("Cache Configuration:")
    logger.info(f"  Lakebase Instance: {env.get('GENIE_LAKEBASE_INSTANCE')}")
    logger.info(f"  Warehouse ID: {env.get('GENIE_WAREHOUSE_ID')}")
    logger.info(f"  Cache TTL: {args.cache_ttl}s")
    logger.info(f"  Similarity Threshold: {args.similarity_threshold}")
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
