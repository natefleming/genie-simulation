"""
CLI wrapper to run Genie load tests with Locust.

This provides a convenient way to run the load test without having to remember
all the locust command-line arguments.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

from loguru import logger

# Configure logging on import
from genie_simulation.logging_config import configure_logging

configure_logging()


def find_locustfile() -> Path:
    """Find the locustfile.py in the package or current directory."""
    # Check current directory first
    cwd_locustfile = Path.cwd() / "locustfile.py"
    if cwd_locustfile.exists():
        return cwd_locustfile

    # Check package directory
    package_dir = Path(__file__).parent.parent
    package_locustfile = package_dir / "locustfile.py"
    if package_locustfile.exists():
        return package_locustfile

    raise FileNotFoundError(
        "Could not find locustfile.py. Please run this command from the "
        "genie-simulation directory or ensure locustfile.py exists."
    )


def main() -> int:
    """Run the Genie load test using Locust."""
    parser = argparse.ArgumentParser(
        description="Run Genie load tests using Locust.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with web UI (opens http://localhost:8089)
  genie-loadtest

  # Run headless with 10 users, spawning 2/sec, for 5 minutes
  genie-loadtest --headless -u 10 -r 2 -t 5m

  # Use a custom conversations file
  genie-loadtest --conversations my_conversations.yaml

  # Run with custom wait times
  genie-loadtest --min-wait 3 --max-wait 15

  # Sample 10 conversations and reuse them throughout the test
  genie-loadtest --sample-size 10

  # Use a fixed seed for reproducible sampling
  genie-loadtest --sample-size 10 --sample-seed 42
        """,
    )

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

    # Build locust command
    cmd = [
        "locust",
        "-f", str(locustfile),
        "--host", args.host or "https://localhost",  # Locust requires a host
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

    logger.info("Starting Genie load test...")
    logger.info(f"Conversations file: {args.conversations}")
    logger.info(f"Wait time range: {args.min_wait}s - {args.max_wait}s")
    if args.sample_size is not None:
        logger.info(f"Sample size: {args.sample_size} conversations")
        if args.sample_seed is not None:
            logger.info(f"Sample seed: {args.sample_seed}")
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
