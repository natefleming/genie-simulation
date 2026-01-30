"""
Export Genie conversations and messages to YAML format.

This tool retrieves all conversations and their underlying messages from a Genie space
using the Databricks SDK and exports them to a YAML configuration file for load testing.
"""

import argparse
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieMessage
from loguru import logger

# Configure logging on import
from genie_simulation.logging_config import configure_logging

configure_logging()


def parse_timestamp(timestamp_str: str) -> int:
    """
    Parse timestamp string into Unix milliseconds.

    Supports multiple formats:
    - Relative: 7d (days), 24h (hours), 30m (minutes), 1w (weeks)
    - ISO 8601: 2026-01-15T10:30:00 or 2026-01-15T10:30:00Z
    - Date: 2026-01-15 (assumes start of day in UTC)

    Args:
        timestamp_str: The timestamp string to parse.

    Returns:
        Unix timestamp in milliseconds.

    Raises:
        ValueError: If the timestamp format is not recognized.
    """
    timestamp_str = timestamp_str.strip()

    # Try relative format first (e.g., 7d, 24h, 30m, 1w)
    relative_pattern = r"^(\d+)([dhwm])$"
    match = re.match(relative_pattern, timestamp_str, re.IGNORECASE)
    if match:
        value = int(match.group(1))
        unit = match.group(2).lower()

        now = datetime.now(timezone.utc)
        if unit == "m":
            delta = timedelta(minutes=value)
        elif unit == "h":
            delta = timedelta(hours=value)
        elif unit == "d":
            delta = timedelta(days=value)
        elif unit == "w":
            delta = timedelta(weeks=value)
        else:
            raise ValueError(f"Unknown time unit: {unit}")

        target_time = now - delta
        return int(target_time.timestamp() * 1000)

    # Try ISO 8601 format with timezone
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]:
        try:
            # Handle Z suffix explicitly
            ts_str = timestamp_str.replace("Z", "+00:00") if "Z" in timestamp_str else timestamp_str
            dt = datetime.strptime(ts_str, fmt)
            # If no timezone info, assume UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue

    # Try simple date format (assumes start of day UTC)
    try:
        dt = datetime.strptime(timestamp_str, "%Y-%m-%d")
        dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        pass

    raise ValueError(
        f"Unable to parse timestamp: {timestamp_str}\n"
        f"Supported formats:\n"
        f"  - Relative: 7d, 24h, 30m, 1w\n"
        f"  - ISO 8601: 2026-01-15T10:30:00, 2026-01-15T10:30:00Z\n"
        f"  - Date: 2026-01-15"
    )


def get_message_content(message: GenieMessage) -> str | None:
    """Extract the user's question content from a Genie message."""
    # The content field contains the user's question
    return message.content


def export_conversations(
    space_id: str,
    output_path: Path,
    include_all: bool = False,
    max_conversations: int | None = None,
    from_timestamp: int | None = None,
    to_timestamp: int | None = None,
    client: WorkspaceClient | None = None,
) -> dict[str, Any]:
    """
    Export all conversations and messages from a Genie space to YAML.

    Args:
        space_id: The Genie space ID to export conversations from.
        output_path: Path to write the YAML output file.
        include_all: If True, include conversations from all users (requires CAN MANAGE permission).
        max_conversations: Maximum number of conversations to export (None for all).
        from_timestamp: Optional start timestamp in milliseconds (inclusive). Conversations created before this are excluded.
        to_timestamp: Optional end timestamp in milliseconds (inclusive). Conversations created after this are excluded.
        client: Optional WorkspaceClient instance. If not provided, one will be created.

    Returns:
        The exported data as a dictionary.
    """
    if client is None:
        client = WorkspaceClient()

    logger.info(f"Connecting to Genie space: {space_id}")

    # Collect all conversations with pagination
    all_conversations: list[dict[str, Any]] = []
    page_token: str | None = None
    conversation_count = 0

    while True:
        # List conversations for the space
        response = client.genie.list_conversations(
            space_id=space_id,
            include_all=include_all,
            page_size=100,
            page_token=page_token,
        )

        if not response.conversations:
            break

        for conv in response.conversations:
            if max_conversations and conversation_count >= max_conversations:
                break

            # Use conversation_id attribute (SDK returns GenieConversationSummary)
            conv_id = conv.conversation_id
            conv_title = conv.title or "Untitled"
            
            # Filter by timestamp range if specified
            if from_timestamp is not None and conv.created_timestamp < from_timestamp:
                logger.debug(f"Skipping conversation {conv_id} (created before from_timestamp)")
                continue
            if to_timestamp is not None and conv.created_timestamp > to_timestamp:
                logger.debug(f"Skipping conversation {conv_id} (created after to_timestamp)")
                continue

            logger.debug(f"Processing conversation: {conv_id} - {conv_title}")

            # Get all messages for this conversation
            messages: list[dict[str, Any]] = []
            msg_page_token: str | None = None

            while True:
                msg_response = client.genie.list_conversation_messages(
                    space_id=space_id,
                    conversation_id=conv_id,
                    page_size=100,
                    page_token=msg_page_token,
                )

                if not msg_response.messages:
                    break

                for msg in msg_response.messages:
                    content = get_message_content(msg)
                    if content:  # Only include messages with user content
                        messages.append({
                            "content": content,
                            "timestamp": msg.created_timestamp,
                            "message_id": msg.id,
                        })

                # Check for next page
                if msg_response.next_page_token:
                    msg_page_token = msg_response.next_page_token
                else:
                    break

            # Sort messages by timestamp to ensure correct order
            messages.sort(key=lambda m: m.get("timestamp") or 0)

            if messages:  # Only include conversations that have messages
                all_conversations.append({
                    "id": conv_id,
                    "title": conv_title,
                    "created_timestamp": conv.created_timestamp,
                    "messages": messages,
                })
                conversation_count += 1

            if max_conversations and conversation_count >= max_conversations:
                break

        # Check for next page of conversations
        if response.next_page_token and (
            not max_conversations or conversation_count < max_conversations
        ):
            page_token = response.next_page_token
        else:
            break

    # Build the output data structure
    export_data: dict[str, Any] = {
        "space_id": space_id,
        "exported_at": datetime.now().isoformat(),
        "total_conversations": len(all_conversations),
        "total_messages": sum(len(c["messages"]) for c in all_conversations),
        "conversations": all_conversations,
    }
    
    # Add filter information if timestamps were specified
    if from_timestamp is not None or to_timestamp is not None:
        export_data["filter"] = {}
        if from_timestamp is not None:
            export_data["filter"]["from_timestamp"] = from_timestamp
            export_data["filter"]["from_timestamp_iso"] = datetime.fromtimestamp(
                from_timestamp / 1000, tz=timezone.utc
            ).isoformat()
        if to_timestamp is not None:
            export_data["filter"]["to_timestamp"] = to_timestamp
            export_data["filter"]["to_timestamp_iso"] = datetime.fromtimestamp(
                to_timestamp / 1000, tz=timezone.utc
            ).isoformat()

    # Write to YAML file
    with open(output_path, "w") as f:
        yaml.dump(export_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    logger.success(
        f"Exported {len(all_conversations)} conversations with "
        f"{export_data['total_messages']} messages to {output_path}"
    )

    return export_data


def main() -> int:
    """CLI entrypoint for exporting Genie conversations."""
    parser = argparse.ArgumentParser(
        description="Export Genie conversations and messages to YAML format for load testing.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export conversations from a Genie space
  export-conversations --space-id abc123 -o conversations.yaml

  # Export all users' conversations (requires CAN MANAGE permission)
  export-conversations --space-id abc123 -o conversations.yaml --include-all

  # Export only the first 10 conversations
  export-conversations --space-id abc123 -o conversations.yaml --max-conversations 10

  # Export conversations from the last 7 days
  export-conversations --space-id abc123 --from 7d

  # Export conversations from the last 24 hours
  export-conversations --space-id abc123 --from 24h

  # Export conversations within a specific date range
  export-conversations --space-id abc123 --from 2026-01-01 --to 2026-01-31

  # Export conversations since a specific date/time
  export-conversations --space-id abc123 --from 2026-01-15T10:00:00

  # Export conversations up to a specific date
  export-conversations --space-id abc123 --to 2026-01-31
        """,
    )

    parser.add_argument(
        "--space-id",
        required=True,
        help="The Genie space ID to export conversations from.",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=Path("conversations.yaml"),
        help="Output YAML file path (default: conversations.yaml).",
    )
    parser.add_argument(
        "--include-all",
        action="store_true",
        help="Include conversations from all users (requires CAN MANAGE permission on the space).",
    )
    parser.add_argument(
        "--max-conversations",
        type=int,
        default=None,
        help="Maximum number of conversations to export (default: all).",
    )
    parser.add_argument(
        "--from",
        dest="from_timestamp",
        type=str,
        default=None,
        help="Start of timestamp range (inclusive). Supports: relative (7d, 24h, 30m, 1w), ISO 8601 (2026-01-15T10:30:00), or date (2026-01-15).",
    )
    parser.add_argument(
        "--to",
        dest="to_timestamp",
        type=str,
        default=None,
        help="End of timestamp range (inclusive). Supports: relative (7d, 24h, 30m, 1w), ISO 8601 (2026-01-15T10:30:00), or date (2026-01-15).",
    )

    args = parser.parse_args()

    try:
        # Parse timestamp arguments if provided
        from_ts: int | None = None
        to_ts: int | None = None
        
        if args.from_timestamp:
            from_ts = parse_timestamp(args.from_timestamp)
            logger.info(
                f"Filtering conversations from: {datetime.fromtimestamp(from_ts / 1000, tz=timezone.utc).isoformat()}"
            )
        
        if args.to_timestamp:
            to_ts = parse_timestamp(args.to_timestamp)
            logger.info(
                f"Filtering conversations to: {datetime.fromtimestamp(to_ts / 1000, tz=timezone.utc).isoformat()}"
            )

        export_conversations(
            space_id=args.space_id,
            output_path=args.output,
            include_all=args.include_all,
            max_conversations=args.max_conversations,
            from_timestamp=from_ts,
            to_timestamp=to_ts,
        )
        return 0
    except Exception as e:
        logger.error(f"Export failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
