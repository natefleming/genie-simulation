# Databricks notebook source
# MAGIC %md
# MAGIC # Export Genie Conversations
# MAGIC 
# MAGIC This notebook exports conversations from a Genie space to a YAML file for load testing.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC 1. **Configuration**: Ensure `.env` file exists in the parent directory with `GENIE_SPACE_ID` set
# MAGIC 
# MAGIC ## Configuration
# MAGIC 
# MAGIC The Space ID is loaded from `.env`. You can override it with the widget if needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("space_id_override", "", "Space ID Override (leave empty to use .env)")
dbutils.widgets.text("output_filename", "../conversations.yaml", "Output Filename")
dbutils.widgets.dropdown("include_all", "false", ["true", "false"], "Include All Users' Conversations")
dbutils.widgets.text("max_conversations", "", "Max Conversations (empty = all)")
dbutils.widgets.text("from_timestamp", "", "From Timestamp (e.g., 7d, 2026-01-15, empty = no filter)")
dbutils.widgets.text("to_timestamp", "", "To Timestamp (e.g., 2026-01-31, empty = no filter)")

# COMMAND ----------

import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv("../.env")

# Get widget values
space_id_override = dbutils.widgets.get("space_id_override").strip()
output_filename = dbutils.widgets.get("output_filename")
include_all = dbutils.widgets.get("include_all") == "true"
max_conversations_str = dbutils.widgets.get("max_conversations")
max_conversations = int(max_conversations_str) if max_conversations_str else None
from_timestamp_str = dbutils.widgets.get("from_timestamp").strip()
to_timestamp_str = dbutils.widgets.get("to_timestamp").strip()

# Determine final space_id
space_id = space_id_override if space_id_override else os.environ.get("GENIE_SPACE_ID", "")

if not space_id:
    raise ValueError(
        "Space ID is required.\n\n"
        "Option 1: Set GENIE_SPACE_ID in your .env file\n"
        "Option 2: Enter a value in the 'Space ID Override' widget"
    )

print("Export Configuration:")
print("-" * 50)
print(f"Space ID: {space_id}")
print(f"Output Filename: {output_filename}")
print(f"Include All Users: {include_all}")
print(f"Max Conversations: {max_conversations or 'All'}")
print(f"From Timestamp: {from_timestamp_str or 'None (no filter)'}")
print(f"To Timestamp: {to_timestamp_str or 'None (no filter)'}")
print("-" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Conversations

# COMMAND ----------

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieMessage

client = WorkspaceClient()


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
    return message.content


def export_conversations(
    space_id: str,
    output_path: Path,
    include_all: bool = False,
    max_conversations: int | None = None,
    from_timestamp: int | None = None,
    to_timestamp: int | None = None,
) -> dict[str, Any]:
    """Export all conversations and messages from a Genie space to YAML."""
    print(f"Connecting to Genie space: {space_id}")
    
    if from_timestamp is not None:
        print(f"  Filtering from: {datetime.fromtimestamp(from_timestamp / 1000, tz=timezone.utc).isoformat()}")
    if to_timestamp is not None:
        print(f"  Filtering to: {datetime.fromtimestamp(to_timestamp / 1000, tz=timezone.utc).isoformat()}")

    all_conversations: list[dict[str, Any]] = []
    page_token: str | None = None
    conversation_count = 0

    while True:
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

            conv_id = conv.conversation_id
            conv_title = conv.title or "Untitled"
            
            # Filter by timestamp range if specified
            if from_timestamp is not None and conv.created_timestamp < from_timestamp:
                continue
            if to_timestamp is not None and conv.created_timestamp > to_timestamp:
                continue

            print(f"  Processing: {conv_id[:20]}... - {conv_title[:50]}")

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
                    if content:
                        messages.append({
                            "content": content,
                            "timestamp": msg.created_timestamp,
                            "message_id": msg.id,
                        })

                if msg_response.next_page_token:
                    msg_page_token = msg_response.next_page_token
                else:
                    break

            messages.sort(key=lambda m: m.get("timestamp") or 0)

            if messages:
                all_conversations.append({
                    "id": conv_id,
                    "title": conv_title,
                    "created_timestamp": conv.created_timestamp,
                    "messages": messages,
                })
                conversation_count += 1

            if max_conversations and conversation_count >= max_conversations:
                break

        if response.next_page_token and (
            not max_conversations or conversation_count < max_conversations
        ):
            page_token = response.next_page_token
        else:
            break

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

    with open(output_path, "w") as f:
        yaml.dump(export_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"\nExported {len(all_conversations)} conversations with "
          f"{export_data['total_messages']} messages")

    return export_data

# COMMAND ----------

output_path = Path(output_filename)

# Parse timestamp filters if provided
from_ts: int | None = None
to_ts: int | None = None

if from_timestamp_str:
    from_ts = parse_timestamp(from_timestamp_str)
    print(f"Parsed from_timestamp: {datetime.fromtimestamp(from_ts / 1000, tz=timezone.utc).isoformat()}")

if to_timestamp_str:
    to_ts = parse_timestamp(to_timestamp_str)
    print(f"Parsed to_timestamp: {datetime.fromtimestamp(to_ts / 1000, tz=timezone.utc).isoformat()}")

if output_path.exists():
    print(f"\nFile already exists: {output_filename}")
    print("Loading existing data. Delete the file if you want to re-export.")
    with open(output_path) as f:
        export_data = yaml.safe_load(f)
else:
    print("\nStarting export...")
    print("=" * 50)

    export_data = export_conversations(
        space_id=space_id,
        output_path=output_path,
        include_all=include_all,
        max_conversations=max_conversations,
        from_timestamp=from_ts,
        to_timestamp=to_ts,
    )

    print("=" * 50)
    print(f"Export completed! File saved to: {output_filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

import pandas as pd

summary_data = [
    {"Metric": "Space ID", "Value": str(space_id)},
    {"Metric": "Total Conversations", "Value": str(export_data["total_conversations"])},
    {"Metric": "Total Messages", "Value": str(export_data["total_messages"])},
    {"Metric": "Exported At", "Value": str(export_data["exported_at"])},
    {"Metric": "Output File", "Value": str(output_filename)},
]

# Add filter information if present
if "filter" in export_data:
    if "from_timestamp_iso" in export_data["filter"]:
        summary_data.append({"Metric": "Filtered From", "Value": export_data["filter"]["from_timestamp_iso"]})
    if "to_timestamp_iso" in export_data["filter"]:
        summary_data.append({"Metric": "Filtered To", "Value": export_data["filter"]["to_timestamp_iso"]})

display(pd.DataFrame(summary_data))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview

# COMMAND ----------

preview_data = []
for conv in export_data["conversations"][:10]:
    preview_data.append({
        "ID": conv["id"][:20] + "...",
        "Title": conv["title"][:60] + ("..." if len(conv["title"]) > 60 else ""),
        "Messages": str(len(conv["messages"])),
    })

if preview_data:
    display(pd.DataFrame(preview_data))
else:
    print("No conversations exported")
