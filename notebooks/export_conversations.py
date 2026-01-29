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
dbutils.widgets.text("output_filename", "conversations.yaml", "Output Filename")
dbutils.widgets.dropdown("include_all", "false", ["true", "false"], "Include All Users' Conversations")
dbutils.widgets.text("max_conversations", "", "Max Conversations (empty = all)")

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
print("-" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Conversations

# COMMAND ----------

from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieMessage

client = WorkspaceClient()


def get_message_content(message: GenieMessage) -> str | None:
    """Extract the user's question content from a Genie message."""
    return message.content


def export_conversations(
    space_id: str,
    output_path: Path,
    include_all: bool = False,
    max_conversations: int | None = None,
) -> dict[str, Any]:
    """Export all conversations and messages from a Genie space to YAML."""
    print(f"Connecting to Genie space: {space_id}")

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

    export_data = {
        "space_id": space_id,
        "exported_at": datetime.now().isoformat(),
        "total_conversations": len(all_conversations),
        "total_messages": sum(len(c["messages"]) for c in all_conversations),
        "conversations": all_conversations,
    }

    with open(output_path, "w") as f:
        yaml.dump(export_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"\nExported {len(all_conversations)} conversations with "
          f"{export_data['total_messages']} messages")

    return export_data

# COMMAND ----------

print("Starting export...")
print("=" * 50)

export_data = export_conversations(
    space_id=space_id,
    output_path=Path(output_filename),
    include_all=include_all,
    max_conversations=max_conversations,
)

print("=" * 50)
print(f"Export completed! File saved to: {output_filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

import pandas as pd

summary_data = [
    {"Metric": "Space ID", "Value": space_id},
    {"Metric": "Total Conversations", "Value": export_data["total_conversations"]},
    {"Metric": "Total Messages", "Value": export_data["total_messages"]},
    {"Metric": "Exported At", "Value": export_data["exported_at"]},
    {"Metric": "Output File", "Value": output_filename},
]

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
        "Messages": len(conv["messages"]),
    })

if preview_data:
    display(pd.DataFrame(preview_data))
else:
    print("No conversations exported")
