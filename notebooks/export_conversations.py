# Databricks notebook source
# MAGIC %md
# MAGIC # Export Genie Conversations
# MAGIC 
# MAGIC This notebook exports conversations from a Genie space to a YAML file for load testing.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC 1. **Configuration**: Upload a `.env` file to this directory with `GENIE_SPACE_ID` set
# MAGIC 2. **Install Package**: The `genie-simulation` package must be installed on the cluster
# MAGIC 
# MAGIC ## Configuration
# MAGIC 
# MAGIC The Space ID is loaded from `.env`. You can override it with the widget if needed.
# MAGIC 
# MAGIC The exported file will be saved to the same workspace directory as this notebook.

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

# Minimal widgets - space_id can be overridden, output filename adjustable
dbutils.widgets.text("space_id_override", "", "Space ID Override (leave empty to use .env)")
dbutils.widgets.text("output_filename", "conversations.yaml", "Output Filename")
dbutils.widgets.dropdown("include_all", "false", ["true", "false"], "Include All Users' Conversations")
dbutils.widgets.text("max_conversations", "", "Max Conversations (empty = all)")

# COMMAND ----------

import os
from io import StringIO

from databricks.sdk import WorkspaceClient
from dotenv import dotenv_values
from genie_simulation.config import get_notebook_directory

# Get the notebook directory
notebook_dir = get_notebook_directory(dbutils)
print(f"Notebook directory: {notebook_dir}")

# Initialize workspace client
client = WorkspaceClient()

# Try to load space_id from .env
space_id_from_env = None
try:
    workspace_env_path = os.path.join(notebook_dir, ".env")
    with client.workspace.download(workspace_env_path) as f:
        content = f.read().decode("utf-8")
    env_vars = dotenv_values(stream=StringIO(content))
    space_id_from_env = env_vars.get("GENIE_SPACE_ID", "")
    if space_id_from_env:
        print(f"Loaded GENIE_SPACE_ID from .env: {space_id_from_env}")
except Exception as e:
    print(f"Note: Could not load .env file: {e}")
    print("You can still use the space_id_override widget")

# Get widget values
space_id_override = dbutils.widgets.get("space_id_override").strip()
output_filename = dbutils.widgets.get("output_filename")
include_all = dbutils.widgets.get("include_all") == "true"
max_conversations_str = dbutils.widgets.get("max_conversations")
max_conversations = int(max_conversations_str) if max_conversations_str else None

# Determine final space_id
space_id = space_id_override if space_id_override else space_id_from_env

if not space_id:
    raise ValueError(
        "Space ID is required.\n\n"
        "Option 1: Set GENIE_SPACE_ID in your .env file\n"
        "Option 2: Enter a value in the 'Space ID Override' widget"
    )

# Display configuration
print("\nExport Configuration:")
print("-" * 50)
print(f"Space ID: {space_id}")
print(f"Output Filename: {output_filename}")
print(f"Include All Users: {include_all}")
print(f"Max Conversations: {max_conversations or 'All'}")
print("-" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Determine Output Path

# COMMAND ----------

import tempfile
from pathlib import Path

# Construct the output path in the workspace
workspace_output_path = os.path.join(notebook_dir, output_filename)

print(f"Output Path: {workspace_output_path}")

# Create a temp directory for the output
temp_dir = tempfile.mkdtemp()
local_output_path = Path(temp_dir) / output_filename

print(f"Temporary Local Path: {local_output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Conversations

# COMMAND ----------

from datetime import datetime
from typing import Any

import yaml
from databricks.sdk.service.dashboards import GenieMessage


def get_message_content(message: GenieMessage) -> str | None:
    """Extract the user's question content from a Genie message."""
    return message.content


def export_conversations(
    space_id: str,
    output_path: Path,
    include_all: bool = False,
    max_conversations: int | None = None,
    client: WorkspaceClient | None = None,
) -> dict[str, Any]:
    """Export all conversations and messages from a Genie space to YAML."""
    if client is None:
        client = WorkspaceClient()

    print(f"Connecting to Genie space: {space_id}")

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

            conv_id = conv.conversation_id
            conv_title = conv.title or "Untitled"

            print(f"  Processing: {conv_id[:20]}... - {conv_title[:50]}")

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

            # Sort messages by timestamp
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

    # Build the output data structure
    export_data = {
        "space_id": space_id,
        "exported_at": datetime.now().isoformat(),
        "total_conversations": len(all_conversations),
        "total_messages": sum(len(c["messages"]) for c in all_conversations),
        "conversations": all_conversations,
    }

    # Write to YAML file
    with open(output_path, "w") as f:
        yaml.dump(export_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"\nExported {len(all_conversations)} conversations with "
          f"{export_data['total_messages']} messages")

    return export_data

# COMMAND ----------

# Run the export
print("Starting export...")
print("=" * 50)

export_data = export_conversations(
    space_id=space_id,
    output_path=local_output_path,
    include_all=include_all,
    max_conversations=max_conversations,
    client=client,
)

print("=" * 50)
print("Export completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy to Workspace Directory

# COMMAND ----------

# Read the exported file content
with open(local_output_path, "r") as f:
    yaml_content = f.read()

# Write to workspace using the Workspace API
from databricks.sdk.service.workspace import ImportFormat

client.workspace.import_(
    path=workspace_output_path,
    content=yaml_content.encode("utf-8"),
    format=ImportFormat.AUTO,
    overwrite=True,
)

print(f"File saved to workspace: {workspace_output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

import pandas as pd

# Display summary as a table
summary_data = [
    {"Metric": "Space ID", "Value": space_id},
    {"Metric": "Total Conversations", "Value": export_data["total_conversations"]},
    {"Metric": "Total Messages", "Value": export_data["total_messages"]},
    {"Metric": "Exported At", "Value": export_data["exported_at"]},
    {"Metric": "Output Path", "Value": workspace_output_path},
]

display(pd.DataFrame(summary_data))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Exported Data
# MAGIC 
# MAGIC First few conversations:

# COMMAND ----------

# Show preview of conversations
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up

# COMMAND ----------

# Clean up temp files
import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
print("Temporary files cleaned up")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC The conversations file has been exported to the workspace directory. You can now:
# MAGIC 
# MAGIC 1. Run the **load_test** notebook for baseline performance metrics
# MAGIC 2. Run the **load_test_cached** notebook to test with caching enabled
