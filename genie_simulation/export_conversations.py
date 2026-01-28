"""
Export Genie conversations and messages to YAML format.

This tool retrieves all conversations and their underlying messages from a Genie space
using the Databricks SDK and exports them to a YAML configuration file for load testing.
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieMessage
from loguru import logger

# Configure logging on import
from genie_simulation.logging_config import configure_logging

configure_logging()


def get_message_content(message: GenieMessage) -> str | None:
    """Extract the user's question content from a Genie message."""
    # The content field contains the user's question
    return message.content


def export_conversations(
    space_id: str,
    output_path: Path,
    include_all: bool = False,
    max_conversations: int | None = None,
    client: WorkspaceClient | None = None,
) -> dict[str, Any]:
    """
    Export all conversations and messages from a Genie space to YAML.

    Args:
        space_id: The Genie space ID to export conversations from.
        output_path: Path to write the YAML output file.
        include_all: If True, include conversations from all users (requires CAN MANAGE permission).
        max_conversations: Maximum number of conversations to export (None for all).
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

    args = parser.parse_args()

    try:
        export_conversations(
            space_id=args.space_id,
            output_path=args.output,
            include_all=args.include_all,
            max_conversations=args.max_conversations,
        )
        return 0
    except Exception as e:
        logger.error(f"Export failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
