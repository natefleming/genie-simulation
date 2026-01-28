# Genie Load Test Simulation

A tool for testing how well your Databricks Genie chatbot handles multiple users asking questions at the same time.

## What This Tool Does

This tool helps you:

1. **Export past conversations** - Downloads all the questions people have asked your Genie chatbot
2. **Simulate real users** - Replays those questions to test how Genie performs under load
3. **Measure performance** - Shows you how fast Genie responds and if any requests fail

This is useful for understanding if your Genie setup can handle many users before you roll it out to your organization.

---

## Before You Start

You'll need:

1. **Python 3.11 or newer** installed on your computer
2. **Access to a Databricks workspace** with a Genie space set up
3. **Your Genie Space ID** (a long string of letters and numbers that identifies your Genie)

### Finding Your Genie Space ID

1. Open your Databricks workspace in a web browser
2. Navigate to your Genie space
3. Look at the URL in your browser - it will look something like:
   ```
   https://your-workspace.databricks.com/genie/rooms/01f0c482e842191587af6a40ad4044d8
   ```
4. The Space ID is the last part: `01f0c482e842191587af6a40ad4044d8`

---

## Installation

### Step 1: Open a Terminal

- **On Mac**: Open the "Terminal" app (search for it in Spotlight)
- **On Windows**: Open "Command Prompt" or "PowerShell"

### Step 2: Navigate to the Project Folder

```bash
cd /path/to/genie-simulation
```

Replace `/path/to/genie-simulation` with the actual location where you saved this project.

### Step 3: Install the Tool

If you have `uv` installed (recommended):
```bash
uv sync
```

Or with pip:
```bash
pip install -e .
```

### Step 4: Set Up Databricks Authentication

You need to tell the tool how to connect to your Databricks workspace.

**Option A: Using environment variables (simplest)**

```bash
export DATABRICKS_HOST="https://your-workspace.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
```

To get a personal access token:
1. In Databricks, click your username in the top right
2. Click "Settings"
3. Click "Developer"
4. Click "Manage" next to "Access tokens"
5. Click "Generate new token"
6. Copy the token (you won't be able to see it again!)

**Option B: Using Databricks CLI**

If you have the Databricks CLI configured, the tool will use those credentials automatically.

---

## How to Use

### Step 1: Export Conversations

First, download the conversations from your Genie space:

```bash
uv run export-conversations --space-id YOUR_SPACE_ID --include-all -o conversations.yaml
```

Replace `YOUR_SPACE_ID` with your actual Genie Space ID.

**What the options mean:**
- `--space-id` - Your Genie Space ID (required)
- `--include-all` - Include conversations from all users, not just yours
- `-o conversations.yaml` - Save the conversations to a file called "conversations.yaml"

**Example:**
```bash
uv run export-conversations --space-id 01f0c482e842191587af6a40ad4044d8 --include-all -o conversations.yaml
```

You should see output like:
```
2026-01-28 10:30:00 | INFO     | Connecting to Genie space: 01f0c482e842191587af6a40ad4044d8
2026-01-28 10:30:02 | SUCCESS  | Exported 25 conversations with 47 messages to conversations.yaml
```

### Step 2: Run the Load Test

Now run the simulation to test Genie's performance:

**Option A: With a Web Interface (recommended for first time)**

```bash
uv run genie-loadtest
```

Then open your web browser and go to: http://localhost:8089

You'll see a page where you can:
- Set the number of simulated users
- Set how fast to add new users
- Start and stop the test
- Watch real-time performance charts

**Option B: Without a Web Interface (headless mode)**

```bash
uv run genie-loadtest --headless -u 5 -r 1 -t 5m
```

**What the options mean:**
- `--headless` - Run without the web interface
- `-u 5` - Simulate 5 users
- `-r 1` - Add 1 new user per second
- `-t 5m` - Run the test for 5 minutes

---

## Understanding the Results

When the test runs, you'll see metrics like:

| Metric | What It Means |
|--------|---------------|
| **Requests** | Total number of questions sent to Genie |
| **Failures** | Questions that Genie couldn't answer |
| **Median Response Time** | How long a typical response takes (in milliseconds) |
| **95th Percentile** | 95% of responses are faster than this time |
| **Requests/s** | How many questions Genie handles per second |

### What Good Results Look Like

- **0% failure rate** - All questions were answered
- **Response times under 30 seconds** - Genie is responding quickly
- **Stable response times** - Times don't increase as more users are added

### Warning Signs

- **High failure rate** - Genie is struggling to handle the load
- **Response times over 60 seconds** - Users will get frustrated waiting
- **Response times increasing over time** - System is getting overwhelmed

---

## Common Options

### Export Options

```bash
# Export only the first 10 conversations (for quick testing)
uv run export-conversations --space-id YOUR_SPACE_ID --include-all --max-conversations 10 -o conversations.yaml
```

### Load Test Options

```bash
# Use a specific conversations file
uv run genie-loadtest -c my_conversations.yaml

# Sample only 10 conversations and reuse them
uv run genie-loadtest --sample-size 10

# Change the wait time between messages (in seconds)
uv run genie-loadtest --min-wait 3 --max-wait 15

# Run a longer test with more users
uv run genie-loadtest --headless -u 20 -r 2 -t 30m
```

---

## Troubleshooting

### "Conversations file not found"

Make sure you ran the export step first:
```bash
uv run export-conversations --space-id YOUR_SPACE_ID --include-all -o conversations.yaml
```

### "Exported 0 conversations"

Try adding the `--include-all` flag to see conversations from all users:
```bash
uv run export-conversations --space-id YOUR_SPACE_ID --include-all -o conversations.yaml
```

### "Authentication error" or "401 Unauthorized"

Your Databricks credentials aren't set up correctly. Make sure you've set:
```bash
export DATABRICKS_HOST="https://your-workspace.databricks.com"
export DATABRICKS_TOKEN="your-token-here"
```

### "Permission denied" errors during the test

The user running the test needs access to:
- The Genie space
- The underlying data tables that Genie queries

Ask your Databricks administrator to grant the necessary permissions.

### The test shows 100% failures

Check the error messages in the output. Common causes:
- Missing table permissions
- Genie space is not properly configured
- Network connectivity issues

---

## Quick Reference

| Command | What It Does |
|---------|--------------|
| `uv run export-conversations --space-id ID --include-all -o conversations.yaml` | Download conversations |
| `uv run genie-loadtest` | Run test with web UI |
| `uv run genie-loadtest --headless -u 5 -t 10m` | Run test for 10 min with 5 users |
| `uv run genie-simulation` | Show help and available commands |

---

## Getting Help

If you run into issues:

1. Check the error messages in the terminal output
2. Make sure your Databricks credentials are correct
3. Verify you have access to the Genie space
4. Contact your Databricks administrator for permission issues
