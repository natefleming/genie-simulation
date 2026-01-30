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

There are three types of load tests:
- **Standard Load Test** - Tests Genie directly without caching
- **Cached Load Test** - Tests Genie with PostgreSQL/Lakebase semantic caching
- **In-Memory Semantic Cache Test** - Tests Genie with in-memory semantic caching (no database required)

You can run tests from either the **terminal** or from **Databricks notebooks**.

---

## Running from Terminal

### Standard Load Test (Terminal)

**With Web Interface (recommended for first time):**

```bash
uv run genie-loadtest
```

Then open your web browser and go to: http://localhost:8089

**Headless mode (no web interface):**

```bash
uv run genie-loadtest --headless -u 5 -r 1 -t 5m
```

**What the options mean:**
- `--headless` - Run without the web interface
- `-u 5` - Simulate 5 users
- `-r 1` - Add 1 new user per second
- `-t 5m` - Run the test for 5 minutes

### Cached Load Test (Terminal)

First, set up cache service credentials in your `.env` file (see Configuration section below).

```bash
uv run genie-loadtest-cached --headless -u 5 -r 1 -t 5m
```

Or with web interface:

```bash
uv run genie-loadtest-cached
```

### In-Memory Semantic Cache Load Test (Terminal)

Simpler setup than cached version - only requires warehouse ID (no database credentials).

```bash
uv run genie-loadtest-in-memory-semantic --headless -u 5 -r 1 -t 5m
```

Or with web interface:

```bash
uv run genie-loadtest-in-memory-semantic
```

**Customize cache settings:**

```bash
uv run genie-loadtest-in-memory-semantic --capacity 2000 --context-window-size 5 --headless -u 10 -t 10m
```

---

## Running from Databricks Notebooks

Load tests can be run directly from Databricks notebooks, which is useful for testing in the same environment where Genie is deployed.

### Setup

1. **Upload the project** to your Databricks workspace (e.g., `/Workspace/Users/you@company.com/genie-simulation/`)

2. **Create a `.env` file** in the project root with your configuration:

```bash
# Copy .env.example to .env and edit
cp .env.example .env
```

3. **Export conversations** using the `notebooks/export_conversations.py` notebook, or use the terminal command

### Standard Load Test (Notebook)

1. Open `notebooks/load_test.py` in Databricks
2. Configure the widgets at the top:
   - **conversations_file**: Path to conversations YAML (default: `../conversations.yaml`)
   - **user_count**: Number of concurrent users (default: 10)
   - **spawn_rate**: Users to spawn per second (default: 2)
   - **run_time**: Test duration (default: 5m)
3. Run all cells

The notebook will:
- Install dependencies automatically
- Load configuration from `.env`
- Run the load test via Locust subprocess
- Display results as DataFrames

### Cached Load Test (Notebook)

1. Open `notebooks/load_test_cached.py` in Databricks
2. Ensure your `.env` file has the cache service credentials:
   ```
   GENIE_LAKEBASE_CLIENT_ID=your-client-id
   GENIE_LAKEBASE_CLIENT_SECRET=your-client-secret
   GENIE_LAKEBASE_INSTANCE=your-instance
   GENIE_WAREHOUSE_ID=your-warehouse-id
   ```
3. Configure the widgets and run all cells

The cached test will show additional metrics:
- **GENIE_LRU_HIT**: Requests served from LRU cache (fastest)
- **GENIE_SEMANTIC_HIT**: Requests served from semantic cache
- **GENIE_LIVE**: Cache misses, fetched from Genie API

### In-Memory Semantic Cache Load Test (Notebook)

1. Open `notebooks/load_test_in_memory_semantic.py` in Databricks
2. Ensure your `.env` file has the warehouse ID:
   ```
   GENIE_WAREHOUSE_ID=your-warehouse-id
   ```
3. Optionally configure cache settings in `.env`:
   ```
   GENIE_CACHE_CAPACITY=1000
   GENIE_CONTEXT_WINDOW_SIZE=3
   GENIE_CONTEXT_SIMILARITY_THRESHOLD=0.80
   GENIE_EMBEDDING_MODEL=databricks-gte-large-en
   ```
4. Configure the widgets and run all cells

The in-memory semantic cache test shows the same metrics as the cached version but with simpler setup (no database required).

### Export Conversations (Notebook)

1. Open `notebooks/export_conversations.py` in Databricks
2. Set your Genie space ID (via widget or `.env`)
3. Run all cells

The notebook will export conversations to `../conversations.yaml`

---

## Configuration

Create a `.env` file in the project root with your settings:

```bash
# Required
GENIE_SPACE_ID=your-space-id

# Path to conversations file (relative to notebooks directory)
GENIE_CONVERSATIONS_FILE=../conversations.yaml

# Wait time between messages (seconds)
GENIE_MIN_WAIT=8
GENIE_MAX_WAIT=30

# Optional: Sampling configuration
GENIE_SAMPLE_SIZE=10
GENIE_SAMPLE_SEED=42

# Cache service configuration (for cached load test only - requires PostgreSQL/Lakebase)
GENIE_LAKEBASE_CLIENT_ID=your-client-id
GENIE_LAKEBASE_CLIENT_SECRET=your-client-secret
GENIE_LAKEBASE_INSTANCE=your-instance
GENIE_WAREHOUSE_ID=your-warehouse-id
GENIE_CACHE_TTL=86400
GENIE_SIMILARITY_THRESHOLD=0.85
GENIE_LRU_CAPACITY=100

# In-memory semantic cache configuration (simpler - no database required)
# GENIE_WAREHOUSE_ID=your-warehouse-id  # Already defined above
GENIE_CACHE_CAPACITY=1000
GENIE_CONTEXT_WINDOW_SIZE=3
GENIE_CONTEXT_SIMILARITY_THRESHOLD=0.80
GENIE_EMBEDDING_MODEL=databricks-gte-large-en
```

---

## Choosing the Right Load Test

| Feature | Standard | Cached (PostgreSQL) | In-Memory Semantic |
|---------|----------|---------------------|-------------------|
| **Database Required** | No | PostgreSQL/Lakebase | No |
| **Setup Complexity** | Low | High | Medium |
| **Cache Persistence** | N/A | Yes | No |
| **Multi-Instance** | N/A | Yes | No |
| **Memory Usage** | Low | Low | Medium |
| **Cache Type** | None | Semantic + LRU | Semantic + LRU |
| **Best For** | Baseline testing | Production load testing | Development/single-node testing |

### When to Use Each Test

**Standard Load Test:**
- Establishing baseline performance
- Testing Genie without caching
- Simplest setup for quick tests

**Cached Load Test (PostgreSQL/Lakebase):**
- Production performance testing
- Multi-instance deployments
- When cache persistence is needed
- Testing cache sharing across instances

**In-Memory Semantic Cache Test:**
- Development and testing environments
- Single-node deployments
- When database setup is not feasible
- Quick performance testing without infrastructure overhead
- Evaluating semantic caching benefits without database complexity

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

### Cached and In-Memory Semantic Load Test Metrics

Both cached load tests (PostgreSQL and in-memory) show additional request types:

| Request Type | What It Means |
|--------------|---------------|
| **GENIE_LRU_HIT** | Served from in-memory LRU cache (fastest, ~1-10ms) |
| **GENIE_SEMANTIC_HIT** | Served from semantic cache (fast, ~100-500ms) |
| **GENIE_LIVE** | Cache miss, fetched from Genie API (slowest, 5-30s) |

At the end of a cached test, you'll also see cache hit rate summary:
```
test_complete total_requests=100 lru_hits=45 lru_hit_rate=45.0% semantic_hits=30 semantic_hit_rate=30.0% misses=25 miss_rate=25.0%
```

**Performance Comparison:**
- Standard test: All requests are GENIE_LIVE (~5-30s each)
- Cached tests: Mix of LRU hits (~1-10ms), semantic hits (~100-500ms), and live calls (~5-30s)
- Expected cache hit rate: 50-80% for repeated queries (varies by conversation patterns)

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

### Terminal Commands

| Command | What It Does |
|---------|--------------|
| `uv run export-conversations --space-id ID --include-all -o conversations.yaml` | Download conversations |
| `uv run genie-loadtest` | Run standard load test with web UI |
| `uv run genie-loadtest --headless -u 5 -t 10m` | Run standard test for 10 min with 5 users |
| `uv run genie-loadtest-cached` | Run PostgreSQL cached load test with web UI |
| `uv run genie-loadtest-cached --headless -u 5 -t 10m` | Run PostgreSQL cached test for 10 min with 5 users |
| `uv run genie-loadtest-in-memory-semantic` | Run in-memory semantic cached test with web UI |
| `uv run genie-loadtest-in-memory-semantic --headless -u 5 -t 10m` | Run in-memory cached test for 10 min with 5 users |

### Databricks Notebooks

| Notebook | What It Does |
|----------|--------------|
| `notebooks/export_conversations.py` | Export conversations from Genie space |
| `notebooks/load_test.py` | Run standard load test (no caching) |
| `notebooks/load_test_cached.py` | Run load test with PostgreSQL semantic cache |
| `notebooks/load_test_in_memory_semantic.py` | Run load test with in-memory semantic cache |

---

## Getting Help

If you run into issues:

1. Check the error messages in the terminal output
2. Make sure your Databricks credentials are correct
3. Verify you have access to the Genie space
4. Contact your Databricks administrator for permission issues
