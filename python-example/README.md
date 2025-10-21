# Python Snowpipe Streaming SDK Example

This example demonstrates how to use the Snowflake Streaming Ingest SDK in Python to ingest data into Snowflake in real-time.

## Prerequisites

- Python 3.9 or higher
- pip (Python package manager)
- A Snowflake account with appropriate permissions
- A Snowflake table to ingest data into

## Setup

### 1. Create a Snowflake Table

Before running the example, create a table in your Snowflake account:

```sql
CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE (
    c1 NUMBER,
    c2 VARCHAR,
    ts TIMESTAMP_NTZ
);
```

### 2. Create a Snowpipe

Create a Snowpipe for streaming ingestion:

```sql
CREATE OR REPLACE PIPE MY_DATABASE.MY_SCHEMA.MY_PIPE 
AS COPY INTO MY_DATABASE.MY_SCHEMA.MY_TABLE 
FROM @%MY_TABLE;
```

### 3. Install Dependencies

Create and activate a virtual environment (recommended):

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

Install the required packages:

```bash
pip install -r requirements.txt
```

### 4. Configure Authentication

Create a `profile.json` file in the root of the `python-example` directory with your Snowflake credentials. You can use `profile.json.example` as a template:

```json
{
  "account": "<account>",
  "user": "your_username",
  "url": "https://<account>.<locator>.snowflakecomputing.com:443",
  "private_key": "your_private_key_path_or_content",
  "role": "your_role"
}
```

**Note:** For production use, consider using environment variables or a secure credential manager instead of storing credentials in a file.

### 5. Update Configuration

Edit `streaming_ingest_example.py` and update the following values to match your Snowflake setup:

- `MY_DATABASE` - Your database name
- `MY_SCHEMA` - Your schema name  
- `MY_PIPE` - Your pipe name

## Run

```bash
python streaming_ingest_example.py
```

## What the Example Does

1. **Creates a Streaming Ingest Client** - Initializes a connection to Snowflake using the credentials from `profile.json`
2. **Opens a Channel** - Creates a channel for streaming data into the specified pipe
3. **Ingests Data** - Sends 100,000 rows of sample data with three columns:
   - `c1`: Integer counter
   - `c2`: String representation of the counter
   - `ts`: Current timestamp
4. **Waits for Completion** - Polls the channel status to ensure all data has been committed
5. **Closes Resources** - Properly closes the channel and client

## Expected Output

```
Client created successfully
Channel opened: MY_CHANNEL_<uuid>
Ingesting 100000 rows...
Ingested 10000 rows...
Ingested 20000 rows...
...
All rows submitted. Waiting for ingestion to complete...
Latest offset token: 99999
All data committed successfully
Data ingestion completed
```

## Logging

You can adjust the logging level by setting the `SS_LOG_LEVEL` environment variable:

```bash
# For more detailed logs
export SS_LOG_LEVEL=info
python streaming_ingest_example.py

# For debug logs
export SS_LOG_LEVEL=debug
python streaming_ingest_example.py
```

The script sets this to `warn` by default to reduce output noise.

## Troubleshooting

- **Connection Issues**: Verify your `profile.json` credentials and network connectivity to Snowflake
- **Permission Errors**: Ensure your user has the necessary privileges to write to the specified database, schema, and pipe
- **Table Not Found**: Verify the table exists and the pipe is configured correctly
- **Import Errors**: Make sure you've installed all dependencies with `pip install -r requirements.txt`

## Additional Resources

- [Snowflake Streaming Ingest SDK Documentation](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview)
- [Snowflake Ingest Python SDK on PyPI](https://pypi.org/project/snowpipe-streaming/)

