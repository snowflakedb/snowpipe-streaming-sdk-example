# Java Snowpipe Streaming SDK Example

This example demonstrates how to use the Snowflake Streaming Ingest SDK in Java to ingest data into Snowflake in real-time.

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
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

### 3. Configure Authentication

Create a `profile.json` file in the root of the `java-example` directory with your Snowflake credentials. You can use `profile.json.example` as a template:

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

### 4. Update Configuration

Edit `src/main/java/com/snowflake/example/StreamingIngestExample.java` and update the following values to match your Snowflake setup:

- `MY_DATABASE` - Your database name
- `MY_SCHEMA` - Your schema name  
- `MY_PIPE` - Your pipe name

## Build

```bash
mvn clean package
```

## Run

```bash
mvn exec:java
```

Or run directly:

```bash
mvn clean compile exec:java -Dexec.mainClass="com.snowflake.example.StreamingIngestExample"
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
Latest offset token: 100000
All data committed successfully
Data ingestion completed
```

## Troubleshooting

- **Connection Issues**: Verify your `profile.json` credentials and network connectivity to Snowflake
- **Permission Errors**: Ensure your user has the necessary privileges to write to the specified database, schema, and pipe
- **Table Not Found**: Verify the table exists and the pipe is configured correctly

## Additional Resources

- [Snowflake Streaming Ingest SDK Documentation](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming)
- [Snowflake Snowpipe Streaming SDK on Maven Central](https://repo1.maven.org/maven2/com/snowflake/snowpipe-streaming/)

