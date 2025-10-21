"""
Example demonstrating how to use the Snowflake Streaming Ingest SDK in Python.

This example shows how to:
1. Create a Snowflake Streaming Ingest Client
2. Open a channel for data ingestion
3. Ingest rows of data
4. Wait for ingestion to complete
5. Close resources properly
"""

from datetime import datetime
import time
import uuid
import os

# Change Environment Variable SS_LOG_LEVEL="info" to increase logging details
os.environ["SS_LOG_LEVEL"] = "warn"

from snowflake.ingest.streaming import StreamingIngestClient


MAX_ROWS = 100_000
POLL_ATTEMPTS = 30
POLL_INTERVAL_MS = 1000


def main():
    """Main function to demonstrate streaming data ingestion."""
    
    # Create Snowflake Streaming Ingest Client using context manager
    with StreamingIngestClient(
        client_name=f"MY_CLIENT_{uuid.uuid4()}",
        db_name="MY_DATABASE",
        schema_name="MY_SCHEMA",
        pipe_name="MY_PIPE",
        profile_json="profile.json"
    ) as client:
        
        print("Client created successfully")
        
        # Open a channel for data ingestion using context manager
        with client.open_channel(f"MY_CHANNEL_{uuid.uuid4()}")[0] as channel:
            print(f"Channel opened: {channel.channel_name}")
            
            # Ingest rows
            print(f"Ingesting {MAX_ROWS} rows...")
            for i in range(MAX_ROWS):
                row_id = str(i)
                channel.append_row(
                    {
                        "c1": i,
                        "c2": row_id,
                        "ts": datetime.now()
                    },
                    row_id
                )
                
                # Print progress every 10,000 rows
                if (i + 1) % 10_000 == 0:
                    print(f"Ingested {i + 1} rows...")
            
            print("All rows submitted. Waiting for ingestion to complete...")
            
            # Wait for ingestion to complete
            for attempt in range(POLL_ATTEMPTS):
                latest_offset = channel.get_latest_committed_offset_token()
                print(f"Latest offset token: {latest_offset}")
                
                if latest_offset == str(MAX_ROWS - 1):
                    print("All data committed successfully")
                    break
                
                time.sleep(POLL_INTERVAL_MS / 1000)
            else:
                raise Exception("Ingestion failed after all attempts")
        
        # Channel automatically closed here
        print("Data ingestion completed")
    
    # Client automatically closed here


if __name__ == "__main__":
    main()

