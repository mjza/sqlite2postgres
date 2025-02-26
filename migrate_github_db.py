import os
import sqlite3
import psycopg2
import json
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime, UTC

# Load environment variables from .env
load_dotenv()

# Database connection details
DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
POSTGRES_DB = os.getenv("DB_NAME")  # PostgreSQL Database Name
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH")  # SQLite Database Path

# Batch size for processing
BATCH_SIZE = 5000  


# **Function to get PostgreSQL connection**
def get_postgres_connection():
    try:
        return psycopg2.connect(
            dbname=POSTGRES_DB,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
    except Exception as e:
        print(f"❌ Error connecting to PostgreSQL: {e}")
        return None


# **Function to get SQLite connection**
def get_sqlite_connection():
    try:
        return sqlite3.connect(SQLITE_DB_PATH)
    except Exception as e:
        print(f"❌ Error connecting to SQLite: {e}")
        return None


# **Function to convert BLOB fields to JSON STRING**
def convert_blob_to_json(blob_value):
    """ Convert SQLite BLOB to JSON STRING (not dict) for PostgreSQL JSONB compatibility. """
    if blob_value:
        try:
            json_data = json.loads(blob_value) if isinstance(blob_value, str) else json.loads(blob_value.decode("utf-8"))
            return json.dumps(json_data)  # Ensure it is a JSON string
        except Exception:
            return None
    return None


# **Function to convert Unix timestamp to TIMESTAMPTZ**
# **Function to convert Unix timestamp (milliseconds) to TIMESTAMPTZ**
def convert_timestamp(timestamp):
    """ Convert SQLite UNIX timestamp (milliseconds) to PostgreSQL TIMESTAMPTZ. """
    if timestamp and isinstance(timestamp, (int, float)) and timestamp > 0:
        try:
            return datetime.fromtimestamp(timestamp / 1000, UTC)  # Convert from ms to seconds
        except OSError as e:
            print(f"⚠️ Invalid timestamp: {timestamp} -> Error: {e}")  # Debugging log
            return None  # Return None for invalid timestamps
    return None  # Return None if invalid


# **Function to get the last processed ID from PostgreSQL**
def get_last_processed_id():
    """ Get the last processed ID to continue migration from that point. """
    conn = get_postgres_connection()
    if not conn:
        return 0

    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(id) FROM issues_2;")
        last_id = cur.fetchone()[0] or 0
        cur.close()
        conn.close()
        return last_id
    except Exception as e:
        print(f"❌ Error fetching last processed ID: {e}")
        conn.close()
        return 0


# **Function to migrate data in batches**
def migrate_issues():
    sqlite_conn = get_sqlite_connection()
    postgres_conn = get_postgres_connection()

    if not sqlite_conn or not postgres_conn:
        return

    sqlite_cursor = sqlite_conn.cursor()
    postgres_cursor = postgres_conn.cursor()

    # Check if the `issues` table exists in SQLite
    try:
        sqlite_cursor.execute("SELECT COUNT(*) FROM issues;")
        total_rows = sqlite_cursor.fetchone()[0]
        print(f"🔄 Found {total_rows} rows in SQLite `issues`. Starting migration...")
    except sqlite3.OperationalError:
        print("⚠️ Table `issues` does not exist in SQLite. Skipping...")
        sqlite_cursor.close()
        sqlite_conn.close()
        postgres_cursor.close()
        postgres_conn.close()
        return

    # Get last processed ID to resume migration
    last_processed_id = 0

    offset = 0
    while True:
        sqlite_cursor.execute("""
            SELECT id, url, repository_id, repository_url, node_id, "number", title, owner, owner_type, owner_id, 
                   labels, state, locked, comments, created_at, updated_at, closed_at, 
                   author_association, active_lock_reason, body, reactions, state_reason 
            FROM issues
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?;
        """, (last_processed_id, BATCH_SIZE))

        rows = sqlite_cursor.fetchall()
        if not rows:
            break  # No more data

        # Convert data types
        processed_rows = []
        for row in rows:
            processed_row = list(row)

            # Convert BLOB fields to JSON strings
            processed_row[10] = convert_blob_to_json(row[10])  # labels (JSONB)
            processed_row[20] = convert_blob_to_json(row[20])  # reactions (JSONB)

            # Convert boolean values
            processed_row[12] = bool(row[12]) if row[12] is not None else None  # locked (BOOLEAN)

            # Convert timestamps from UNIX epoch to TIMESTAMPTZ
            processed_row[14] = convert_timestamp(row[14])  # created_at
            processed_row[15] = convert_timestamp(row[15])  # updated_at
            processed_row[16] = convert_timestamp(row[16])  # closed_at

            processed_rows.append(tuple(processed_row))

        # Insert data into PostgreSQL
        insert_query = """
            INSERT INTO issues (
                id, url, repository_id, repository_url, node_id, "number", title, "owner", owner_type, owner_id, 
                labels, state, "locked", "comments", created_at, updated_at, closed_at, 
                author_association, active_lock_reason, body, reactions, state_reason
            ) VALUES %s 
            ON CONFLICT (id) DO NOTHING;
        """
        execute_values(postgres_cursor, insert_query, processed_rows)
        postgres_conn.commit()

        # Update last processed ID
        last_processed_id = processed_rows[-1][0]
        offset += BATCH_SIZE
        print(f"✅ Transferred up to ID {last_processed_id}. Total Processed: {offset}/{total_rows}")

    # Close connections
    sqlite_cursor.close()
    postgres_cursor.close()
    sqlite_conn.close()
    postgres_conn.close()
    print("🎉 Migration of `issues` table completed successfully!")


# **Run Migration**
if __name__ == "__main__":
    migrate_issues()
