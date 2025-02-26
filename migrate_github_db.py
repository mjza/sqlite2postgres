import os
import sqlite3
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Database connection details
DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")  # PostgreSQL Database Name

# SQLite database path
SQLITE_DB_PATH = os.getenv("GITHUB_DB_PATH")

# Batch size
BATCH_SIZE = 5000  # Adjust based on performance


# **Function to get PostgreSQL connection**
def get_postgres_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"❌ Error connecting to PostgreSQL: {e}")
        return None


# **Function to get SQLite connection**
def get_sqlite_connection():
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        return conn
    except Exception as e:
        print(f"❌ Error connecting to SQLite: {e}")
        return None


# **Function to convert JSON text to valid PostgreSQL JSONB**
def convert_json_fields(data, json_fields):
    """Convert JSON string fields to proper PostgreSQL JSONB."""
    return [
        tuple(
            json.loads(value) if col in json_fields and value else value
            for col, value in zip(json_fields, row)
        ) for row in data
    ]


# **Function to transfer data**
def transfer_data(table_name, columns, json_fields=None):
    """ Transfers data from SQLite to PostgreSQL in batches """
    sqlite_conn = get_sqlite_connection()
    postgres_conn = get_postgres_connection()

    if not sqlite_conn or not postgres_conn:
        return

    sqlite_cursor = sqlite_conn.cursor()
    postgres_cursor = postgres_conn.cursor()

    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))  # %s for PostgreSQL
    
    try:
        sqlite_cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        total_rows = sqlite_cursor.fetchone()[0]
        print(f"🔄 Transferring {total_rows} rows from {table_name}...")
    except sqlite3.OperationalError:
        print(f"⚠️ Table {table_name} does not exist in SQLite. Skipping...")
        sqlite_cursor.close()
        sqlite_conn.close()
        postgres_cursor.close()
        postgres_conn.close()
        return

    offset = 0
    while True:
        sqlite_cursor.execute(f"SELECT {columns_str} FROM {table_name} ORDER BY id LIMIT ? OFFSET ?;", (BATCH_SIZE, offset))
        rows = sqlite_cursor.fetchall()

        if not rows:
            break  # No more data

        # Convert JSON fields
        if json_fields:
            rows = convert_json_fields(rows, json_fields)

        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES %s ON CONFLICT (id) DO NOTHING;"
        execute_values(postgres_cursor, insert_query, rows)
        postgres_conn.commit()

        offset += BATCH_SIZE
        print(f"✅ Transferred {offset}/{total_rows} rows from {table_name}...")

    sqlite_cursor.close()
    postgres_cursor.close()
    sqlite_conn.close()
    postgres_conn.close()
    print(f"🎉 Completed transfer for {table_name}!")


# **Tables and Columns Mapping**
tables = {
    "comments": {
        "columns": ["id", "node_id", "url", "issue_id", "issue_url", "user", "created_at", "updated_at",
                    "author_association", "body", "reactions"],
        "json_fields": ["reactions"]
    },
    "issues": {
        "columns": ["id", "url", "repository_id", "repository_url", "node_id", "number", "title", "owner",
                    "owner_type", "owner_id", "labels", "state", "locked", "comments", "created_at", "updated_at",
                    "closed_at", "author_association", "active_lock_reason", "body", "reactions", "state_reason"],
        "json_fields": ["labels", "reactions"]
    },
    "logs": {
        "columns": ["id", "last_org_id", "last_user_id", "last_org_repository_id", "last_user_repository_id",
                    "created_at"],
        "json_fields": []
    },
    "organizations": {
        "columns": ["id", "login", "node_id", "description"],
        "json_fields": []
    },
    "repositories": {
        "columns": ["id", "node_id", "name", "full_name", "private", "owner", "owner_type", "owner_id",
                    "html_url", "description", "fork", "url", "created_at", "updated_at", "pushed_at", "homepage",
                    "size", "stargazers_count", "watchers_count", "language", "has_issues", "has_projects",
                    "has_downloads", "has_wiki", "has_pages", "has_discussions", "forks_count", "mirror_url",
                    "archived", "disabled", "open_issues_count", "license", "allow_forking", "is_template",
                    "web_commit_signoff_required", "topics", "visibility", "forks", "open_issues", "watchers",
                    "default_branch", "permissions"],
        "json_fields": ["license", "topics", "permissions"]
    }
}

# **Run Migration**
if __name__ == "__main__":
    for table, config in tables.items():
        transfer_data(table, config["columns"], config["json_fields"])
