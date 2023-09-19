import os

from dotenv import load_dotenv

load_dotenv()


class PostgresDBConfig:
    SCHEMA = os.environ.get("POSTGRES_SCHEMA", "public")
    TRANSFER_EVENT_TABLE = os.environ.get("POSTGRES_TRANSFER_EVENT_TABLE", "transfer_event")
    CONNECTION_URL = os.environ.get("POSTGRES_CONNECTION_URL", "postgresql://user:password@localhost:5432/database")
