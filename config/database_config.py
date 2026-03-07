import os
from typing import Dict, Optional
from dotenv import load_dotenv

load_dotenv()


class DatabaseConfig:

    def __init__(self):
        # PostgreSQL Configuration (Local)
        self.postgres_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DB', 'lab05_SQL'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', '')
        }

        # MongoDB Configuration (Atlas)
        self.mongo_uri = os.getenv('MONGO_URI', '')
        self.mongo_database = os.getenv('MONGO_DB', 'lab05')

        # Data Warehouse Configuration (PostgreSQL)
        self.warehouse_config = {
            'host': os.getenv('WAREHOUSE_HOST', 'localhost'),
            'port': int(os.getenv('WAREHOUSE_PORT', 5432)),
            'database': os.getenv('WAREHOUSE_DB', 'lab05_warehouse'),
            'user': os.getenv('WAREHOUSE_USER', 'postgres'),
            'password': os.getenv('WAREHOUSE_PASSWORD', '')
        }

    def get_postgres_config(self) -> Dict[str, any]:
        return self.postgres_config

    def get_mongo_uri(self) -> str:
        return self.mongo_uri

    def get_mongo_database(self) -> str:
        return self.mongo_database

    def get_warehouse_config(self) -> Dict[str, any]:
        return self.warehouse_config

    def validate(self) -> bool:
        if not self.postgres_config['password']:
            print("Warning: PostgreSQL password not set")

        if not self.mongo_uri:
            print("Warning: MongoDB URI not set")
            return False

        if not self.warehouse_config['password']:
            print("Warning: Data Warehouse password not set")

        return True
