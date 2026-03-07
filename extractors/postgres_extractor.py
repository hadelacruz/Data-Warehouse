import psycopg2
import os
import sys
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Optional
from utils.logger import get_logger

# Set default encoding to handle Windows locale issues
if sys.platform == 'win32':
    os.environ['PGCLIENTENCODING'] = 'UTF8'


class PostgresExtractor:

    def __init__(self, config: Dict[str, any]):
        self.config = config
        self.connection = None
        self.logger = get_logger(__name__)

    def connect(self) -> bool:
 
        try:
            # Add client_encoding and options to handle encoding issues
            config = self.config.copy()

            # Try with explicit encoding options
            if 'options' not in config:
                config['options'] = '-c client_encoding=UTF8'

            self.connection = psycopg2.connect(**config)

            # Set encoding after connection
            self.connection.set_client_encoding('UTF8')

            self.logger.info(f"Connected to PostgreSQL database: {self.config['database']}")
            return True
        except Exception as e:
            # Try alternative connection method without special encoding
            try:
                self.logger.warning(f"First connection attempt failed: {e}. Trying alternative method...")
                # Remove encoding options and try again
                config = self.config.copy()
                self.connection = psycopg2.connect(**config)
                self.logger.info(f"Connected to PostgreSQL database: {self.config['database']}")
                return True
            except Exception as e2:
                self.logger.error(f"Error connecting to PostgreSQL: {e2}")
                return False

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.logger.info("Disconnected from PostgreSQL")

    def get_all_tables(self) -> List[str]:
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            self.logger.info(f"Found {len(tables)} tables in PostgreSQL")
            return tables
        except Exception as e:
            self.logger.error(f"Error getting tables: {e}")
            return []

    def extract_table(self, table_name: str) -> List[Dict]:
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(f"SELECT * FROM {table_name}")
            data = cursor.fetchall()
            cursor.close()

            # Convert RealDictRow to regular dict
            result = [dict(row) for row in data]

            self.logger.info(f"Extracted {len(result)} rows from table '{table_name}'")
            return result
        except Exception as e:
            self.logger.error(f"Error extracting data from table '{table_name}': {e}")
            return []

    def extract_all_data(self) -> Dict[str, List[Dict]]:
        all_data = {}
        tables = self.get_all_tables()

        for table in tables:
            data = self.extract_table(table)
            if data:
                all_data[table] = data

        self.logger.info(f"Extracted data from {len(all_data)} tables")
        return all_data

    def get_table_info(self, table_name: str) -> List[Dict]:
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position;
            """, (table_name,))
            columns = cursor.fetchall()
            cursor.close()
            return [dict(col) for col in columns]
        except Exception as e:
            self.logger.error(f"Error getting table info for '{table_name}': {e}")
            return []

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
