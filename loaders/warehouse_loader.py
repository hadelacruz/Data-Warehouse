import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
from typing import Dict, List, Optional
from sqlalchemy import create_engine, text
from utils.logger import get_logger


class WarehouseLoader:

    def __init__(self, config: Dict[str, any]):
        self.config = config
        self.connection = None
        self.engine = None
        self.logger = get_logger(__name__)

    def connect(self) -> bool:
        try:
            # Create psycopg2 connection for admin operations
            self.connection = psycopg2.connect(**self.config)
            self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            # Create SQLAlchemy engine for pandas operations
            connection_string = (
                f"postgresql://{self.config['user']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            )
            self.engine = create_engine(connection_string)

            self.logger.info(f"Connected to Data Warehouse: {self.config['database']}")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to Data Warehouse: {e}")
            return False

    def disconnect(self):
        if self.engine:
            self.engine.dispose()
        if self.connection:
            self.connection.close()
            self.logger.info("Disconnected from Data Warehouse")

    def create_database_if_not_exists(self) -> bool:
        try:
            # Connect to postgres database to create the warehouse database
            temp_config = self.config.copy()
            temp_config['database'] = 'postgres'

            temp_conn = psycopg2.connect(**temp_config)
            temp_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = temp_conn.cursor()

            # Check if database exists
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (self.config['database'],)
            )

            if not cursor.fetchone():
                cursor.execute(f"CREATE DATABASE {self.config['database']}")
                self.logger.info(f"Created database '{self.config['database']}'")
            else:
                self.logger.info(f"Database '{self.config['database']}' already exists")

            cursor.close()
            temp_conn.close()
            return True

        except Exception as e:
            self.logger.error(f"Error creating database: {e}")
            return False

    def create_schema(self, schema_name: str = 'warehouse') -> bool:
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            self.logger.info(f"Created schema '{schema_name}'")
            cursor.close()
            return True
        except Exception as e:
            self.logger.error(f"Error creating schema '{schema_name}': {e}")
            return False

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = 'warehouse',
        if_exists: str = 'replace'
    ) -> bool:
        try:
            # Ensure schema exists before loading data
            self.create_schema(schema)

            # Clean table name (remove special characters)
            clean_table_name = self._clean_table_name(table_name)

            # Use pandas to_sql with SQLAlchemy engine for proper PostgreSQL support
            df.to_sql(
                clean_table_name,
                self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )

            self.logger.info(
                f"Loaded {len(df)} rows into table '{schema}.{clean_table_name}'"
            )
            return True

        except Exception as e:
            self.logger.error(f"Error loading data into '{table_name}': {e}")
            return False

    def load_all_data(
        self,
        integrated_data: Dict[str, pd.DataFrame],
        schema: str = 'warehouse',
        if_exists: str = 'replace'
    ) -> Dict[str, bool]:
        results = {}

        # Create schema if it doesn't exist
        self.create_schema(schema)

        for table_name, df in integrated_data.items():
            success = self.load_dataframe(df, table_name, schema, if_exists)
            results[table_name] = success

        successful_loads = sum(results.values())
        self.logger.info(
            f"Loaded {successful_loads}/{len(results)} tables into warehouse"
        )

        return results

    def _clean_table_name(self, table_name: str) -> str:
        # Replace invalid characters with underscore
        clean_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in table_name)

        # Ensure it starts with a letter or underscore
        if clean_name and clean_name[0].isdigit():
            clean_name = '_' + clean_name

        # Convert to lowercase
        clean_name = clean_name.lower()

        return clean_name

    def get_table_count(self, schema: str = 'warehouse') -> int:

        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = %s
            """, (schema,))
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            self.logger.error(f"Error getting table count: {e}")
            return 0

    def get_table_info(self, table_name: str, schema: str = 'warehouse') -> Optional[Dict]:

        try:
            cursor = self.connection.cursor()

            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
            row_count = cursor.fetchone()[0]

            # Get column info
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table_name))

            columns = [{'name': row[0], 'type': row[1]} for row in cursor.fetchall()]

            cursor.close()

            return {
                'table_name': table_name,
                'schema': schema,
                'row_count': row_count,
                'column_count': len(columns),
                'columns': columns
            }

        except Exception as e:
            self.logger.error(f"Error getting table info for '{table_name}': {e}")
            return None

    def create_metadata_table(self, schema: str = 'warehouse') -> bool:

        try:
            # Ensure schema exists first
            self.create_schema(schema)

            cursor = self.connection.cursor()
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.etl_metadata (
                    id SERIAL PRIMARY KEY,
                    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    tables_loaded INTEGER,
                    total_rows INTEGER,
                    status VARCHAR(50),
                    notes TEXT
                )
            """)
            self.logger.info(f"Created metadata table in schema '{schema}'")
            cursor.close()
            return True
        except Exception as e:
            self.logger.error(f"Error creating metadata table: {e}")
            return False

    def log_etl_run(
        self,
        tables_loaded: int,
        total_rows: int,
        status: str,
        notes: str = '',
        schema: str = 'warehouse'
    ) -> bool:

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"""
                INSERT INTO {schema}.etl_metadata
                (tables_loaded, total_rows, status, notes)
                VALUES (%s, %s, %s, %s)
            """, (tables_loaded, total_rows, status, notes))
            self.logger.info(f"Logged ETL run: {status}")
            cursor.close()
            return True
        except Exception as e:
            self.logger.error(f"Error logging ETL run: {e}")
            return False

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
