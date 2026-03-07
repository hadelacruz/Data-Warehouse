import pandas as pd
from typing import Dict, List, Any
from datetime import datetime
from utils.logger import get_logger


class DataCleaner:
    """Cleans and transforms data from different sources."""

    def __init__(self):
        """Initialize the data cleaner."""
        self.logger = get_logger(__name__)

    def clean_postgres_data(self, data: Dict[str, List[Dict]]) -> Dict[str, pd.DataFrame]:
        """
        Clean data extracted from PostgreSQL.

        Args:
            data: Dictionary with table names as keys and list of rows as values

        Returns:
            Dictionary with table names as keys and cleaned DataFrames as values
        """
        cleaned_data = {}

        for table_name, rows in data.items():
            if not rows:
                self.logger.warning(f"No data to clean for table '{table_name}'")
                continue

            try:
                df = pd.DataFrame(rows)

                # Basic cleaning operations
                df = self._remove_duplicates(df, table_name)
                df = self._handle_missing_values(df, table_name)
                df = self._standardize_data_types(df, table_name)
                df = self._trim_strings(df, table_name)

                cleaned_data[table_name] = df
                self.logger.info(f"Cleaned PostgreSQL table '{table_name}': {len(df)} rows")

            except Exception as e:
                self.logger.error(f"Error cleaning table '{table_name}': {e}")

        return cleaned_data

    def clean_mongo_data(self, data: Dict[str, List[Dict]]) -> Dict[str, pd.DataFrame]:
        """
        Clean data extracted from MongoDB.

        Args:
            data: Dictionary with collection names as keys and list of documents as values

        Returns:
            Dictionary with collection names as keys and cleaned DataFrames as values
        """
        cleaned_data = {}

        for collection_name, documents in data.items():
            if not documents:
                self.logger.warning(f"No data to clean for collection '{collection_name}'")
                continue

            try:
                df = pd.DataFrame(documents)

                # Skip complex cleaning for collections with nested structures
                # These will be handled by the warehouse builder
                if 'costos_diarios_estimados_en_dólares' in df.columns:
                    self.logger.info(f"Skipping cleaning for nested collection '{collection_name}': {len(df)} rows (will be processed by warehouse builder)")
                    cleaned_data[collection_name] = df
                    continue

                # Basic cleaning operations for simple structures
                df = self._remove_duplicates(df, collection_name)
                df = self._handle_missing_values(df, collection_name)
                df = self._standardize_data_types(df, collection_name)
                df = self._trim_strings(df, collection_name)

                cleaned_data[collection_name] = df
                self.logger.info(f"Cleaned MongoDB collection '{collection_name}': {len(df)} rows")

            except Exception as e:
                self.logger.error(f"Error cleaning collection '{collection_name}': {e}")
                # On error, add the raw data anyway so it can be processed later
                try:
                    cleaned_data[collection_name] = pd.DataFrame(documents)
                except:
                    pass

        return cleaned_data

    def _remove_duplicates(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """Remove duplicate rows from DataFrame."""
        initial_count = len(df)
        df = df.drop_duplicates()
        duplicates_removed = initial_count - len(df)

        if duplicates_removed > 0:
            self.logger.info(f"Removed {duplicates_removed} duplicate rows from '{name}'")

        return df

    def _handle_missing_values(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """Handle missing values in DataFrame."""
        missing_counts = df.isnull().sum()
        total_missing = missing_counts.sum()

        if total_missing > 0:
            self.logger.info(f"Found {total_missing} missing values in '{name}'")

            # Fill numeric columns with median
            numeric_columns = df.select_dtypes(include=['number']).columns
            for col in numeric_columns:
                if df[col].isnull().any():
                    df[col] = df[col].fillna(df[col].median())

            # Fill string columns with empty string or 'Unknown'
            string_columns = df.select_dtypes(include=['object']).columns
            for col in string_columns:
                if df[col].isnull().any():
                    df[col] = df[col].fillna('Unknown')

        return df

    def _standardize_data_types(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """Standardize data types in DataFrame."""
        try:
            # Convert date columns
            date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
            for col in date_columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except Exception as e:
                    self.logger.warning(f"Could not convert column '{col}' to datetime in '{name}': {e}")

            # Ensure numeric columns are properly typed
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Try to convert to numeric if possible
                    try:
                        numeric_values = pd.to_numeric(df[col], errors='coerce')
                        if numeric_values.notna().sum() / len(df) > 0.5:  # If more than 50% can be converted
                            df[col] = numeric_values
                    except:
                        pass

        except Exception as e:
            self.logger.warning(f"Error standardizing data types for '{name}': {e}")

        return df

    def _trim_strings(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """Trim whitespace from string columns."""
        string_columns = df.select_dtypes(include=['object']).columns

        for col in string_columns:
            try:
                df[col] = df[col].astype(str).str.strip()
            except Exception as e:
                self.logger.warning(f"Error trimming column '{col}' in '{name}': {e}")

        return df

    def _flatten_nested_fields(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """Flatten nested dictionary/list fields in MongoDB documents."""
        try:
            # Find columns with nested structures
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Check if column contains dictionaries
                    sample = df[col].iloc[0] if len(df) > 0 else None
                    if isinstance(sample, dict):
                        # Flatten dictionary fields
                        nested_df = pd.json_normalize(df[col])
                        nested_df.columns = [f"{col}_{subcol}" for subcol in nested_df.columns]
                        df = pd.concat([df.drop(columns=[col]), nested_df], axis=1)
                    elif isinstance(sample, list):
                        # Convert lists to strings for now
                        df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)

        except Exception as e:
            self.logger.warning(f"Error flattening nested fields in '{name}': {e}")

        return df

    def validate_data_quality(self, df: pd.DataFrame, name: str) -> Dict[str, Any]:
        """
        Validate data quality and return metrics.

        Args:
            df: DataFrame to validate
            name: Name of the dataset

        Returns:
            Dictionary containing quality metrics
        """
        metrics = {
            'name': name,
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'missing_values': df.isnull().sum().sum(),
            'duplicate_rows': df.duplicated().sum(),
            'memory_usage': df.memory_usage(deep=True).sum() / 1024 / 1024,  # MB
            'columns': list(df.columns),
            'dtypes': df.dtypes.value_counts().to_dict()
        }

        self.logger.info(f"Data quality metrics for '{name}': {metrics['total_rows']} rows, "
                        f"{metrics['total_columns']} columns, {metrics['missing_values']} missing values")

        return metrics
