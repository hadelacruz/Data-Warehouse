
import pandas as pd
import unicodedata
from typing import Dict, List
from utils.logger import get_logger


class DataWarehouseBuilder:

    def __init__(self):
        self.logger = get_logger(__name__)

    def _normalize_country_name(self, name):
        if pd.isna(name) or name is None:
            return None

        # Convert to string
        name = str(name)

        # Strip whitespace
        name = name.strip()

        # Remove extra spaces
        name = ' '.join(name.split())

        # Normalize to title case for consistency
        name = name.title()

        return name

    def _normalize_dataframe_countries(self, df: pd.DataFrame, country_col: str) -> pd.DataFrame:
        if country_col in df.columns:
            df[country_col] = df[country_col].apply(self._normalize_country_name)
            self.logger.info(f"Normalized {len(df)} country names in column '{country_col}'")

        return df

    def build_unified_table(
        self,
        postgres_data: Dict[str, pd.DataFrame],
        mongo_data: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        self.logger.info("Building unified data warehouse table...")

        # Step 1: Flatten MongoDB nested structures
        mongo_flattened = {}
        for collection_name, df in mongo_data.items():
            flat_df = self._flatten_mongo_collection(df, collection_name)
            if flat_df is not None and not flat_df.empty:
                # Normalize country names in MongoDB data
                if 'país' in flat_df.columns:
                    flat_df = self._normalize_dataframe_countries(flat_df, 'país')
                elif 'pais' in flat_df.columns:
                    flat_df = self._normalize_dataframe_countries(flat_df, 'pais')

                mongo_flattened[collection_name] = flat_df
                self.logger.info(f"Flattened MongoDB collection '{collection_name}': {len(flat_df)} rows")

        # Step 2: Normalize PostgreSQL country names
        postgres_normalized = {}
        for table_name, df in postgres_data.items():
            df_copy = df.copy()
            if 'nombre_pais' in df_copy.columns:
                df_copy = self._normalize_dataframe_countries(df_copy, 'nombre_pais')
            elif 'pais' in df_copy.columns:
                df_copy = self._normalize_dataframe_countries(df_copy, 'pais')
            elif 'país' in df_copy.columns:
                df_copy = self._normalize_dataframe_countries(df_copy, 'país')

            postgres_normalized[table_name] = df_copy

        # Step 3: Merge all MongoDB collections into one
        mongo_unified = self._merge_mongo_collections(mongo_flattened)

        # Step 4: Merge PostgreSQL tables
        postgres_unified = self._merge_postgres_tables(postgres_normalized)

        # Step 5: Create final unified table
        unified_table = self._create_final_table(postgres_unified, mongo_unified)

        self.logger.info(f"Unified table created: {len(unified_table)} rows, {len(unified_table.columns)} columns")

        return unified_table

    def _flatten_mongo_collection(self, df: pd.DataFrame, collection_name: str) -> pd.DataFrame:
        try:
            df_flat = df.copy()

            # Check if there's a nested 'costos_diarios_estimados_en_dólares' field
            if 'costos_diarios_estimados_en_dólares' in df_flat.columns:
                # Extract nested cost data
                cost_data = df_flat['costos_diarios_estimados_en_dólares']

                for idx, costs in cost_data.items():
                    if isinstance(costs, dict):
                        # Extract hospedaje (accommodation)
                        if 'hospedaje' in costs and isinstance(costs['hospedaje'], dict):
                            df_flat.at[idx, 'hospedaje_bajo_usd'] = costs['hospedaje'].get('precio_bajo_usd', None)
                            df_flat.at[idx, 'hospedaje_promedio_usd'] = costs['hospedaje'].get('precio_promedio_usd', None)
                            df_flat.at[idx, 'hospedaje_alto_usd'] = costs['hospedaje'].get('precio_alto_usd', None)

                        # Extract comida (food)
                        if 'comida' in costs and isinstance(costs['comida'], dict):
                            df_flat.at[idx, 'comida_bajo_usd'] = costs['comida'].get('precio_bajo_usd', None)
                            df_flat.at[idx, 'comida_promedio_usd'] = costs['comida'].get('precio_promedio_usd', None)
                            df_flat.at[idx, 'comida_alto_usd'] = costs['comida'].get('precio_alto_usd', None)

                        # Extract transporte (transportation)
                        if 'transporte' in costs and isinstance(costs['transporte'], dict):
                            df_flat.at[idx, 'transporte_bajo_usd'] = costs['transporte'].get('precio_bajo_usd', None)
                            df_flat.at[idx, 'transporte_promedio_usd'] = costs['transporte'].get('precio_promedio_usd', None)
                            df_flat.at[idx, 'transporte_alto_usd'] = costs['transporte'].get('precio_alto_usd', None)

                        # Extract entretenimiento (entertainment)
                        if 'entretenimiento' in costs and isinstance(costs['entretenimiento'], dict):
                            df_flat.at[idx, 'entretenimiento_bajo_usd'] = costs['entretenimiento'].get('precio_bajo_usd', None)
                            df_flat.at[idx, 'entretenimiento_promedio_usd'] = costs['entretenimiento'].get('precio_promedio_usd', None)
                            df_flat.at[idx, 'entretenimiento_alto_usd'] = costs['entretenimiento'].get('precio_alto_usd', None)

                # Drop the nested column
                df_flat = df_flat.drop(columns=['costos_diarios_estimados_en_dólares'])

            # Standardize column names - lowercase and normalize accents
            column_mapping = {}
            for col in df_flat.columns:
                new_col = col.lower().replace(' ', '_')
                # Normalize common Spanish accents
                new_col = new_col.replace('ó', 'o').replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ú', 'u')
                new_col = new_col.replace('ñ', 'n')
                column_mapping[col] = new_col

            df_flat = df_flat.rename(columns=column_mapping)

            return df_flat

        except Exception as e:
            self.logger.error(f"Error flattening collection '{collection_name}': {e}")
            return None

    def _merge_mongo_collections(self, mongo_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        if not mongo_data:
            return pd.DataFrame()

        # Separate different types of collections
        costos_collections = []
        big_mac_df = None

        for name, df in mongo_data.items():
            if 'costos_turisticos' in name:
                costos_collections.append(df)
            elif 'big_mac' in name:
                big_mac_df = df

        # Merge all costos collections
        if costos_collections:
            costos_merged = pd.concat(costos_collections, ignore_index=True)
            self.logger.info(f"Merged {len(costos_collections)} cost collections: {len(costos_merged)} rows")
        else:
            costos_merged = pd.DataFrame()

        # Merge with Big Mac data
        if big_mac_df is not None and not big_mac_df.empty:
            if not costos_merged.empty:
                # Merge on 'país' field
                unified = pd.merge(
                    costos_merged,
                    big_mac_df,
                    on='país' if 'país' in costos_merged.columns else 'pais',
                    how='outer',
                    suffixes=('', '_bigmac')
                )
                self.logger.info(f"Merged with Big Mac data: {len(unified)} rows")
            else:
                unified = big_mac_df
        else:
            unified = costos_merged

        return unified

    def _merge_postgres_tables(self, postgres_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        if not postgres_data:
            return pd.DataFrame()

        # Get tables
        envejecimiento_df = None
        poblacion_df = None

        for name, df in postgres_data.items():
            if 'envejecimiento' in name.lower():
                envejecimiento_df = df
            elif 'poblacion' in name.lower():
                poblacion_df = df

        # Merge tables
        if envejecimiento_df is not None and poblacion_df is not None:
            # Try different join keys
            join_keys = ['nombre_pais', 'pais', 'país']
            join_key = None

            for key in join_keys:
                if key in envejecimiento_df.columns and key in poblacion_df.columns:
                    join_key = key
                    break

            if join_key:
                unified = pd.merge(
                    envejecimiento_df,
                    poblacion_df,
                    on=join_key,
                    how='outer',
                    suffixes=('', '_pob')
                )
                self.logger.info(f"Merged PostgreSQL tables on '{join_key}': {len(unified)} rows")
                return unified

        # If can't merge, concatenate or return available data
        if envejecimiento_df is not None:
            return envejecimiento_df
        elif poblacion_df is not None:
            return poblacion_df

        return pd.DataFrame()

    def _create_final_table(
        self,
        postgres_df: pd.DataFrame,
        mongo_df: pd.DataFrame
    ) -> pd.DataFrame:
        # Find the join key
        postgres_keys = ['nombre_pais', 'pais', 'país']
        mongo_keys = ['país', 'pais', 'nombre_pais']

        postgres_key = None
        mongo_key = None

        for key in postgres_keys:
            if key in postgres_df.columns:
                postgres_key = key
                break

        for key in mongo_keys:
            if key in mongo_df.columns:
                mongo_key = key
                break

        # Standardize the key name
        if postgres_key and postgres_df is not None and not postgres_df.empty:
            postgres_df = postgres_df.rename(columns={postgres_key: 'pais'})
            # Normalize again after renaming
            postgres_df = self._normalize_dataframe_countries(postgres_df, 'pais')

        if mongo_key and mongo_df is not None and not mongo_df.empty:
            mongo_df = mongo_df.rename(columns={mongo_key: 'pais'})
            # Normalize again after renaming
            mongo_df = self._normalize_dataframe_countries(mongo_df, 'pais')

        # Merge PostgreSQL and MongoDB data
        if not postgres_df.empty and not mongo_df.empty:
            unified = pd.merge(
                postgres_df,
                mongo_df,
                on='pais',
                how='outer',
                suffixes=('_sql', '_nosql')
            )
        elif not postgres_df.empty:
            unified = postgres_df
        elif not mongo_df.empty:
            unified = mongo_df
        else:
            self.logger.warning("No data available to create unified table")
            return pd.DataFrame()

        # Clean up column names and select final columns
        unified = self._finalize_columns(unified)

        # Deduplicate rows - consolidate all data for each country into a single row
        unified = self._deduplicate_countries(unified)

        return unified

    def _finalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:

        # Resolve duplicate columns (keep non-null values)
        for col in df.columns:
            if col.endswith('_sql') or col.endswith('_nosql'):
                base_col = col.rsplit('_', 1)[0]
                sql_col = f"{base_col}_sql"
                nosql_col = f"{base_col}_nosql"

                if sql_col in df.columns and nosql_col in df.columns:
                    # Prefer non-null, non-empty values from either source
                    # NoSQL (MongoDB) values take precedence as they often have more complete data
                    df[base_col] = df[nosql_col].fillna(df[sql_col])

                    # But also handle cases where NoSQL has NaN but SQL has value
                    mask_nosql_null = df[nosql_col].isna()
                    mask_sql_notnull = df[sql_col].notna()
                    df.loc[mask_nosql_null & mask_sql_notnull, base_col] =  df.loc[mask_nosql_null & mask_sql_notnull, sql_col]

                    df = df.drop(columns=[sql_col, nosql_col])

        # Merge duplicate columns with different encodings (región/region, población/poblacion)
        if 'región' in df.columns and 'region' in df.columns:
            df['region'] = df['region'].combine_first(df['región'])
            df = df.drop(columns=['región'])
        elif 'región' in df.columns:
            df = df.rename(columns={'región': 'region'})

        if 'población' in df.columns and 'poblacion' in df.columns:
            df['poblacion'] = df['poblacion'].combine_first(df['población'])
            df = df.drop(columns=['población'])
        elif 'población' in df.columns:
            df = df.rename(columns={'población': 'poblacion'})

        # Remove duplicate columns with suffixes from merge operations
        cols_to_drop = [col for col in df.columns if col.endswith('_bigmac')]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
            self.logger.info(f"Removed {len(cols_to_drop)} duplicate columns with suffixes")

        # Remove ID columns that are not needed in the final warehouse
        id_cols_to_remove = ['_id', 'id_pais', 'id']
        existing_id_cols = [col for col in id_cols_to_remove if col in df.columns]
        if existing_id_cols:
            df = df.drop(columns=existing_id_cols)
            self.logger.info(f"Removed ID columns: {', '.join(existing_id_cols)}")

        # Replace "Unknown" values with None/NULL
        df = df.replace('Unknown', None)
        df = df.replace('', None)

        # Clean text columns
        text_cols = ['continente', 'region', 'capital']
        for col in text_cols:
            if col in df.columns:
                # Remove extra spaces and standardize
                df[col] = df[col].astype(str).str.strip()
                # Convert 'nan' string back to None
                df[col] = df[col].replace('nan', None)

        # Define desired column order (if they exist)
        preferred_order = [
            'pais',
            'continente',
            'region',
            'capital',
            'poblacion',
            'tasa_de_envejecimiento',
            'precio_big_mac_usd',
            'hospedaje_bajo_usd',
            'hospedaje_promedio_usd',
            'hospedaje_alto_usd',
            'comida_bajo_usd',
            'comida_promedio_usd',
            'comida_alto_usd',
            'transporte_bajo_usd',
            'transporte_promedio_usd',
            'transporte_alto_usd',
            'entretenimiento_bajo_usd',
            'entretenimiento_promedio_usd',
            'entretenimiento_alto_usd'
        ]

        # Reorder columns
        existing_preferred = [col for col in preferred_order if col in df.columns]
        other_cols = [col for col in df.columns if col not in preferred_order]
        final_order = existing_preferred + other_cols

        df = df[final_order]

        # Convert numeric columns
        numeric_cols = [col for col in df.columns if 'usd' in col.lower() or 'poblacion' in col.lower() or 'tasa' in col.lower()]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Log final schema
        self.logger.info(f"Final table schema: {list(df.columns)}")

        return df

    def _deduplicate_countries(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or 'pais' not in df.columns:
            return df

        # Count duplicates before deduplication
        duplicates_before = len(df) - df['pais'].nunique()

        if duplicates_before == 0:
            self.logger.info("No duplicate countries found")
            return df

        self.logger.info(f"Found {duplicates_before} duplicate rows across {df['pais'].nunique()} unique countries")

        # Group by country and aggregate
        # For each column, take the first non-null value
        def first_valid(series):
            """Return the first non-null value in the series."""
            valid_values = series.dropna()
            if len(valid_values) > 0:
                return valid_values.iloc[0]
            return None

        # Aggregate all columns except 'pais'
        agg_dict = {col: first_valid for col in df.columns if col != 'pais'}

        deduplicated = df.groupby('pais', as_index=False).agg(agg_dict)

        self.logger.info(f"Deduplicated from {len(df)} to {len(deduplicated)} rows")

        return deduplicated
