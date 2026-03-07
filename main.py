import sys
from datetime import datetime

from config.database_config import DatabaseConfig
from extractors.postgres_extractor import PostgresExtractor
from extractors.mongo_extractor import MongoExtractor
from transformers.data_cleaner import DataCleaner
from integrators.warehouse_builder import DataWarehouseBuilder
from loaders.warehouse_loader import WarehouseLoader
from utils.logger import setup_logger


def main():

    # Setup logger
    log_file = f"etl_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = setup_logger("ETL_Pipeline", log_file=log_file)

    logger.info("=" * 80)
    logger.info("Starting ETL Pipeline - Lab05 Data Warehouse Integration")
    logger.info("=" * 80)

    try:
        # Step 0: Load and validate configuration
        logger.info("\n[STEP 0] Loading configuration...")
        config = DatabaseConfig()

        if not config.validate():
            logger.error("Configuration validation failed. Check your .env file.")
            return 1

        # Step 1: Extract data from PostgreSQL
        logger.info("\n[STEP 1] Extracting data from PostgreSQL (local)...")
        postgres_data = {}

        try:
            with PostgresExtractor(config.get_postgres_config()) as pg_extractor:
                if not pg_extractor.connection:
                    logger.warning("Failed to connect to PostgreSQL - continuing without PostgreSQL data")
                else:
                    postgres_data = pg_extractor.extract_all_data()

                    if not postgres_data:
                        logger.warning("No data extracted from PostgreSQL")
                    else:
                        logger.info(f"[OK] Extracted data from {len(postgres_data)} PostgreSQL tables")
        except Exception as e:
            logger.warning(f"PostgreSQL extraction failed: {e} - continuing without PostgreSQL data")

        # Step 2: Extract data from MongoDB
        logger.info("\n[STEP 2] Extracting data from MongoDB (Atlas)...")
        mongo_data = {}

        with MongoExtractor(config.get_mongo_uri(), config.get_mongo_database()) as mongo_extractor:
            if mongo_extractor.db is None:
                logger.error("Failed to connect to MongoDB")
                if not postgres_data:  # If both failed
                    logger.error("No data sources available. Exiting.")
                    return 1

            mongo_data = mongo_extractor.extract_all_data()

            if not mongo_data:
                logger.warning("No data extracted from MongoDB")
            else:
                logger.info(f"[OK] Extracted data from {len(mongo_data)} MongoDB collections")

        # Check if we have any data
        if not postgres_data and not mongo_data:
            logger.error("No data extracted from any source. Exiting.")
            return 1

        # Step 3: Clean and transform data
        logger.info("\n[STEP 3] Cleaning and transforming data...")
        cleaner = DataCleaner()

        postgres_cleaned = cleaner.clean_postgres_data(postgres_data)
        mongo_cleaned = cleaner.clean_mongo_data(mongo_data)

        logger.info(f"[OK] Cleaned {len(postgres_cleaned)} PostgreSQL tables")
        logger.info(f"[OK] Cleaned {len(mongo_cleaned)} MongoDB collections")

        # Step 4: Build unified data warehouse table
        logger.info("\n[STEP 4] Building unified data warehouse table...")
        builder = DataWarehouseBuilder()

        unified_table = builder.build_unified_table(postgres_cleaned, mongo_cleaned)

        if unified_table.empty:
            logger.error("Failed to build unified table. No data to load.")
            return 1

        logger.info(f"[OK] Built unified table: {len(unified_table)} rows, {len(unified_table.columns)} columns")
        logger.info(f"  Columns: {', '.join(unified_table.columns.tolist())}")

        # Step 5: Load unified table into Data Warehouse
        logger.info("\n[STEP 5] Loading unified table into Data Warehouse...")

        try:
            with WarehouseLoader(config.get_warehouse_config()) as warehouse:
                # Create database if not exists
                warehouse.create_database_if_not_exists()

                # Reconnect to the warehouse database
                if not warehouse.connect():
                    logger.warning("Failed to connect to Data Warehouse - saving to file instead")
                    raise Exception("Connection failed")

                # Create metadata table
                warehouse.create_metadata_table()

                # Load the unified table
                table_name = 'paises_turismo'
                success = warehouse.load_dataframe(
                    unified_table,
                    table_name,
                    schema='warehouse',
                    if_exists='replace'
                )

                if success:
                    logger.info(f"[OK] Loaded unified table 'warehouse.{table_name}'")
                    logger.info(f"     Rows: {len(unified_table)}")
                    logger.info(f"     Columns: {len(unified_table.columns)}")

                    # Log ETL run
                    warehouse.log_etl_run(
                        tables_loaded=1,
                        total_rows=len(unified_table),
                        status='SUCCESS',
                        notes=f"PostgreSQL: {len(postgres_cleaned)} tables, MongoDB: {len(mongo_cleaned)} collections"
                    )
                else:
                    logger.error(f"Failed to load unified table")
                    raise Exception("Load failed")

        except Exception as e:
            logger.warning(f"Data Warehouse loading failed: {e}")
            logger.info("Saving unified table to CSV file as fallback...")

            # Save unified table to CSV file as fallback
            output_file = f"unified_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            try:
                unified_table.to_csv(output_file, index=False, encoding='utf-8')
                logger.info(f"[OK] Unified table saved to: {output_file}")
                logger.info(f"     Rows: {len(unified_table)}")
                logger.info(f"     Columns: {len(unified_table.columns)}")

            except Exception as csv_error:
                logger.error(f"Failed to save CSV file: {csv_error}")

        # Final summary
        logger.info("\n" + "=" * 80)
        logger.info("ETL Pipeline completed successfully!")
        logger.info("=" * 80)
        logger.info(f"\nSummary:")
        logger.info(f"  PostgreSQL tables extracted: {len(postgres_data)}")
        logger.info(f"  MongoDB collections extracted: {len(mongo_data)}")
        logger.info(f"  Unified table: warehouse.paises_turismo")
        logger.info(f"  Total rows: {len(unified_table)}")
        logger.info(f"  Total columns: {len(unified_table.columns)}")
        logger.info(f"  Log file: {log_file}")

        return 0

    except KeyboardInterrupt:
        logger.warning("\nETL Pipeline interrupted by user")
        return 1

    except Exception as e:
        logger.error(f"\nETL Pipeline failed with error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
