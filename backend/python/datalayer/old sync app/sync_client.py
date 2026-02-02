"""
Sync Client Module

Main synchronization client that orchestrates database sync operations
using connection pooling, smart delta detection, and parallel processing.
"""

import logging
import logging.handlers
import sys
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import concurrent.futures
import tqdm

from sqlalchemy import text

# Import our custom modules
from sync_db_connection import PooledConnectionManager
from data_handler import DataProcessor
from sync_manager import SyncStrategyManager, SyncStrategy, TableSyncConfig


class PooledSQLSyncClient:
    """Main SQL synchronization client with pooled connections and smart sync strategies"""

    def __init__(self):
        """Initialize the sync client with all required components"""
        # Initialize components
        self.connection_manager = PooledConnectionManager()
        self.data_processor = DataProcessor()
        self.sync_manager = SyncStrategyManager()

        # Setup logging
        self.logger = self._setup_logging()

        # Configuration
        self.max_workers = int(os.getenv('SYNC_WORKERS', '4'))
        self.use_fast_sync = os.getenv('USE_FAST_SYNC', 'true').lower() == 'true'

        self.logger.debug("PooledSQLSyncClient initialized successfully")

    def _setup_logging(self):
        """Set up clean logging with minimal console output"""
        logger = logging.getLogger('PooledSQLSyncClient')

        # Set log level from environment or default to INFO
        log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
        logger.setLevel(getattr(logging, log_level, logging.INFO))

        # Remove any existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Create console handler - can be completely disabled
        console_level = os.getenv('CONSOLE_LOG_LEVEL', 'WARNING').upper()

        if console_level not in ['NONE', 'OFF', 'DISABLED']:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(getattr(logging, console_level, logging.WARNING))

            console_formatter = logging.Formatter('%(levelname)s: %(message)s')
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)

        # Create detailed file handler for debugging
        file_handler = logging.handlers.RotatingFileHandler(
            'pooled_sync.log', maxBytes=5 * 1024 * 1024, backupCount=5, encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)

        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        return logger

    def test_connections(self) -> bool:
        """
        Test database connections

        Returns:
            bool: True if both connections successful
        """
        return self.connection_manager.test_connections()

    def get_sync_config(self) -> List[TableSyncConfig]:
        """Get table sync configuration"""
        return self.sync_manager.get_sync_config(self.connection_manager.get_azure_engine())

    def sync_table_upsert_bulk_fast(self, config: TableSyncConfig,
                                    force_full_sync: bool = False) -> Dict[str, Any]:
        """
        Ultra-fast UPSERT sync with parallel processing and smart delta detection

        Args:
            config: Table sync configuration
            force_full_sync: Whether to force a full sync (bypass delta)

        Returns:
            Dict[str, Any]: Sync result with status and metrics
        """
        table_name = config.table_name
        primary_keys = [pk.strip() for pk in config.primary_key_columns.split(',') if pk.strip()]

        if not primary_keys:
            self.logger.error(f"No primary keys defined for UPSERT on {table_name}")
            return {"status": "error", "message": "No primary keys for UPSERT"}

        self.logger.debug(f"Starting fast bulk UPSERT sync for {table_name} (keys: {', '.join(primary_keys)})")

        # Get engines
        onprem_engine = self.connection_manager.get_onprem_engine()
        azure_engine = self.connection_manager.get_azure_engine()

        # Get syncable columns
        columns = self.data_processor.get_syncable_columns(onprem_engine, table_name)
        if not columns:
            return {"status": "error", "message": "No syncable columns"}

        # Get column data types from Azure
        column_types = self.data_processor.get_column_types(azure_engine, table_name)

        # Determine sync strategy
        strategy = self.sync_manager.determine_sync_strategy(
            onprem_engine, azure_engine, table_name, columns, force_full_sync
        )

        # Display strategy decision
        if force_full_sync:
            print(f"🔄 {table_name}{strategy.site_info}: FULL CORRECTIVE sync - {strategy.total_rows:,} rows")
        elif strategy.use_delta_sync and strategy.last_sync:
            print(
                f"🔄 {table_name}{strategy.site_info}: Delta sync - {strategy.delta_rows:,} changed rows (skipping {strategy.total_rows - strategy.delta_rows:,} unchanged)")
        else:
            print(f"🔄 {table_name}{strategy.site_info}: Full sync - {strategy.total_rows:,} rows")

        # Early exit for no changes
        if strategy.sync_rows == 0 and not force_full_sync:
            print(f"✅ {table_name}{strategy.site_info}: No changes detected, skipping sync")
            return {"status": "success", "rows_affected": 0, "delta_sync": True, "site_ids": strategy.site_ids}

        # Create temp table name
        temp_table = f"##temp_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            return self._execute_upsert_sync(
                strategy, columns, column_types, temp_table, primary_keys
            )

        except Exception as e:
            self.logger.error(f"Failed to sync {table_name}: {str(e)}")
            return {"status": "error", "error": str(e), "table_name": table_name}

    def _execute_upsert_sync(self, strategy: SyncStrategy, columns: List[str],
                             column_types: Dict[str, str], temp_table: str,
                             primary_keys: List[str]) -> Dict[str, Any]:
        """
        Execute the UPSERT sync operation

        Args:
            strategy: Sync strategy with all parameters
            columns: List of syncable columns
            column_types: Column type mappings
            temp_table: Temporary table name
            primary_keys: Primary key columns

        Returns:
            Dict[str, Any]: Sync result
        """
        onprem_engine = self.connection_manager.get_onprem_engine()
        azure_engine = self.connection_manager.get_azure_engine()
        table_name = strategy.table_name

        with azure_engine.connect() as azure_conn:
            trans = azure_conn.begin()

            try:
                # Create temp table with proper data types
                self._create_temp_table(azure_conn, temp_table, columns, column_types)

                # Load data with parallel processing
                total_invalid_rows = self._load_data_parallel(
                    strategy, columns, column_types, temp_table, azure_conn
                )

                # Execute MERGE operation
                merge_rows = self._execute_merge(
                    azure_conn, table_name, temp_table, columns, column_types, primary_keys
                )

                # Clean up temp table
                azure_conn.execute(text(f"DROP TABLE {temp_table}"))
                trans.commit()

                # Check for discrepancies and handle auto-correction
                discrepancy_result = self.sync_manager.check_sync_discrepancy(
                    azure_engine, strategy, merge_rows
                )

                return self._handle_sync_result(
                    strategy, merge_rows, total_invalid_rows, discrepancy_result
                )

            except Exception as e:
                trans.rollback()
                try:
                    azure_conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                except:
                    pass
                raise e

    def _create_temp_table(self, azure_conn, temp_table: str, columns: List[str],
                           column_types: Dict[str, str]):
        """Create temporary table with proper data types"""
        columns_def = []
        for col in columns:
            col_type = column_types.get(col, 'NVARCHAR(MAX)')
            if col.lower() == 'uts':
                col_type = 'BINARY(8)'  # Ensure uTS is BINARY(8)
            columns_def.append(f'[{col}] {col_type}')

        create_temp = text(f"CREATE TABLE {temp_table} ({', '.join(columns_def)})")
        azure_conn.execute(create_temp)

    def _load_data_parallel(self, strategy: SyncStrategy, columns: List[str],
                            column_types: Dict[str, str], temp_table: str,
                            azure_conn) -> int:
        """Load data using parallel processing"""
        onprem_engine = self.connection_manager.get_onprem_engine()
        table_name = strategy.table_name

        # Build query with filters
        columns_list = ', '.join([f'[{col}]' for col in columns])
        base_query = f"SELECT {columns_list} FROM {table_name}"
        where_clause, query_params = self.sync_manager.build_where_conditions(strategy, columns)

        # Get optimal batch sizes based on table size
        batch_sizes = self.sync_manager.get_batch_sizes(table_name, strategy.total_rows)
        fetch_batch_size = batch_sizes['fetch_batch_size']

        num_batches = (strategy.sync_rows + fetch_batch_size - 1) // fetch_batch_size

        # Progress bar for parallel loading
        sync_type = 'Delta' if strategy.use_delta_sync else 'Full'
        pbar = tqdm.tqdm(
            total=strategy.sync_rows,
            desc=f"Loading {table_name} ({sync_type} - Workers: {self.max_workers}, Batch: {fetch_batch_size})",
            unit="rows", ncols=100, leave=False
        )

        total_invalid_rows = 0

        def process_batch_delta(batch_offset: int, batch_num: int):
            """Process a single batch in parallel"""
            try:
                with onprem_engine.connect() as worker_conn:
                    query = text(f"""
                        {base_query} {where_clause}
                        ORDER BY (SELECT NULL)
                        OFFSET :offset ROWS
                        FETCH NEXT :batch_size ROWS ONLY
                    """)

                    params = {**query_params, "offset": batch_offset, "batch_size": fetch_batch_size}
                    batch_result = worker_conn.execute(query, params)
                    batch_data = batch_result.fetchall()

                    if not batch_data:
                        return batch_num, [], 0, 0

                    # Validate and clean data
                    cleaned_params, invalid_count = self.data_processor.validate_batch_data(
                        batch_data, columns, column_types
                    )

                    return batch_num, cleaned_params, invalid_count, len(batch_data)

            except Exception as e:
                self.logger.error(f"Error processing batch {batch_num}: {str(e)}")
                return batch_num, [], 0, 0

        # Execute parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_batch = {
                executor.submit(process_batch_delta, i * fetch_batch_size, i): i
                for i in range(num_batches)
            }

            batch_results = {}
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_num, cleaned_params, invalid_count, processed_count = future.result()
                batch_results[batch_num] = (cleaned_params, invalid_count, processed_count)
                pbar.update(processed_count)
                total_invalid_rows += invalid_count

        pbar.close()

        # Sequential insertion of processed batches
        insert_pbar = tqdm.tqdm(total=num_batches, desc=f"Inserting {table_name}",
                                unit="batch", ncols=100, leave=False)

        for batch_num in sorted(batch_results.keys()):
            cleaned_params, invalid_count, processed_count = batch_results[batch_num]

            if cleaned_params:
                placeholders = ', '.join([f':col{i}' for i in range(len(columns))])
                columns_list_temp = ', '.join([f'[{col}]' for col in columns])
                insert_temp = text(f"INSERT INTO {temp_table} ({columns_list_temp}) VALUES ({placeholders})")
                azure_conn.execute(insert_temp, cleaned_params)

            insert_pbar.update(1)

        insert_pbar.close()

        if total_invalid_rows > 0:
            self.logger.warning(f"Skipped {total_invalid_rows} rows due to data quality issues")

        return total_invalid_rows

    def _execute_merge(self, azure_conn, table_name: str, temp_table: str,
                       columns: List[str], column_types: Dict[str, str],
                       primary_keys: List[str]) -> int:
        """Execute MERGE operation"""
        merge_pbar = tqdm.tqdm(total=1, desc=f"Merging {table_name}", unit="op",
                               ncols=100, leave=False)

        # Build MERGE statement
        key_conditions = ' AND '.join([f"target.[{key}] = source.[{key}]" for key in primary_keys])
        update_columns = [col for col in columns if col not in primary_keys]

        merge_parts = [f"MERGE {table_name} AS target", f"USING {temp_table} AS source",
                       f"ON {key_conditions}"]

        if update_columns:
            update_assignments = []
            for col in update_columns:
                col_type = column_types.get(col, 'NVARCHAR(MAX)')
                if col.lower() == 'uts':
                    update_assignments.append(
                        f"target.[{col}] = CASE WHEN source.[{col}] IS NULL THEN NULL ELSE CONVERT(BINARY(8), source.[{col}], 1) END")
                elif 'datetime' in col_type.lower() or 'date' in col_type.lower():
                    update_assignments.append(
                        f"target.[{col}] = CASE WHEN source.[{col}] IS NULL THEN NULL ELSE CAST(source.[{col}] AS {col_type}) END")
                else:
                    update_assignments.append(f"target.[{col}] = source.[{col}]")

            update_set = ', '.join(update_assignments)
            merge_parts.append(f"WHEN MATCHED THEN UPDATE SET {update_set}")

        # Handle INSERT with proper casting
        insert_columns = ', '.join([f'[{col}]' for col in columns])
        insert_values = []
        for col in columns:
            col_type = column_types.get(col, 'NVARCHAR(MAX)')
            if col.lower() == 'uts':
                insert_values.append(
                    f"CASE WHEN source.[{col}] IS NULL THEN NULL ELSE CONVERT(BINARY(8), source.[{col}], 1) END")
            elif 'datetime' in col_type.lower() or 'date' in col_type.lower():
                insert_values.append(
                    f"CASE WHEN source.[{col}] IS NULL THEN NULL ELSE CAST(source.[{col}] AS {col_type}) END")
            else:
                insert_values.append(f"source.[{col}]")

        insert_values_str = ', '.join(insert_values)
        merge_parts.append(
            f"WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_columns}) VALUES ({insert_values_str});")

        merge_sql = text('\n'.join(merge_parts))

        # Execute merge
        result = azure_conn.execute(merge_sql)
        merge_rows = result.rowcount

        merge_pbar.update(1)
        merge_pbar.close()

        return merge_rows

    def _handle_sync_result(self, strategy: SyncStrategy, merge_rows: int,
                            total_invalid_rows: int, discrepancy_result: Dict[str, Any]) -> Dict[str, Any]:
        """Handle sync result and discrepancy analysis"""
        table_name = strategy.table_name
        site_info = strategy.site_info

        # Check for discrepancies and provide feedback
        if discrepancy_result.get("has_discrepancy"):
            row_diff = discrepancy_result["row_diff"]
            source_count = discrepancy_result["source_count"]
            azure_count = discrepancy_result["azure_count"]

            if strategy.use_delta_sync:
                print(f"⚠️  {table_name}{site_info}: Delta sync complete but count mismatch detected!")
                print(f"   Source: {source_count:,} → Azure: {azure_count:,} (diff: {row_diff:+,})")

                if discrepancy_result.get("needs_correction"):
                    print(f"   🔄 Triggering full UPSERT to correct discrepancy...")
                    return {
                        "status": "needs_full_sync",
                        "reason": "count_discrepancy_after_delta",
                        "source_count": source_count,
                        "azure_count": azure_count,
                        "row_diff": row_diff,
                        "table_name": table_name,
                        "site_ids": strategy.site_ids
                    }
            else:
                print(
                    f"⚠️  {table_name}{site_info}: Source {source_count:,} → Azure {azure_count:,} (diff: {row_diff:+,})")
                print(f"   Data quality issue detected - {abs(row_diff)} rows lost during sync")
        else:
            azure_count = discrepancy_result.get("azure_count")
            if strategy.use_delta_sync:
                print(
                    f"📊 {table_name}{site_info}: Processed {strategy.sync_rows:,} delta rows, Azure site total: {azure_count:,} ✅")
            else:
                print(f"✅ {table_name}{site_info}: Perfect sync - {azure_count:,} rows")

        self.logger.debug(f"Successfully synced {table_name}: {merge_rows:,} rows affected")
        if total_invalid_rows > 0:
            self.logger.debug(f"Note: {total_invalid_rows} rows were skipped due to invalid data")

        return {
            "status": "success",
            "rows_affected": merge_rows,
            "rows_processed": strategy.sync_rows - total_invalid_rows,
            "invalid_rows": total_invalid_rows,
            "table_name": table_name,
            "workers_used": self.max_workers,
            "delta_sync": strategy.use_delta_sync,
            "total_rows": strategy.total_rows,
            "delta_rows": strategy.delta_rows,
            "final_azure_count": discrepancy_result.get("azure_count"),
            "site_ids": strategy.site_ids
        }

    def sync_table_replace_bulk(self, config: TableSyncConfig) -> Dict[str, Any]:
        """
        High-performance REPLACE sync using bulk operations

        Args:
            config: Table sync configuration

        Returns:
            Dict[str, Any]: Sync result
        """
        table_name = config.table_name
        self.logger.debug(f"Starting bulk REPLACE sync for {table_name}")

        # Get engines
        onprem_engine = self.connection_manager.get_onprem_engine()
        azure_engine = self.connection_manager.get_azure_engine()

        # Get syncable columns
        columns = self.data_processor.get_syncable_columns(onprem_engine, table_name)
        if not columns:
            return {"status": "error", "message": "No syncable columns"}

        columns_list = ', '.join([f'[{col}]' for col in columns])

        try:
            # Get row count
            with onprem_engine.connect() as onprem_conn:
                count_result = onprem_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                total_rows = count_result.fetchone()[0]

                if total_rows == 0:
                    self.logger.debug(f"Table {table_name} is empty")
                    return {"status": "success", "rows_affected": 0}

            # Use Azure connection for bulk operations
            with azure_engine.connect() as azure_conn:
                trans = azure_conn.begin()

                try:
                    # Check for incoming FKs and clear table
                    fk_check = text("""
                                    SELECT COUNT(*)
                                    FROM sys.foreign_keys fk
                                             INNER JOIN sys.tables t ON fk.referenced_object_id = t.object_id
                                    WHERE t.name = :table_name
                                    """)
                    fk_result = azure_conn.execute(fk_check, {"table_name": table_name})
                    has_incoming_fks = fk_result.fetchone()[0] > 0

                    if has_incoming_fks:
                        self.logger.debug(f"Using DELETE for {table_name} (has incoming FKs)")
                        azure_conn.execute(text(f"DELETE FROM {table_name}"))
                    else:
                        self.logger.debug(f"Using TRUNCATE for {table_name}")
                        azure_conn.execute(text(f"TRUNCATE TABLE {table_name}"))

                    # Bulk insert using configurable batch sizes
                    batch_sizes = self.sync_manager.get_batch_sizes(table_name, total_rows)
                    batch_size = batch_sizes['replace_batch_size']
                    rows_processed = 0

                    pbar = tqdm.tqdm(total=total_rows, desc=f"Syncing {table_name}", unit="rows",
                                     ncols=100, leave=False)

                    while rows_processed < total_rows:
                        with onprem_engine.connect() as onprem_conn:
                            select_query = text(f"""
                                SELECT {columns_list} FROM {table_name}
                                ORDER BY (SELECT NULL)
                                OFFSET :offset ROWS
                                FETCH NEXT :batch_size ROWS ONLY
                            """)

                            batch_result = onprem_conn.execute(select_query, {
                                "offset": rows_processed,
                                "batch_size": batch_size
                            })

                            batch_data = batch_result.fetchall()
                            if not batch_data:
                                break

                            # Bulk insert into Azure
                            placeholders = ', '.join([':' + f'col{i}' for i in range(len(columns))])
                            insert_query = text(f"""
                                INSERT INTO {table_name} ({columns_list}) 
                                VALUES ({placeholders})
                            """)

                            param_list = []
                            for row in batch_data:
                                params = {f'col{i}': value for i, value in enumerate(row)}
                                param_list.append(params)

                            azure_conn.execute(insert_query, param_list)

                            rows_processed += len(batch_data)
                            pbar.update(len(batch_data))

                    pbar.close()
                    trans.commit()

                    return {
                        "status": "success",
                        "rows_affected": rows_processed,
                        "table_name": table_name
                    }

                except Exception as e:
                    trans.rollback()
                    raise e

        except Exception as e:
            self.logger.error(f"Failed to sync {table_name}: {str(e)}")
            return {"status": "error", "error": str(e), "table_name": table_name}

    def sync_all_tables(self, dry_run: bool = False, table_filter: List[str] = None) -> List[Dict[str, Any]]:
        """
        Main method to sync all tables using pooled connections

        Args:
            dry_run: If True, only analyze without syncing
            table_filter: List of specific table names to sync

        Returns:
            List[Dict[str, Any]]: List of sync results
        """
        print("\n" + "=" * 60)
        print("🔄 SQL SYNC PROCESS")
        print("=" * 60)

        if dry_run:
            print("📋 DRY RUN MODE - No actual syncing will occur")

        sync_start_time = datetime.now()

        try:
            # Get sync configuration
            configs = self.get_sync_config()

            # Apply table filter if provided
            if table_filter:
                configs = [c for c in configs if c.table_name in table_filter]
                print(f"📌 Filtered to {len(configs)} tables: {', '.join(table_filter)}")

            print(f"📊 Processing {len(configs)} tables in dependency order")

            # Track sync progress
            successful_syncs = 0
            failed_syncs = 0
            sync_results = []

            # Main progress bar for tables
            table_pbar = tqdm.tqdm(configs, desc="Overall Progress", unit="table", ncols=100)

            # Process each table
            for i, config in enumerate(table_pbar):
                table_pbar.set_description(f"Processing {config.table_name}")

                self.logger.debug(f"[{i + 1}/{len(configs)}] Processing table: {config.table_name}")

                if config.is_problem_table:
                    self.logger.warning(f"{config.table_name} is marked as a problem table")

                if config.has_foreign_keys:
                    self.logger.debug(f"{config.table_name} has foreign key constraints")

                try:
                    if dry_run:
                        # Dry run - just analyze
                        onprem_engine = self.connection_manager.get_onprem_engine()
                        with onprem_engine.connect() as conn:
                            result = conn.execute(text(f"SELECT COUNT(*) FROM {config.table_name}"))
                            row_count = result.fetchone()[0]

                        result = {
                            "table_name": config.table_name,
                            "status": "dry_run",
                            "rows_found": row_count,
                            "sync_method": config.sync_method
                        }
                        successful_syncs += 1
                    else:
                        # Actual sync
                        if config.sync_method.upper() == "REPLACE":
                            result = self.sync_table_replace_bulk(config)
                        elif config.sync_method.upper() == "UPSERT":
                            if self.use_fast_sync:
                                result = self.sync_table_upsert_bulk_fast(config)

                                # Handle auto-correction for discrepancies
                                if result.get('status') == 'needs_full_sync':
                                    print(f"🔄 Starting corrective full UPSERT for {config.table_name}...")
                                    result = self.sync_table_upsert_bulk_fast(config, force_full_sync=True)
                                    if result.get('status') == 'success':
                                        print(f"✅ {config.table_name}: Corrective full sync completed successfully")
                                    else:
                                        print(
                                            f"❌ {config.table_name}: Corrective full sync failed - {result.get('error', 'Unknown error')}")
                            else:
                                # Fallback to original method if fast sync disabled
                                result = {"status": "error",
                                          "error": "Original UPSERT method not implemented in modular version"}
                        else:
                            result = {"status": "error", "error": f"Unknown sync method: {config.sync_method}"}

                        if result['status'] == 'success':
                            successful_syncs += 1
                            table_pbar.set_postfix_str(f"✓ {config.table_name}")
                        else:
                            failed_syncs += 1
                            table_pbar.set_postfix_str(f"✗ {config.table_name}")

                    sync_results.append(result)

                except Exception as e:
                    failed_syncs += 1
                    error_result = {
                        "table_name": config.table_name,
                        "status": "error",
                        "error": str(e)
                    }
                    sync_results.append(error_result)
                    self.logger.error(f"Failed to process {config.table_name}: {str(e)}")
                    table_pbar.set_postfix_str(f"✗ {config.table_name} (Error)")

                    # Ask user if they want to continue
                    if not dry_run:
                        table_pbar.close()
                        continue_sync = input(f"\n❌ Table {config.table_name} failed. Continue? (y/n): ")
                        if continue_sync.lower() != 'y':
                            print("🛑 Sync process aborted by user")
                            break
                        # Recreate progress bar for remaining tables
                        remaining_configs = configs[i + 1:]
                        if remaining_configs:
                            table_pbar = tqdm.tqdm(remaining_configs, desc="Overall Progress", unit="table", ncols=100)

            table_pbar.close()

            # Final summary
            total_time = datetime.now() - sync_start_time
            print("\n" + "=" * 60)
            print("📋 SYNC SUMMARY")
            print("=" * 60)
            print(f"📊 Tables processed: {len(configs)}")
            print(f"✅ Successful: {successful_syncs}")
            print(f"❌ Failed: {failed_syncs}")
            print(f"⏱️  Duration: {total_time}")

            # Show failed tables if any
            if failed_syncs > 0:
                print("\n❌ Failed tables:")
                for result in sync_results:
                    if result['status'] == 'error':
                        error_msg = result.get('error', 'Unknown error')
                        # Truncate long error messages
                        if len(error_msg) > 100:
                            error_msg = error_msg[:97] + "..."
                        print(f"   • {result['table_name']}: {error_msg}")

            # Show successful tables summary
            if successful_syncs > 0:
                if dry_run:
                    print(f"\n✅ Successfully analyzed {successful_syncs} tables")
                    # Show row counts for analyzed tables
                    total_rows = sum(
                        result.get('rows_found', 0) for result in sync_results if result['status'] == 'dry_run')
                    print(f"📊 Total rows found: {total_rows:,}")
                else:
                    print(f"\n✅ Successfully synced {successful_syncs} tables")
                    # Show total rows processed
                    total_processed = sum(
                        result.get('rows_affected', 0) for result in sync_results if result['status'] == 'success')
                    print(f"📊 Total rows processed: {total_processed:,}")

            return sync_results

        except Exception as e:
            self.logger.error(f"Sync process failed: {str(e)}")
            raise

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get comprehensive information about a table

        Args:
            table_name: Name of the table to analyze

        Returns:
            Dict[str, Any]: Table information
        """
        onprem_engine = self.connection_manager.get_onprem_engine()
        return self.data_processor.get_table_info(onprem_engine, table_name)

    def show_site_info(self, table_names: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        Show SiteID information for specified tables

        Args:
            table_names: List of table names to analyze, or None for default tables

        Returns:
            Dict[str, Dict[str, Any]]: SiteID information for each table
        """
        if table_names is None:
            table_names = ['Payments', 'Waitings', 'Customers', 'Sites']

        onprem_engine = self.connection_manager.get_onprem_engine()
        return self.sync_manager.get_site_info_summary(onprem_engine, table_names)

    def cleanup_connections(self):
        """Clean up all database connections"""
        try:
            self.connection_manager.close_connections()
            self.logger.info("Database connections closed successfully")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")


# Factory function for creating PooledSQLSyncClient instances
def create_sync_client() -> PooledSQLSyncClient:
    """
    Factory function to create a PooledSQLSyncClient instance

    Returns:
        PooledSQLSyncClient: Configured sync client
    """
    return PooledSQLSyncClient()


# For testing the module independently
if __name__ == "__main__":
    print("Sync Client Module Test")
    print("=" * 40)

    try:
        # Test client initialization
        client = create_sync_client()
        print("✅ Sync client initialized successfully")

        # Test connection
        if client.test_connections():
            print("✅ Database connections tested successfully")
        else:
            print("❌ Database connection test failed")

        # Test configuration loading
        configs = client.get_sync_config()
        print(f"✅ Loaded {len(configs)} table configurations")

        # Show some sample configurations
        if configs:
            print("\nSample configurations:")
            for config in configs[:3]:  # Show first 3
                print(f"  - {config.table_name}: {config.sync_method} (Order: {config.process_order})")

    except Exception as e:
        print(f"❌ Error during testing: {e}")

    print("\n✅ Sync Client module loaded successfully!")