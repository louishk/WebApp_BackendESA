"""
Sync Strategy Management Module

Handles delta sync detection, SiteID management, sync strategy decisions,
and auto-correction logic for database synchronization operations.
"""

import logging
import os
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class SyncStrategy:
    """Data class representing a sync strategy decision"""
    use_delta_sync: bool
    total_rows: int
    delta_rows: int
    sync_rows: int
    site_ids: List[int]
    site_info: str
    last_sync: Optional[datetime]
    table_name: str
    reason: str


@dataclass
class TableSyncConfig:
    """Data class for table sync configuration"""
    table_name: str
    has_primary_key: bool
    primary_key_columns: str
    sync_method: str
    process_order: int
    dependency_level: int
    has_foreign_keys: bool
    is_problem_table: bool
    error_count: int
    key_count: int


class SyncStrategyManager:
    """Manages sync strategies, delta detection, and SiteID handling"""

    def __init__(self):
        """Initialize sync strategy manager"""
        self.logger = logging.getLogger('SyncStrategyManager')

        # Configuration from environment with defaults
        self.delta_threshold = float(os.getenv('DELTA_THRESHOLD', '0.5'))
        self.auto_correct_discrepancies = os.getenv('AUTO_CORRECT_DISCREPANCIES', 'true').lower() == 'true'
        self.discrepancy_threshold = int(os.getenv('DISCREPANCY_THRESHOLD', '0'))

        # NEW: Configurable batch sizes
        self.fetch_batch_size = int(os.getenv('FETCH_BATCH_SIZE', '2000'))
        self.insert_batch_size = int(os.getenv('INSERT_BATCH_SIZE', '1000'))
        self.replace_batch_size = int(os.getenv('REPLACE_BATCH_SIZE', '50000'))

        # Large table optimization
        self.large_table_threshold = int(os.getenv('LARGE_TABLE_THRESHOLD', '100000'))
        self.large_table_fetch_batch = int(os.getenv('LARGE_TABLE_FETCH_BATCH', '5000'))
        self.large_table_insert_batch = int(os.getenv('LARGE_TABLE_INSERT_BATCH', '2000'))

        # Tables that don't support delta sync
        self.no_delta_tables = {'__RefactorLog', 'SitesRunning', 'TableSyncConfig'}

        self.logger.debug(
            f"Batch sizes - Fetch: {self.fetch_batch_size}, Insert: {self.insert_batch_size}, Replace: {self.replace_batch_size}")
        self.logger.debug(
            f"Large table optimization - Threshold: {self.large_table_threshold}, Fetch: {self.large_table_fetch_batch}, Insert: {self.large_table_insert_batch}")

    def get_batch_sizes(self, table_name: str, total_rows: int) -> Dict[str, int]:
        """
        Get optimal batch sizes based on table size and configuration

        Args:
            table_name: Name of the table
            total_rows: Total number of rows in the table

        Returns:
            Dict[str, int]: Dictionary with fetch_batch_size and insert_batch_size
        """
        # Check if this is a large table
        is_large_table = total_rows >= self.large_table_threshold

        if is_large_table:
            self.logger.debug(f"Table {table_name} has {total_rows:,} rows - using large table optimization")
            return {
                'fetch_batch_size': self.large_table_fetch_batch,
                'insert_batch_size': self.large_table_insert_batch,
                'replace_batch_size': self.replace_batch_size  # Keep replace batch size the same
            }
        else:
            return {
                'fetch_batch_size': self.fetch_batch_size,
                'insert_batch_size': self.insert_batch_size,
                'replace_batch_size': self.replace_batch_size
            }

    def get_sync_config(self, azure_engine: Engine) -> List[TableSyncConfig]:
        """
        Get table sync configuration from Azure database

        Args:
            azure_engine: SQLAlchemy engine for Azure connection

        Returns:
            List[TableSyncConfig]: List of table configurations
        """
        self.logger.debug("Fetching sync configuration from Azure")

        try:
            with azure_engine.connect() as conn:
                query = text("""
                             SELECT TableName,
                                    HasPrimaryKey,
                                    PrimaryKeyColumns,
                                    SyncMethod,
                                    IsActive,
                                    ProcessOrder,
                                    DependencyLevel,
                                    HasForeignKeys,
                                    ErrorCount,
                                    IsProblemTable,
                                    KeyCount
                             FROM TableSyncConfig
                             WHERE IsActive = 1
                             ORDER BY ProcessOrder,
                                      CASE WHEN SyncMethod = 'REPLACE' THEN 1 ELSE 2 END,
                                      TableName
                             """)

                result = conn.execute(query)
                rows = result.fetchall()

                configs = []
                for row in rows:
                    configs.append(TableSyncConfig(
                        table_name=row[0],
                        has_primary_key=row[1],
                        primary_key_columns=row[2] or '',
                        sync_method=row[3],
                        process_order=row[5] or 999,
                        dependency_level=row[6] or 999,
                        has_foreign_keys=row[7] or False,
                        is_problem_table=row[9] or False,
                        error_count=row[8] or 0,
                        key_count=row[10] or 1
                    ))

                self.logger.debug(f"Retrieved configuration for {len(configs)} tables")
                return configs

        except Exception as e:
            self.logger.error(f"Failed to get sync configuration: {str(e)}")
            raise

    def get_timestamp_comparison(self, onprem_engine: Engine, azure_engine: Engine, table_name: str) -> Dict[str, Any]:
        """
        Compare MAX timestamps between OnPrem and Azure to determine sync strategy

        Args:
            onprem_engine: SQLAlchemy engine for on-premises connection
            azure_engine: SQLAlchemy engine for Azure connection
            table_name: Name of the table to analyze

        Returns:
            Dict[str, Any]: Timestamp comparison results and sync recommendation
        """
        try:
            # Check if table has dUpdated column
            with onprem_engine.connect() as conn:
                has_dupdated = conn.execute(text("""
                                                 SELECT COUNT(*)
                                                 FROM INFORMATION_SCHEMA.COLUMNS
                                                 WHERE TABLE_NAME = :table_name
                                                   AND COLUMN_NAME = 'dUpdated'
                                                 """), {"table_name": table_name}).fetchone()[0] > 0

            if not has_dupdated:
                return {
                    'has_dupdated': False,
                    'sync_decision': 'full',
                    'reason': 'no_dupdated_column'
                }

            # Get MAX timestamps from both databases
            with onprem_engine.connect() as onprem_conn:
                onprem_result = onprem_conn.execute(text(f"""
                    SELECT MAX(dUpdated) as MaxTimestamp, COUNT(*) as TotalRows
                    FROM [{table_name}]
                """)).fetchone()
                onprem_max = onprem_result[0]
                onprem_count = onprem_result[1]

            with azure_engine.connect() as azure_conn:
                azure_result = azure_conn.execute(text(f"""
                    SELECT MAX(dUpdated) as MaxTimestamp, COUNT(*) as TotalRows  
                    FROM [{table_name}]
                """)).fetchone()
                azure_max = azure_result[0]
                azure_count = azure_result[1]

            # Build comparison result
            result = {
                'has_dupdated': True,
                'onprem_max': onprem_max,
                'azure_max': azure_max,
                'onprem_count': onprem_count,
                'azure_count': azure_count,
                'count_diff': onprem_count - azure_count
            }

            # Decision logic based on timestamp and count comparison
            if not onprem_max and not azure_max:
                # Both tables have no timestamps
                result['sync_decision'] = 'full'
                result['reason'] = 'no_timestamps_in_either_table'
                result['delta_threshold'] = None

            elif not onprem_max:
                # OnPrem has no timestamps but Azure does - unusual
                result['sync_decision'] = 'full'
                result['reason'] = 'onprem_has_no_timestamps'
                result['delta_threshold'] = None

            elif not azure_max:
                # Azure is empty or has no timestamps - full sync needed
                result['sync_decision'] = 'full'
                result['reason'] = 'azure_empty_or_no_timestamps'
                result['delta_threshold'] = None

            elif onprem_max > azure_max:
                # OnPrem has newer data - delta sync possible
                result['sync_decision'] = 'delta'
                result['reason'] = f'onprem_newer_by_{(onprem_max - azure_max).total_seconds():.0f}s'
                result['delta_threshold'] = azure_max

                # Apply safety buffer if configured
                buffer_hours = int(os.getenv('DELTA_SYNC_BUFFER_HOURS', '0'))
                if buffer_hours > 0:
                    from datetime import timedelta
                    buffered_threshold = azure_max - timedelta(hours=buffer_hours)
                    result['delta_threshold'] = buffered_threshold
                    result['reason'] += f'_with_{buffer_hours}h_buffer'

            elif azure_max > onprem_max:
                # Azure has newer data than OnPrem - investigate!
                time_diff = (azure_max - onprem_max).total_seconds()
                result['sync_decision'] = 'investigate'
                result['reason'] = f'azure_newer_by_{time_diff:.0f}s_investigate_required'
                result['delta_threshold'] = None

                # If difference is small (< 1 hour), might be timezone issue
                if time_diff < 3600:
                    result['sync_decision'] = 'full'
                    result['reason'] += '_possible_timezone_issue'

            else:
                # Timestamps are equal - check counts
                if onprem_count == azure_count:
                    result['sync_decision'] = 'skip'
                    result['reason'] = 'timestamps_and_counts_match'
                    result['delta_threshold'] = None
                else:
                    result['sync_decision'] = 'full'
                    result['reason'] = f'same_timestamps_but_count_diff_{result["count_diff"]}'
                    result['delta_threshold'] = None

            self.logger.debug(f"Timestamp comparison for {table_name}: {result['sync_decision']} - {result['reason']}")
            return result

        except Exception as e:
            self.logger.error(f"Error comparing timestamps for {table_name}: {e}")
            return {
                'has_dupdated': False,
                'sync_decision': 'full',
                'reason': f'error_{str(e)}',
                'error': str(e)
            }

    def get_last_sync_timestamp(self, azure_engine: Engine, table_name: str) -> Optional[datetime]:
        """
        DEPRECATED: Use get_timestamp_comparison instead

        This method is kept for backward compatibility but the new approach
        uses direct timestamp comparison rather than guessing last sync time.
        """
        self.logger.warning(
            f"Using deprecated get_last_sync_timestamp for {table_name} - consider using get_timestamp_comparison")

        try:
            with azure_engine.connect() as conn:
                has_dupdated = conn.execute(text("""
                                                 SELECT COUNT(*)
                                                 FROM INFORMATION_SCHEMA.COLUMNS
                                                 WHERE TABLE_NAME = :table_name
                                                   AND COLUMN_NAME = 'dUpdated'
                                                 """), {"table_name": table_name}).fetchone()[0] > 0

                if not has_dupdated:
                    return None

                max_updated = conn.execute(text(f"""
                    SELECT MAX(dUpdated) FROM [{table_name}]
                """)).fetchone()[0]

                if max_updated:
                    buffer_hours = int(os.getenv('DELTA_SYNC_BUFFER_HOURS', '0'))
                    if buffer_hours > 0:
                        from datetime import timedelta
                        safe_timestamp = max_updated - timedelta(hours=buffer_hours)
                        self.logger.debug(f"Applied {buffer_hours}h buffer: {max_updated} -> {safe_timestamp}")
                        max_updated = safe_timestamp

                return max_updated

        except Exception as e:
            self.logger.debug(f"Could not get last sync timestamp for {table_name}: {e}")
            return None

    def get_site_ids_for_table(self, onprem_engine: Engine, table_name: str,
                               has_siteid: bool) -> List[int]:
        """
        Get SiteIDs for this sync - from env, auto-detect, or return empty list

        Args:
            onprem_engine: SQLAlchemy engine for on-premises connection
            table_name: Name of the table to analyze
            has_siteid: Whether the table has a SiteID column

        Returns:
            List[int]: List of SiteIDs to filter by
        """
        if not has_siteid:
            return []

        # Check environment variables for SiteID configuration
        site_ids_env = os.getenv('SITE_IDS')  # Comma-separated: "123,456,789"
        single_site_env = os.getenv('SITE_ID')  # Single site: "123"

        if site_ids_env:
            try:
                site_ids = [int(sid.strip()) for sid in site_ids_env.split(',') if sid.strip()]
                self.logger.debug(f"Using SITE_IDS from environment: {site_ids}")
                return site_ids
            except ValueError as e:
                self.logger.warning(f"Invalid SITE_IDS format in environment: {site_ids_env}. Error: {e}")

        if single_site_env:
            try:
                site_id = int(single_site_env)
                self.logger.debug(f"Using SITE_ID from environment: {site_id}")
                return [site_id]
            except ValueError as e:
                self.logger.warning(f"Invalid SITE_ID format in environment: {single_site_env}. Error: {e}")

        # Auto-detect SiteIDs from on-premises data
        try:
            with onprem_engine.connect() as conn:
                site_result = conn.execute(text(f"""
                    SELECT DISTINCT SiteID FROM [{table_name}]
                    WHERE SiteID IS NOT NULL 
                    ORDER BY SiteID
                """))
                auto_detected = [row[0] for row in site_result.fetchall()]

                if len(auto_detected) <= 5:  # Only auto-use if reasonable number
                    self.logger.debug(f"Auto-detected SiteIDs for {table_name}: {auto_detected}")
                    return auto_detected
                else:
                    self.logger.warning(
                        f"Too many SiteIDs detected ({len(auto_detected)}) for {table_name}. "
                        f"Consider setting SITE_IDS environment variable."
                    )
                    return []

        except Exception as e:
            self.logger.debug(f"Could not auto-detect SiteIDs for {table_name}: {e}")
            return []

    def get_delta_row_count(self, onprem_engine: Engine, table_name: str,
                            last_sync: Optional[datetime], site_ids: List[int],
                            has_siteid: bool) -> Tuple[int, int]:
        """
        Get total rows and delta rows (changed since last sync)

        Args:
            onprem_engine: SQLAlchemy engine for on-premises connection
            table_name: Name of the table to analyze
            last_sync: Last sync timestamp
            site_ids: List of SiteIDs to filter by
            has_siteid: Whether table has SiteID column

        Returns:
            Tuple[int, int]: (total_rows, delta_rows)
        """
        try:
            with onprem_engine.connect() as conn:
                # Build base query with optional SiteID filter
                base_count_query = f"SELECT COUNT(*) FROM [{table_name}]"
                site_filter = ""
                site_params = {}

                if has_siteid and site_ids:
                    if len(site_ids) == 1:
                        site_filter = " WHERE SiteID = :site_id"
                        site_params = {"site_id": site_ids[0]}
                    else:
                        placeholders = ', '.join([f':site_id_{i}' for i in range(len(site_ids))])
                        site_filter = f" WHERE SiteID IN ({placeholders})"
                        site_params = {f'site_id_{i}': site_id for i, site_id in enumerate(site_ids)}

                # Get total row count
                total_query = base_count_query + site_filter
                total_result = conn.execute(text(total_query), site_params)
                total_rows = total_result.fetchone()[0]

                # Check if table supports delta sync
                has_dupdated = conn.execute(text("""
                                                 SELECT COUNT(*)
                                                 FROM INFORMATION_SCHEMA.COLUMNS
                                                 WHERE TABLE_NAME = :table_name
                                                   AND COLUMN_NAME = 'dUpdated'
                                                 """), {"table_name": table_name}).fetchone()[0] > 0

                if not has_dupdated or table_name in self.no_delta_tables:
                    return total_rows, total_rows

                if last_sync is None:
                    return total_rows, total_rows

                # Get count of rows updated since last sync
                delta_where = "(dUpdated > :last_sync OR dUpdated IS NULL)"
                if site_filter:
                    # Extract WHERE condition from site_filter and combine
                    site_condition = site_filter.replace(" WHERE ", "")
                    delta_where = f"({site_condition}) AND {delta_where}"

                delta_query = f"{base_count_query} WHERE {delta_where}"
                delta_params = {**site_params, "last_sync": last_sync}

                delta_result = conn.execute(text(delta_query), delta_params)
                delta_rows = delta_result.fetchone()[0]

                return total_rows, delta_rows

        except Exception as e:
            self.logger.warning(f"Error calculating delta for {table_name}: {e}")
            return 0, 0

    def determine_sync_strategy(self, onprem_engine: Engine, azure_engine: Engine,
                                table_name: str, columns: List[str],
                                force_full_sync: bool = False) -> SyncStrategy:
        """
        Determine the optimal sync strategy for a table using intelligent timestamp comparison

        Args:
            onprem_engine: SQLAlchemy engine for on-premises connection
            azure_engine: SQLAlchemy engine for Azure connection
            table_name: Name of the table to sync
            columns: List of syncable columns
            force_full_sync: Whether to force a full sync

        Returns:
            SyncStrategy: Complete sync strategy with all parameters
        """
        # Get SiteID information
        has_siteid = 'SiteID' in columns
        site_ids = self.get_site_ids_for_table(onprem_engine, table_name, has_siteid)

        # Build site info string
        if site_ids:
            if len(site_ids) == 1:
                site_info = f" (SiteID: {site_ids[0]})"
            else:
                site_info = f" (SiteIDs: {', '.join(map(str, site_ids))})"
        else:
            site_info = ""

        # Force full sync if requested
        if force_full_sync:
            total_rows, delta_rows = self.get_delta_row_count(
                onprem_engine, table_name, None, site_ids, has_siteid
            )
            return SyncStrategy(
                use_delta_sync=False,
                total_rows=total_rows,
                delta_rows=total_rows,
                sync_rows=total_rows,
                site_ids=site_ids,
                site_info=site_info,
                last_sync=None,
                table_name=table_name,
                reason="forced_full_sync"
            )

        # Check if table is in no-delta list
        if table_name in self.no_delta_tables:
            total_rows, delta_rows = self.get_delta_row_count(
                onprem_engine, table_name, None, site_ids, has_siteid
            )
            return SyncStrategy(
                use_delta_sync=False,
                total_rows=total_rows,
                delta_rows=total_rows,
                sync_rows=total_rows,
                site_ids=site_ids,
                site_info=site_info,
                last_sync=None,
                table_name=table_name,
                reason="table_excluded_from_delta_sync"
            )

        # NEW: Use intelligent timestamp comparison
        timestamp_comparison = self.get_timestamp_comparison(onprem_engine, azure_engine, table_name)
        sync_decision = timestamp_comparison.get('sync_decision', 'full')
        delta_threshold = timestamp_comparison.get('delta_threshold')

        # Get row counts based on sync decision
        if sync_decision == 'skip':
            # No sync needed - timestamps and counts match
            return SyncStrategy(
                use_delta_sync=False,
                total_rows=timestamp_comparison.get('onprem_count', 0),
                delta_rows=0,
                sync_rows=0,
                site_ids=site_ids,
                site_info=site_info,
                last_sync=delta_threshold,
                table_name=table_name,
                reason=timestamp_comparison.get('reason', 'no_sync_needed')
            )

        elif sync_decision == 'delta':
            # Delta sync possible - OnPrem has newer data
            total_rows, delta_rows = self.get_delta_row_count(
                onprem_engine, table_name, delta_threshold, site_ids, has_siteid
            )

            # Apply delta threshold check
            use_delta = delta_rows < total_rows * self.delta_threshold if total_rows > 0 else False
            sync_rows = delta_rows if use_delta else total_rows

            return SyncStrategy(
                use_delta_sync=use_delta,
                total_rows=total_rows,
                delta_rows=delta_rows,
                sync_rows=sync_rows,
                site_ids=site_ids,
                site_info=site_info,
                last_sync=delta_threshold,
                table_name=table_name,
                reason=f"intelligent_timestamp_comparison_{timestamp_comparison.get('reason', 'unknown')}"
            )

        elif sync_decision == 'investigate':
            # Azure has newer data - log warning and do full sync
            self.logger.warning(
                f"Table {table_name}: Azure has newer timestamps than OnPrem - investigate data source!")
            self.logger.warning(f"  OnPrem MAX: {timestamp_comparison.get('onprem_max')}")
            self.logger.warning(f"  Azure MAX: {timestamp_comparison.get('azure_max')}")

            total_rows, delta_rows = self.get_delta_row_count(
                onprem_engine, table_name, None, site_ids, has_siteid
            )

            return SyncStrategy(
                use_delta_sync=False,
                total_rows=total_rows,
                delta_rows=total_rows,
                sync_rows=total_rows,
                site_ids=site_ids,
                site_info=site_info,
                last_sync=None,
                table_name=table_name,
                reason=f"azure_has_newer_data_{timestamp_comparison.get('reason', 'investigate')}"
            )

        else:
            # Full sync needed (default case)
            total_rows, delta_rows = self.get_delta_row_count(
                onprem_engine, table_name, None, site_ids, has_siteid
            )

            return SyncStrategy(
                use_delta_sync=False,
                total_rows=total_rows,
                delta_rows=total_rows,
                sync_rows=total_rows,
                site_ids=site_ids,
                site_info=site_info,
                last_sync=None,
                table_name=table_name,
                reason=f"full_sync_required_{timestamp_comparison.get('reason', 'default')}"
            )

    def check_sync_discrepancy(self, azure_engine: Engine, strategy: SyncStrategy,
                               rows_affected: int) -> Dict[str, Any]:
        """
        Check for discrepancies after sync and determine if correction is needed

        Args:
            azure_engine: SQLAlchemy engine for Azure connection
            strategy: The sync strategy that was used
            rows_affected: Number of rows affected by the sync

        Returns:
            Dict[str, Any]: Discrepancy analysis and correction recommendation
        """
        try:
            with azure_engine.connect() as conn:
                table_name = strategy.table_name
                site_ids = strategy.site_ids

                # Get Azure count with SiteID filtering if applicable
                if site_ids:
                    if len(site_ids) == 1:
                        azure_count = conn.execute(
                            text(f"SELECT COUNT(*) FROM [{table_name}] WHERE SiteID = :site_id"),
                            {"site_id": site_ids[0]}
                        ).fetchone()[0]
                    else:
                        placeholders = ', '.join([f':site_id_{i}' for i in range(len(site_ids))])
                        site_params = {f'site_id_{i}': site_id for i, site_id in enumerate(site_ids)}
                        azure_count = conn.execute(
                            text(f"SELECT COUNT(*) FROM [{table_name}] WHERE SiteID IN ({placeholders})"),
                            site_params
                        ).fetchone()[0]
                else:
                    azure_count = conn.execute(
                        text(f"SELECT COUNT(*) FROM [{table_name}]")
                    ).fetchone()[0]

                # Calculate discrepancy
                row_diff = strategy.total_rows - azure_count
                has_discrepancy = abs(row_diff) > self.discrepancy_threshold

                result = {
                    "source_count": strategy.total_rows,
                    "azure_count": azure_count,
                    "row_diff": row_diff,
                    "has_discrepancy": has_discrepancy,
                    "needs_correction": has_discrepancy and self.auto_correct_discrepancies and strategy.use_delta_sync,
                    "rows_affected": rows_affected,
                    "sync_type": "delta" if strategy.use_delta_sync else "full"
                }

                self.logger.debug(
                    f"Discrepancy check for {table_name}: "
                    f"source={strategy.total_rows}, azure={azure_count}, diff={row_diff}"
                )

                return result

        except Exception as e:
            self.logger.error(f"Error checking discrepancy for {strategy.table_name}: {e}")
            return {
                "source_count": strategy.total_rows,
                "azure_count": None,
                "row_diff": None,
                "has_discrepancy": False,
                "needs_correction": False,
                "error": str(e)
            }

    def build_where_conditions(self, strategy: SyncStrategy, columns: List[str]) -> Tuple[str, Dict[str, Any]]:
        """
        Build WHERE clause and parameters for sync queries

        Args:
            strategy: The sync strategy with SiteID and delta information
            columns: List of column names

        Returns:
            Tuple[str, Dict[str, Any]]: (where_clause, query_parameters)
        """
        where_conditions = []
        query_params = {}

        # Add SiteID filtering
        if strategy.site_ids and 'SiteID' in columns:
            if len(strategy.site_ids) == 1:
                where_conditions.append("SiteID = :site_id")
                query_params['site_id'] = strategy.site_ids[0]
            else:
                placeholders = ', '.join([f':site_id_{i}' for i in range(len(strategy.site_ids))])
                where_conditions.append(f"SiteID IN ({placeholders})")
                query_params.update({f'site_id_{i}': site_id for i, site_id in enumerate(strategy.site_ids)})

        # Add delta sync filtering
        if strategy.use_delta_sync and strategy.last_sync and 'dUpdated' in columns:
            where_conditions.append("(dUpdated > :last_sync OR dUpdated IS NULL)")
            query_params['last_sync'] = strategy.last_sync

        # Build final WHERE clause
        where_clause = ""
        if where_conditions:
            where_clause = " WHERE " + " AND ".join(where_conditions)

        return where_clause, query_params

    def get_site_info_summary(self, onprem_engine: Engine, table_names: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get SiteID information summary for multiple tables

        Args:
            onprem_engine: SQLAlchemy engine for on-premises connection
            table_names: List of table names to analyze

        Returns:
            Dict[str, Dict[str, Any]]: SiteID information for each table
        """
        summary = {}

        for table_name in table_names:
            try:
                with onprem_engine.connect() as conn:
                    # Check if table exists and has SiteID
                    table_exists = conn.execute(text("""
                                                     SELECT COUNT(*)
                                                     FROM INFORMATION_SCHEMA.TABLES
                                                     WHERE TABLE_NAME = :table_name
                                                     """), {"table_name": table_name}).fetchone()[0] > 0

                    if not table_exists:
                        summary[table_name] = {"status": "not_found"}
                        continue

                    has_siteid = conn.execute(text("""
                                                   SELECT COUNT(*)
                                                   FROM INFORMATION_SCHEMA.COLUMNS
                                                   WHERE TABLE_NAME = :table_name
                                                     AND COLUMN_NAME = 'SiteID'
                                                   """), {"table_name": table_name}).fetchone()[0] > 0

                    if not has_siteid:
                        summary[table_name] = {"status": "no_siteid"}
                        continue

                    # FIXED: Get SiteID statistics with proper alias handling
                    site_info = conn.execute(text(f"""
                        SELECT COUNT(*) as TotalRows, 
                               COUNT(DISTINCT SiteID) as UniqueSites,
                               MIN(SiteID) as MinSiteID,
                               MAX(SiteID) as MaxSiteID
                        FROM [{table_name}] WHERE SiteID IS NOT NULL
                    """)).fetchone()

                    if site_info[0] == 0:
                        summary[table_name] = {"status": "no_data"}
                    else:
                        summary[table_name] = {
                            "status": "ok",
                            "row_count": site_info[0],
                            "unique_sites": site_info[1],
                            "min_site_id": site_info[2],
                            "max_site_id": site_info[3],
                            "single_site": site_info[1] == 1
                        }

            except Exception as e:
                summary[table_name] = {"status": "error", "error": str(e)}

        return summary


# Factory function for creating SyncStrategyManager instances
def create_sync_manager() -> SyncStrategyManager:
    """
    Factory function to create a SyncStrategyManager instance

    Returns:
        SyncStrategyManager: Configured sync strategy manager
    """
    return SyncStrategyManager()


# For testing the module independently
if __name__ == "__main__":
    print("Sync Manager Module Test")
    print("=" * 40)

    # Test sync strategy manager
    manager = SyncStrategyManager()

    # Test strategy creation
    print(f"Delta threshold: {manager.delta_threshold}")
    print(f"Auto-correct discrepancies: {manager.auto_correct_discrepancies}")
    print(f"No-delta tables: {manager.no_delta_tables}")
    print(f"Fetch batch size: {manager.fetch_batch_size}")
    print(f"Insert batch size: {manager.insert_batch_size}")
    print(f"Replace batch size: {manager.replace_batch_size}")

    # Test batch size optimization
    batch_sizes_small = manager.get_batch_sizes("small_table", 5000)
    batch_sizes_large = manager.get_batch_sizes("large_table", 500000)

    print(f"Small table batch sizes: {batch_sizes_small}")
    print(f"Large table batch sizes: {batch_sizes_large}")

    print("\n✅ Sync Manager module loaded successfully!")