"""
Data Control Module

Compares row counts and data integrity between on-premises and Azure databases.
Provides detailed analysis of sync accuracy and data discrepancies.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
from sqlalchemy import text
from sqlalchemy.engine import Engine
import os
import concurrent.futures
import tqdm


@dataclass
class TableComparison:
    """Data class for table comparison results"""
    table_name: str
    onprem_count: int
    azure_count: int
    difference: int
    percentage_diff: float
    status: str
    has_siteid: bool
    site_filtered: bool
    site_ids: List[int]
    onprem_max_updated: Optional[datetime]
    azure_max_updated: Optional[datetime]
    has_dupdated: bool
    sync_method: str


class DataControlManager:
    """Manages data control and comparison operations"""

    def __init__(self):
        """Initialize data control manager"""
        self.logger = logging.getLogger('DataControlManager')

        # Get configured SiteIDs for filtering
        self.configured_site_ids = self._get_configured_site_ids()

        # Thresholds for status determination
        self.warning_threshold = int(os.getenv('DATA_CONTROL_WARNING_THRESHOLD', '10'))
        self.error_threshold = int(os.getenv('DATA_CONTROL_ERROR_THRESHOLD', '100'))

        # Parallel processing configuration
        self.max_workers = int(os.getenv('SYNC_WORKERS', '4'))  # Use same workers as sync
        self.use_parallel = os.getenv('DATA_CONTROL_PARALLEL', 'true').lower() == 'true'

    def _get_configured_site_ids(self) -> List[int]:
        """Get configured SiteIDs from environment"""
        site_ids_env = os.getenv('SITE_IDS')
        single_site_env = os.getenv('SITE_ID')

        if site_ids_env:
            try:
                return [int(sid.strip()) for sid in site_ids_env.split(',') if sid.strip()]
            except ValueError:
                return []
        elif single_site_env:
            try:
                return [int(single_site_env)]
            except ValueError:
                return []
        else:
            return []

    def compare_all_tables(self, onprem_engine: Engine, azure_engine: Engine,
                          table_filter: List[str] = None) -> List[TableComparison]:
        """
        Compare row counts for all tables between on-premises and Azure using parallel processing

        Args:
            onprem_engine: SQLAlchemy engine for on-premises connection
            azure_engine: SQLAlchemy engine for Azure connection
            table_filter: Optional list of specific tables to compare

        Returns:
            List[TableComparison]: List of comparison results
        """
        self.logger.info("Starting data control comparison")

        # Get list of tables to compare
        tables_to_compare = self._get_tables_to_compare(azure_engine, table_filter)

        if not tables_to_compare:
            self.logger.warning("No tables found to compare")
            return []

        total_tables = len(tables_to_compare)
        self.logger.info(f"Comparing {total_tables} tables using {self.max_workers} workers")

        comparisons = []

        if self.use_parallel and total_tables > 1:
            # Use parallel processing for better performance
            comparisons = self._compare_tables_parallel(onprem_engine, azure_engine, tables_to_compare)
        else:
            # Sequential processing (fallback or single table)
            comparisons = self._compare_tables_sequential(onprem_engine, azure_engine, tables_to_compare)

        return comparisons

    def _compare_tables_parallel(self, onprem_engine: Engine, azure_engine: Engine,
                                tables_to_compare: List[Dict[str, str]]) -> List[TableComparison]:
        """Compare tables using parallel processing"""

        # Progress bar for parallel comparison
        pbar = tqdm.tqdm(
            total=len(tables_to_compare),
            desc=f"Data Control ({self.max_workers} workers)",
            unit="table", ncols=100, leave=False
        )

        def compare_single_table_worker(table_info: Dict[str, str]) -> TableComparison:
            """Worker function for parallel table comparison"""
            table_name = table_info['table_name']
            sync_method = table_info.get('sync_method', 'UNKNOWN')

            try:
                # Each worker gets its own connection
                return self._compare_single_table(onprem_engine, azure_engine, table_name, sync_method)

            except Exception as e:
                self.logger.error(f"Error comparing table {table_name}: {e}")
                # Return error comparison result
                return TableComparison(
                    table_name=table_name,
                    onprem_count=0,
                    azure_count=0,
                    difference=0,
                    percentage_diff=0.0,
                    status="ERROR",
                    has_siteid=False,
                    site_filtered=False,
                    site_ids=[],
                    onprem_max_updated=None,
                    azure_max_updated=None,
                    has_dupdated=False,
                    sync_method=sync_method
                )

        comparisons = []

        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all table comparison jobs
            future_to_table = {
                executor.submit(compare_single_table_worker, table_info): table_info
                for table_info in tables_to_compare
            }

            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_table):
                table_info = future_to_table[future]
                try:
                    comparison = future.result()
                    comparisons.append(comparison)
                    pbar.update(1)
                    pbar.set_postfix_str(f"✓ {comparison.table_name}")

                except Exception as e:
                    table_name = table_info['table_name']
                    self.logger.error(f"Worker error for {table_name}: {e}")
                    pbar.update(1)
                    pbar.set_postfix_str(f"✗ {table_name}")

        pbar.close()
        return comparisons

    def _compare_tables_sequential(self, onprem_engine: Engine, azure_engine: Engine,
                                  tables_to_compare: List[Dict[str, str]]) -> List[TableComparison]:
        """Compare tables sequentially (fallback method)"""

        comparisons = []

        # Progress bar for sequential comparison
        pbar = tqdm.tqdm(
            tables_to_compare,
            desc="Data Control (sequential)",
            unit="table", ncols=100, leave=False
        )

        for table_info in pbar:
            table_name = table_info['table_name']
            sync_method = table_info.get('sync_method', 'UNKNOWN')

            pbar.set_postfix_str(f"Checking {table_name}")

            try:
                comparison = self._compare_single_table(
                    onprem_engine, azure_engine, table_name, sync_method
                )
                comparisons.append(comparison)
                pbar.set_postfix_str(f"✓ {table_name}")

            except Exception as e:
                self.logger.error(f"Error comparing table {table_name}: {e}")
                # Create error comparison result
                comparisons.append(TableComparison(
                    table_name=table_name,
                    onprem_count=0,
                    azure_count=0,
                    difference=0,
                    percentage_diff=0.0,
                    status="ERROR",
                    has_siteid=False,
                    site_filtered=False,
                    site_ids=[],
                    onprem_max_updated=None,
                    azure_max_updated=None,
                    has_dupdated=False,
                    sync_method=sync_method
                ))
                pbar.set_postfix_str(f"✗ {table_name}")

        pbar.close()
        return comparisons

    def _get_tables_to_compare(self, azure_engine: Engine, table_filter: List[str] = None) -> List[Dict[str, str]]:
        """Get list of tables to compare from sync configuration"""
        try:
            with azure_engine.connect() as conn:
                if table_filter:
                    # Filter to specific tables
                    placeholders = ', '.join([f':table_{i}' for i in range(len(table_filter))])
                    params = {f'table_{i}': table for i, table in enumerate(table_filter)}

                    query = text(f"""
                        SELECT TableName, SyncMethod 
                        FROM TableSyncConfig 
                        WHERE IsActive = 1 AND TableName IN ({placeholders})
                        ORDER BY TableName
                    """)
                    result = conn.execute(query, params)
                else:
                    # Get all active tables
                    query = text("""
                        SELECT TableName, SyncMethod 
                        FROM TableSyncConfig 
                        WHERE IsActive = 1
                        ORDER BY TableName
                    """)
                    result = conn.execute(query)

                return [{"table_name": row[0], "sync_method": row[1]} for row in result.fetchall()]

        except Exception as e:
            self.logger.warning(f"Could not get tables from sync config: {e}")
            # Fallback: get tables from information schema
            return self._get_tables_from_schema(azure_engine, table_filter)

    def _get_tables_from_schema(self, azure_engine: Engine, table_filter: List[str] = None) -> List[Dict[str, str]]:
        """Fallback method to get tables from information schema"""
        try:
            with azure_engine.connect() as conn:
                if table_filter:
                    placeholders = ', '.join([f':table_{i}' for i in range(len(table_filter))])
                    params = {f'table_{i}': table for i, table in enumerate(table_filter)}

                    query = text(f"""
                        SELECT TABLE_NAME 
                        FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME IN ({placeholders})
                        ORDER BY TABLE_NAME
                    """)
                    result = conn.execute(query, params)
                else:
                    query = text("""
                        SELECT TABLE_NAME 
                        FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_TYPE = 'BASE TABLE'
                        ORDER BY TABLE_NAME
                    """)
                    result = conn.execute(query)

                return [{"table_name": row[0], "sync_method": "UNKNOWN"} for row in result.fetchall()]

        except Exception as e:
            self.logger.error(f"Could not get tables from schema: {e}")
            return []

    def _compare_single_table(self, onprem_engine: Engine, azure_engine: Engine,
                             table_name: str, sync_method: str) -> TableComparison:
        """Compare a single table between on-premises and Azure"""

        # Check if table has SiteID column
        has_siteid = self._check_table_has_siteid(onprem_engine, table_name)

        # Check if table has dUpdated column
        has_dupdated = self._check_table_has_dupdated(onprem_engine, table_name)

        # Determine if we should apply SiteID filtering
        site_ids = self.configured_site_ids if has_siteid else []
        site_filtered = bool(site_ids and has_siteid)

        # Build SiteID filter for queries
        site_filter = ""
        site_params = {}

        if site_filtered:
            if len(site_ids) == 1:
                site_filter = " WHERE SiteID = :site_id"
                site_params = {"site_id": site_ids[0]}
            else:
                placeholders = ', '.join([f':site_id_{i}' for i in range(len(site_ids))])
                site_filter = f" WHERE SiteID IN ({placeholders})"
                site_params = {f'site_id_{i}': site_id for i, site_id in enumerate(site_ids)}

        # Get row counts
        onprem_count = self._get_table_count(onprem_engine, table_name, site_filter, site_params)
        azure_count = self._get_table_count(azure_engine, table_name, site_filter, site_params)

        # Get timestamp information if available
        onprem_max_updated = None
        azure_max_updated = None

        if has_dupdated:
            onprem_max_updated = self._get_max_timestamp(onprem_engine, table_name, site_filter, site_params)
            azure_max_updated = self._get_max_timestamp(azure_engine, table_name, site_filter, site_params)

        # Calculate difference and status
        difference = onprem_count - azure_count
        percentage_diff = (abs(difference) / max(onprem_count, 1)) * 100

        # Determine status
        if difference == 0:
            status = "PERFECT"
        elif abs(difference) <= self.warning_threshold:
            status = "GOOD"
        elif abs(difference) <= self.error_threshold:
            status = "WARNING"
        else:
            status = "ERROR"

        return TableComparison(
            table_name=table_name,
            onprem_count=onprem_count,
            azure_count=azure_count,
            difference=difference,
            percentage_diff=percentage_diff,
            status=status,
            has_siteid=has_siteid,
            site_filtered=site_filtered,
            site_ids=site_ids,
            onprem_max_updated=onprem_max_updated,
            azure_max_updated=azure_max_updated,
            has_dupdated=has_dupdated,
            sync_method=sync_method
        )

    def _check_table_has_siteid(self, engine: Engine, table_name: str) -> bool:
        """Check if table has SiteID column"""
        try:
            with engine.connect() as conn:
                return conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = :table_name AND COLUMN_NAME = 'SiteID'
                """), {"table_name": table_name}).fetchone()[0] > 0
        except:
            return False

    def _check_table_has_dupdated(self, engine: Engine, table_name: str) -> bool:
        """Check if table has dUpdated column"""
        try:
            with engine.connect() as conn:
                return conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = :table_name AND COLUMN_NAME = 'dUpdated'
                """), {"table_name": table_name}).fetchone()[0] > 0
        except:
            return False

    def _get_table_count(self, engine: Engine, table_name: str, site_filter: str, site_params: dict) -> int:
        """Get row count for a table with optional SiteID filtering"""
        try:
            with engine.connect() as conn:
                query = f"SELECT COUNT(*) FROM [{table_name}]{site_filter}"
                result = conn.execute(text(query), site_params)
                return result.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Error getting count for {table_name}: {e}")
            return 0

    def _get_max_timestamp(self, engine: Engine, table_name: str, site_filter: str, site_params: dict) -> Optional[datetime]:
        """Get MAX(dUpdated) for a table with optional SiteID filtering"""
        try:
            with engine.connect() as conn:
                base_query = f"SELECT MAX(dUpdated) FROM [{table_name}]"
                if site_filter:
                    query = base_query + site_filter
                else:
                    query = base_query

                result = conn.execute(text(query), site_params)
                return result.fetchone()[0]
        except Exception as e:
            self.logger.debug(f"Error getting max timestamp for {table_name}: {e}")
            return None

    def generate_summary_report(self, comparisons: List[TableComparison]) -> Dict[str, Any]:
        """Generate a summary report of all comparisons"""

        total_tables = len(comparisons)
        perfect_count = sum(1 for c in comparisons if c.status == "PERFECT")
        good_count = sum(1 for c in comparisons if c.status == "GOOD")
        warning_count = sum(1 for c in comparisons if c.status == "WARNING")
        error_count = sum(1 for c in comparisons if c.status == "ERROR")

        # Calculate totals
        total_onprem = sum(c.onprem_count for c in comparisons)
        total_azure = sum(c.azure_count for c in comparisons)
        total_difference = total_onprem - total_azure

        # Find biggest discrepancies
        biggest_discrepancies = sorted(
            [c for c in comparisons if c.status in ["WARNING", "ERROR"]],
            key=lambda x: abs(x.difference),
            reverse=True
        )[:5]

        # Tables with SiteID filtering
        site_filtered_count = sum(1 for c in comparisons if c.site_filtered)

        # Tables with delta sync capability
        delta_capable_count = sum(1 for c in comparisons if c.has_dupdated)

        return {
            "total_tables": total_tables,
            "perfect_count": perfect_count,
            "good_count": good_count,
            "warning_count": warning_count,
            "error_count": error_count,
            "total_onprem_rows": total_onprem,
            "total_azure_rows": total_azure,
            "total_difference": total_difference,
            "biggest_discrepancies": biggest_discrepancies,
            "site_filtered_count": site_filtered_count,
            "delta_capable_count": delta_capable_count,
            "accuracy_percentage": (perfect_count + good_count) / max(total_tables, 1) * 100,
            "configured_site_ids": self.configured_site_ids
        }

    def print_comparison_report(self, comparisons: List[TableComparison],
                               show_details: bool = True, show_timestamps: bool = False,
                               execution_time: float = None):
        """Print a formatted comparison report"""

        if not comparisons:
            print("❌ No tables to compare")
            return

        # Generate summary
        summary = self.generate_summary_report(comparisons)

        # Print header
        print(f"\n{'='*80}")
        print(f"📊 DATA CONTROL REPORT")
        print(f"{'='*80}")

        # Performance info
        if execution_time:
            tables_per_sec = len(comparisons) / max(execution_time, 0.1)
            print(f"⚡ PERFORMANCE:")
            print(f"   Execution time: {execution_time:.1f} seconds")
            print(f"   Tables/second: {tables_per_sec:.1f}")
            print(f"   Workers used: {self.max_workers}")
            print(f"   Parallel mode: {'✅ Enabled' if self.use_parallel else '❌ Disabled'}")

        # Print summary
        print(f"\n📈 SUMMARY:")
        print(f"   Tables compared: {summary['total_tables']}")
        print(f"   ✅ Perfect matches: {summary['perfect_count']}")
        print(f"   ✅ Good (≤{self.warning_threshold} diff): {summary['good_count']}")
        print(f"   ⚠️  Warnings (≤{self.error_threshold} diff): {summary['warning_count']}")
        print(f"   ❌ Errors (>{self.error_threshold} diff): {summary['error_count']}")
        print(f"   🎯 Overall accuracy: {summary['accuracy_percentage']:.1f}%")

        print(f"\n📊 TOTALS:")
        print(f"   OnPrem total rows: {summary['total_onprem_rows']:,}")
        print(f"   Azure total rows:  {summary['total_azure_rows']:,}")
        print(f"   Total difference:  {summary['total_difference']:+,}")

        if summary['configured_site_ids']:
            print(f"\n🎯 FILTERING:")
            print(f"   SiteID filtering: ENABLED")
            print(f"   Configured SiteIDs: {', '.join(map(str, summary['configured_site_ids']))}")
            print(f"   Tables with SiteID: {summary['site_filtered_count']}/{summary['total_tables']}")
        else:
            print(f"\n🎯 FILTERING: No SiteID filtering configured")

        print(f"\n⏱️  DELTA SYNC CAPABILITY:")
        print(f"   Tables with dUpdated: {summary['delta_capable_count']}/{summary['total_tables']}")

        # Show biggest discrepancies
        if summary['biggest_discrepancies']:
            print(f"\n🚨 BIGGEST DISCREPANCIES:")
            for i, comp in enumerate(summary['biggest_discrepancies'], 1):
                site_info = f" (SiteID filtered)" if comp.site_filtered else ""
                print(f"   {i}. {comp.table_name}{site_info}: {comp.difference:+,} rows ({comp.percentage_diff:.1f}%)")

        # Detailed table listing
        if show_details:
            print(f"\n📋 DETAILED COMPARISON:")
            print(f"{'Table':<25} {'OnPrem':<12} {'Azure':<12} {'Diff':<8} {'%':<8} {'Status':<8} {'Info':<20}")
            print("-" * 95)

            # Sort by status (errors first) then by absolute difference
            sorted_comparisons = sorted(comparisons, key=lambda x: (
                {"ERROR": 0, "WARNING": 1, "GOOD": 2, "PERFECT": 3}[x.status],
                -abs(x.difference)
            ))

            for comp in sorted_comparisons:
                # Status emoji
                status_emoji = {
                    "PERFECT": "✅",
                    "GOOD": "✅",
                    "WARNING": "⚠️",
                    "ERROR": "❌"
                }[comp.status]

                # Build info string
                info_parts = []
                if comp.site_filtered:
                    info_parts.append(f"SiteID({len(comp.site_ids)})")
                if comp.has_dupdated:
                    info_parts.append("dUpdated")
                info_parts.append(comp.sync_method)
                info_str = ", ".join(info_parts)

                print(f"{comp.table_name:<25} {comp.onprem_count:<12,} {comp.azure_count:<12,} "
                      f"{comp.difference:<+8,} {comp.percentage_diff:<7.1f}% {status_emoji}{comp.status:<7} {info_str:<20}")

        # Timestamp comparison if requested
        if show_timestamps:
            tables_with_timestamps = [c for c in comparisons if c.has_dupdated and (c.onprem_max_updated or c.azure_max_updated)]

            if tables_with_timestamps:
                print(f"\n⏱️  TIMESTAMP COMPARISON:")
                print(f"{'Table':<25} {'OnPrem MAX':<20} {'Azure MAX':<20} {'Status':<15}")
                print("-" * 85)

                for comp in tables_with_timestamps:
                    onprem_ts = comp.onprem_max_updated.strftime('%Y-%m-%d %H:%M:%S') if comp.onprem_max_updated else 'NULL'
                    azure_ts = comp.azure_max_updated.strftime('%Y-%m-%d %H:%M:%S') if comp.azure_max_updated else 'NULL'

                    # Compare timestamps
                    if comp.onprem_max_updated and comp.azure_max_updated:
                        if comp.onprem_max_updated > comp.azure_max_updated:
                            ts_status = "OnPrem newer"
                        elif comp.azure_max_updated > comp.onprem_max_updated:
                            ts_status = "Azure newer"
                        else:
                            ts_status = "Same"
                    else:
                        ts_status = "Missing data"

                    print(f"{comp.table_name:<25} {onprem_ts:<20} {azure_ts:<20} {ts_status:<15}")

        print(f"\n{'='*80}")


def create_data_control_manager() -> DataControlManager:
    """Factory function to create a DataControlManager instance"""
    return DataControlManager()