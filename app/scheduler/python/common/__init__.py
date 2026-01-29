"""
Common Data Pipeline Module (API → Cache → SQL)

A framework-agnostic module for handling the complete data pipeline:
- Retrieve data from APIs (HTTP client)
- Cache for performance (Redis)
- Push to SQL databases (Azure SQL, MariaDB, PostgreSQL)

Optimized for large data payloads (1+ MB) with compression, chunking,
and memory-efficient processing.

Example Usage:
    from Scripts.common import DataLayerConfig, create_engine_from_config
    from Scripts.common import HTTPClient, CacheManager
    from Scripts.common import SessionManager, BaseRepository, UpsertOperations

    # Load configuration
    config = DataLayerConfig.from_env()

    # Initialize components
    http_client = HTTPClient()
    engine = create_engine_from_config(config.databases['postgresql'])
    session_manager = SessionManager(engine)

    # Use in pipeline
    with session_manager.session_scope() as session:
        repo = BaseRepository(session, MyModel)
        upsert_ops = UpsertOperations(session, config.databases['postgresql'].db_type)
        # Perform operations...
"""

__version__ = '1.0.0'

# Configuration
from .config import (
    DataLayerConfig,
    DatabaseConfig,
    DatabaseType,
    SOAPConfig,
)

# Database engine and session management
from .engine import (
    create_engine_from_config,
    get_pool_stats,
)

from .session import SessionManager

# Models
from .models import Base, BaseModel, TimestampMixin, SoftDeleteMixin
from .models import RentRoll, SiteInfo, LOSRange, PriceRange, UnitsInfo  # Data models
from .models import Discount, MoveInsAndMoveOuts  # Discount and Move-in/out models
from .models import FXRate, FXRateMonthly  # FX rate models
from .models import Tenant, Ledger, Charge  # CallCenterWs models
# Management Summary models
from .models import (
    MSDeposits, MSReceipts, MSConcessions, MSDiscounts, MSLiabilities,
    MSMisc, MSRentalActivity, MSDelinquency, MSUnpaid, MSRentLastChanged,
    MSVarFromStdRate, MSUnitActivity, MSAlerts, MSTenantStats, MSInsuranceStats,
)

# Operations
from .operations import (
    BaseRepository,
    UpsertOperations,
    BatchOperations,
)

# Upsert strategies
from .upsert_strategies import (
    UpsertStrategy,
    UpsertFactory,
    PostgreSQLUpsertStrategy,
    MariaDBUpsertStrategy,
    AzureSQLUpsertStrategy,
    delete_current_month_records,
)

# HTTP client
from .http_client import HTTPClient

# Cache manager
from .cache_manager import CacheManager, TTL_PROFILES

# SOAP client and reports
from .soap_client import SOAPClient, SOAPFaultError
from .soap_reports import SOAPReportClient, ReportConfig, REPORT_REGISTRY

# SugarCRM client
from .sugarcrm_client import SugarCRMClient

# Date utilities
from .date_utils import (
    get_first_day_of_month,
    get_last_day_of_month,
    is_current_month,
    get_extract_date,
    get_date_range_manual,
    get_date_range_auto,
    get_date_range_days_back,
    parse_date_string,
)

# Data utilities
from .data_utils import (
    convert_to_bool,
    convert_to_int,
    convert_to_decimal,
    convert_to_datetime,
    deduplicate_records,
)


__all__ = [
    # Version
    '__version__',

    # Configuration
    'DataLayerConfig',
    'DatabaseConfig',
    'DatabaseType',
    'SOAPConfig',

    # Database
    'create_engine_from_config',
    'get_pool_stats',
    'SessionManager',

    # Models
    'Base',
    'BaseModel',
    'TimestampMixin',
    'SoftDeleteMixin',
    'RentRoll',
    'UnitsInfo',
    'Discount',
    'MoveInsAndMoveOuts',
    'FXRate',
    'FXRateMonthly',
    'Tenant',
    'Ledger',
    'Charge',
    # Management Summary models
    'MSDeposits',
    'MSReceipts',
    'MSConcessions',
    'MSDiscounts',
    'MSLiabilities',
    'MSMisc',
    'MSRentalActivity',
    'MSDelinquency',
    'MSUnpaid',
    'MSRentLastChanged',
    'MSVarFromStdRate',
    'MSUnitActivity',
    'MSAlerts',
    'MSTenantStats',
    'MSInsuranceStats',

    # Operations
    'BaseRepository',
    'UpsertOperations',
    'BatchOperations',

    # Upsert strategies
    'UpsertStrategy',
    'UpsertFactory',
    'PostgreSQLUpsertStrategy',
    'MariaDBUpsertStrategy',
    'AzureSQLUpsertStrategy',
    'delete_current_month_records',

    # HTTP & Cache
    'HTTPClient',
    'CacheManager',
    'TTL_PROFILES',

    # SOAP Client & Reports
    'SOAPClient',
    'SOAPFaultError',
    'SOAPReportClient',
    'ReportConfig',
    'REPORT_REGISTRY',

    # SugarCRM Client
    'SugarCRMClient',

    # Date utilities
    'get_first_day_of_month',
    'get_last_day_of_month',
    'is_current_month',
    'get_extract_date',
    'get_date_range_manual',
    'get_date_range_auto',
    'get_date_range_days_back',
    'parse_date_string',

    # Data utilities
    'convert_to_bool',
    'convert_to_int',
    'convert_to_decimal',
    'convert_to_datetime',
    'deduplicate_records',
]
