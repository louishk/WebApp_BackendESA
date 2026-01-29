"""
SOAP Report Library Module

High-level API for SOAP-based reports with built-in report configurations.
Provides type-safe methods for common reports and a generic interface for custom reports.

Key Features:
- Centralized report registry (REPORT_REGISTRY)
- Type-safe report methods (get_rent_roll, get_occupancy, etc.)
- Automatic parameter validation
- Date formatting for datetime parameters

Example Usage:
    from Scripts.common import SOAPClient, SOAPReportClient
    from datetime import datetime

    # Initialize base SOAP client
    soap_client = SOAPClient(
        base_url="https://api.example.com/Service.asmx",
        corp_code="C234",
        api_key="CODIGO3E57HV9VJER9WY",
        corp_password="password"
    )

    # Initialize report client
    report_client = SOAPReportClient(soap_client)

    # Fetch rent roll report
    results = report_client.get_rent_roll(
        location_code="L001",
        start_date=datetime(2025, 12, 1),
        end_date=datetime(2025, 12, 31)
    )
"""

from dataclasses import dataclass
from typing import List, Dict, Any
from datetime import datetime


@dataclass
class ReportConfig:
    """
    Configuration for a specific SOAP report.

    Attributes:
        operation: SOAP operation name (e.g., "RentRoll")
        soap_action: SOAP action header value
        namespace: XML namespace for the operation
        result_tag: XML tag name to extract from response
        required_params: List of required parameter names
    """
    operation: str
    soap_action: str
    namespace: str
    result_tag: str
    required_params: List[str]


# Report Registry: All 51 SOAP report endpoints
# Auth params (sCorpCode, sCorpUserName, sCorpPassword) are auto-injected by SOAPClient
REPORT_REGISTRY = {
    # =========================================================================
    # Tenant Lists (7 reports)
    # =========================================================================
    'rent_roll': ReportConfig(
        operation='RentRoll',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/RentRoll',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='RentRoll',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'tenant_list_complete': ReportConfig(
        operation='TenantListComplete',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/TenantListComplete',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='TenantListComplete',
        required_params=['sLocationCode']
    ),
    'tenant_list_complete_v2': ReportConfig(
        operation='TenantListComplete_v2',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/TenantListComplete_v2',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='TenantListComplete',
        required_params=['sLocationCode', 'lngLastTimePolled']
    ),
    'tenant_list_complete_moved_in_only': ReportConfig(
        operation='TenantListCompleteMovedInTenantsOnly',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/TenantListCompleteMovedInTenantsOnly',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='TenantListComplete',
        required_params=['sLocationCode', 'sUsagePassword']
    ),
    'charges_and_payments_complete': ReportConfig(
        operation='ChargesAndPaymentsComplete',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/ChargesAndPaymentsComplete',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='ChargesAndPaymentsComplete',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'past_due_custom': ReportConfig(
        operation='PastDueCustom',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/PastDueCustom',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='PastDueCustom',
        required_params=['sLocationCode', 'sUsagePassword']
    ),
    'tenant_rent_change_history': ReportConfig(
        operation='TenantRentChangeHistory',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/TenantRentChangeHistory',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='TenantRentChangeHistory',
        required_params=['sLocationCode', 'dReportDateEnd']
    ),

    # =========================================================================
    # Financial (8 reports)
    # =========================================================================
    'income_analysis': ReportConfig(
        operation='IncomeAnalysis',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/IncomeAnalysis',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='IncomeAnalysis',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'accounts_receivable': ReportConfig(
        operation='AccountsReceivable',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/AccountsReceivable',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='AccountsReceivable',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'financial_summary': ReportConfig(
        operation='FinancialSummary',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/FinancialSummary',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='Charge',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'daily_deposits': ReportConfig(
        operation='DailyDeposits',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/DailyDeposits',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='DailyDeposits',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'daily_deposits_pre_close': ReportConfig(
        operation='DailyDepositsPreClose',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/DailyDepositsPreClose',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='DailyDepositsPreClose',
        required_params=['sLocationCode']
    ),
    'receipts': ReportConfig(
        operation='Receipts',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/Receipts',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='Receipts',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'past_due_balances': ReportConfig(
        operation='PastDueBalances',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/PastDueBalances',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='PastDueBalances',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'general_journal_entries': ReportConfig(
        operation='GeneralJournalEntries',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/GeneralJournalEntries',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='GeneralJournalEntries',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),

    # =========================================================================
    # Insurance (9 reports)
    # =========================================================================
    'insurance_activity': ReportConfig(
        operation='InsuranceActivity',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/InsuranceActivity',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='Insur_InsuranceActivity',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'insurance_roll': ReportConfig(
        operation='InsuranceRoll',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/InsuranceRoll',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='InsuranceRoll',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'insurance_activity_for_api': ReportConfig(
        operation='InsuranceActivityForAPI',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/InsuranceActivityForAPI',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='InsuranceActivityForAPI',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'insurance_custom_report': ReportConfig(
        operation='InsuranceCustomReport',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/InsuranceCustomReport',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='InsuranceCustomReport',
        required_params=['sLocationCode']
    ),
    'insurance_custom_activity_for_api': ReportConfig(
        operation='InsuranceCustomActivityForAPI',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/InsuranceCustomActivityForAPI',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='InsuranceCustomActivityForAPI',
        required_params=['sLocationCodesCommaDelimited', 'sInsProvider', 'dReportDateStart', 'dReportDateEnd']
    ),
    'insurance_statement': ReportConfig(
        operation='InsuranceStatement',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/InsuranceStatement',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='InsuranceStatement',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'insurance_summary': ReportConfig(
        operation='InsuranceSummary',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/InsuranceSummary',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='InsuranceSummary',
        required_params=['sLocationCodesCommaDelimited', 'dReportDateStart', 'dReportDateEnd']
    ),
    'insurance_policy_num_update': ReportConfig(
        operation='InsurancePolicyNumUpdate',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/InsurancePolicyNumUpdate',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='InsurancePolicyNumUpdate',
        required_params=['sLocationCode', 'sUnitName', 'sPolicyNum']
    ),
    'insurance_overview': ReportConfig(
        operation='InsuranceOverview',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/InsuranceOverview',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='InsuranceOverview',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),

    # =========================================================================
    # Management Summary (4 reports)
    # =========================================================================
    'management_summary': ReportConfig(
        operation='ManagementSummary',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/ManagementSummary',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='NewDataSet',  # Returns nested structure with all tables
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'consolidated_management_summary': ReportConfig(
        operation='ConsolidatedManagementSummary',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/ConsolidatedManagementSummary',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='ConsolidatedManagementSummary',
        required_params=['sLocationCodesCommaDelimited', 'dReportDateStart', 'dReportDateEnd']
    ),
    'management_history': ReportConfig(
        operation='ManagementHistory',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/ManagementHistory',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='ManagementHistory',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'occupancy_statistics': ReportConfig(
        operation='OccupancyStatistics',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/OccupancyStatistics',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='OccupancyStatistics',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),

    # =========================================================================
    # Activity Reports (5 reports)
    # =========================================================================
    'rental_activity': ReportConfig(
        operation='RentalActivity',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/RentalActivity',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='RentalActivity',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'move_ins_and_move_outs': ReportConfig(
        operation='MoveInsAndMoveOuts',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/MoveInsAndMoveOuts',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='UnitMoveInsAndMoveOuts',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'exceptions': ReportConfig(
        operation='Exceptions',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/Exceptions',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='Exceptions',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'bad_debts': ReportConfig(
        operation='BadDebts',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/BadDebts',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='BadDebts',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'bad_debt_written_off': ReportConfig(
        operation='BadDebtWrittenOff',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/BadDebtWrittenOff',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='BadDebtWrittenOff',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),

    # =========================================================================
    # Marketing (3 reports)
    # =========================================================================
    'marketing_summary': ReportConfig(
        operation='MarketingSummary',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/MarketingSummary',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='MarketingSummary',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'marketing_roll': ReportConfig(
        operation='MarketingRoll',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/MarketingRoll',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='MarketingRoll',
        required_params=['sLocationCode', 'dReportDateEnd', 'iFilter']
    ),
    'inquiry_tracking': ReportConfig(
        operation='InquiryTracking',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/InquiryTracking',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='InquiryTracking',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),

    # =========================================================================
    # Merchandise (2 reports)
    # =========================================================================
    'merchandise_summary': ReportConfig(
        operation='MerchandiseSummary',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/MerchandiseSummary',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='MerchandiseSummary',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'merchandise_activity': ReportConfig(
        operation='MerchandiseActivity',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/MerchandiseActivity',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='MerchandiseActivity',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),

    # =========================================================================
    # Units Pricing (3 reports)
    # =========================================================================
    'unit_price_list': ReportConfig(
        operation='UnitPriceList',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/UnitPriceList',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='UnitPriceList',
        required_params=['sLocationCode']
    ),
    'site_rates': ReportConfig(
        operation='SiteRates',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/SiteRates',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='SiteRates',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'unit_payments': ReportConfig(
        operation='UnitPayments',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/UnitPayments',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='UnitPayments',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd', 'sUnitName']
    ),

    # =========================================================================
    # System Utilities (7 reports)
    # =========================================================================
    'discounts': ReportConfig(
        operation='Discounts',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/Discounts',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='Mgmt_Discounts',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'misc_reporting_tables': ReportConfig(
        operation='MiscReportingTables',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/MiscReportingTables',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='MiscReportingTables',
        required_params=['sLocationCode']
    ),
    'misc_reporting_tables_v2': ReportConfig(
        operation='MiscReportingTables_v2',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/MiscReportingTables_v2',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='MiscReportingTables',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'scheduled_auctions': ReportConfig(
        operation='ScheduledAuctions',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/ScheduledAuctions',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='ScheduledAuctions',
        required_params=['sLocationCode']
    ),
    'scheduled_move_outs': ReportConfig(
        operation='ScheduledMoveOuts',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/ScheduledMoveOuts',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='ScheduledMoveOuts',
        required_params=['sLocationCode']
    ),
    # NOTE: site_list_global removed - requires special sUsagePassword not available
    'mob_dispatch_sched': ReportConfig(
        operation='MobDispatchSched',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/MobDispatchSched',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='MobDispatchSched',
        required_params=['sLocationCodesCommaDelimited', 'dReportDateStart', 'dReportDateEnd']
    ),

    # =========================================================================
    # Data Export (9 reports)
    # =========================================================================
    'data_mine_sp': ReportConfig(
        operation='DataMineSP',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/DataMineSP',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='DataMineSP',
        required_params=['sLocationCode', 'sStoredProcedureName', 'dReportDateStart', 'dReportDateEnd']
    ),
    'rms_data_carve': ReportConfig(
        operation='RMSDataCarve',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/RMSDataCarve',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='RT',
        required_params=['sLocationCode', 'dDateStart', 'dDateEnd', 'iFilter']
    ),
    'rms_data_carve_v2': ReportConfig(
        operation='RMSDataCarve_v2',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/RMSDataCarve_v2',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='RT',
        required_params=['sLocationCode', 'dDateStart', 'dDateEnd', 'iFilter', 'iRatesTaxInclusive']
    ),
    'rms_data_carve_v3': ReportConfig(
        operation='RMSDataCarve_v3',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/RMSDataCarve_v3',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='RT',
        required_params=['sLocationCode', 'dDateStart', 'dDateEnd', 'iFilter', 'iRatesTaxInclusive']
    ),
    'rms_data_carve_v4': ReportConfig(
        operation='RMSDataCarve_v4',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/RMSDataCarve_v4',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='RT',
        required_params=['sLocationCode', 'dDateStart', 'dDateEnd', 'iFilter', 'iRatesTaxInclusive']
    ),
    'factura_retrieve': ReportConfig(
        operation='FacturaRetrieve',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/FacturaRetrieve',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='FacturaRetrieve',
        required_params=['sLocationCode', 'dDateStart', 'dDateEnd', 'iFilter']
    ),
    'erp_transactions': ReportConfig(
        operation='ERPTransactionsByDateAndLedgerID',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/ERPTransactionsByDateAndLedgerID',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='ERPTransactions',
        required_params=['sLocationCode', 'dDateStart', 'dDateEnd', 'iLedgerID']
    ),
    'data_carve': ReportConfig(
        operation='DataCarve',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/DataCarve',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='DataCarve',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),
    'dashboard_tables_refresh': ReportConfig(
        operation='DashboardTablesRefresh',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/DashboardTablesRefresh',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='DashboardTablesRefresh',
        required_params=['sLocationCode', 'dReportDateStart', 'dReportDateEnd']
    ),

    # =========================================================================
    # Custom Reports (2 reports)
    # =========================================================================
    'custom_report_list_by_corp': ReportConfig(
        operation='CustomReportListByCorp',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/CustomReportListByCorp',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='CustomReports',
        required_params=[]
    ),
    'custom_report_by_id': ReportConfig(
        operation='CustomReportByReportID',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/CustomReportByReportID',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='CustomReport',
        required_params=['sLocationCode', 'ReportID', 'dReportDateStart', 'dReportDateEnd']
    ),

    # =========================================================================
    # Data Exchange (3 reports)
    # =========================================================================
    'de_get_index': ReportConfig(
        operation='DEGetIndex',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/DEGetIndex',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='DEGetIndex',
        required_params=['password']
    ),
    'de_start_job': ReportConfig(
        operation='DEStartJob',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/DEStartJob',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='DEStartJob',
        required_params=['password', 'file_name', 'trace_id']
    ),
    'de_get_job_status': ReportConfig(
        operation='DEGetJobStatus',
        soap_action='http://tempuri.org/CallCenterWs/ReportingWs/DEGetJobStatus',
        namespace='http://tempuri.org/CallCenterWs/ReportingWs',
        result_tag='DEGetJobStatus',
        required_params=['password', 'file_name', 'job_id']
    ),

    # =========================================================================
    # CallCenterWs Endpoints (operational, per-tenant/ledger queries)
    # NOTE: These use CallCenterWs service, not ReportingWs
    # =========================================================================
    'tenant_list': ReportConfig(
        operation='TenantList',
        soap_action='http://tempuri.org/CallCenterWs/CallCenterWs/TenantList',
        namespace='http://tempuri.org/CallCenterWs/CallCenterWs',
        result_tag='Table',
        required_params=['sLocationCode', 'sTenantFirstName', 'sTenantLastName']
    ),
    'ledgers_by_tenant_id_v3': ReportConfig(
        operation='LedgersByTenantID_v3',
        soap_action='http://tempuri.org/CallCenterWs/CallCenterWs/LedgersByTenantID_v3',
        namespace='http://tempuri.org/CallCenterWs/CallCenterWs',
        result_tag='Ledgers',
        required_params=['sLocationCode', 'sTenantID']
    ),
    'charges_all_by_ledger_id': ReportConfig(
        operation='ChargesAllByLedgerID',
        soap_action='http://tempuri.org/CallCenterWs/CallCenterWs/ChargesAllByLedgerID',
        namespace='http://tempuri.org/CallCenterWs/CallCenterWs',
        result_tag='Table',
        required_params=['sLocationCode', 'ledgerId']
    ),
}


class SOAPReportClient:
    """
    High-level client for SOAP reports with built-in report configurations.

    Provides type-safe methods for common reports and a generic interface
    for custom reports using the REPORT_REGISTRY.
    """

    def __init__(self, soap_client):
        """
        Initialize report client with base SOAP client.

        Args:
            soap_client: Instance of SOAPClient with configured authentication
        """
        self.soap_client = soap_client

    def get_rent_roll(
        self,
        location_code: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Fetch RentRoll report.

        Args:
            location_code: Location code (e.g., "L001", "L002", ...)
            start_date: Report start date
            end_date: Report end date

        Returns:
            List of rent roll records as dictionaries

        Example:
            results = report_client.get_rent_roll(
                location_code="L001",
                start_date=datetime(2025, 12, 1),
                end_date=datetime(2025, 12, 31)
            )
            for row in results:
                print(row['TenantName'], row['UnitNumber'], row['MonthlyRent'])
        """
        config = REPORT_REGISTRY['rent_roll']

        parameters = {
            'sLocationCode': location_code,
            'dReportDateStart': start_date.strftime('%Y-%m-%dT00:00:00'),
            'dReportDateEnd': end_date.strftime('%Y-%m-%dT00:00:00')
        }

        return self.soap_client.call(
            operation=config.operation,
            parameters=parameters,
            soap_action=config.soap_action,
            namespace=config.namespace,
            result_tag=config.result_tag
        )

    def get_occupancy(
        self,
        location_code: str,
        report_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Fetch Occupancy report.

        Args:
            location_code: Location code (e.g., "L001", "L002", ...)
            report_date: Report date

        Returns:
            List of occupancy records as dictionaries

        Example:
            results = report_client.get_occupancy(
                location_code="L001",
                report_date=datetime(2025, 12, 31)
            )
            for row in results:
                print(row['UnitNumber'], row['Status'], row['SquareFeet'])
        """
        config = REPORT_REGISTRY['occupancy']

        parameters = {
            'sLocationCode': location_code,
            'dReportDate': report_date.strftime('%Y-%m-%dT00:00:00')
        }

        return self.soap_client.call(
            operation=config.operation,
            parameters=parameters,
            soap_action=config.soap_action,
            namespace=config.namespace,
            result_tag=config.result_tag
        )

    def call_report(
        self,
        report_name: str,
        parameters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Generic method to call any registered report.

        Args:
            report_name: Report name from REPORT_REGISTRY (e.g., 'rent_roll', 'occupancy')
            parameters: Report-specific parameters (auth is auto-injected)

        Returns:
            List of report records as dictionaries

        Raises:
            ValueError: If report_name is not in REPORT_REGISTRY or missing required parameters

        Example:
            results = report_client.call_report(
                report_name='rent_roll',
                parameters={
                    'sLocationCode': 'L001',
                    'dReportDateStart': '2025-12-01T00:00:00',
                    'dReportDateEnd': '2025-12-31T00:00:00'
                }
            )
        """
        if report_name not in REPORT_REGISTRY:
            available = ", ".join(REPORT_REGISTRY.keys())
            raise ValueError(
                f"Unknown report: '{report_name}'. "
                f"Available reports: {available}"
            )

        config = REPORT_REGISTRY[report_name]

        # Validate required parameters
        missing = [p for p in config.required_params if p not in parameters]
        if missing:
            raise ValueError(
                f"Missing required parameters for report '{report_name}': "
                f"{', '.join(missing)}"
            )

        return self.soap_client.call(
            operation=config.operation,
            parameters=parameters,
            soap_action=config.soap_action,
            namespace=config.namespace,
            result_tag=config.result_tag
        )

    def list_available_reports(self) -> List[str]:
        """
        Get list of available report names.

        Returns:
            List of report names registered in REPORT_REGISTRY
        """
        return list(REPORT_REGISTRY.keys())

    def get_report_config(self, report_name: str) -> ReportConfig:
        """
        Get configuration for a specific report.

        Args:
            report_name: Report name from REPORT_REGISTRY

        Returns:
            ReportConfig for the specified report

        Raises:
            ValueError: If report_name is not in REPORT_REGISTRY
        """
        if report_name not in REPORT_REGISTRY:
            available = ", ".join(REPORT_REGISTRY.keys())
            raise ValueError(
                f"Unknown report: '{report_name}'. "
                f"Available reports: {available}"
            )

        return REPORT_REGISTRY[report_name]
