"""
Management Summary to SQL Pipeline

Fetches ManagementSummary data from SOAP API and pushes to PostgreSQL database.

The ManagementSummary endpoint returns multiple tables:
- Deposits: Daily/MTD/YTD deposit amounts by payment type
- Receipts: Receipts by category (Rent, Insurance, Late Fee, etc.)
- Concessions: Concession amounts by category
- Discounts: Discount amounts by category
- Liabilities: Liability counts and amounts
- Misc: Various operational metrics
- RentalActivity: Occupancy and revenue metrics
- Delinquency: Delinquency by period type
- Unpaid: Unpaid by aging bucket
- RentLastChanged: Rent change recency categories
- VarFromStdRate: Variance from standard rate categories
- UnitActivity: Activity counts by type
- Alerts: Alert counts by category
- TenantStats: Tenant statistics
- InsuranceStats: Insurance statistics

Features:
- Two modes: manual (specify date range) and auto (previous + current month)
- Smart extract_date: closed months use last day, current month uses today
- Fetches data for multiple locations
- Uses merge/upsert with appropriate composite keys for each table
- Processes in chunks for large datasets

Usage:
    # Manual mode - specify date range (for historical loads)
    python managementsummary_to_sql.py --mode manual --start 2025-01 --end 2025-12

    # Automatic mode - previous month + current month (for scheduler)
    python managementsummary_to_sql.py --mode auto

Configuration (in .env):
    - SOAP_* : SOAP API connection settings
    - MANAGEMENTSUMMARY_LOCATION_CODES: Comma-separated location codes
    - MANAGEMENTSUMMARY_SQL_CHUNK_SIZE: Batch size for upsert (default: 500)
"""

import argparse
from datetime import datetime, date
from typing import List, Dict, Any, Tuple
from decouple import config as env_config, Csv
from tqdm import tqdm

from common import (
    DataLayerConfig,
    SOAPClient,
    create_engine_from_config,
    SessionManager,
    UpsertOperations,
    Base,
    # Management Summary models
    MSDeposits,
    MSReceipts,
    MSConcessions,
    MSDiscounts,
    MSLiabilities,
    MSMisc,
    MSRentalActivity,
    MSDelinquency,
    MSUnpaid,
    MSRentLastChanged,
    MSVarFromStdRate,
    MSUnitActivity,
    MSAlerts,
    MSTenantStats,
    MSInsuranceStats,
    # Date utilities
    get_last_day_of_month,
    get_extract_date,
    get_date_range_manual,
    get_date_range_auto,
    # Data utilities
    convert_to_bool,
    convert_to_int,
    convert_to_decimal,
    convert_to_datetime,
)


# =============================================================================
# Model Configuration - Maps table names to models and their constraint columns
# =============================================================================

MODEL_CONFIG = {
    'Deposits': {
        'model': MSDeposits,
        'constraints': ['extract_date', 'SiteID'],
    },
    'Receipts': {
        'model': MSReceipts,
        'constraints': ['extract_date', 'SiteID', 'SortID'],
    },
    'Concessions': {
        'model': MSConcessions,
        'constraints': ['extract_date', 'SiteID', 'iSortOrder'],
    },
    'Discounts': {
        'model': MSDiscounts,
        'constraints': ['extract_date', 'SiteID', 'iSortOrder', 'bNeverExpires'],
    },
    'Liabilities': {
        'model': MSLiabilities,
        'constraints': ['extract_date', 'SiteID'],
    },
    'Misc': {
        'model': MSMisc,
        'constraints': ['extract_date', 'SiteID'],
    },
    'RentalActivity': {
        'model': MSRentalActivity,
        'constraints': ['extract_date', 'SiteID'],
    },
    'Delinquency': {
        'model': MSDelinquency,
        'constraints': ['extract_date', 'SiteID', 'Period'],
    },
    'Unpaid': {
        'model': MSUnpaid,
        'constraints': ['extract_date', 'SiteID', 'Period'],
    },
    'RentLastChanged': {
        'model': MSRentLastChanged,
        'constraints': ['extract_date', 'SiteID', 'SortID'],
    },
    'VarFromStdRate': {
        'model': MSVarFromStdRate,
        'constraints': ['extract_date', 'SiteID', 'SortID'],
    },
    'UnitActivity': {
        'model': MSUnitActivity,
        'constraints': ['extract_date', 'SiteID', 'SortID'],
    },
    'Alerts': {
        'model': MSAlerts,
        'constraints': ['extract_date', 'SiteID', 'iSortOrder'],
    },
    'TenantStats': {
        'model': MSTenantStats,
        'constraints': ['extract_date', 'SiteID'],
    },
    'InsuranceStats': {
        'model': MSInsuranceStats,
        'constraints': ['extract_date', 'SiteID'],
    },
}


# =============================================================================
# Transformation Functions
# =============================================================================

def transform_deposits(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform Deposits record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'DCash': convert_to_decimal(record.get('DCash')),
        'DCheck': convert_to_decimal(record.get('DCheck')),
        'DCharge': convert_to_decimal(record.get('DCharge')),
        'DACH': convert_to_decimal(record.get('DACH')),
        'DDebit': convert_to_decimal(record.get('DDebit')),
        'DDepTot': convert_to_decimal(record.get('DDepTot')),
        'DMiscDep': convert_to_decimal(record.get('DMiscDep')),
        'DNet': convert_to_decimal(record.get('DNet')),
        'MCash': convert_to_decimal(record.get('MCash')),
        'MCheck': convert_to_decimal(record.get('MCheck')),
        'MCharge': convert_to_decimal(record.get('MCharge')),
        'MACH': convert_to_decimal(record.get('MACH')),
        'MDebit': convert_to_decimal(record.get('MDebit')),
        'MDepTot': convert_to_decimal(record.get('MDepTot')),
        'MMiscDep': convert_to_decimal(record.get('MMiscDep')),
        'MNet': convert_to_decimal(record.get('MNet')),
        'YCash': convert_to_decimal(record.get('YCash')),
        'YCheck': convert_to_decimal(record.get('YCheck')),
        'YCharge': convert_to_decimal(record.get('YCharge')),
        'YACH': convert_to_decimal(record.get('YACH')),
        'YDebit': convert_to_decimal(record.get('YDebit')),
        'YDepTot': convert_to_decimal(record.get('YDepTot')),
        'YMiscDep': convert_to_decimal(record.get('YMiscDep')),
        'YNet': convert_to_decimal(record.get('YNet')),
    }


def transform_receipts(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform Receipts record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'SortID': convert_to_int(record.get('SortID')),
        'sDesc': record.get('sDesc'),
        'dcD': convert_to_decimal(record.get('dcD')),
        'dcM': convert_to_decimal(record.get('dcM')),
        'dcY': convert_to_decimal(record.get('dcY')),
    }


def transform_concessions(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform Concessions record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'iSortOrder': convert_to_int(record.get('iSortOrder')),
        'sCatName': record.get('sCatName'),
        'DAmt': convert_to_decimal(record.get('DAmt')),
        'MAmt': convert_to_decimal(record.get('MAmt')),
        'YAmt': convert_to_decimal(record.get('YAmt')),
    }


def transform_discounts(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform Discounts record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'iSortOrder': convert_to_int(record.get('iSortOrder')),
        'bNeverExpires': convert_to_bool(record.get('bNeverExpires')),
        'sCatName': record.get('sCatName'),
        'DAmt': convert_to_decimal(record.get('DAmt')),
        'MAmt': convert_to_decimal(record.get('MAmt')),
        'YAmt': convert_to_decimal(record.get('YAmt')),
    }


def transform_liabilities(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform Liabilities record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'iCountRent': convert_to_int(record.get('iCountRent')),
        'dcAmtRent': convert_to_decimal(record.get('dcAmtRent')),
        'iCountInsurance': convert_to_int(record.get('iCountInsurance')),
        'dcAmtInsurance': convert_to_decimal(record.get('dcAmtInsurance')),
        'iCountRecurring': convert_to_int(record.get('iCountRecurring')),
        'dcAmtRecurring': convert_to_decimal(record.get('dcAmtRecurring')),
        'iCountDeposit': convert_to_int(record.get('iCountDeposit')),
        'dcAmtDeposit': convert_to_decimal(record.get('dcAmtDeposit')),
    }


def transform_misc(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform Misc record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'sDaily': record.get('sDaily'),
        'sMTD': record.get('sMTD'),
        'sYTD': record.get('sYTD'),
        'dDStrt': convert_to_datetime(record.get('dDStrt')),
        'dDEnd': convert_to_datetime(record.get('dDEnd')),
        'dMStrt': convert_to_datetime(record.get('dMStrt')),
        'dMEnd': convert_to_datetime(record.get('dMEnd')),
        'dYStrt': convert_to_datetime(record.get('dYStrt')),
        'dYEnd': convert_to_datetime(record.get('dYEnd')),
        'sDSDate': record.get('sDSDate'),
        'sDEDate': record.get('sDEDate'),
        'sMSDate': record.get('sMSDate'),
        'sMEDate': record.get('sMEDate'),
        'sYSDate': record.get('sYSDate'),
        'sYEDate': record.get('sYEDate'),
        'DPreR': convert_to_decimal(record.get('DPreR')),
        'MPreR': convert_to_decimal(record.get('MPreR')),
        'DCurrR': convert_to_decimal(record.get('DCurrR')),
        'MCurrR': convert_to_decimal(record.get('MCurrR')),
        'DPastR': convert_to_decimal(record.get('DPastR')),
        'MPastR': convert_to_decimal(record.get('MPastR')),
        'DTotR': convert_to_decimal(record.get('DTotR')),
        'MTotR': convert_to_decimal(record.get('MTotR')),
        'DCurrLF': convert_to_decimal(record.get('DCurrLF')),
        'MCurrLF': convert_to_decimal(record.get('MCurrLF')),
        'DPastLF': convert_to_decimal(record.get('DPastLF')),
        'MPastLF': convert_to_decimal(record.get('MPastLF')),
        'DTotLF': convert_to_decimal(record.get('DTotLF')),
        'MTotLF': convert_to_decimal(record.get('MTotLF')),
        'DNNSF': convert_to_int(record.get('DNNSF')),
        'MNNSF': convert_to_int(record.get('MNNSF')),
        'YNNSF': convert_to_int(record.get('YNNSF')),
        'DNSFTot': convert_to_decimal(record.get('DNSFTot')),
        'MNSFTot': convert_to_decimal(record.get('MNSFTot')),
        'YNSFTot': convert_to_decimal(record.get('YNSFTot')),
        'dcDBadDebts': convert_to_decimal(record.get('dcDBadDebts')),
        'dcMBadDebts': convert_to_decimal(record.get('dcMBadDebts')),
        'dcYBadDebts': convert_to_decimal(record.get('dcYBadDebts')),
        'DIns': convert_to_int(record.get('DIns')),
        'MIns': convert_to_int(record.get('MIns')),
        'YIns': convert_to_int(record.get('YIns')),
        'DInsN': convert_to_int(record.get('DInsN')),
        'MInsN': convert_to_int(record.get('MInsN')),
        'YInsN': convert_to_int(record.get('YInsN')),
        'DOuts': convert_to_int(record.get('DOuts')),
        'MOuts': convert_to_int(record.get('MOuts')),
        'YOuts': convert_to_int(record.get('YOuts')),
        'DXFers': convert_to_int(record.get('DXFers')),
        'MXFers': convert_to_int(record.get('MXFers')),
        'YXFers': convert_to_int(record.get('YXFers')),
        'DCallsIn': convert_to_int(record.get('DCallsIn')),
        'MCallsIn': convert_to_int(record.get('MCallsIn')),
        'YCallsIn': convert_to_int(record.get('YCallsIn')),
        'DWalkIns': convert_to_int(record.get('DWalkIns')),
        'MWalkIns': convert_to_int(record.get('MWalkIns')),
        'YWalkIns': convert_to_int(record.get('YWalkIns')),
        'DWInsConv': convert_to_int(record.get('DWInsConv')),
        'MWInsConv': convert_to_int(record.get('MWInsConv')),
        'YWInsConv': convert_to_int(record.get('YWInsConv')),
        'DLetters': convert_to_int(record.get('DLetters')),
        'MLetters': convert_to_int(record.get('MLetters')),
        'YLetters': convert_to_int(record.get('YLetters')),
        'DCalls': convert_to_int(record.get('DCalls')),
        'MCalls': convert_to_int(record.get('MCalls')),
        'YCalls': convert_to_int(record.get('YCalls')),
        'DPmts': convert_to_int(record.get('DPmts')),
        'MPmts': convert_to_int(record.get('MPmts')),
        'DFeesChg': convert_to_int(record.get('DFeesChg')),
        'MFeesChg': convert_to_int(record.get('MFeesChg')),
        'DMerch': convert_to_int(record.get('DMerch')),
        'MMerch': convert_to_int(record.get('MMerch')),
        'sRHSCap': record.get('sRHSCap'),
        'WaitNum': convert_to_int(record.get('WaitNum')),
        'Overlocks': convert_to_int(record.get('Overlocks')),
        'AutoBilled': convert_to_int(record.get('AutoBilled')),
        'Insurance': convert_to_int(record.get('Insurance')),
        'PrepaidRentUnits': convert_to_int(record.get('PrepaidRentUnits')),
        'PrepaidAmt': convert_to_decimal(record.get('PrepaidAmt')),
        'PrepaidInsurUnits': convert_to_int(record.get('PrepaidInsurUnits')),
        'PrepaidInsurAmt': convert_to_decimal(record.get('PrepaidInsurAmt')),
        'PrepaidRecUnits': convert_to_int(record.get('PrepaidRecUnits')),
        'PrepaidRecAmt': convert_to_decimal(record.get('PrepaidRecAmt')),
        'SecDepLiabilityUnits': convert_to_int(record.get('SecDepLiabilityUnits')),
        'SecDepLiabilityAmt': convert_to_decimal(record.get('SecDepLiabilityAmt')),
    }


def transform_rental_activity(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform RentalActivity record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'Occupied': convert_to_int(record.get('Occupied')),
        'Vacant': convert_to_int(record.get('Vacant')),
        'Unrentable': convert_to_int(record.get('Unrentable')),
        'Complimentary': convert_to_int(record.get('Complimentary')),
        'TotalUnits': convert_to_int(record.get('TotalUnits')),
        'OccupiedPC': convert_to_decimal(record.get('OccupiedPC')),
        'VacantPC': convert_to_decimal(record.get('VacantPC')),
        'UnrentablePC': convert_to_decimal(record.get('UnrentablePC')),
        'ComplimentaryPC': convert_to_decimal(record.get('ComplimentaryPC')),
        'TotalUnitsPC': convert_to_decimal(record.get('TotalUnitsPC')),
        'OccupiedArea': convert_to_decimal(record.get('OccupiedArea')),
        'VacantArea': convert_to_decimal(record.get('VacantArea')),
        'UnrentableArea': convert_to_decimal(record.get('UnrentableArea')),
        'ComplimentaryArea': convert_to_decimal(record.get('ComplimentaryArea')),
        'TotalArea': convert_to_decimal(record.get('TotalArea')),
        'COccupiedAreaPC': convert_to_decimal(record.get('COccupiedAreaPC')),
        'VacantAreaPC': convert_to_decimal(record.get('VacantAreaPC')),
        'UnrentableAreaPC': convert_to_decimal(record.get('UnrentableAreaPC')),
        'ComplimentaryAreaPC': convert_to_decimal(record.get('ComplimentaryAreaPC')),
        'TotalAreaPC': convert_to_decimal(record.get('TotalAreaPC')),
        'StdRateOccupied': convert_to_decimal(record.get('StdRateOccupied')),
        'StdRateVacant': convert_to_decimal(record.get('StdRateVacant')),
        'StdRateUnrentable': convert_to_decimal(record.get('StdRateUnrentable')),
        'StdRateComplimentary': convert_to_decimal(record.get('StdRateComplimentary')),
        'StdRateTotal': convert_to_decimal(record.get('StdRateTotal')),
        'StdRateOccupiedPC': convert_to_decimal(record.get('StdRateOccupiedPC')),
        'StdRateVacantPC': convert_to_decimal(record.get('StdRateVacantPC')),
        'StdRateUnrentablePC': convert_to_decimal(record.get('StdRateUnrentablePC')),
        'StdRateComplimentaryPC': convert_to_decimal(record.get('StdRateComplimentaryPC')),
        'StdTotalPC': convert_to_decimal(record.get('StdTotalPC')),
        'OccAvgAreaOverUnit': convert_to_decimal(record.get('OccAvgAreaOverUnit')),
        'OccAvgRentOverUnit': convert_to_decimal(record.get('OccAvgRentOverUnit')),
        'OccAvgRentOverArea': convert_to_decimal(record.get('OccAvgRentOverArea')),
        'VacAvgAreaOverUnit': convert_to_decimal(record.get('VacAvgAreaOverUnit')),
        'VacAvgRentOverUnit': convert_to_decimal(record.get('VacAvgRentOverUnit')),
        'VacAvgRentOverArea': convert_to_decimal(record.get('VacAvgRentOverArea')),
        'UnRAvgAreaOverUnit': convert_to_decimal(record.get('UnRAvgAreaOverUnit')),
        'UnRAvgRentOverUnit': convert_to_decimal(record.get('UnRAvgRentOverUnit')),
        'UnRAvgRentOverArea': convert_to_decimal(record.get('UnRAvgRentOverArea')),
        'CompAvgAreaOverUnit': convert_to_decimal(record.get('CompAvgAreaOverUnit')),
        'CompAvgRentOverUnit': convert_to_decimal(record.get('CompAvgRentOverUnit')),
        'CompAvgRentOverArea': convert_to_decimal(record.get('CompAvgRentOverArea')),
        'TotAvgAreaOverUnit': convert_to_decimal(record.get('TotAvgAreaOverUnit')),
        'TotAvgRentOverUnit': convert_to_decimal(record.get('TotAvgRentOverUnit')),
        'TotAvgRentOverArea': convert_to_decimal(record.get('TotAvgRentOverArea')),
        'GrossPotential': convert_to_decimal(record.get('GrossPotential')),
        'GrossComplimentary': convert_to_decimal(record.get('GrossComplimentary')),
        'GrossOccupied': convert_to_decimal(record.get('GrossOccupied')),
        'GrossVacant': convert_to_decimal(record.get('GrossVacant')),
        'GrossUnrentable': convert_to_decimal(record.get('GrossUnrentable')),
        'ActualOccupied': convert_to_decimal(record.get('ActualOccupied')),
        'OccupiedRateVariance': convert_to_decimal(record.get('OccupiedRateVariance')),
        'EffectiveRate': convert_to_decimal(record.get('EffectiveRate')),
        'GrossPotentialPC': convert_to_decimal(record.get('GrossPotentialPC')),
        'GrossComplimentaryPC': convert_to_decimal(record.get('GrossComplimentaryPC')),
        'GrossOccupiedPC': convert_to_decimal(record.get('GrossOccupiedPC')),
        'GrossVacantPC': convert_to_decimal(record.get('GrossVacantPC')),
        'GrossUnrentablePC': convert_to_decimal(record.get('GrossUnrentablePC')),
        'ActualOccupiedPC': convert_to_decimal(record.get('ActualOccupiedPC')),
        'OccupiedRateVariancePC': convert_to_decimal(record.get('OccupiedRateVariancePC')),
        'EffectiveRatePC': convert_to_decimal(record.get('EffectiveRatePC')),
        'GrossPotentialPerArea': convert_to_decimal(record.get('GrossPotentialPerArea')),
        'GrossComplimentaryPerArea': convert_to_decimal(record.get('GrossComplimentaryPerArea')),
        'GrossOccupiedPerArea': convert_to_decimal(record.get('GrossOccupiedPerArea')),
        'GrossVacantPerArea': convert_to_decimal(record.get('GrossVacantPerArea')),
        'GrossUnrentablePerArea': convert_to_decimal(record.get('GrossUnrentablePerArea')),
        'ActualOccupiedPerArea': convert_to_decimal(record.get('ActualOccupiedPerArea')),
        'OccupiedRateVariancePerArea': convert_to_decimal(record.get('OccupiedRateVariancePerArea')),
        'EffectiveRatePerArea': convert_to_decimal(record.get('EffectiveRatePerArea')),
        'IncomePC': convert_to_decimal(record.get('IncomePC')),
        'UnitPC': convert_to_decimal(record.get('UnitPC')),
        'AreaPC': convert_to_decimal(record.get('AreaPC')),
        'EconomicPC': convert_to_decimal(record.get('EconomicPC')),
    }


def transform_delinquency(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform Delinquency record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'Period': record.get('Period'),
        'dcDlqntTot': convert_to_decimal(record.get('dcDlqntTot')),
        'iDelUnits': convert_to_int(record.get('iDelUnits')),
        'dcPctUnits': convert_to_decimal(record.get('dcPctUnits')),
        'dcPctGrossPot': convert_to_decimal(record.get('dcPctGrossPot')),
        'dcPctActOcc': convert_to_decimal(record.get('dcPctActOcc')),
        'iDatePeriod': convert_to_int(record.get('iDatePeriod')),
    }


def transform_unpaid(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform Unpaid record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'Period': record.get('Period'),
        'dcDlqntTot': convert_to_decimal(record.get('dcDlqntTot')),
        'iDelUnits': convert_to_int(record.get('iDelUnits')),
        'dcPctUnits': convert_to_decimal(record.get('dcPctUnits')),
        'dcPctGrossPot': convert_to_decimal(record.get('dcPctGrossPot')),
        'dcPctActOcc': convert_to_decimal(record.get('dcPctActOcc')),
        'iDatePeriod': convert_to_int(record.get('iDatePeriod')),
    }


def transform_rent_last_changed(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform RentLastChanged record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'SortID': convert_to_int(record.get('SortID')),
        'sRentLastChangedCat': record.get('sRentLastChangedCat'),
        'RentLastChangedCount': convert_to_int(record.get('RentLastChangedCount')),
    }


def transform_var_from_std_rate(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform VarFromStdRate record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'SortID': convert_to_int(record.get('SortID')),
        'sVarFromStdRateCat': record.get('sVarFromStdRateCat'),
        'VarFromStdRateCount': convert_to_int(record.get('VarFromStdRateCount')),
    }


def transform_unit_activity(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform UnitActivity record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'SortID': convert_to_int(record.get('SortID')),
        'sDesc': record.get('sDesc'),
        'iDCount': convert_to_int(record.get('iDCount')),
        'iMCount': convert_to_int(record.get('iMCount')),
        'iYCount': convert_to_int(record.get('iYCount')),
    }


def transform_alerts(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform Alerts record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'iSortOrder': convert_to_int(record.get('iSortOrder')),
        'sCatName': record.get('sCatName'),
        'iCnt': convert_to_int(record.get('iCnt')),
    }


def transform_tenant_stats(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform TenantStats record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'iOccupied': convert_to_int(record.get('iOccupied')),
        'iInsurance': convert_to_int(record.get('iInsurance')),
        'pctInsurancePer': convert_to_decimal(record.get('pctInsurancePer')),
        'iAutoACH': convert_to_int(record.get('iAutoACH')),
        'pctAutoACHPer': convert_to_decimal(record.get('pctAutoACHPer')),
        'iAutoCC': convert_to_int(record.get('iAutoCC')),
        'pctAutoCCPer': convert_to_decimal(record.get('pctAutoCCPer')),
        'iWebPmt': convert_to_int(record.get('iWebPmt')),
        'pctWebPmtPer': convert_to_decimal(record.get('pctWebPmtPer')),
    }


def transform_insurance_stats(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform InsuranceStats record to database format."""
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'Premiums': convert_to_decimal(record.get('Premiums')),
        'Coverage': convert_to_decimal(record.get('Coverage')),
    }


# Mapping of table names to transform functions
TRANSFORM_FUNCTIONS = {
    'Deposits': transform_deposits,
    'Receipts': transform_receipts,
    'Concessions': transform_concessions,
    'Discounts': transform_discounts,
    'Liabilities': transform_liabilities,
    'Misc': transform_misc,
    'RentalActivity': transform_rental_activity,
    'Delinquency': transform_delinquency,
    'Unpaid': transform_unpaid,
    'RentLastChanged': transform_rent_last_changed,
    'VarFromStdRate': transform_var_from_std_rate,
    'UnitActivity': transform_unit_activity,
    'Alerts': transform_alerts,
    'TenantStats': transform_tenant_stats,
    'InsuranceStats': transform_insurance_stats,
}


# =============================================================================
# Data Operations
# =============================================================================

def parse_management_summary_response(raw_results: List[Dict], extract_date: date) -> Dict[str, List[Dict]]:
    """
    Parse the ManagementSummary API response into separate tables.

    The API returns a NewDataSet containing all tables as nested structures:
    [{'Deposits': {...}, 'Receipts': [{...}, {...}], 'Concessions': [...], ...}]

    Each table value can be either a single dict or a list of dicts.
    """
    tables = {name: [] for name in MODEL_CONFIG.keys()}

    if not raw_results:
        return tables

    # The API returns a list with one item containing all tables
    data_set = raw_results[0] if raw_results else {}

    for table_name, transform_func in TRANSFORM_FUNCTIONS.items():
        if table_name not in data_set:
            continue

        table_data = data_set[table_name]

        # Handle both single dict and list of dicts
        if isinstance(table_data, dict):
            # Single record
            transformed = transform_func(table_data, extract_date)
            tables[table_name].append(transformed)
        elif isinstance(table_data, list):
            # Multiple records
            for record in table_data:
                if isinstance(record, dict):
                    transformed = transform_func(record, extract_date)
                    tables[table_name].append(transformed)

    return tables


def fetch_management_summary_data(
    soap_client,
    location_codes: List[str],
    start_date: datetime,
    end_date: datetime,
    extract_date: date
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch ManagementSummary data for multiple locations using specialized API call."""
    all_tables = {name: [] for name in MODEL_CONFIG.keys()}
    table_names = list(MODEL_CONFIG.keys())

    with tqdm(total=len(location_codes), desc="  Fetching locations", unit="loc") as pbar:
        for location_code in location_codes:
            try:
                # Use specialized method that extracts all tables from single API call
                raw_tables = soap_client.call_management_summary(
                    parameters={
                        'sLocationCode': location_code,
                        'dReportDateStart': start_date.strftime('%Y-%m-%dT00:00:00'),
                        'dReportDateEnd': end_date.strftime('%Y-%m-%dT00:00:00'),
                    },
                    soap_action='http://tempuri.org/CallCenterWs/ReportingWs/ManagementSummary',
                    namespace='http://tempuri.org/CallCenterWs/ReportingWs',
                    table_names=table_names
                )

                # Transform and aggregate records
                total_records = 0
                for table_name, records in raw_tables.items():
                    if table_name in TRANSFORM_FUNCTIONS:
                        transform_func = TRANSFORM_FUNCTIONS[table_name]
                        for record in records:
                            transformed = transform_func(record, extract_date)
                            all_tables[table_name].append(transformed)
                            total_records += 1

                pbar.set_postfix({"location": location_code, "records": total_records})
                pbar.update(1)

            except Exception as e:
                pbar.set_postfix({"location": location_code, "status": "ERROR"})
                pbar.update(1)
                tqdm.write(f"  ✗ {location_code}: Error - {str(e)}")
                continue

    return all_tables


def push_to_database(
    all_tables: Dict[str, List[Dict[str, Any]]],
    config: DataLayerConfig
) -> Dict[str, int]:
    """Push all management summary tables to PostgreSQL database."""
    record_counts = {}

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    engine = create_engine_from_config(db_config)

    # Create all tables
    models_to_create = [cfg['model'].__table__ for cfg in MODEL_CONFIG.values()]
    with tqdm(total=1, desc="  Preparing database", bar_format='{desc}') as pbar:
        Base.metadata.create_all(engine, tables=models_to_create)
        pbar.update(1)
    tqdm.write("  ✓ All ManagementSummary tables ready")

    session_manager = SessionManager(engine)
    chunk_size = env_config('MANAGEMENTSUMMARY_SQL_CHUNK_SIZE', default=500, cast=int)

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        for table_name, records in all_tables.items():
            if not records:
                record_counts[table_name] = 0
                continue

            model_cfg = MODEL_CONFIG[table_name]
            model = model_cfg['model']
            constraints = model_cfg['constraints']

            num_chunks = (len(records) + chunk_size - 1) // chunk_size

            with tqdm(total=len(records), desc=f"  Upserting {table_name}", unit="rec") as pbar:
                for i in range(0, len(records), chunk_size):
                    chunk = records[i:i + chunk_size]

                    upsert_ops.upsert_batch(
                        model=model,
                        records=chunk,
                        constraint_columns=constraints,
                        chunk_size=chunk_size
                    )

                    pbar.update(len(chunk))
                    pbar.set_postfix({"chunk": f"{i//chunk_size + 1}/{num_chunks}"})

            record_counts[table_name] = len(records)
            tqdm.write(f"  ✓ {table_name}: {len(records)} records")

    return record_counts


# =============================================================================
# CLI and Main
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='ManagementSummary to SQL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Manual mode - load historical data
  python managementsummary_to_sql.py --mode manual --start 2025-01 --end 2025-12

  # Automatic mode - previous month + current month (for scheduler)
  python managementsummary_to_sql.py --mode auto
        """
    )

    parser.add_argument(
        '--mode',
        choices=['manual', 'auto'],
        required=True,
        help='Extraction mode: manual (specify date range) or auto (prev + current month)'
    )

    parser.add_argument(
        '--start',
        type=str,
        help='Start month in YYYY-MM format (required for manual mode)'
    )

    parser.add_argument(
        '--end',
        type=str,
        help='End month in YYYY-MM format (required for manual mode)'
    )

    args = parser.parse_args()

    # Validate manual mode requires start and end
    if args.mode == 'manual':
        if not args.start or not args.end:
            parser.error("Manual mode requires --start and --end arguments")

    return args


def main():
    """Main function to fetch and push ManagementSummary data to SQL."""

    args = parse_args()

    # Load configuration
    config = DataLayerConfig.from_env()

    if not config.soap:
        raise ValueError("SOAP configuration not found in .env")

    # Load location codes from .env (use MANAGEMENTSUMMARY_LOCATION_CODES or fallback to RENTROLL_LOCATION_CODES)
    try:
        location_codes = env_config('MANAGEMENTSUMMARY_LOCATION_CODES', cast=Csv())
    except Exception:
        location_codes = env_config('RENTROLL_LOCATION_CODES', cast=Csv())

    # Determine date range based on mode
    if args.mode == 'manual':
        months = get_date_range_manual(args.start, args.end)
        mode_label = "MANUAL"
        date_range_str = f"{args.start} to {args.end}"
    else:
        months = get_date_range_auto()
        mode_label = "AUTOMATIC"
        date_range_str = ", ".join([f"{y}-{m:02d}" for y, m in months])

    # Initialize SOAP client
    soap_client = SOAPClient(
        base_url=config.soap.base_url,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=config.soap.timeout,
        retries=config.soap.retries
    )

    # Print header
    print("=" * 70)
    print("ManagementSummary to SQL Pipeline")
    print("=" * 70)
    print(f"Mode: {mode_label}")
    print(f"Date Range: {date_range_str}")
    print(f"Locations: {', '.join(location_codes)}")
    print(f"Target: PostgreSQL - {config.databases['postgresql'].database}")
    print(f"Tables: {len(MODEL_CONFIG)} tables")
    print("=" * 70)

    total_records = 0

    # Process each month
    for year, month in months:
        # Calculate first and last day of month
        first_day = datetime(year, month, 1)
        last_day_dt = datetime.combine(get_last_day_of_month(year, month), datetime.min.time())

        # Get extract_date based on whether month is closed or current
        extract_date, status = get_extract_date(year, month)

        print(f"\n[{first_day.strftime('%b %Y')}] - Extract Date: {extract_date} ({status})")

        # Fetch data for all locations
        all_tables = fetch_management_summary_data(
            soap_client=soap_client,
            location_codes=location_codes,
            start_date=first_day,
            end_date=last_day_dt,
            extract_date=extract_date
        )

        # Push to database
        total_table_records = sum(len(records) for records in all_tables.values())
        if total_table_records > 0:
            record_counts = push_to_database(all_tables, config)
            total_records += sum(record_counts.values())
        else:
            print(f"  ⚠ No data found for {first_day.strftime('%b %Y')}")

    # Close SOAP client
    soap_client.close()

    print("\n" + "=" * 70)
    print(f"Pipeline completed! Total records: {total_records}")
    print("=" * 70)


if __name__ == "__main__":
    main()
