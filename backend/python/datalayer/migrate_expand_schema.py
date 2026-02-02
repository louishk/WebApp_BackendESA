"""
Migration: Expand PostgreSQL schema to match local SQL Server structure.

Adds all columns from local SQL Server tables:
- Tenants: 129 columns
- Ledgers: 186 columns
- Charges: 33 columns

Run this once before using the expanded unified sync script.

Usage:
    python -m datalayer.migrate_expand_schema
"""

from sqlalchemy import text
from common import DataLayerConfig, create_engine_from_config


# =============================================================================
# Column Definitions for Migration
# =============================================================================

# Tenants table - new columns to add
TENANT_NEW_COLUMNS = [
    # Employee Reference
    ("EmployeeID", "INTEGER"),
    # Access & Security
    ("sAccessCode2", "VARCHAR(50)"),
    ("iAccessCode2Type", "INTEGER"),
    ("sWebPassword", "VARCHAR(255)"),
    # Primary Contact additions
    ("sMrMrs", "VARCHAR(20)"),
    ("sCountry", "VARCHAR(100)"),
    ("sFax", "VARCHAR(50)"),
    ("sPager", "VARCHAR(50)"),
    ("sCountryCodeMobile", "VARCHAR(10)"),
    # Alternate Contact
    ("sMrMrsAlt", "VARCHAR(20)"),
    ("sFNameAlt", "VARCHAR(100)"),
    ("sMIAlt", "VARCHAR(10)"),
    ("sLNameAlt", "VARCHAR(100)"),
    ("sAddr1Alt", "VARCHAR(255)"),
    ("sAddr2Alt", "VARCHAR(255)"),
    ("sCityAlt", "VARCHAR(100)"),
    ("sRegionAlt", "VARCHAR(100)"),
    ("sPostalCodeAlt", "VARCHAR(20)"),
    ("sCountryAlt", "VARCHAR(100)"),
    ("sPhoneAlt", "VARCHAR(50)"),
    ("sEmailAlt", "VARCHAR(255)"),
    ("sRelationshipAlt", "VARCHAR(100)"),
    # Business Contact
    ("sEmployer", "VARCHAR(255)"),
    ("sMrMrsBus", "VARCHAR(20)"),
    ("sFNameBus", "VARCHAR(100)"),
    ("sMIBus", "VARCHAR(10)"),
    ("sLNameBus", "VARCHAR(100)"),
    ("sCompanyBus", "VARCHAR(255)"),
    ("sAddr1Bus", "VARCHAR(255)"),
    ("sAddr2Bus", "VARCHAR(255)"),
    ("sCityBus", "VARCHAR(100)"),
    ("sRegionBus", "VARCHAR(100)"),
    ("sPostalCodeBus", "VARCHAR(20)"),
    ("sCountryBus", "VARCHAR(100)"),
    ("sPhoneBus", "VARCHAR(50)"),
    ("sEmailBus", "VARCHAR(255)"),
    # Additional Contact
    ("sMrMrsAdd", "VARCHAR(20)"),
    ("sFNameAdd", "VARCHAR(100)"),
    ("sMIAdd", "VARCHAR(10)"),
    ("sLNameAdd", "VARCHAR(100)"),
    ("sAddr1Add", "VARCHAR(255)"),
    ("sAddr2Add", "VARCHAR(255)"),
    ("sCityAdd", "VARCHAR(100)"),
    ("sRegionAdd", "VARCHAR(100)"),
    ("sPostalCodeAdd", "VARCHAR(20)"),
    ("sCountryAdd", "VARCHAR(100)"),
    ("sPhoneAdd", "VARCHAR(50)"),
    ("sEmailAdd", "VARCHAR(255)"),
    # Identification
    ("sLicRegion", "VARCHAR(50)"),
    ("sSSN", "VARCHAR(50)"),
    ("sTaxID", "VARCHAR(100)"),
    ("sTaxExemptCode", "VARCHAR(50)"),
    ("dDOB", "TIMESTAMP"),
    ("iGender", "INTEGER"),
    # Status Flags
    ("bCommercial", "BOOLEAN"),
    ("bTaxExempt", "BOOLEAN"),
    ("bSpecial", "BOOLEAN"),
    ("bNeverLockOut", "BOOLEAN"),
    ("bCompanyIsTenant", "BOOLEAN"),
    ("bOnWaitingList", "BOOLEAN"),
    ("bNoChecks", "BOOLEAN"),
    ("bPermanent", "BOOLEAN"),
    ("bWalkInPOS", "BOOLEAN"),
    ("bSpecialAlert", "BOOLEAN"),
    ("bPermanentGateLockout", "BOOLEAN"),
    ("bSMSOptIn", "BOOLEAN"),
    ("iBlackListRating", "INTEGER"),
    ("iTenEvents_OptOut", "INTEGER"),
    # Marketing
    ("MarketingID", "INTEGER"),
    ("MktgDistanceID", "INTEGER"),
    ("MktgWhatID", "INTEGER"),
    ("MktgReasonID", "INTEGER"),
    ("MktgWhyID", "INTEGER"),
    ("MktgTypeID", "INTEGER"),
    ("iHowManyOtherStorageCosDidYouContact", "INTEGER"),
    ("iUsedSelfStorageInThePast", "INTEGER"),
    ("iMktg_DidYouVisitWebSite", "INTEGER"),
    # Exit Survey
    ("dExit_SurveyTaken", "TIMESTAMP"),
    ("sExit_Comment", "TEXT"),
    ("bExit_OnEmailOfferList", "BOOLEAN"),
    ("dExit_WhenNeedAgain", "TIMESTAMP"),
    ("MktgExitRentAgainID", "INTEGER"),
    ("MktgExitReasonID", "INTEGER"),
    ("MktgExitSatisfactionID", "INTEGER"),
    ("iExitSat_Cleanliness", "INTEGER"),
    ("iExitSat_Safety", "INTEGER"),
    ("iExitSat_Services", "INTEGER"),
    ("iExitSat_Staff", "INTEGER"),
    ("iExitSat_Price", "INTEGER"),
    # Web Security
    ("sWebSecurityQ", "VARCHAR(255)"),
    ("sWebSecurityQA", "VARCHAR(255)"),
    # Geographic
    ("dcLongitude", "NUMERIC(14,10)"),
    ("dcLatitude", "NUMERIC(14,10)"),
    # Notes
    ("sTenNote", "TEXT"),
    ("sIconList", "VARCHAR(255)"),
    # Pictures
    ("iPrimaryPic", "INTEGER"),
    ("sPicFileN1", "VARCHAR(255)"),
    ("sPicFileN2", "VARCHAR(255)"),
    ("sPicFileN3", "VARCHAR(255)"),
    ("sPicFileN4", "VARCHAR(255)"),
    ("sPicFileN5", "VARCHAR(255)"),
    ("sPicFileN6", "VARCHAR(255)"),
    ("sPicFileN7", "VARCHAR(255)"),
    ("sPicFileN8", "VARCHAR(255)"),
    ("sPicFileN9", "VARCHAR(255)"),
    # Global Account
    ("bi_Tenant_GlobalNum", "BIGINT"),
    ("iGlobalNum_NationalMasterAccount", "INTEGER"),
    ("iGlobalNum_NationalFranchiseAccount", "INTEGER"),
    # Timestamps
    ("dCreated", "TIMESTAMP"),
    ("dUpdated", "TIMESTAMP"),
    ("dDeleted", "TIMESTAMP"),
    ("dArchived", "TIMESTAMP"),
    # Tracking
    ("extract_date", "DATE"),
]

# Ledgers table - new columns to add
LEDGER_NEW_COLUMNS = [
    # Foreign Keys
    ("BillingFreqID", "INTEGER"),
    ("ConcessionID", "INTEGER"),
    ("PromoRentalID", "INTEGER"),
    ("CreditCardID", "INTEGER"),
    ("ACHBankInfoID", "INTEGER"),
    # Lease Info
    ("sPurchOrderCode", "VARCHAR(100)"),
    ("dLease", "TIMESTAMP"),
    ("dSchedOut", "TIMESTAMP"),
    ("dMovedOut", "TIMESTAMP"),
    ("dMovedOutExpected", "TIMESTAMP"),
    ("dSchedOutCreated", "TIMESTAMP"),
    # Anniversary
    ("bAnniv", "BOOLEAN"),
    ("dInsurPaidThru", "TIMESTAMP"),
    ("dPmtLast", "TIMESTAMP"),
    ("dcPmtLastAmt", "NUMERIC(14,4)"),
    # Employee Tracking
    ("iInByEmpID", "INTEGER"),
    ("iOutByEmpID", "INTEGER"),
    ("iTferByEmpID", "INTEGER"),
    ("iTferToLedID", "INTEGER"),
    ("iTferFromLedID", "INTEGER"),
    # Rent
    ("dcSchedRent", "NUMERIC(14,4)"),
    ("dSchedRentStrt", "TIMESTAMP"),
    ("dRentLastChanged", "TIMESTAMP"),
    ("dcPushRateAtMoveIn", "NUMERIC(14,4)"),
    # Recurring Charges
    ("dcRecChg1", "NUMERIC(14,4)"),
    ("dcRecChg2", "NUMERIC(14,4)"),
    ("dcRecChg3", "NUMERIC(14,4)"),
    ("dcRecChg4", "NUMERIC(14,4)"),
    ("dcRecChg5", "NUMERIC(14,4)"),
    ("dcRecChg6", "NUMERIC(14,4)"),
    ("dcRecChg7", "NUMERIC(14,4)"),
    ("dcRecChg8", "NUMERIC(14,4)"),
    ("iRecChg1Qty", "INTEGER"),
    ("iRecChg2Qty", "INTEGER"),
    ("iRecChg3Qty", "INTEGER"),
    ("iRecChg4Qty", "INTEGER"),
    ("iRecChg5Qty", "INTEGER"),
    ("iRecChg6Qty", "INTEGER"),
    ("iRecChg7Qty", "INTEGER"),
    ("iRecChg8Qty", "INTEGER"),
    # Fees
    ("dcAdminFee", "NUMERIC(14,4)"),
    ("dcCutLockFee", "NUMERIC(14,4)"),
    ("dcNSFFee", "NUMERIC(14,4)"),
    ("dcAuctionFee", "NUMERIC(14,4)"),
    # Late Fees
    ("dcLateFee1", "NUMERIC(14,4)"),
    ("dcLateFee2", "NUMERIC(14,4)"),
    ("dcLateFee3", "NUMERIC(14,4)"),
    ("dcLateFee4", "NUMERIC(14,4)"),
    ("dcLateFee5", "NUMERIC(14,4)"),
    ("dLF1Strt", "TIMESTAMP"),
    ("dLF2Strt", "TIMESTAMP"),
    ("dLF3Strt", "TIMESTAMP"),
    ("dLF4Strt", "TIMESTAMP"),
    ("dLF5Strt", "TIMESTAMP"),
    ("iLateFeeType", "INTEGER"),
    ("iLateFeeType1", "INTEGER"),
    ("iLateFeeType2", "INTEGER"),
    ("iLateFeeType3", "INTEGER"),
    ("iLateFeeType4", "INTEGER"),
    ("iLateFeeType5", "INTEGER"),
    ("dcPercentLateFee", "NUMERIC(14,4)"),
    ("dcPercentLateFee2", "NUMERIC(14,4)"),
    ("dcPercentLateFee3", "NUMERIC(14,4)"),
    ("dcPercentLateFee4", "NUMERIC(14,4)"),
    ("dcPercentLateFee5", "NUMERIC(14,4)"),
    ("bLateFee1IsApplied", "BOOLEAN"),
    ("bLateFee2IsApplied", "BOOLEAN"),
    ("bLateFee3IsApplied", "BOOLEAN"),
    ("bLateFee4IsApplied", "BOOLEAN"),
    ("bLateFee5IsApplied", "BOOLEAN"),
    # Credit Card
    ("iCreditCardTypeID", "INTEGER"),
    ("sCreditCardNum", "VARCHAR(255)"),
    ("dCreditCardExpir", "TIMESTAMP"),
    ("sCreditCardHolderName", "VARCHAR(255)"),
    ("sCreditCardCVV2", "VARCHAR(10)"),
    ("sCreditCardStreet", "VARCHAR(255)"),
    ("sCreditCardZip", "VARCHAR(20)"),
    ("iCreditCardAVSResult", "INTEGER"),
    # ACH
    ("sACH_CheckWriterAcctNum", "VARCHAR(255)"),
    ("sACH_CheckWriterAcctName", "VARCHAR(255)"),
    ("sACH_ABA_RoutingNum", "VARCHAR(50)"),
    ("sACH_RDFI", "VARCHAR(100)"),
    ("sACH_Check_SavingsCode", "VARCHAR(10)"),
    # Auto-Billing
    ("iProcessDayOfMonth", "INTEGER"),
    ("bAutoBillChargeFee", "BOOLEAN"),
    ("bAutoBillEmailNotify", "BOOLEAN"),
    ("dAutoBillEnabled", "TIMESTAMP"),
    # Past Due
    ("bDisablePDue", "BOOLEAN"),
    ("dDisablePDueStrt", "TIMESTAMP"),
    ("dDisablePDueEnd", "TIMESTAMP"),
    # NSF
    ("bHadNSF", "BOOLEAN"),
    ("nNSF", "INTEGER"),
    # Invoice
    ("bInvoiceEmail", "BOOLEAN"),
    ("iInvoiceDaysBefore", "INTEGER"),
    ("dInvoiceLast", "TIMESTAMP"),
    ("bWaiveInvoiceFee", "BOOLEAN"),
    # Flags
    ("bTaxRent", "BOOLEAN"),
    ("bGateLocked", "BOOLEAN"),
    ("bExcludeFromRevenueMgmt", "BOOLEAN"),
    # Security Deposit Balances
    ("dcSecDepPaid", "NUMERIC(14,4)"),
    ("dcSecDepBal", "NUMERIC(14,4)"),
    # Rent & Fee Balances
    ("dcRentBal", "NUMERIC(14,4)"),
    ("dcLateFee1Bal", "NUMERIC(14,4)"),
    ("dcLateFee2Bal", "NUMERIC(14,4)"),
    ("dcLateFee3Bal", "NUMERIC(14,4)"),
    ("dcLateFee4Bal", "NUMERIC(14,4)"),
    ("dcLateFee5Bal", "NUMERIC(14,4)"),
    ("dcLateFee1CurrBal", "NUMERIC(14,4)"),
    ("dcLateFee2CurrBal", "NUMERIC(14,4)"),
    ("dcLateFee3CurrBal", "NUMERIC(14,4)"),
    ("dcLateFee4CurrBal", "NUMERIC(14,4)"),
    ("dcLateFee5CurrBal", "NUMERIC(14,4)"),
    ("dcNSFBal", "NUMERIC(14,4)"),
    ("dcAdminFeeBal", "NUMERIC(14,4)"),
    ("dcCutLockFeeBal", "NUMERIC(14,4)"),
    ("dcAuctionFeeBal", "NUMERIC(14,4)"),
    # Recurring Charge Balances
    ("dcRecChg1Bal", "NUMERIC(14,4)"),
    ("dcRecChg2Bal", "NUMERIC(14,4)"),
    ("dcRecChg3Bal", "NUMERIC(14,4)"),
    ("dcRecChg4Bal", "NUMERIC(14,4)"),
    ("dcRecChg5Bal", "NUMERIC(14,4)"),
    ("dcRecChg6Bal", "NUMERIC(14,4)"),
    ("dcRecChg7Bal", "NUMERIC(14,4)"),
    ("dcRecChg8Bal", "NUMERIC(14,4)"),
    # Other Balances
    ("dcInsurBal", "NUMERIC(14,4)"),
    ("dcPOSBal", "NUMERIC(14,4)"),
    ("dcCreditBal", "NUMERIC(14,4)"),
    ("dcOtherBal", "NUMERIC(14,4)"),
    ("dcRefundDue", "NUMERIC(14,4)"),
    # Tax Balances
    ("dcRentTaxBal", "NUMERIC(14,4)"),
    ("dcLateFeeTaxBal", "NUMERIC(14,4)"),
    ("dcOtherTaxBal", "NUMERIC(14,4)"),
    ("dcRecChgTaxBal", "NUMERIC(14,4)"),
    ("dcInsurTaxBal", "NUMERIC(14,4)"),
    ("dcPOSTaxBal", "NUMERIC(14,4)"),
    # Charge Period Dates
    ("dRentLastChgStrt", "TIMESTAMP"),
    ("dRentLastChgEnd", "TIMESTAMP"),
    ("dInsurLastChgStrt", "TIMESTAMP"),
    ("dInsurLastChgEnd", "TIMESTAMP"),
    ("dRecChg1LastChgStrt", "TIMESTAMP"),
    ("dRecChg1LastChgEnd", "TIMESTAMP"),
    ("dRecChg2LastChgStrt", "TIMESTAMP"),
    ("dRecChg2LastChgEnd", "TIMESTAMP"),
    ("dRecChg3LastChgStrt", "TIMESTAMP"),
    ("dRecChg3LastChgEnd", "TIMESTAMP"),
    ("dRecChg4LastChgStrt", "TIMESTAMP"),
    ("dRecChg4LastChgEnd", "TIMESTAMP"),
    ("dRecChg5LastChgStrt", "TIMESTAMP"),
    ("dRecChg5LastChgEnd", "TIMESTAMP"),
    ("dRecChg6LastChgStrt", "TIMESTAMP"),
    ("dRecChg6LastChgEnd", "TIMESTAMP"),
    ("dRecChg7LastChgStrt", "TIMESTAMP"),
    ("dRecChg7LastChgEnd", "TIMESTAMP"),
    ("dRecChg8LastChgStrt", "TIMESTAMP"),
    ("dRecChg8LastChgEnd", "TIMESTAMP"),
    # Vehicle
    ("sLicPlate", "VARCHAR(50)"),
    ("sVehicleDesc", "VARCHAR(255)"),
    # Complimentary
    ("sReasonComplimentary", "TEXT"),
    ("sCompanySub", "VARCHAR(255)"),
    # Revenue Management
    ("dcTR_RateIncreaseAmt", "NUMERIC(14,4)"),
    ("dTR_LastRateIncreaseNotice", "TIMESTAMP"),
    ("dTR_NextRateReview", "TIMESTAMP"),
    ("iTR_RateIncreasePendingStatus", "INTEGER"),
    ("iRemoveDiscPlanOnSchedRateChange", "INTEGER"),
    # Auction
    ("iAuctionStatus", "INTEGER"),
    ("dAuctionDate", "TIMESTAMP"),
    # Timestamps
    ("dDeleted", "TIMESTAMP"),
    ("dArchived", "TIMESTAMP"),
]

# Charges table - new columns to add
CHARGE_NEW_COLUMNS = [
    # Foreign Keys
    ("InsurLedgerID", "INTEGER"),
    ("FiscalID", "INTEGER"),
    ("ConcessionID", "INTEGER"),
    ("EmployeeID", "INTEGER"),
    ("ACHID", "INTEGER"),
    ("Disc_MemoID", "INTEGER"),
    ("ReceiptID_NSF", "INTEGER"),
    ("QTChargeID", "INTEGER"),
    # Amounts
    ("dcStdPrice", "NUMERIC(14,4)"),
    ("dcCost", "NUMERIC(14,4)"),
    ("dcPriceTax1", "NUMERIC(14,4)"),
    ("dcPriceTax2", "NUMERIC(14,4)"),
    # Dates
    ("dCreated", "TIMESTAMP"),
    # Flags
    ("bNSF", "BOOLEAN"),
    ("iNSFFlag", "INTEGER"),
    # Promotional
    ("iPromoGlobalNum", "INTEGER"),
    # Timestamps
    ("dUpdated", "TIMESTAMP"),
    ("dArchived", "TIMESTAMP"),
    ("dDeleted", "TIMESTAMP"),
]


def column_exists(conn, table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table."""
    query = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = :table_name
        AND column_name = :column_name
    """)
    result = conn.execute(query, {"table_name": table_name, "column_name": column_name})
    return result.fetchone() is not None


def add_column(conn, table_name: str, column_name: str, column_type: str) -> bool:
    """Add a column to a table if it doesn't exist."""
    if column_exists(conn, table_name, column_name):
        return False

    alter_query = text(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_type}')
    conn.execute(alter_query)
    return True


def run_migration():
    """Add all new columns to tenants, ledgers, and charges tables."""
    config = DataLayerConfig.from_env()
    db_config = config.databases.get('postgresql')

    if not db_config:
        print("ERROR: PostgreSQL configuration not found in .env")
        return

    engine = create_engine_from_config(db_config)

    print("=" * 70)
    print("Migration: Expand Schema to Match Local SQL Server")
    print("=" * 70)
    print(f"Database: {db_config.database}")
    print()

    with engine.connect() as conn:
        # Migrate Tenants table
        print(f"[TENANTS] Adding {len(TENANT_NEW_COLUMNS)} potential columns...")
        added = 0
        skipped = 0
        for col_name, col_type in TENANT_NEW_COLUMNS:
            if add_column(conn, 'tenants', col_name, col_type):
                added += 1
            else:
                skipped += 1
        print(f"  Added: {added}, Already existed: {skipped}")

        # Migrate Ledgers table
        print(f"\n[LEDGERS] Adding {len(LEDGER_NEW_COLUMNS)} potential columns...")
        added = 0
        skipped = 0
        for col_name, col_type in LEDGER_NEW_COLUMNS:
            if add_column(conn, 'ledgers', col_name, col_type):
                added += 1
            else:
                skipped += 1
        print(f"  Added: {added}, Already existed: {skipped}")

        # Migrate Charges table
        print(f"\n[CHARGES] Adding {len(CHARGE_NEW_COLUMNS)} potential columns...")
        added = 0
        skipped = 0
        for col_name, col_type in CHARGE_NEW_COLUMNS:
            if add_column(conn, 'charges', col_name, col_type):
                added += 1
            else:
                skipped += 1
        print(f"  Added: {added}, Already existed: {skipped}")

        conn.commit()

    # Verify final column counts
    print("\n" + "-" * 70)
    print("Verifying final column counts...")

    with engine.connect() as conn:
        for table in ['tenants', 'ledgers', 'charges']:
            query = text("""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_name = :table_name
            """)
            result = conn.execute(query, {"table_name": table})
            count = result.scalar()
            print(f"  {table}: {count} columns")

    print("\n" + "=" * 70)
    print("Migration completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    run_migration()
