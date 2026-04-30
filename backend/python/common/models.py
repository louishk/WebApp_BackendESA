"""
SQLAlchemy ORM models with base classes and mixins.
Provides type-safe database operations replacing raw SQL queries.
"""

from datetime import datetime, date
from typing import Dict, Any
from sqlalchemy import Column, String, Integer, BigInteger, DateTime, Date, Boolean, Numeric, Text, ForeignKey, Index, ARRAY, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


# Declarative base for all models
Base = declarative_base()


class TimestampMixin:
    """Mixin for automatic timestamp tracking"""
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class SoftDeleteMixin:
    """Mixin for soft delete functionality (optional)"""
    is_deleted = Column(Boolean, default=False, nullable=False)
    deleted_at = Column(DateTime, nullable=True)


class BaseModel:
    """Base model with common functionality"""

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert model instance to dictionary.

        Returns:
            dict: Dictionary representation of the model
        """
        result = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            # Convert datetime to ISO format string
            if isinstance(value, datetime):
                value = value.isoformat()
            result[column.name] = value
        return result

    def __repr__(self) -> str:
        """String representation of model"""
        return f"<{self.__class__.__name__}({self.to_dict()})>"


# ============================================================================
# Domain Models
# ============================================================================


class RentRoll(Base, BaseModel, TimestampMixin):
    """
    RentRoll data model for all locations.

    Composite unique key: extract_date + SiteID + UnitID
    This allows tracking same unit over time (historical data).

    Data Source: SOAP API RentRoll endpoint
    Fields: All 75 fields from API response, typed appropriately for SQL storage
    """
    __tablename__ = 'rentroll'

    # ========================================================================
    # Composite Primary Key (3-column key for historical tracking)
    # ========================================================================
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted from API")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    UnitID = Column(Integer, primary_key=True, nullable=False, comment="Unique Unit ID")

    # ========================================================================
    # Core Unit Information
    # ========================================================================
    LedgerID = Column(Integer, index=True, comment="Ledger ID for accounting")
    sUnit = Column(String(50), comment="Unit number/code")
    sSize = Column(String(50), comment="Unit size description (e.g., '4.8x4.8')")
    Area = Column(Numeric(10, 4), comment="Unit area in square feet")
    sUnitName = Column(String(100), comment="Full unit name")
    UnitTypeID = Column(Integer, comment="Unit type classification ID")
    sTypeName = Column(String(100), comment="Unit type name (e.g., 'Walk-In', 'AC Walk-In')")

    # ========================================================================
    # Physical Unit Attributes
    # ========================================================================
    iFloor = Column(Integer, comment="Floor number")
    dcWidth = Column(Numeric(10, 4), comment="Unit width")
    dcLength = Column(Numeric(10, 4), comment="Unit length")
    iWalkThruOrder = Column(Integer, comment="Walk-through order number")
    iDoorType = Column(Integer, comment="Door type code")

    # ========================================================================
    # Map/Location Attributes
    # ========================================================================
    dcMapTop = Column(Numeric(10, 4), comment="Map Y coordinate")
    dcMapLeft = Column(Numeric(10, 4), comment="Map X coordinate")
    dcMapTheta = Column(Numeric(10, 4), comment="Map rotation angle")
    bMapReversWL = Column(Boolean, comment="Map reverse width/length flag")
    iEntryLoc = Column(Integer, comment="Entry location code")

    # ========================================================================
    # Pricing Information (Numeric(14,4) to handle multi-currency like KRW)
    # ========================================================================
    dcPushRate = Column(Numeric(14, 4), comment="Push/Promotional rate")
    dcStdRate = Column(Numeric(14, 4), comment="Standard monthly rate")
    dcStdWeeklyRate = Column(Numeric(14, 4), comment="Standard weekly rate")
    dcStdSecDep = Column(Numeric(14, 4), comment="Standard security deposit")
    dcStdLateFee = Column(Numeric(14, 4), comment="Standard late fee")
    dcWebRate = Column(Numeric(14, 4), comment="Web advertised rate")
    dcWebPushRate = Column(Numeric(14, 4), comment="Web push rate")
    dcWebRateDated = Column(Numeric(14, 4), comment="Web rate with date")
    dcSchedRateMonthly = Column(Numeric(14, 4), comment="Scheduled monthly rate")
    dcSchedRateWeekly = Column(Numeric(14, 4), comment="Scheduled weekly rate")

    # ========================================================================
    # Unit Features (Booleans)
    # ========================================================================
    bPower = Column(Boolean, comment="Has power/electricity")
    bClimate = Column(Boolean, comment="Climate controlled")
    bInside = Column(Boolean, comment="Inside unit")
    bAlarm = Column(Boolean, comment="Has alarm")
    bRentable = Column(Boolean, comment="Unit is rentable")
    bRented = Column(Boolean, comment="Currently rented")
    bCorporate = Column(Boolean, comment="Corporate unit")
    bMobile = Column(Boolean, comment="Mobile unit")
    bDamaged = Column(Boolean, comment="Unit is damaged")
    bCollapsible = Column(Boolean, comment="Collapsible unit")
    bPermanent = Column(Boolean, comment="Permanent unit")
    bExcludeFromSqftReports = Column(Boolean, comment="Exclude from square footage reports")
    bExcludeFromWebsite = Column(Boolean, comment="Exclude from website listings")
    bNotReadyToRent = Column(Boolean, comment="Not ready to rent")
    bExcludeFromInsurance = Column(Boolean, comment="Exclude from insurance")

    # ========================================================================
    # Unit Status & Tracking
    # ========================================================================
    iMobileStatus = Column(Integer, comment="Mobile unit status code")
    iADA = Column(Integer, comment="ADA compliance code")
    iVehicleStorageAllowed = Column(Integer, comment="Vehicle storage allowed flag")
    iDaysVacant = Column(Integer, comment="Number of days vacant")
    EmployeeID = Column(Integer, comment="Responsible employee ID")

    # ========================================================================
    # Dates
    # ========================================================================
    dCreated = Column(DateTime, comment="Unit creation date")
    dUpdated = Column(DateTime, comment="Last updated date")
    dUnitNote = Column(DateTime, nullable=True, comment="Unit note date")
    dLeaseDate = Column(DateTime, nullable=True, comment="Lease start date")
    dPaidThru = Column(DateTime, nullable=True, comment="Rent paid through date")
    dRentLastChanged = Column(DateTime, nullable=True, comment="Date rent last changed")
    dSchedRentStrt = Column(DateTime, nullable=True, comment="Scheduled rent start date")

    # ========================================================================
    # Tenant Information
    # ========================================================================
    TenantID = Column(Integer, nullable=True, comment="Tenant ID (null if vacant)")
    sTenant = Column(String(255), nullable=True, comment="Tenant name")
    sCompany = Column(String(255), nullable=True, comment="Company name")
    sEmail = Column(String(255), nullable=True, comment="Tenant email")
    iAnnivDays = Column(Integer, nullable=True, comment="Anniversary days")
    sTaxExempt = Column(String(50), nullable=True, comment="Tax exempt status")

    # ========================================================================
    # Rental Rates & Charges (Numeric(14,4) to handle multi-currency like KRW)
    # ========================================================================
    dcSecDep = Column(Numeric(14, 4), nullable=True, comment="Actual security deposit")
    dcStandardRate = Column(Numeric(14, 4), nullable=True, comment="Standard rate for tenant")
    dcRent = Column(Numeric(14, 4), nullable=True, comment="Actual rent charged")
    dcVar = Column(Numeric(14, 4), nullable=True, comment="Variance from standard rate")
    dcSchedRent = Column(Numeric(14, 4), nullable=True, comment="Scheduled rent amount")
    dcPrePaidRentLiability = Column(Numeric(14, 4), nullable=True, comment="Pre-paid rent liability")
    dcInsurPremium = Column(Numeric(14, 4), nullable=True, comment="Insurance premium")

    # ========================================================================
    # Billing & Payment
    # ========================================================================
    iAutoBillType = Column(Integer, nullable=True, comment="Auto-billing type code")
    DaysSame = Column(Integer, nullable=True, comment="Days at same rate")

    # ========================================================================
    # Legacy & System Fields
    # ========================================================================
    SiteID1 = Column(Integer, comment="Duplicate SiteID field (legacy)")
    Area1 = Column(Numeric(10, 4), comment="Duplicate Area field (legacy)")
    OldPK = Column(String(50), comment="Old primary key (migration tracking)")
    uTS = Column(String(50), comment="Update timestamp/version marker")
    sUnitNote = Column(Text, nullable=True, comment="Unit notes/comments")

    # ========================================================================
    # Indexes for Performance
    # ========================================================================
    __table_args__ = (
        Index('idx_rentroll_composite', 'extract_date', 'SiteID', 'UnitID'),
        Index('idx_rentroll_extract_date', 'extract_date'),
        Index('idx_rentroll_site', 'SiteID'),
        Index('idx_rentroll_unit', 'UnitID'),
        Index('idx_rentroll_ledger', 'LedgerID'),
        Index('idx_rentroll_tenant', 'TenantID'),
        Index('idx_rentroll_rented', 'bRented'),
        Index('idx_rentroll_site_rented', 'SiteID', 'bRented'),
    )


# ============================================================================
# Shared Models (used across multiple reports)
# ============================================================================


class Site(Base, BaseModel, TimestampMixin):
    """
    Site/Location information - shared across all location-based reports.

    Data Source: Sites table returned by most SOAP report endpoints
    """
    __tablename__ = 'sites'

    SiteID = Column(Integer, primary_key=True, comment="Unique site identifier")
    sSiteName = Column(String(255), comment="Site name")
    sSiteAddress = Column(String(500), comment="Full site address")
    sSiteAddr1 = Column(String(255), comment="Address line 1")
    sSiteAddr2 = Column(String(255), nullable=True, comment="Address line 2")
    sSiteCity = Column(String(100), comment="City")
    sSiteRegion = Column(String(100), nullable=True, comment="State/Region")
    sSitePostalCode = Column(String(20), comment="Postal/ZIP code")
    sSiteCountry = Column(String(100), comment="Country")
    sLocationCode = Column(String(50), index=True, comment="Location code used in API calls")
    sSiteCode = Column(String(50), comment="Site code")
    sEmailAddress = Column(String(255), nullable=True, comment="Site email")
    sSitePhone = Column(String(50), nullable=True, comment="Site phone")
    sSiteFax = Column(String(50), nullable=True, comment="Site fax")
    CurrencyDecimalPlaces = Column(Integer, comment="Currency decimal places")
    iTaxDecimalPlaces = Column(Integer, comment="Tax decimal places")
    bShowAreasInsteadOfSize = Column(Boolean, comment="Display preference")
    bAccrual = Column(Boolean, comment="Accrual accounting flag")
    sAcctSoftwareCode = Column(String(50), nullable=True, comment="Accounting software code")
    bAcctProrateAR = Column(Boolean, comment="Prorate AR flag")
    bPrepaidRentLiabilityAcct = Column(Boolean, comment="Prepaid rent liability flag")
    sSoftwareName = Column(String(100), nullable=True, comment="Software name")
    bUseInclusiveTaxation = Column(Boolean, comment="Inclusive taxation flag")
    bIncludeSiteNameAndLocOnExports = Column(Boolean, comment="Export preference")


# ============================================================================
# Financial Models
# ============================================================================


class AccountsReceivable(Base, BaseModel, TimestampMixin):
    """
    Accounts Receivable data by tenant/unit.

    Data Source: SOAP API AccountsReceivable endpoint (FinAccountsReceivable table)
    """
    __tablename__ = 'fin_accounts_receivable'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    LedgerID = Column(Integer, primary_key=True, nullable=False, comment="Ledger ID")

    # Unit/Tenant Info
    sUnitName = Column(String(100), comment="Unit name/number")
    Tenant = Column(String(255), comment="Tenant name")
    sCompany = Column(String(255), nullable=True, comment="Company name")

    # Balance Information
    BeginningBalance = Column(Numeric(12, 4), comment="Beginning balance")
    EndingBalance = Column(Numeric(12, 4), comment="Ending balance")
    EndingBalanceAdditions = Column(Numeric(12, 4), comment="Ending balance additions")
    EndingBalanceSubtractions = Column(Numeric(12, 4), comment="Ending balance subtractions")

    # Charges
    RentCharges = Column(Numeric(12, 4), comment="Rent charges")
    AdminCharges = Column(Numeric(12, 4), comment="Admin charges")
    LateCharges = Column(Numeric(12, 4), comment="Late charges")
    OtherCharges = Column(Numeric(12, 4), comment="Other charges")
    Tax1 = Column(Numeric(12, 4), comment="Tax 1 amount")
    Tax2 = Column(Numeric(12, 4), comment="Tax 2 amount")

    # Payments & Credits
    CreditsIssued = Column(Numeric(12, 4), comment="Credits issued")
    Payments = Column(Numeric(12, 4), comment="Payments received")
    NSF = Column(Numeric(12, 4), comment="NSF amount")

    # Prepayments - Rent
    PrepmtsCollectedMoneyRent = Column(Numeric(12, 4), comment="Prepayments collected (money) - rent")
    PrepmtsCollectedCreditsRent = Column(Numeric(12, 4), comment="Prepayments collected (credits) - rent")
    PrepmtsAppliedRent = Column(Numeric(12, 4), comment="Prepayments applied - rent")
    PrepaymentsCollected_NSF = Column(Numeric(12, 4), comment="Prepayments collected - NSF")

    # Prepayments - Insurance
    PrepmtsCollectedMoneyInsur = Column(Numeric(12, 4), comment="Prepayments collected (money) - insurance")
    PrepmtsCollectedCreditsInsur = Column(Numeric(12, 4), comment="Prepayments collected (credits) - insurance")
    PrepmtsAppliedInsur = Column(Numeric(12, 4), comment="Prepayments applied - insurance")

    # Refunds
    RefundsAppliedRent = Column(Numeric(12, 4), comment="Refunds applied - rent")
    RefundsTfrFromRent = Column(Numeric(12, 4), comment="Refunds transferred from rent")
    RefundsAppliedInsur = Column(Numeric(12, 4), comment="Refunds applied - insurance")
    RefundsTfrFromInsur = Column(Numeric(12, 4), comment="Refunds transferred from insurance")

    # Lease Info
    dPaidThru = Column(DateTime, nullable=True, comment="Paid through date")
    dLease = Column(DateTime, nullable=True, comment="Lease date")
    dMovedOut = Column(DateTime, nullable=True, comment="Move out date")
    dcRentalRate = Column(Numeric(10, 4), comment="Rental rate")
    dcStdRate = Column(Numeric(10, 4), comment="Standard rate")
    dcVariance = Column(Numeric(10, 4), comment="Rate variance")
    LdmId = Column(String(100), nullable=True, comment="LDM ID")

    __table_args__ = (
        Index('idx_ar_composite', 'extract_date', 'SiteID', 'LedgerID'),
        Index('idx_ar_extract_date', 'extract_date'),
        Index('idx_ar_site', 'SiteID'),
    )


class DailyDeposit(Base, BaseModel, TimestampMixin):
    """
    Daily deposit data.

    Data Source: SOAP API DailyDeposits endpoint
    """
    __tablename__ = 'fin_daily_deposits'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    DepositDate = Column(Date, primary_key=True, nullable=False, comment="Deposit date")

    # Deposit Amounts
    dcCash = Column(Numeric(12, 4), comment="Cash amount")
    dcCheck = Column(Numeric(12, 4), comment="Check amount")
    dcCreditCard = Column(Numeric(12, 4), comment="Credit card amount")
    dcACH = Column(Numeric(12, 4), comment="ACH amount")
    dcOther = Column(Numeric(12, 4), comment="Other payment amount")
    dcTotal = Column(Numeric(12, 4), comment="Total deposit")

    __table_args__ = (
        Index('idx_dd_composite', 'extract_date', 'SiteID', 'DepositDate'),
        Index('idx_dd_extract_date', 'extract_date'),
    )


class Receipt(Base, BaseModel, TimestampMixin):
    """
    Receipt/Payment transaction data.

    Data Source: SOAP API Receipts endpoint
    """
    __tablename__ = 'fin_receipts'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    ReceiptID = Column(Integer, primary_key=True, nullable=False, comment="Receipt ID")

    # Receipt Info
    LedgerID = Column(Integer, index=True, comment="Ledger ID")
    TenantID = Column(Integer, index=True, nullable=True, comment="Tenant ID")
    sUnitName = Column(String(100), comment="Unit name")
    sTenant = Column(String(255), nullable=True, comment="Tenant name")

    # Transaction Details
    dReceiptDate = Column(DateTime, comment="Receipt date")
    sPaymentType = Column(String(50), comment="Payment type")
    dcAmount = Column(Numeric(12, 4), comment="Payment amount")
    sCheckNum = Column(String(50), nullable=True, comment="Check number")
    sCardType = Column(String(50), nullable=True, comment="Card type")
    sNotes = Column(Text, nullable=True, comment="Notes")

    __table_args__ = (
        Index('idx_rcpt_composite', 'extract_date', 'SiteID', 'ReceiptID'),
        Index('idx_rcpt_extract_date', 'extract_date'),
        Index('idx_rcpt_ledger', 'LedgerID'),
    )


class PastDueBalance(Base, BaseModel, TimestampMixin):
    """
    Past due balance data by tenant.

    Data Source: SOAP API PastDueBalances endpoint
    """
    __tablename__ = 'fin_past_due_balances'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    LedgerID = Column(Integer, primary_key=True, nullable=False, comment="Ledger ID")

    # Tenant/Unit Info
    sUnitName = Column(String(100), comment="Unit name")
    sTenant = Column(String(255), comment="Tenant name")
    sCompany = Column(String(255), nullable=True, comment="Company name")

    # Past Due Amounts
    dcCurrent = Column(Numeric(12, 4), comment="Current balance")
    dc1to30 = Column(Numeric(12, 4), comment="1-30 days past due")
    dc31to60 = Column(Numeric(12, 4), comment="31-60 days past due")
    dc61to90 = Column(Numeric(12, 4), comment="61-90 days past due")
    dc91Plus = Column(Numeric(12, 4), comment="91+ days past due")
    dcTotal = Column(Numeric(12, 4), comment="Total past due")

    # Lease Info
    dPaidThru = Column(DateTime, nullable=True, comment="Paid through date")
    dLease = Column(DateTime, nullable=True, comment="Lease start date")

    __table_args__ = (
        Index('idx_pdb_composite', 'extract_date', 'SiteID', 'LedgerID'),
        Index('idx_pdb_extract_date', 'extract_date'),
    )


# ============================================================================
# Activity Report Models
# ============================================================================


class MoveInsAndMoveOuts(Base, BaseModel, TimestampMixin):
    """
    Move-in and Move-out activity data (cumulative).

    Data Source: SOAP API MoveInsAndMoveOuts endpoint (UnitMoveInsAndMoveOuts table)

    Note: This is cumulative data - no extract_date in primary key.
    MoveDate is DateTime to handle same-day move-in/out scenarios.
    """
    __tablename__ = 'mimo'

    # Composite Primary Key (cumulative - no extract_date)
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    TenantID = Column(Integer, primary_key=True, nullable=False, comment="Tenant ID")
    MoveDate = Column(DateTime, primary_key=True, nullable=False, comment="Move date/time")

    # Tracking column (not part of PK)
    extract_date = Column(Date, nullable=True, comment="Date when data was last refreshed")

    # Activity Type
    MoveIn = Column(Integer, comment="Move-in flag (1=yes)")
    MoveOut = Column(Integer, comment="Move-out flag (1=yes)")
    Transfer = Column(Integer, comment="Transfer flag (1=yes)")

    # Unit Info
    UnitName = Column(String(100), comment="Unit name")
    UnitSize = Column(String(50), comment="Unit size description")
    Width = Column(Numeric(10, 4), comment="Unit width")
    Length = Column(Numeric(10, 4), comment="Unit length")
    sUnitType = Column(String(100), comment="Unit type name")

    # Tenant Info
    TenantName = Column(String(255), comment="Tenant name")
    sCompany = Column(String(255), nullable=True, comment="Company name")
    sEmail = Column(String(255), nullable=True, comment="Email address")
    Address = Column(String(500), nullable=True, comment="Address")
    City = Column(String(100), nullable=True, comment="City")
    Region = Column(String(100), nullable=True, comment="Region/State")
    PostalCode = Column(String(20), nullable=True, comment="Postal code")
    Country = Column(String(100), nullable=True, comment="Country")

    # Rate Info
    StandardRate = Column(Numeric(14, 4), comment="Standard rate")
    MovedInArea = Column(Numeric(10, 4), comment="Moved in area")
    MovedInRentalRate = Column(Numeric(14, 4), comment="Moved in rental rate")
    MovedInVariance = Column(Numeric(14, 4), comment="Moved in variance")
    MovedInDaysVacant = Column(Integer, comment="Days vacant before move-in")
    MovedOutArea = Column(Numeric(10, 4), comment="Moved out area")
    MovedOutRentalRate = Column(Numeric(14, 4), comment="Moved out rental rate")
    MovedOutVariance = Column(Numeric(14, 4), comment="Moved out variance")
    MovedOutDaysRented = Column(Integer, nullable=True, comment="Days rented before move-out")

    # Additional Fields
    iLeaseNum = Column(Integer, comment="Lease number")
    dRentLastChanged = Column(DateTime, nullable=True, comment="Rent last changed date")
    sLicPlate = Column(String(50), nullable=True, comment="License plate")
    sEmpInitials = Column(String(10), nullable=True, comment="Employee initials")
    sPlanTerm = Column(String(50), nullable=True, comment="Plan term")
    dcInsurPremium = Column(Numeric(14, 4), nullable=True, comment="Insurance premium")
    dcDiscount = Column(Numeric(14, 4), comment="Discount amount")
    sDiscountPlan = Column(String(100), nullable=True, comment="Discount plan name")
    iAuctioned = Column(Integer, comment="Auctioned flag")
    sAuctioned = Column(String(100), nullable=True, comment="Auction info")
    iDaysSinceMoveOut = Column(Integer, nullable=True, comment="Days since move out")
    dcAmtPaid = Column(Numeric(14, 4), comment="Total amount paid")
    sSource = Column(String(100), nullable=True, comment="Inquiry source")

    # Features
    bPower = Column(Boolean, comment="Has power")
    bClimate = Column(Boolean, comment="Climate controlled")
    bAlarm = Column(Boolean, comment="Has alarm")
    bInside = Column(Boolean, comment="Inside unit")

    # Move-in Rates
    dcPushRateAtMoveIn = Column(Numeric(14, 4), comment="Push rate at move-in")
    dcStdRateAtMoveIn = Column(Numeric(14, 4), comment="Standard rate at move-in")
    dcInsurPremiumAtMoveIn = Column(Numeric(14, 4), nullable=True, comment="Insurance premium at move-in")
    sDiscountPlanAtMoveIn = Column(String(100), nullable=True, comment="Discount plan at move-in")

    # Inquiry/Waiting Info
    WaitingID = Column(Integer, nullable=True, comment="Waiting list ID")
    InquiryEmployeeID = Column(Integer, nullable=True, comment="Inquiry employee ID")
    sInquiryPlacedBy = Column(String(100), nullable=True, comment="Inquiry placed by")
    CorpUserID_Placed = Column(Integer, nullable=True, comment="Corp user ID who placed")
    CorpUserID_ConvertedToMoveIn = Column(Integer, nullable=True, comment="Corp user ID who converted")

    __table_args__ = (
        Index('idx_mimo_composite', 'SiteID', 'TenantID', 'MoveDate'),
        Index('idx_mimo_site', 'SiteID'),
        Index('idx_mimo_move_date', 'MoveDate'),
    )


class RentalActivity(Base, BaseModel, TimestampMixin):
    """
    Rental activity summary data.

    Data Source: SOAP API RentalActivity endpoint
    """
    __tablename__ = 'activity_rental'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")

    # Activity Counts
    iMoveIns = Column(Integer, comment="Number of move-ins")
    iMoveOuts = Column(Integer, comment="Number of move-outs")
    iTransfers = Column(Integer, comment="Number of transfers")
    iReservations = Column(Integer, comment="Number of reservations")
    iReservationsCancelled = Column(Integer, comment="Reservations cancelled")
    iReservationsExpired = Column(Integer, comment="Reservations expired")

    # Area/Revenue
    dcMoveInArea = Column(Numeric(12, 4), comment="Total move-in area")
    dcMoveOutArea = Column(Numeric(12, 4), comment="Total move-out area")
    dcMoveInRevenue = Column(Numeric(12, 4), comment="Move-in revenue")
    dcMoveOutRevenue = Column(Numeric(12, 4), comment="Move-out revenue")

    __table_args__ = (
        Index('idx_ra_composite', 'extract_date', 'SiteID'),
        Index('idx_ra_extract_date', 'extract_date'),
    )


# ============================================================================
# Management Summary Models
# ============================================================================


class OccupancyStatistics(Base, BaseModel, TimestampMixin):
    """
    Occupancy statistics data.

    Data Source: SOAP API OccupancyStatistics endpoint
    """
    __tablename__ = 'mgmt_occupancy_statistics'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")

    # Unit Counts
    iTotalUnits = Column(Integer, comment="Total units")
    iRentableUnits = Column(Integer, comment="Rentable units")
    iOccupiedUnits = Column(Integer, comment="Occupied units")
    iVacantUnits = Column(Integer, comment="Vacant units")
    iReservedUnits = Column(Integer, comment="Reserved units")
    iNotReadyUnits = Column(Integer, comment="Not ready units")

    # Area
    dcTotalArea = Column(Numeric(12, 4), comment="Total area")
    dcRentableArea = Column(Numeric(12, 4), comment="Rentable area")
    dcOccupiedArea = Column(Numeric(12, 4), comment="Occupied area")
    dcVacantArea = Column(Numeric(12, 4), comment="Vacant area")

    # Occupancy Rates
    dcUnitOccupancy = Column(Numeric(8, 4), comment="Unit occupancy rate")
    dcAreaOccupancy = Column(Numeric(8, 4), comment="Area occupancy rate")
    dcEconomicOccupancy = Column(Numeric(8, 4), comment="Economic occupancy rate")

    # Revenue
    dcPotentialRevenue = Column(Numeric(12, 4), comment="Potential revenue")
    dcActualRevenue = Column(Numeric(12, 4), comment="Actual revenue")
    dcVariance = Column(Numeric(12, 4), comment="Revenue variance")

    __table_args__ = (
        Index('idx_occ_composite', 'extract_date', 'SiteID'),
        Index('idx_occ_extract_date', 'extract_date'),
    )


# ============================================================================
# Insurance Models
# ============================================================================


class InsuranceActivity(Base, BaseModel, TimestampMixin):
    """
    Insurance activity data.

    Data Source: SOAP API InsuranceActivity endpoint (Insur_InsuranceActivity table)
    """
    __tablename__ = 'insur_activity'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    InsurLedgerID = Column(Integer, primary_key=True, nullable=False, comment="Insurance ledger ID")

    # Tenant/Unit Info
    LedgerID = Column(Integer, index=True, comment="Ledger ID")
    TenantID = Column(Integer, index=True, nullable=True, comment="Tenant ID")
    sUnitName = Column(String(100), comment="Unit name")
    sTenant = Column(String(255), comment="Tenant name")
    sCompany = Column(String(255), nullable=True, comment="Company name")

    # Policy Info
    sPolicyNum = Column(String(100), nullable=True, comment="Policy number")
    sInsProvider = Column(String(100), nullable=True, comment="Insurance provider")
    dcCoverage = Column(Numeric(12, 4), comment="Coverage amount")
    dcPremium = Column(Numeric(10, 4), comment="Premium amount")

    # Dates
    dPolicyStart = Column(DateTime, nullable=True, comment="Policy start date")
    dPolicyEnd = Column(DateTime, nullable=True, comment="Policy end date")
    dActivityDate = Column(DateTime, nullable=True, comment="Activity date")

    # Status
    sActivityType = Column(String(50), nullable=True, comment="Activity type")
    bActive = Column(Boolean, comment="Active flag")

    __table_args__ = (
        Index('idx_insact_composite', 'extract_date', 'SiteID', 'InsurLedgerID'),
        Index('idx_insact_extract_date', 'extract_date'),
        Index('idx_insact_ledger', 'LedgerID'),
    )


class InsuranceRoll(Base, BaseModel, TimestampMixin):
    """
    Insurance roll (list of insured tenants).

    Data Source: SOAP API InsuranceRoll endpoint
    """
    __tablename__ = 'insur_roll'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    LedgerID = Column(Integer, primary_key=True, nullable=False, comment="Ledger ID")

    # Tenant/Unit Info
    TenantID = Column(Integer, index=True, nullable=True, comment="Tenant ID")
    sUnitName = Column(String(100), comment="Unit name")
    sTenant = Column(String(255), comment="Tenant name")
    sCompany = Column(String(255), nullable=True, comment="Company name")

    # Policy Info
    sPolicyNum = Column(String(100), nullable=True, comment="Policy number")
    sInsProvider = Column(String(100), nullable=True, comment="Insurance provider")
    dcCoverage = Column(Numeric(12, 4), comment="Coverage amount")
    dcPremium = Column(Numeric(10, 4), comment="Premium amount")
    dcMonthlyPremium = Column(Numeric(10, 4), comment="Monthly premium")

    # Dates
    dPolicyStart = Column(DateTime, nullable=True, comment="Policy start date")
    dPolicyEnd = Column(DateTime, nullable=True, comment="Policy end date")
    dLeaseStart = Column(DateTime, nullable=True, comment="Lease start date")

    __table_args__ = (
        Index('idx_insroll_composite', 'extract_date', 'SiteID', 'LedgerID'),
        Index('idx_insroll_extract_date', 'extract_date'),
    )


# ============================================================================
# Tenant List Models
# ============================================================================


class TenantListComplete(Base, BaseModel, TimestampMixin):
    """
    Complete tenant list with all details.

    Data Source: SOAP API TenantListComplete endpoint
    """
    __tablename__ = 'tenant_list_complete'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    TenantID = Column(Integer, primary_key=True, nullable=False, comment="Tenant ID")

    # Tenant Info
    sFirstName = Column(String(100), comment="First name")
    sLastName = Column(String(100), comment="Last name")
    sCompany = Column(String(255), nullable=True, comment="Company name")
    sEmail = Column(String(255), nullable=True, comment="Email address")
    sPhone = Column(String(50), nullable=True, comment="Phone number")
    sMobile = Column(String(50), nullable=True, comment="Mobile number")

    # Address
    sAddress = Column(String(500), nullable=True, comment="Address")
    sCity = Column(String(100), nullable=True, comment="City")
    sRegion = Column(String(100), nullable=True, comment="Region/State")
    sPostalCode = Column(String(20), nullable=True, comment="Postal code")
    sCountry = Column(String(100), nullable=True, comment="Country")

    # Unit Info
    LedgerID = Column(Integer, index=True, comment="Ledger ID")
    sUnitName = Column(String(100), comment="Unit name")
    UnitID = Column(Integer, comment="Unit ID")
    sUnitType = Column(String(100), nullable=True, comment="Unit type")
    dcUnitArea = Column(Numeric(10, 4), comment="Unit area")

    # Lease Info
    dMoveIn = Column(DateTime, nullable=True, comment="Move-in date")
    dMoveOut = Column(DateTime, nullable=True, comment="Move-out date")
    dPaidThru = Column(DateTime, nullable=True, comment="Paid through date")
    dcRent = Column(Numeric(10, 4), comment="Rent amount")
    dcStdRate = Column(Numeric(10, 4), comment="Standard rate")
    dcVariance = Column(Numeric(10, 4), comment="Rate variance")

    # Balance Info
    dcBalance = Column(Numeric(12, 4), comment="Current balance")
    dcSecDep = Column(Numeric(10, 4), nullable=True, comment="Security deposit")

    # Status
    bActive = Column(Boolean, comment="Active tenant flag")
    iLeaseNum = Column(Integer, nullable=True, comment="Lease number")

    __table_args__ = (
        Index('idx_tlc_composite', 'extract_date', 'SiteID', 'TenantID'),
        Index('idx_tlc_extract_date', 'extract_date'),
        Index('idx_tlc_ledger', 'LedgerID'),
    )


class ChargesAndPaymentsComplete(Base, BaseModel, TimestampMixin):
    """
    Complete charges and payments transaction data.

    Data Source: SOAP API ChargesAndPaymentsComplete endpoint
    """
    __tablename__ = 'tenant_charges_payments'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    TransactionID = Column(Integer, primary_key=True, nullable=False, comment="Transaction ID")

    # Tenant/Unit Info
    LedgerID = Column(Integer, index=True, comment="Ledger ID")
    TenantID = Column(Integer, index=True, nullable=True, comment="Tenant ID")
    sUnitName = Column(String(100), comment="Unit name")
    sTenant = Column(String(255), nullable=True, comment="Tenant name")

    # Transaction Info
    dTransaction = Column(DateTime, comment="Transaction date")
    sTransType = Column(String(50), comment="Transaction type (Charge/Payment)")
    sDescription = Column(String(255), nullable=True, comment="Description")
    dcAmount = Column(Numeric(12, 4), comment="Transaction amount")
    dcBalance = Column(Numeric(12, 4), comment="Running balance")

    # Payment Details (if payment)
    sPaymentMethod = Column(String(50), nullable=True, comment="Payment method")
    sCheckNum = Column(String(50), nullable=True, comment="Check number")

    __table_args__ = (
        Index('idx_cap_composite', 'extract_date', 'SiteID', 'TransactionID'),
        Index('idx_cap_extract_date', 'extract_date'),
        Index('idx_cap_ledger', 'LedgerID'),
        Index('idx_cap_trans_date', 'dTransaction'),
    )


# ============================================================================
# System Utilities Models
# ============================================================================


class Discount(Base, BaseModel, TimestampMixin):
    """
    Discount/Concession data for tenants.

    Data Source: SOAP API Discounts endpoint (Mgmt_Discounts table)
    """
    __tablename__ = 'discount'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    ChargeID = Column(Integer, primary_key=True, nullable=False, comment="Charge ID")

    # Unit/Tenant Info
    sUnitName = Column(String(100), comment="Unit name")
    sTypeName = Column(String(100), comment="Unit type name")
    sName = Column(String(255), comment="Tenant name")
    sCompany = Column(String(255), nullable=True, comment="Company name")

    # Charge Info
    sChgDesc = Column(String(100), comment="Charge description")
    dChgStrt = Column(DateTime, nullable=True, comment="Charge start date")
    dcPrice = Column(Numeric(14, 4), comment="Standard price")
    dcAmt = Column(Numeric(14, 4), comment="Actual amount charged")
    dcDiscount = Column(Numeric(14, 4), comment="Discount amount")

    # Discount Details
    sDiscMemo = Column(Text, nullable=True, comment="Discount memo")
    sConcessionPlan = Column(String(255), nullable=True, comment="Concession plan name")
    sBy = Column(String(100), nullable=True, comment="Employee initials")
    sPlanTerm = Column(String(100), nullable=True, comment="Plan term")
    dcPercentDiscount = Column(Numeric(8, 4), nullable=True, comment="Discount percentage")

    # Lease Info
    dMovedIn = Column(DateTime, nullable=True, comment="Move-in date")
    dMovedOut = Column(DateTime, nullable=True, comment="Move-out date")
    dPaidThru = Column(DateTime, nullable=True, comment="Paid through date")
    dcInsurPremium = Column(Numeric(14, 4), nullable=True, comment="Insurance premium")

    # Rate Info
    dcSchedRent = Column(Numeric(14, 4), nullable=True, comment="Scheduled rent")
    dSchedRentStrt = Column(DateTime, nullable=True, comment="Scheduled rent start")
    dRentLastChanged = Column(DateTime, nullable=True, comment="Rent last changed date")
    dcStdRateAtMoveIn = Column(Numeric(14, 4), nullable=True, comment="Standard rate at move-in")
    dcVariance = Column(Numeric(14, 4), nullable=True, comment="Rate variance")

    __table_args__ = (
        Index('idx_disc_composite', 'extract_date', 'SiteID', 'ChargeID'),
        Index('idx_disc_extract_date', 'extract_date'),
        Index('idx_disc_site', 'SiteID'),
    )


class ScheduledMoveOut(Base, BaseModel, TimestampMixin):
    """
    Scheduled move-out data.

    Data Source: SOAP API ScheduledMoveOuts endpoint
    """
    __tablename__ = 'sys_scheduled_move_outs'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    LedgerID = Column(Integer, primary_key=True, nullable=False, comment="Ledger ID")

    # Unit Info
    UnitID = Column(Integer, index=True, comment="Unit ID")
    sUnitName = Column(String(100), comment="Unit name")

    # Tenant Info
    TenantID = Column(Integer, index=True, comment="Tenant ID")
    sFName = Column(String(100), comment="First name")
    sLName = Column(String(100), comment="Last name")

    # Scheduled Move-out
    dSchedOut = Column(DateTime, comment="Scheduled move-out date")

    __table_args__ = (
        Index('idx_smo_composite', 'extract_date', 'SiteID', 'LedgerID'),
        Index('idx_smo_extract_date', 'extract_date'),
        Index('idx_smo_sched_date', 'dSchedOut'),
    )


# ============================================================================
# Dimension Tables
# ============================================================================


class UnitsInfo(Base, BaseModel, TimestampMixin):
    """
    Units Information dimension table (raw data).

    Stores raw unit master data from UnitsInformation_v3 endpoint.
    Standardization/transformation is done in SQL views, not in this table.

    Data Source: CallCenterWs UnitsInformation_v3 endpoint
    """
    __tablename__ = 'units_info'

    # ========================================================================
    # Primary Key
    # ========================================================================
    id = Column(Integer, primary_key=True, autoincrement=True, comment="Auto-increment primary key")

    # ========================================================================
    # Core Identifiers
    # ========================================================================
    SiteID = Column(Integer, nullable=False, index=True, comment="Site/Location ID")
    UnitID = Column(Integer, nullable=False, index=True, comment="Unique Unit ID")
    UnitTypeID = Column(Integer, index=True, comment="Unit type classification ID")
    sLocationCode = Column(String(20), index=True, comment="Location code (e.g., LSETUP)")

    # ========================================================================
    # Unit Identification
    # ========================================================================
    sUnitName = Column(String(100), comment="Full unit name")
    sTypeName = Column(String(100), index=True, comment="Unit type name from API")
    sUnitNote = Column(String(500), comment="Unit notes")
    sUnitDesc = Column(String(500), comment="Unit description")

    # ========================================================================
    # Physical Dimensions
    # ========================================================================
    dcWidth = Column(Numeric(10, 4), comment="Unit width")
    dcLength = Column(Numeric(10, 4), comment="Unit length")
    iFloor = Column(Integer, comment="Floor number")
    dcMapTheta = Column(Numeric(10, 4), comment="Map theta/rotation angle")
    bMapReversWL = Column(Boolean, comment="Map reverse width/length flag")
    iEntryLoc = Column(Integer, comment="Entry location type")
    iDoorType = Column(Integer, comment="Door type")
    iADA = Column(Integer, comment="ADA accessibility flag")

    # ========================================================================
    # Feature Flags
    # ========================================================================
    bClimate = Column(Boolean, comment="Climate controlled flag")
    bPower = Column(Boolean, comment="Power available flag")
    bInside = Column(Boolean, comment="Inside unit flag")
    bAlarm = Column(Boolean, comment="Alarm equipped flag")
    bRentable = Column(Boolean, comment="Rentable status flag")
    bMobile = Column(Boolean, comment="Mobile/portable unit flag")
    bServiceRequired = Column(Boolean, comment="Service required flag")
    bExcludeFromWebsite = Column(Boolean, comment="Exclude from website flag")

    # ========================================================================
    # Rental Status
    # ========================================================================
    bRented = Column(Boolean, index=True, comment="Currently rented flag")
    bWaitingListReserved = Column(Boolean, comment="Reserved via waiting list")
    bCorporate = Column(Boolean, comment="Corporate unit flag")
    iDaysVacant = Column(Integer, comment="Days unit has been vacant")
    iDaysRented = Column(Integer, comment="Days unit has been rented")
    dMovedIn = Column(DateTime, comment="Current tenant move-in date")

    # ========================================================================
    # Lease Configuration
    # ========================================================================
    iDefLeaseNum = Column(Integer, comment="Default lease number")
    DefaultCoverageID = Column(Integer, comment="Default insurance coverage ID")

    # ========================================================================
    # Pricing (Numeric(14,4) for multi-currency support)
    # ========================================================================
    dcStdRate = Column(Numeric(14, 4), comment="Standard monthly rate")
    dcWebRate = Column(Numeric(14, 4), comment="Web display rate")
    dcPushRate = Column(Numeric(14, 4), comment="Push/promotional rate")
    dcPushRate_NotRounded = Column(Numeric(14, 4), comment="Push rate before rounding")
    dcBoardRate = Column(Numeric(14, 4), comment="Board/street rate")
    dcPreferredRate = Column(Numeric(14, 4), comment="Preferred rate")
    dcStdWeeklyRate = Column(Numeric(14, 4), comment="Standard weekly rate")
    dcStdSecDep = Column(Numeric(14, 4), comment="Standard security deposit")
    dcRM_RoundTo = Column(Numeric(10, 4), comment="Revenue management round-to value")

    # ========================================================================
    # Tax Rates
    # ========================================================================
    dcTax1Rate = Column(Numeric(10, 4), comment="Tax rate 1")
    dcTax2Rate = Column(Numeric(10, 4), comment="Tax rate 2")

    # ========================================================================
    # Preferred Channel
    # ========================================================================
    iPreferredChannelType = Column(Integer, comment="Preferred channel type")
    bPreferredIsPushRate = Column(Boolean, comment="Preferred rate is push rate flag")

    # ========================================================================
    # Lifecycle
    # ========================================================================
    deleted_at = Column(Date, comment="Date unit was deleted from SiteLink")

    # ========================================================================
    # Indexes and Constraints
    # ========================================================================
    __table_args__ = (
        Index('idx_units_info_site_unit', 'SiteID', 'UnitID', unique=True),
    )


class SiteInfo(Base, BaseModel):
    """
    Site dimension table with location information.

    This is a static dimension table containing site metadata.
    SiteID is the primary key and foreign key to fact tables.
    """
    __tablename__ = 'siteinfo'

    # Primary Key
    SiteID = Column(Integer, primary_key=True, comment="Unique site identifier (from SOAP API)")

    # Site Identification
    SiteCode = Column(String(10), unique=True, nullable=False, comment="Location code (L001, L002, etc.)")
    Name = Column(String(255), nullable=False, comment="Site/Company name")
    InternalLabel = Column(String(20), comment="Internal label/code (e.g., IMM, BKR, AMK)")

    # Geographic Information
    Country = Column(String(100), nullable=False, comment="Country name")
    CityDistrict = Column(String(100), comment="City or district name")
    Street = Column(String(500), comment="Street address")

    # Coordinates (optional)
    Longitude = Column(Numeric(11, 8), nullable=True, comment="Longitude coordinate")
    Latitude = Column(Numeric(10, 8), nullable=True, comment="Latitude coordinate")

    # Contact / Postal
    PostalCode = Column(String(20), nullable=True, comment="Postal/ZIP code")
    PrimaryEmail = Column(String(255), nullable=True, comment="Primary site email address")

    # EmbedSocial / Google Reviews
    google_place_id = Column(String(50), nullable=True, comment="Google Maps Place ID")
    embedsocial_source_id = Column(String(50), nullable=True, comment="EmbedSocial source identifier")

    # Igloo Smart Lock mapping
    igloo_department_id = Column(String(50), nullable=True, comment="Igloo department ID (country level)")
    igloo_property_id = Column(String(50), nullable=True, comment="Igloo property ID (site level)")

    __table_args__ = (
        Index('idx_siteinfo_code', 'SiteCode'),
        Index('idx_siteinfo_country', 'Country'),
    )


class LOSRange(Base, BaseModel):
    """
    Length of Stay Range dimension table.

    Used for categorizing tenant tenure into predefined ranges.
    """
    __tablename__ = 'losrange'

    # Primary Key
    SortOrder = Column(Integer, primary_key=True, comment="Sort order for display")

    # Range Definition
    RangeMin = Column(Integer, nullable=False, comment="Minimum days (inclusive)")
    RangeMax = Column(Integer, nullable=False, comment="Maximum days (exclusive)")
    RangeLabel = Column(String(20), nullable=False, comment="Display label (e.g., '<3M', '>3M-<6M')")


class PriceRange(Base, BaseModel):
    """
    Price Range dimension table (SGD per square foot).

    Used for categorizing unit prices into predefined ranges.
    """
    __tablename__ = 'pricerange'

    # Primary Key
    SortOrder = Column(Integer, primary_key=True, comment="Sort order for display")

    # Range Definition
    RangeMin = Column(Numeric(10, 2), nullable=False, comment="Minimum price (inclusive)")
    RangeMax = Column(Numeric(10, 2), nullable=False, comment="Maximum price (exclusive)")
    RangeLabel = Column(String(30), nullable=False, comment="Display label (e.g., '0-2 SGD/SQF')")


# ============================================================================
# FX Rate Models
# ============================================================================


class FXRate(Base, BaseModel, TimestampMixin):
    """
    Daily FX rates with SGD as base currency.

    Data Source: Yahoo Finance (yfinance library)
    Stores daily exchange rates from 2010-01-01 to present.
    Includes forward-filled values for weekends/holidays.

    Composite unique key: rate_date + target_currency
    """
    __tablename__ = 'fx_rates'

    # ========================================================================
    # Composite Primary Key
    # ========================================================================
    rate_date = Column(Date, primary_key=True, nullable=False,
                       comment="Date of the FX rate")
    target_currency = Column(String(3), primary_key=True, nullable=False,
                            comment="Target currency code (USD, HKD, etc.)")

    # ========================================================================
    # Date Dimension Columns (for Power BI filtering)
    # ========================================================================
    year = Column(Integer, nullable=False, index=True,
                  comment="Year extracted from rate_date")
    month = Column(Integer, nullable=False, index=True,
                   comment="Month extracted from rate_date (1-12)")
    year_month = Column(String(7), nullable=False, index=True,
                        comment="YYYY-MM format for grouping")

    # ========================================================================
    # FX Rate Data
    # ========================================================================
    base_currency = Column(String(3), nullable=False, default='SGD',
                          comment="Base currency (always SGD)")
    rate = Column(Numeric(18, 10), nullable=False,
                  comment="Exchange rate: 1 SGD = X target_currency")

    # ========================================================================
    # Data Quality Flags
    # ========================================================================
    is_trading_day = Column(Boolean, nullable=False, default=True,
                           comment="True if actual trading data, False if forward-filled")
    data_source = Column(String(50), nullable=False, default='yfinance',
                        comment="Data source identifier")

    # ========================================================================
    # Indexes for Performance
    # ========================================================================
    __table_args__ = (
        Index('idx_fx_rate_date', 'rate_date'),
        Index('idx_fx_target_currency', 'target_currency'),
        Index('idx_fx_year_month', 'year_month'),
        Index('idx_fx_composite', 'rate_date', 'target_currency'),
    )


# ============================================================================
# Tenant/Ledger/Charge Models (CallCenterWs endpoints)
# ============================================================================


class Tenant(Base, BaseModel, TimestampMixin):
    """
    Tenant master data - expanded schema matching local SQL Server.

    Data Sources:
    - Local SQL Server (sldbclnt.Tenants) - 129 columns
    - SOAP API TenantList (CallCenterWs) - subset of fields

    Composite unique key: SiteID + TenantID
    """
    __tablename__ = 'cc_tenants'

    # =========================================================================
    # Primary Keys
    # =========================================================================
    TenantID = Column(Integer, primary_key=True, nullable=False, comment="Unique tenant identifier")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")

    # =========================================================================
    # Employee Reference
    # =========================================================================
    EmployeeID = Column(Integer, nullable=True, comment="Employee who created/manages tenant")

    # =========================================================================
    # Access & Security
    # =========================================================================
    sAccessCode = Column(String(100), nullable=True, comment="Primary gate access code")
    sAccessCode2 = Column(String(100), nullable=True, comment="Secondary access code")
    iAccessCode2Type = Column(Integer, nullable=True, comment="Type of secondary access code")
    sWebPassword = Column(String(255), nullable=True, comment="Web portal password (encrypted)")

    # =========================================================================
    # Primary Contact Information
    # =========================================================================
    sMrMrs = Column(String(20), nullable=True, comment="Title (Mr., Mrs., etc.)")
    sFName = Column(String(100), nullable=True, comment="First name")
    sMI = Column(String(10), nullable=True, comment="Middle initial")
    sLName = Column(String(100), nullable=True, comment="Last name")
    sCompany = Column(String(255), nullable=True, comment="Company name")
    sAddr1 = Column(String(255), nullable=True, comment="Address line 1")
    sAddr2 = Column(String(255), nullable=True, comment="Address line 2")
    sCity = Column(String(100), nullable=True, comment="City")
    sRegion = Column(String(100), nullable=True, comment="State/Region")
    sPostalCode = Column(String(20), nullable=True, comment="Postal code")
    sCountry = Column(String(100), nullable=True, comment="Country")
    sPhone = Column(String(100), nullable=True, comment="Phone number")
    sFax = Column(String(100), nullable=True, comment="Fax number")
    sEmail = Column(String(255), nullable=True, comment="Email address")
    sPager = Column(String(100), nullable=True, comment="Pager number")
    sMobile = Column(String(100), nullable=True, comment="Mobile phone")
    sCountryCodeMobile = Column(String(10), nullable=True, comment="Mobile country code")

    # =========================================================================
    # Alternate Contact Information
    # =========================================================================
    sMrMrsAlt = Column(String(20), nullable=True, comment="Alt contact title")
    sFNameAlt = Column(String(100), nullable=True, comment="Alt contact first name")
    sMIAlt = Column(String(10), nullable=True, comment="Alt contact middle initial")
    sLNameAlt = Column(String(100), nullable=True, comment="Alt contact last name")
    sAddr1Alt = Column(String(255), nullable=True, comment="Alt contact address line 1")
    sAddr2Alt = Column(String(255), nullable=True, comment="Alt contact address line 2")
    sCityAlt = Column(String(100), nullable=True, comment="Alt contact city")
    sRegionAlt = Column(String(100), nullable=True, comment="Alt contact state/region")
    sPostalCodeAlt = Column(String(20), nullable=True, comment="Alt contact postal code")
    sCountryAlt = Column(String(100), nullable=True, comment="Alt contact country")
    sPhoneAlt = Column(String(100), nullable=True, comment="Alt contact phone")
    sEmailAlt = Column(String(255), nullable=True, comment="Alt contact email")
    sRelationshipAlt = Column(String(100), nullable=True, comment="Relationship to tenant")

    # =========================================================================
    # Business Contact Information
    # =========================================================================
    sEmployer = Column(String(255), nullable=True, comment="Employer name")
    sMrMrsBus = Column(String(20), nullable=True, comment="Business contact title")
    sFNameBus = Column(String(100), nullable=True, comment="Business contact first name")
    sMIBus = Column(String(10), nullable=True, comment="Business contact middle initial")
    sLNameBus = Column(String(100), nullable=True, comment="Business contact last name")
    sCompanyBus = Column(String(255), nullable=True, comment="Business company name")
    sAddr1Bus = Column(String(255), nullable=True, comment="Business address line 1")
    sAddr2Bus = Column(String(255), nullable=True, comment="Business address line 2")
    sCityBus = Column(String(100), nullable=True, comment="Business city")
    sRegionBus = Column(String(100), nullable=True, comment="Business state/region")
    sPostalCodeBus = Column(String(20), nullable=True, comment="Business postal code")
    sCountryBus = Column(String(100), nullable=True, comment="Business country")
    sPhoneBus = Column(String(100), nullable=True, comment="Business phone")
    sEmailBus = Column(String(255), nullable=True, comment="Business email")

    # =========================================================================
    # Additional Contact Information
    # =========================================================================
    sMrMrsAdd = Column(String(20), nullable=True, comment="Additional contact title")
    sFNameAdd = Column(String(100), nullable=True, comment="Additional contact first name")
    sMIAdd = Column(String(10), nullable=True, comment="Additional contact middle initial")
    sLNameAdd = Column(String(100), nullable=True, comment="Additional contact last name")
    sAddr1Add = Column(String(255), nullable=True, comment="Additional contact address line 1")
    sAddr2Add = Column(String(255), nullable=True, comment="Additional contact address line 2")
    sCityAdd = Column(String(100), nullable=True, comment="Additional contact city")
    sRegionAdd = Column(String(100), nullable=True, comment="Additional contact state/region")
    sPostalCodeAdd = Column(String(20), nullable=True, comment="Additional contact postal code")
    sCountryAdd = Column(String(100), nullable=True, comment="Additional contact country")
    sPhoneAdd = Column(String(100), nullable=True, comment="Additional contact phone")
    sEmailAdd = Column(String(255), nullable=True, comment="Additional contact email")

    # =========================================================================
    # Identification & License
    # =========================================================================
    sLicense = Column(String(100), nullable=True, comment="Driver's license number")
    sLicRegion = Column(String(100), nullable=True, comment="License issuing state/region")
    sSSN = Column(String(100), nullable=True, comment="Social Security Number (encrypted)")
    sTaxID = Column(String(100), nullable=True, comment="Tax ID number")
    sTaxExemptCode = Column(String(100), nullable=True, comment="Tax exemption code")
    dDOB = Column(DateTime, nullable=True, comment="Date of birth")
    iGender = Column(Integer, nullable=True, comment="Gender code")

    # =========================================================================
    # Status Flags
    # =========================================================================
    bCommercial = Column(Boolean, nullable=True, comment="Commercial tenant flag")
    bTaxExempt = Column(Boolean, nullable=True, comment="Tax exempt flag")
    bSpecial = Column(Boolean, nullable=True, comment="Special tenant flag")
    bNeverLockOut = Column(Boolean, nullable=True, comment="Never lock out flag")
    bCompanyIsTenant = Column(Boolean, nullable=True, comment="Company is the tenant flag")
    bOnWaitingList = Column(Boolean, nullable=True, comment="On waiting list flag")
    bNoChecks = Column(Boolean, nullable=True, comment="No checks accepted flag")
    bPermanent = Column(Boolean, nullable=True, comment="Permanent tenant flag")
    bWalkInPOS = Column(Boolean, nullable=True, comment="Walk-in POS customer flag")
    bSpecialAlert = Column(Boolean, nullable=True, comment="Special alert flag")
    bPermanentGateLockout = Column(Boolean, nullable=True, comment="Permanent gate lockout flag")
    bSMSOptIn = Column(Boolean, nullable=True, comment="SMS opt-in flag")
    iBlackListRating = Column(Integer, nullable=True, comment="Blacklist rating")
    iTenEvents_OptOut = Column(Integer, nullable=True, comment="Tenant events opt-out setting")

    # =========================================================================
    # Marketing Information
    # =========================================================================
    MarketingID = Column(Integer, nullable=True, comment="Marketing source ID")
    MktgDistanceID = Column(Integer, nullable=True, comment="Marketing distance ID")
    MktgWhatID = Column(Integer, nullable=True, comment="Marketing 'what' ID")
    MktgReasonID = Column(Integer, nullable=True, comment="Marketing reason ID")
    MktgWhyID = Column(Integer, nullable=True, comment="Marketing 'why' ID")
    MktgTypeID = Column(Integer, nullable=True, comment="Marketing type ID")
    iHowManyOtherStorageCosDidYouContact = Column(Integer, nullable=True, comment="Other storage companies contacted")
    iUsedSelfStorageInThePast = Column(Integer, nullable=True, comment="Used self storage before")
    iMktg_DidYouVisitWebSite = Column(Integer, nullable=True, comment="Visited website flag")

    # =========================================================================
    # Exit Survey Information
    # =========================================================================
    dExit_SurveyTaken = Column(DateTime, nullable=True, comment="Exit survey date")
    sExit_Comment = Column(Text, nullable=True, comment="Exit survey comment")
    bExit_OnEmailOfferList = Column(Boolean, nullable=True, comment="On email offer list after exit")
    dExit_WhenNeedAgain = Column(DateTime, nullable=True, comment="When might need storage again")
    MktgExitRentAgainID = Column(Integer, nullable=True, comment="Exit survey rent again ID")
    MktgExitReasonID = Column(Integer, nullable=True, comment="Exit survey reason ID")
    MktgExitSatisfactionID = Column(Integer, nullable=True, comment="Exit survey satisfaction ID")
    iExitSat_Cleanliness = Column(Integer, nullable=True, comment="Exit satisfaction: cleanliness")
    iExitSat_Safety = Column(Integer, nullable=True, comment="Exit satisfaction: safety")
    iExitSat_Services = Column(Integer, nullable=True, comment="Exit satisfaction: services")
    iExitSat_Staff = Column(Integer, nullable=True, comment="Exit satisfaction: staff")
    iExitSat_Price = Column(Integer, nullable=True, comment="Exit satisfaction: price")

    # =========================================================================
    # Web Security
    # =========================================================================
    sWebSecurityQ = Column(String(255), nullable=True, comment="Web security question")
    sWebSecurityQA = Column(String(255), nullable=True, comment="Web security answer")

    # =========================================================================
    # Geographic Coordinates
    # =========================================================================
    dcLongitude = Column(Numeric(14, 10), nullable=True, comment="Longitude")
    dcLatitude = Column(Numeric(14, 10), nullable=True, comment="Latitude")

    # =========================================================================
    # Notes & Icons
    # =========================================================================
    sTenNote = Column(Text, nullable=True, comment="Tenant notes")
    sIconList = Column(String(255), nullable=True, comment="Icon list for UI")

    # =========================================================================
    # Pictures
    # =========================================================================
    iPrimaryPic = Column(Integer, nullable=True, comment="Primary picture index")
    sPicFileN1 = Column(String(255), nullable=True, comment="Picture file 1")
    sPicFileN2 = Column(String(255), nullable=True, comment="Picture file 2")
    sPicFileN3 = Column(String(255), nullable=True, comment="Picture file 3")
    sPicFileN4 = Column(String(255), nullable=True, comment="Picture file 4")
    sPicFileN5 = Column(String(255), nullable=True, comment="Picture file 5")
    sPicFileN6 = Column(String(255), nullable=True, comment="Picture file 6")
    sPicFileN7 = Column(String(255), nullable=True, comment="Picture file 7")
    sPicFileN8 = Column(String(255), nullable=True, comment="Picture file 8")
    sPicFileN9 = Column(String(255), nullable=True, comment="Picture file 9")

    # =========================================================================
    # Global/National Account
    # =========================================================================
    bi_Tenant_GlobalNum = Column(BigInteger, nullable=True, comment="Tenant global number")
    iGlobalNum_NationalMasterAccount = Column(Integer, nullable=True, comment="National master account number")
    iGlobalNum_NationalFranchiseAccount = Column(Integer, nullable=True, comment="National franchise account number")

    # =========================================================================
    # Timestamps (from source)
    # =========================================================================
    dCreated = Column(DateTime, nullable=True, comment="Record creation date in source")
    dUpdated = Column(DateTime, nullable=True, comment="Record last update date in source")
    dDeleted = Column(DateTime, nullable=True, comment="Soft delete date")
    dArchived = Column(DateTime, nullable=True, comment="Archive date")

    # =========================================================================
    # Tracking Fields (ETL)
    # =========================================================================
    sLocationCode = Column(String(10), nullable=True, comment="Location code (L001, L002, etc.)")
    extract_date = Column(Date, nullable=True, comment="Date when data was extracted")
    data_source = Column(String(20), nullable=True, comment="Data source: 'api' or 'local_sql'")

    __table_args__ = (
        Index('idx_tenant_site', 'SiteID'),
        Index('idx_tenant_location', 'sLocationCode'),
        Index('idx_tenant_name', 'sLName', 'sFName'),
        Index('idx_tenant_company', 'sCompany'),
    )


class Ledger(Base, BaseModel, TimestampMixin):
    """
    Ledger data - expanded schema matching local SQL Server.

    Data Sources:
    - Local SQL Server (sldbclnt.Ledgers) - 186 columns
    - SOAP API LedgersByTenantID_v3 (CallCenterWs) - subset with tenant info

    Composite unique key: SiteID + LedgerID
    """
    __tablename__ = 'cc_ledgers'

    # =========================================================================
    # Primary Keys
    # =========================================================================
    LedgerID = Column(Integer, primary_key=True, nullable=False, comment="Unique ledger identifier")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")

    # =========================================================================
    # Foreign Keys
    # =========================================================================
    EmployeeID = Column(Integer, nullable=True, comment="Employee ID")
    unitID = Column(Integer, nullable=True, index=True, comment="Unit ID")
    BillingFreqID = Column(Integer, nullable=True, comment="Billing frequency ID")
    ConcessionID = Column(Integer, nullable=True, comment="Concession ID")
    PromoRentalID = Column(Integer, nullable=True, comment="Promotional rental ID")
    CreditCardID = Column(Integer, nullable=True, comment="Credit card ID reference")
    ACHBankInfoID = Column(Integer, nullable=True, comment="ACH bank info ID reference")

    # =========================================================================
    # Lease Information
    # =========================================================================
    sPurchOrderCode = Column(String(100), nullable=True, comment="Purchase order code")
    iLeaseNum = Column(Integer, nullable=True, comment="Lease number")
    dLease = Column(DateTime, nullable=True, comment="Lease date")
    dMovedIn = Column(DateTime, nullable=True, comment="Move-in date")
    dSchedOut = Column(DateTime, nullable=True, comment="Scheduled move-out date")
    dMovedOut = Column(DateTime, nullable=True, comment="Actual move-out date")
    dMovedOutExpected = Column(DateTime, nullable=True, comment="Expected move-out date")
    dSchedOutCreated = Column(DateTime, nullable=True, comment="When scheduled move-out was created")

    # =========================================================================
    # Anniversary & Dates
    # =========================================================================
    bAnniv = Column(Boolean, nullable=True, comment="Anniversary flag")
    dAnniv = Column(DateTime, nullable=True, comment="Anniversary date")
    dPaidThru = Column(DateTime, nullable=True, comment="Paid through date")
    dInsurPaidThru = Column(DateTime, nullable=True, comment="Insurance paid through date")
    dPmtLast = Column(DateTime, nullable=True, comment="Last payment date")
    dcPmtLastAmt = Column(Numeric(14, 4), nullable=True, comment="Last payment amount")

    # =========================================================================
    # Employee Tracking
    # =========================================================================
    iInByEmpID = Column(Integer, nullable=True, comment="Move-in by employee ID")
    iOutByEmpID = Column(Integer, nullable=True, comment="Move-out by employee ID")
    iTferByEmpID = Column(Integer, nullable=True, comment="Transfer by employee ID")
    iTferToLedID = Column(Integer, nullable=True, comment="Transferred to ledger ID")
    iTferFromLedID = Column(Integer, nullable=True, comment="Transferred from ledger ID")

    # =========================================================================
    # Rent & Insurance
    # =========================================================================
    dcRent = Column(Numeric(14, 4), nullable=True, comment="Current rent amount")
    dcSchedRent = Column(Numeric(14, 4), nullable=True, comment="Scheduled rent amount")
    dSchedRentStrt = Column(DateTime, nullable=True, comment="Scheduled rent start date")
    dRentLastChanged = Column(DateTime, nullable=True, comment="Rent last changed date")
    dcInsurPremium = Column(Numeric(14, 4), nullable=True, comment="Insurance premium")
    dcPushRateAtMoveIn = Column(Numeric(14, 4), nullable=True, comment="Push rate at move-in")

    # =========================================================================
    # Recurring Charges (8 types)
    # =========================================================================
    dcRecChg1 = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 1 amount")
    dcRecChg2 = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 2 amount")
    dcRecChg3 = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 3 amount")
    dcRecChg4 = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 4 amount")
    dcRecChg5 = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 5 amount")
    dcRecChg6 = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 6 amount")
    dcRecChg7 = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 7 amount")
    dcRecChg8 = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 8 amount")
    iRecChg1Qty = Column(Integer, nullable=True, comment="Recurring charge 1 quantity")
    iRecChg2Qty = Column(Integer, nullable=True, comment="Recurring charge 2 quantity")
    iRecChg3Qty = Column(Integer, nullable=True, comment="Recurring charge 3 quantity")
    iRecChg4Qty = Column(Integer, nullable=True, comment="Recurring charge 4 quantity")
    iRecChg5Qty = Column(Integer, nullable=True, comment="Recurring charge 5 quantity")
    iRecChg6Qty = Column(Integer, nullable=True, comment="Recurring charge 6 quantity")
    iRecChg7Qty = Column(Integer, nullable=True, comment="Recurring charge 7 quantity")
    iRecChg8Qty = Column(Integer, nullable=True, comment="Recurring charge 8 quantity")

    # =========================================================================
    # Fee Amounts
    # =========================================================================
    dcAdminFee = Column(Numeric(14, 4), nullable=True, comment="Admin fee amount")
    dcCutLockFee = Column(Numeric(14, 4), nullable=True, comment="Cut lock fee amount")
    dcNSFFee = Column(Numeric(14, 4), nullable=True, comment="NSF fee amount")
    dcAuctionFee = Column(Numeric(14, 4), nullable=True, comment="Auction fee amount")

    # =========================================================================
    # Late Fees (5 tiers)
    # =========================================================================
    dcLateFee1 = Column(Numeric(14, 4), nullable=True, comment="Late fee 1 amount")
    dcLateFee2 = Column(Numeric(14, 4), nullable=True, comment="Late fee 2 amount")
    dcLateFee3 = Column(Numeric(14, 4), nullable=True, comment="Late fee 3 amount")
    dcLateFee4 = Column(Numeric(14, 4), nullable=True, comment="Late fee 4 amount")
    dcLateFee5 = Column(Numeric(14, 4), nullable=True, comment="Late fee 5 amount")
    dLF1Strt = Column(DateTime, nullable=True, comment="Late fee 1 start date")
    dLF2Strt = Column(DateTime, nullable=True, comment="Late fee 2 start date")
    dLF3Strt = Column(DateTime, nullable=True, comment="Late fee 3 start date")
    dLF4Strt = Column(DateTime, nullable=True, comment="Late fee 4 start date")
    dLF5Strt = Column(DateTime, nullable=True, comment="Late fee 5 start date")
    iLateFeeType = Column(Integer, nullable=True, comment="Late fee type (legacy)")
    iLateFeeType1 = Column(Integer, nullable=True, comment="Late fee 1 type")
    iLateFeeType2 = Column(Integer, nullable=True, comment="Late fee 2 type")
    iLateFeeType3 = Column(Integer, nullable=True, comment="Late fee 3 type")
    iLateFeeType4 = Column(Integer, nullable=True, comment="Late fee 4 type")
    iLateFeeType5 = Column(Integer, nullable=True, comment="Late fee 5 type")
    dcPercentLateFee = Column(Numeric(14, 4), nullable=True, comment="Percent late fee (legacy)")
    dcPercentLateFee2 = Column(Numeric(14, 4), nullable=True, comment="Percent late fee 2")
    dcPercentLateFee3 = Column(Numeric(14, 4), nullable=True, comment="Percent late fee 3")
    dcPercentLateFee4 = Column(Numeric(14, 4), nullable=True, comment="Percent late fee 4")
    dcPercentLateFee5 = Column(Numeric(14, 4), nullable=True, comment="Percent late fee 5")
    bLateFee1IsApplied = Column(Boolean, nullable=True, comment="Late fee 1 applied flag")
    bLateFee2IsApplied = Column(Boolean, nullable=True, comment="Late fee 2 applied flag")
    bLateFee3IsApplied = Column(Boolean, nullable=True, comment="Late fee 3 applied flag")
    bLateFee4IsApplied = Column(Boolean, nullable=True, comment="Late fee 4 applied flag")
    bLateFee5IsApplied = Column(Boolean, nullable=True, comment="Late fee 5 applied flag")

    # =========================================================================
    # Credit Card Information (may be encrypted/tokenized)
    # =========================================================================
    iCreditCardTypeID = Column(Integer, nullable=True, comment="Credit card type ID")
    sCreditCardNum = Column(String(255), nullable=True, comment="Credit card number (encrypted/tokenized)")
    dCreditCardExpir = Column(DateTime, nullable=True, comment="Credit card expiration")
    sCreditCardHolderName = Column(String(255), nullable=True, comment="Cardholder name")
    sCreditCardCVV2 = Column(String(10), nullable=True, comment="CVV2 (should be empty/encrypted)")
    sCreditCardStreet = Column(String(255), nullable=True, comment="Card billing street")
    sCreditCardZip = Column(String(20), nullable=True, comment="Card billing zip")
    iCreditCardAVSResult = Column(Integer, nullable=True, comment="AVS verification result")

    # =========================================================================
    # ACH/Bank Information (may be encrypted)
    # =========================================================================
    sACH_CheckWriterAcctNum = Column(String(255), nullable=True, comment="ACH account number (encrypted)")
    sACH_CheckWriterAcctName = Column(String(255), nullable=True, comment="ACH account name")
    sACH_ABA_RoutingNum = Column(String(50), nullable=True, comment="ABA routing number")
    sACH_RDFI = Column(String(100), nullable=True, comment="ACH RDFI")
    sACH_Check_SavingsCode = Column(String(10), nullable=True, comment="Checking/Savings code")

    # =========================================================================
    # Auto-Billing
    # =========================================================================
    iAutoBillType = Column(Integer, nullable=True, comment="Auto-billing type")
    iProcessDayOfMonth = Column(Integer, nullable=True, comment="Auto-bill process day of month")
    bAutoBillChargeFee = Column(Boolean, nullable=True, comment="Auto-bill charge fee flag")
    bAutoBillEmailNotify = Column(Boolean, nullable=True, comment="Auto-bill email notify flag")
    dAutoBillEnabled = Column(DateTime, nullable=True, comment="Auto-bill enabled date")

    # =========================================================================
    # Past Due Settings
    # =========================================================================
    bDisablePDue = Column(Boolean, nullable=True, comment="Disable past due flag")
    dDisablePDueStrt = Column(DateTime, nullable=True, comment="Disable past due start date")
    dDisablePDueEnd = Column(DateTime, nullable=True, comment="Disable past due end date")

    # =========================================================================
    # NSF (Non-Sufficient Funds)
    # =========================================================================
    bHadNSF = Column(Boolean, nullable=True, comment="Had NSF flag")
    nNSF = Column(Integer, nullable=True, comment="NSF count")

    # =========================================================================
    # Invoice Settings
    # =========================================================================
    bInvoice = Column(Boolean, nullable=True, comment="Invoice flag")
    bInvoiceEmail = Column(Boolean, nullable=True, comment="Invoice by email flag")
    iInvoiceDeliveryType = Column(Integer, nullable=True, comment="Invoice delivery type")
    iInvoiceDaysBefore = Column(Integer, nullable=True, comment="Invoice days before due")
    dInvoiceLast = Column(DateTime, nullable=True, comment="Last invoice date")
    bWaiveInvoiceFee = Column(Boolean, nullable=True, comment="Waive invoice fee flag")

    # =========================================================================
    # Status Flags
    # =========================================================================
    bTaxRent = Column(Boolean, nullable=True, comment="Tax rent flag")
    bOverlocked = Column(Boolean, nullable=True, comment="Overlocked flag")
    bGateLocked = Column(Boolean, nullable=True, comment="Gate locked flag")
    bPermanent = Column(Boolean, nullable=True, comment="Permanent ledger flag")
    bExcludeFromRevenueMgmt = Column(Boolean, nullable=True, comment="Exclude from revenue management")

    # =========================================================================
    # Balances - Security Deposit
    # =========================================================================
    dcSecDepPaid = Column(Numeric(14, 4), nullable=True, comment="Security deposit paid")
    dcSecDepBal = Column(Numeric(14, 4), nullable=True, comment="Security deposit balance")

    # =========================================================================
    # Balances - Rent & Fees
    # =========================================================================
    dcRentBal = Column(Numeric(14, 4), nullable=True, comment="Rent balance")
    dcLateFee1Bal = Column(Numeric(14, 4), nullable=True, comment="Late fee 1 balance")
    dcLateFee2Bal = Column(Numeric(14, 4), nullable=True, comment="Late fee 2 balance")
    dcLateFee3Bal = Column(Numeric(14, 4), nullable=True, comment="Late fee 3 balance")
    dcLateFee4Bal = Column(Numeric(14, 4), nullable=True, comment="Late fee 4 balance")
    dcLateFee5Bal = Column(Numeric(14, 4), nullable=True, comment="Late fee 5 balance")
    dcLateFee1CurrBal = Column(Numeric(14, 4), nullable=True, comment="Late fee 1 current balance")
    dcLateFee2CurrBal = Column(Numeric(14, 4), nullable=True, comment="Late fee 2 current balance")
    dcLateFee3CurrBal = Column(Numeric(14, 4), nullable=True, comment="Late fee 3 current balance")
    dcLateFee4CurrBal = Column(Numeric(14, 4), nullable=True, comment="Late fee 4 current balance")
    dcLateFee5CurrBal = Column(Numeric(14, 4), nullable=True, comment="Late fee 5 current balance")
    dcNSFBal = Column(Numeric(14, 4), nullable=True, comment="NSF balance")
    dcAdminFeeBal = Column(Numeric(14, 4), nullable=True, comment="Admin fee balance")
    dcCutLockFeeBal = Column(Numeric(14, 4), nullable=True, comment="Cut lock fee balance")
    dcAuctionFeeBal = Column(Numeric(14, 4), nullable=True, comment="Auction fee balance")

    # =========================================================================
    # Balances - Recurring Charges
    # =========================================================================
    dcRecChg1Bal = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 1 balance")
    dcRecChg2Bal = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 2 balance")
    dcRecChg3Bal = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 3 balance")
    dcRecChg4Bal = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 4 balance")
    dcRecChg5Bal = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 5 balance")
    dcRecChg6Bal = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 6 balance")
    dcRecChg7Bal = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 7 balance")
    dcRecChg8Bal = Column(Numeric(14, 4), nullable=True, comment="Recurring charge 8 balance")

    # =========================================================================
    # Balances - Other
    # =========================================================================
    dcInsurBal = Column(Numeric(14, 4), nullable=True, comment="Insurance balance")
    dcPOSBal = Column(Numeric(14, 4), nullable=True, comment="POS balance")
    dcCreditBal = Column(Numeric(14, 4), nullable=True, comment="Credit balance")
    dcOtherBal = Column(Numeric(14, 4), nullable=True, comment="Other balance")
    dcRefundDue = Column(Numeric(14, 4), nullable=True, comment="Refund due amount")

    # =========================================================================
    # Balances - Tax
    # =========================================================================
    dcRentTaxBal = Column(Numeric(14, 4), nullable=True, comment="Rent tax balance")
    dcLateFeeTaxBal = Column(Numeric(14, 4), nullable=True, comment="Late fee tax balance")
    dcOtherTaxBal = Column(Numeric(14, 4), nullable=True, comment="Other tax balance")
    dcRecChgTaxBal = Column(Numeric(14, 4), nullable=True, comment="Recurring charge tax balance")
    dcInsurTaxBal = Column(Numeric(14, 4), nullable=True, comment="Insurance tax balance")
    dcPOSTaxBal = Column(Numeric(14, 4), nullable=True, comment="POS tax balance")

    # =========================================================================
    # Charge Period Dates
    # =========================================================================
    dRentLastChgStrt = Column(DateTime, nullable=True, comment="Rent last charge start")
    dRentLastChgEnd = Column(DateTime, nullable=True, comment="Rent last charge end")
    dInsurLastChgStrt = Column(DateTime, nullable=True, comment="Insurance last charge start")
    dInsurLastChgEnd = Column(DateTime, nullable=True, comment="Insurance last charge end")
    dRecChg1LastChgStrt = Column(DateTime, nullable=True, comment="Recurring charge 1 last start")
    dRecChg1LastChgEnd = Column(DateTime, nullable=True, comment="Recurring charge 1 last end")
    dRecChg2LastChgStrt = Column(DateTime, nullable=True, comment="Recurring charge 2 last start")
    dRecChg2LastChgEnd = Column(DateTime, nullable=True, comment="Recurring charge 2 last end")
    dRecChg3LastChgStrt = Column(DateTime, nullable=True, comment="Recurring charge 3 last start")
    dRecChg3LastChgEnd = Column(DateTime, nullable=True, comment="Recurring charge 3 last end")
    dRecChg4LastChgStrt = Column(DateTime, nullable=True, comment="Recurring charge 4 last start")
    dRecChg4LastChgEnd = Column(DateTime, nullable=True, comment="Recurring charge 4 last end")
    dRecChg5LastChgStrt = Column(DateTime, nullable=True, comment="Recurring charge 5 last start")
    dRecChg5LastChgEnd = Column(DateTime, nullable=True, comment="Recurring charge 5 last end")
    dRecChg6LastChgStrt = Column(DateTime, nullable=True, comment="Recurring charge 6 last start")
    dRecChg6LastChgEnd = Column(DateTime, nullable=True, comment="Recurring charge 6 last end")
    dRecChg7LastChgStrt = Column(DateTime, nullable=True, comment="Recurring charge 7 last start")
    dRecChg7LastChgEnd = Column(DateTime, nullable=True, comment="Recurring charge 7 last end")
    dRecChg8LastChgStrt = Column(DateTime, nullable=True, comment="Recurring charge 8 last start")
    dRecChg8LastChgEnd = Column(DateTime, nullable=True, comment="Recurring charge 8 last end")

    # =========================================================================
    # Vehicle Information
    # =========================================================================
    sLicPlate = Column(String(50), nullable=True, comment="License plate")
    sVehicleDesc = Column(String(255), nullable=True, comment="Vehicle description")

    # =========================================================================
    # Complimentary/Discount
    # =========================================================================
    sReasonComplimentary = Column(Text, nullable=True, comment="Reason for complimentary")
    sCompanySub = Column(String(255), nullable=True, comment="Company subsidiary")

    # =========================================================================
    # Revenue Management / Rate Increase
    # =========================================================================
    dcTR_RateIncreaseAmt = Column(Numeric(14, 4), nullable=True, comment="Target rate increase amount")
    dTR_LastRateIncreaseNotice = Column(DateTime, nullable=True, comment="Last rate increase notice date")
    dTR_NextRateReview = Column(DateTime, nullable=True, comment="Next rate review date")
    iTR_RateIncreasePendingStatus = Column(Integer, nullable=True, comment="Rate increase pending status")
    iRemoveDiscPlanOnSchedRateChange = Column(Integer, nullable=True, comment="Remove discount on rate change")

    # =========================================================================
    # Auction
    # =========================================================================
    iAuctionStatus = Column(Integer, nullable=True, comment="Auction status")
    dAuctionDate = Column(DateTime, nullable=True, comment="Auction date")

    # =========================================================================
    # Timestamps (from source)
    # =========================================================================
    dCreated = Column(DateTime, nullable=True, comment="Record creation date in source")
    dUpdated = Column(DateTime, nullable=True, comment="Record last update date in source")
    dDeleted = Column(DateTime, nullable=True, comment="Soft delete date")
    dArchived = Column(DateTime, nullable=True, comment="Archive date")

    # =========================================================================
    # API-Only Fields (not in local SQL, from LedgersByTenantID_v3)
    # =========================================================================
    TenantID = Column(Integer, nullable=True, index=True, comment="Tenant ID (from API)")
    sUnitName = Column(String(100), nullable=True, comment="Unit name (from API)")
    TenantName = Column(String(255), nullable=True, comment="Full tenant name (from API)")
    sMrMrs = Column(String(20), nullable=True, comment="Title (from API)")
    sFName = Column(String(100), nullable=True, comment="First name (from API)")
    sMI = Column(String(10), nullable=True, comment="Middle initial (from API)")
    sLName = Column(String(100), nullable=True, comment="Last name (from API)")
    sCompany = Column(String(255), nullable=True, comment="Company (from API)")
    sAddr1 = Column(String(255), nullable=True, comment="Address 1 (from API)")
    sAddr2 = Column(String(255), nullable=True, comment="Address 2 (from API)")
    sCity = Column(String(100), nullable=True, comment="City (from API)")
    sRegion = Column(String(100), nullable=True, comment="Region (from API)")
    sPostalCode = Column(String(20), nullable=True, comment="Postal code (from API)")
    sCountry = Column(String(100), nullable=True, comment="Country (from API)")
    sPhone = Column(String(50), nullable=True, comment="Phone (from API)")
    sMobile = Column(String(50), nullable=True, comment="Mobile (from API)")
    sEmail = Column(String(255), nullable=True, comment="Email (from API)")
    sFax = Column(String(50), nullable=True, comment="Fax (from API)")
    sAccessCode = Column(String(50), nullable=True, comment="Access code (from API)")
    sAccessCode2 = Column(String(50), nullable=True, comment="Access code 2 (from API)")
    dcChargeBalance = Column(Numeric(14, 4), nullable=True, comment="Charge balance (from API)")
    dcTotalDue = Column(Numeric(14, 4), nullable=True, comment="Total due (from API)")
    dcTaxRateRent = Column(Numeric(8, 4), nullable=True, comment="Rent tax rate (from API)")
    dcTaxRateInsurance = Column(Numeric(8, 4), nullable=True, comment="Insurance tax rate (from API)")
    sBillingFrequency = Column(String(50), nullable=True, comment="Billing frequency (from API)")
    iDefLeaseNum = Column(Integer, nullable=True, comment="Default lease number (from API)")
    bCommercial = Column(Boolean, nullable=True, comment="Commercial flag (from API)")
    bTaxExempt = Column(Boolean, nullable=True, comment="Tax exempt flag (from API)")
    bSpecial = Column(Boolean, nullable=True, comment="Special flag (from API)")
    bNeverLockOut = Column(Boolean, nullable=True, comment="Never lock out (from API)")
    bCompanyIsTenant = Column(Boolean, nullable=True, comment="Company is tenant (from API)")
    bExcludeFromInsurance = Column(Boolean, nullable=True, comment="Exclude from insurance (from API)")
    bSMSOptIn = Column(Boolean, nullable=True, comment="SMS opt-in (from API)")
    MarketingID = Column(Integer, nullable=True, comment="Marketing ID (from API)")
    MktgDistanceID = Column(Integer, nullable=True, comment="Marketing distance ID (from API)")
    MktgReasonID = Column(Integer, nullable=True, comment="Marketing reason ID (from API)")
    MktgTypeID = Column(Integer, nullable=True, comment="Marketing type ID (from API)")
    sLicense = Column(String(100), nullable=True, comment="License (from API)")
    sTaxID = Column(String(100), nullable=True, comment="Tax ID (from API)")
    sTaxExemptCode = Column(String(50), nullable=True, comment="Tax exempt code (from API)")
    sTenNote = Column(Text, nullable=True, comment="Tenant note (from API)")
    dcLongitude = Column(Numeric(14, 10), nullable=True, comment="Longitude (from API)")
    dcLatitude = Column(Numeric(14, 10), nullable=True, comment="Latitude (from API)")

    # =========================================================================
    # Tracking Fields (ETL)
    # =========================================================================
    extract_date = Column(Date, nullable=True, comment="Date when data was extracted")
    data_source = Column(String(20), nullable=True, comment="Data source: 'api' or 'local_sql'")

    __table_args__ = (
        Index('idx_ledger_site', 'SiteID'),
        Index('idx_ledger_tenant', 'TenantID'),
        Index('idx_ledger_unit', 'unitID'),
        Index('idx_ledger_paid_thru', 'dPaidThru'),
        Index('idx_ledger_moved_in', 'dMovedIn'),
        Index('idx_ledger_moved_out', 'dMovedOut'),
    )


class Charge(Base, BaseModel, TimestampMixin):
    """
    Charge data - expanded schema matching local SQL Server.

    Data Sources:
    - Local SQL Server (sldbclnt.Charges) - 33 columns
    - SOAP API ChargesAllByLedgerID (CallCenterWs) - subset of fields

    Composite unique key: SiteID + ChargeID + dcPmtAmt
    (Same ChargeID can appear multiple times for partial payments)
    """
    __tablename__ = 'cc_charges'

    # =========================================================================
    # Primary Keys
    # =========================================================================
    ChargeID = Column(Integer, primary_key=True, nullable=False, comment="Unique charge identifier")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    dcPmtAmt = Column(Numeric(14, 4), primary_key=True, nullable=False, default=0, comment="Payment amount (part of PK for partial payments)")

    # =========================================================================
    # Foreign Keys
    # =========================================================================
    ChargeDescID = Column(Integer, nullable=True, comment="Charge description ID")
    LedgerID = Column(Integer, nullable=False, index=True, comment="Ledger ID")
    InsurLedgerID = Column(Integer, nullable=True, comment="Insurance ledger ID")
    FiscalID = Column(Integer, nullable=True, comment="Fiscal period ID")
    ConcessionID = Column(Integer, nullable=True, comment="Concession ID")
    EmployeeID = Column(Integer, nullable=True, comment="Employee ID who created charge")
    ACHID = Column(Integer, nullable=True, comment="ACH transaction ID")
    Disc_MemoID = Column(Integer, nullable=True, comment="Discount memo ID")
    ReceiptID_NSF = Column(Integer, nullable=True, comment="NSF receipt ID")
    QTChargeID = Column(Integer, nullable=True, comment="Quick-transfer charge ID")

    # =========================================================================
    # Charge Amounts
    # =========================================================================
    dcAmt = Column(Numeric(14, 4), nullable=True, comment="Charge amount")
    dcTax1 = Column(Numeric(14, 4), nullable=True, comment="Tax 1 amount")
    dcTax2 = Column(Numeric(14, 4), nullable=True, comment="Tax 2 amount")
    dcQty = Column(Numeric(10, 4), nullable=True, comment="Quantity")
    dcStdPrice = Column(Numeric(14, 4), nullable=True, comment="Standard price")
    dcPrice = Column(Numeric(14, 4), nullable=True, comment="Actual price")
    dcCost = Column(Numeric(14, 4), nullable=True, comment="Cost")
    dcPriceTax1 = Column(Numeric(14, 4), nullable=True, comment="Price with tax 1")
    dcPriceTax2 = Column(Numeric(14, 4), nullable=True, comment="Price with tax 2")

    # =========================================================================
    # Charge Dates
    # =========================================================================
    dChgStrt = Column(DateTime, nullable=True, comment="Charge start date")
    dChgEnd = Column(DateTime, nullable=True, comment="Charge end date")
    dCreated = Column(DateTime, nullable=True, comment="Charge creation date")

    # =========================================================================
    # Flags
    # =========================================================================
    bMoveIn = Column(Boolean, nullable=True, comment="Move-in charge flag")
    bMoveOut = Column(Boolean, nullable=True, comment="Move-out charge flag")
    bNSF = Column(Boolean, nullable=True, comment="NSF flag")
    iNSFFlag = Column(Integer, nullable=True, comment="NSF flag type")

    # =========================================================================
    # Promotional
    # =========================================================================
    iPromoGlobalNum = Column(Integer, nullable=True, comment="Promotional global number")

    # =========================================================================
    # Timestamps (from source)
    # =========================================================================
    dUpdated = Column(DateTime, nullable=True, comment="Record last update date in source")
    dArchived = Column(DateTime, nullable=True, comment="Archive date")
    dDeleted = Column(DateTime, nullable=True, comment="Soft delete date")

    # =========================================================================
    # Charge Description (from ChargeDesc join or API)
    # =========================================================================
    sChgCategory = Column(String(50), nullable=True, comment="Charge category")
    sChgDesc = Column(String(255), nullable=True, comment="Charge description")
    sDefChgDesc = Column(String(255), nullable=True, comment="Default charge description")

    # =========================================================================
    # Tracking Fields (ETL)
    # =========================================================================
    extract_date = Column(Date, nullable=True, comment="Date when data was extracted")
    data_source = Column(String(20), nullable=True, comment="Data source: 'api' or 'local_sql'")

    __table_args__ = (
        Index('idx_charge_site', 'SiteID'),
        Index('idx_charge_ledger', 'LedgerID'),
        Index('idx_charge_category', 'sChgCategory'),
        Index('idx_charge_date', 'dChgStrt'),
        Index('idx_charge_created', 'dCreated'),
    )


# =============================================================================
# API-Only Models (SOAP API data, separate from cc_* local SQL dump tables)
# =============================================================================

class CcwsTenant(Base, BaseModel, TimestampMixin):
    """
    Tenant data from CallCenterWs SOAP API.

    Data Sources:
    - TenantList endpoint (17 basic fields — used for initial seed)
    - GetTenantInfoByTenantID endpoint (full tenant record — used for backfill)

    Composite unique key: SiteID + TenantID
    """
    __tablename__ = 'ccws_tenants'

    # Primary Keys
    TenantID = Column(Integer, primary_key=True, nullable=False, comment="Unique tenant identifier")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")

    # Access & Security
    sAccessCode = Column(String(100), nullable=True, comment="Primary gate access code")
    sAccessCode2 = Column(String(100), nullable=True, comment="Secondary access code")
    iAccessCode2Type = Column(Integer, nullable=True, comment="Type of secondary access code")
    sWebPassword = Column(String(255), nullable=True, comment="Web portal password (encrypted)")
    bAllowedFacilityAccess = Column(Boolean, nullable=True, comment="Facility access allowed flag")

    # Primary Contact Information
    sMrMrs = Column(String(20), nullable=True, comment="Title (Mr., Mrs., etc.)")
    sFName = Column(String(100), nullable=True, comment="First name")
    sMI = Column(String(10), nullable=True, comment="Middle initial")
    sLName = Column(String(100), nullable=True, comment="Last name")
    sCompany = Column(String(255), nullable=True, comment="Company name")
    sAddr1 = Column(String(255), nullable=True, comment="Address line 1")
    sAddr2 = Column(String(255), nullable=True, comment="Address line 2")
    sCity = Column(String(100), nullable=True, comment="City")
    sRegion = Column(String(100), nullable=True, comment="State/Region")
    sPostalCode = Column(String(20), nullable=True, comment="Postal code")
    sCountry = Column(String(100), nullable=True, comment="Country")
    sPhone = Column(String(100), nullable=True, comment="Phone")
    sFax = Column(String(100), nullable=True, comment="Fax number")
    sEmail = Column(String(255), nullable=True, comment="Email")
    sPager = Column(String(100), nullable=True, comment="Pager number")
    sMobile = Column(String(100), nullable=True, comment="Mobile")
    sCountryCodeMobile = Column(String(10), nullable=True, comment="Mobile country code")

    # Alternate Contact
    sMrMrsAlt = Column(String(20), nullable=True, comment="Alt contact title")
    sFNameAlt = Column(String(100), nullable=True, comment="Alt contact first name")
    sMIAlt = Column(String(10), nullable=True, comment="Alt contact middle initial")
    sLNameAlt = Column(String(100), nullable=True, comment="Alt contact last name")
    sAddr1Alt = Column(String(255), nullable=True, comment="Alt contact address line 1")
    sAddr2Alt = Column(String(255), nullable=True, comment="Alt contact address line 2")
    sCityAlt = Column(String(100), nullable=True, comment="Alt contact city")
    sRegionAlt = Column(String(100), nullable=True, comment="Alt contact state/region")
    sPostalCodeAlt = Column(String(20), nullable=True, comment="Alt contact postal code")
    sCountryAlt = Column(String(100), nullable=True, comment="Alt contact country")
    sPhoneAlt = Column(String(100), nullable=True, comment="Alt contact phone")
    sEmailAlt = Column(String(255), nullable=True, comment="Alt contact email")
    sRelationshipAlt = Column(String(100), nullable=True, comment="Relationship to tenant")

    # Business Contact
    sMrMrsBus = Column(String(20), nullable=True, comment="Business contact title")
    sFNameBus = Column(String(100), nullable=True, comment="Business contact first name")
    sMIBus = Column(String(10), nullable=True, comment="Business contact middle initial")
    sLNameBus = Column(String(100), nullable=True, comment="Business contact last name")
    sCompanyBus = Column(String(255), nullable=True, comment="Business company name")
    sAddr1Bus = Column(String(255), nullable=True, comment="Business address line 1")
    sAddr2Bus = Column(String(255), nullable=True, comment="Business address line 2")
    sCityBus = Column(String(100), nullable=True, comment="Business city")
    sRegionBus = Column(String(100), nullable=True, comment="Business state/region")
    sPostalCodeBus = Column(String(20), nullable=True, comment="Business postal code")
    sCountryBus = Column(String(100), nullable=True, comment="Business country")
    sPhoneBus = Column(String(100), nullable=True, comment="Business phone")
    sEmailBus = Column(String(255), nullable=True, comment="Business email")

    # Additional Contact
    sMrMrsAdd = Column(String(20), nullable=True, comment="Additional contact title")
    sFNameAdd = Column(String(100), nullable=True, comment="Additional contact first name")
    sMIAdd = Column(String(10), nullable=True, comment="Additional contact middle initial")
    sLNameAdd = Column(String(100), nullable=True, comment="Additional contact last name")
    sAddr1Add = Column(String(255), nullable=True, comment="Additional contact address line 1")
    sAddr2Add = Column(String(255), nullable=True, comment="Additional contact address line 2")
    sCityAdd = Column(String(100), nullable=True, comment="Additional contact city")
    sRegionAdd = Column(String(100), nullable=True, comment="Additional contact state/region")
    sPostalCodeAdd = Column(String(20), nullable=True, comment="Additional contact postal code")
    sCountryAdd = Column(String(100), nullable=True, comment="Additional contact country")
    sPhoneAdd = Column(String(100), nullable=True, comment="Additional contact phone")
    sEmailAdd = Column(String(255), nullable=True, comment="Additional contact email")

    # Identification
    sLicense = Column(String(100), nullable=True, comment="License number")
    sLicRegion = Column(String(100), nullable=True, comment="License issuing region")
    sSSN = Column(String(100), nullable=True, comment="Social Security Number (encrypted)")
    sTaxID = Column(String(100), nullable=True, comment="Tax ID number")
    sTaxExemptCode = Column(String(100), nullable=True, comment="Tax exemption code")
    dDOB = Column(DateTime, nullable=True, comment="Date of birth")
    iGender = Column(Integer, nullable=True, comment="Gender code")
    sEmployer = Column(String(255), nullable=True, comment="Employer name")

    # Status Flags
    bCommercial = Column(Boolean, nullable=True, comment="Commercial tenant flag")
    bTaxExempt = Column(Boolean, nullable=True, comment="Tax exempt flag")
    bSpecial = Column(Boolean, nullable=True, comment="Special tenant flag")
    bNeverLockOut = Column(Boolean, nullable=True, comment="Never lock out flag")
    bCompanyIsTenant = Column(Boolean, nullable=True, comment="Company is the tenant flag")
    bOnWaitingList = Column(Boolean, nullable=True, comment="On waiting list flag")
    bNoChecks = Column(Boolean, nullable=True, comment="No checks accepted flag")
    bPermanent = Column(Boolean, nullable=True, comment="Permanent tenant flag")
    bWalkInPOS = Column(Boolean, nullable=True, comment="Walk-in POS customer flag")
    bSpecialAlert = Column(Boolean, nullable=True, comment="Special alert flag")
    bPermanentGateLockout = Column(Boolean, nullable=True, comment="Permanent gate lockout flag")
    bSMSOptIn = Column(Boolean, nullable=True, comment="SMS opt-in flag")
    bDisabledWebAccess = Column(Boolean, nullable=True, comment="Web access disabled flag")
    bHasActiveLedger = Column(Boolean, nullable=True, comment="Has active ledger flag")
    iBlackListRating = Column(Integer, nullable=True, comment="Blacklist rating")
    iTenEvents_OptOut = Column(Integer, nullable=True, comment="Tenant events opt-out setting")

    # Marketing
    MarketingID = Column(Integer, nullable=True, comment="Marketing source ID")
    MktgDistanceID = Column(Integer, nullable=True, comment="Marketing distance ID")
    MktgWhatID = Column(Integer, nullable=True, comment="Marketing 'what' ID")
    MktgReasonID = Column(Integer, nullable=True, comment="Marketing reason ID")
    MktgWhyID = Column(Integer, nullable=True, comment="Marketing 'why' ID")
    MktgTypeID = Column(Integer, nullable=True, comment="Marketing type ID")
    iHowManyOtherStorageCosDidYouContact = Column(Integer, nullable=True, comment="Other storage companies contacted")
    iUsedSelfStorageInThePast = Column(Integer, nullable=True, comment="Used self storage before")
    iMktg_DidYouVisitWebSite = Column(Integer, nullable=True, comment="Visited website flag")

    # Exit Survey
    bExit_OnEmailOfferList = Column(Boolean, nullable=True, comment="On email offer list after exit")
    iExitSat_Cleanliness = Column(Integer, nullable=True, comment="Exit satisfaction: cleanliness")
    iExitSat_Safety = Column(Integer, nullable=True, comment="Exit satisfaction: safety")
    iExitSat_Services = Column(Integer, nullable=True, comment="Exit satisfaction: services")
    iExitSat_Staff = Column(Integer, nullable=True, comment="Exit satisfaction: staff")
    iExitSat_Price = Column(Integer, nullable=True, comment="Exit satisfaction: price")

    # Geographic
    dcLongitude = Column(Numeric(14, 10), nullable=True, comment="Longitude")
    dcLatitude = Column(Numeric(14, 10), nullable=True, comment="Latitude")

    # Notes & Icons
    sTenNote = Column(Text, nullable=True, comment="Tenant notes")
    sIconList = Column(String(255), nullable=True, comment="Icon list for UI")

    # Pictures
    iPrimaryPic = Column(Integer, nullable=True, comment="Primary picture index")
    sPicFileN1 = Column(String(255), nullable=True, comment="Picture file 1")
    sPicFileN2 = Column(String(255), nullable=True, comment="Picture file 2")
    sPicFileN3 = Column(String(255), nullable=True, comment="Picture file 3")
    sPicFileN4 = Column(String(255), nullable=True, comment="Picture file 4")
    sPicFileN5 = Column(String(255), nullable=True, comment="Picture file 5")
    sPicFileN6 = Column(String(255), nullable=True, comment="Picture file 6")
    sPicFileN7 = Column(String(255), nullable=True, comment="Picture file 7")
    sPicFileN8 = Column(String(255), nullable=True, comment="Picture file 8")
    sPicFileN9 = Column(String(255), nullable=True, comment="Picture file 9")

    # Global/National Account
    iGlobalNum_NationalMasterAccount = Column(Integer, nullable=True, comment="National master account number")
    iGlobalNum_NationalFranchiseAccount = Column(Integer, nullable=True, comment="National franchise account number")

    # Source Timestamps
    dCreated = Column(DateTime, nullable=True, comment="Record creation date in source")
    dUpdated = Column(DateTime, nullable=True, comment="Record last update date in source")
    uTS = Column(String(100), nullable=True, comment="Source timestamp token")

    # ETL Tracking
    sLocationCode = Column(String(10), nullable=True, comment="Location code (L001, etc.)")
    extract_date = Column(Date, nullable=True, comment="Date when data was extracted")

    __table_args__ = (
        Index('idx_ccws_tenant_site', 'SiteID'),
        Index('idx_ccws_tenant_location', 'sLocationCode'),
        Index('idx_ccws_tenant_name', 'sLName', 'sFName'),
        Index('idx_ccws_tenant_extract', 'extract_date'),
    )


class CcwsLedger(Base, BaseModel, TimestampMixin):
    """
    Ledger data from CallCenterWs LedgersByTenantID_v3 SOAP API endpoint.

    Contains denormalized tenant + ledger fields as returned by the API.
    Data Source: CallCenterWs LedgersByTenantID_v3 endpoint (141 fields)
    Composite unique key: SiteID + LedgerID
    """
    __tablename__ = 'ccws_ledgers'

    # =========================================================================
    # Primary Keys
    # =========================================================================
    LedgerID = Column(Integer, primary_key=True, nullable=False, comment="Unique ledger identifier")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")

    # =========================================================================
    # Foreign Keys / IDs
    # =========================================================================
    TenantID = Column(Integer, nullable=True, index=True, comment="Tenant ID")
    EmployeeID = Column(Integer, nullable=True, comment="Employee ID")
    UnitID = Column(Integer, nullable=True, index=True, comment="Unit ID")
    MarketingID = Column(Integer, nullable=True, comment="Marketing ID")
    MktgDistanceID = Column(Integer, nullable=True, comment="Marketing distance ID")
    MktgReasonID = Column(Integer, nullable=True, comment="Marketing reason ID")
    MktgTypeID = Column(Integer, nullable=True, comment="Marketing type ID")
    MktgWhatID = Column(Integer, nullable=True, comment="Marketing 'what' ID")
    MktgWhyID = Column(Integer, nullable=True, comment="Marketing 'why' ID")
    TimeZoneID = Column(Integer, nullable=True, comment="Time zone ID")

    # =========================================================================
    # Tenant Name (denormalized)
    # =========================================================================
    TenantName = Column(String(255), nullable=True, comment="Full tenant name")
    sUnitName = Column(String(100), nullable=True, comment="Unit name")

    # =========================================================================
    # Primary Contact
    # =========================================================================
    sMrMrs = Column(String(20), nullable=True, comment="Title")
    sFName = Column(String(100), nullable=True, comment="First name")
    sMI = Column(String(10), nullable=True, comment="Middle initial")
    sLName = Column(String(100), nullable=True, comment="Last name")
    sCompany = Column(String(255), nullable=True, comment="Company")
    sAddr1 = Column(String(255), nullable=True, comment="Address line 1")
    sAddr2 = Column(String(255), nullable=True, comment="Address line 2")
    sCity = Column(String(100), nullable=True, comment="City")
    sRegion = Column(String(100), nullable=True, comment="State/Region")
    sPostalCode = Column(String(20), nullable=True, comment="Postal code")
    sCountry = Column(String(100), nullable=True, comment="Country")
    sPhone = Column(String(100), nullable=True, comment="Phone")
    sFax = Column(String(100), nullable=True, comment="Fax")
    sEmail = Column(String(255), nullable=True, comment="Email")
    sPager = Column(String(100), nullable=True, comment="Pager")
    sMobile = Column(String(100), nullable=True, comment="Mobile")
    sCountryCodeMobile = Column(String(10), nullable=True, comment="Mobile country code")

    # =========================================================================
    # Alternate Contact
    # =========================================================================
    sMrMrsAlt = Column(String(20), nullable=True, comment="Alt contact title")
    sFNameAlt = Column(String(100), nullable=True, comment="Alt contact first name")
    sMIAlt = Column(String(10), nullable=True, comment="Alt contact middle initial")
    sLNameAlt = Column(String(100), nullable=True, comment="Alt contact last name")
    sAddr1Alt = Column(String(255), nullable=True, comment="Alt contact address 1")
    sAddr2Alt = Column(String(255), nullable=True, comment="Alt contact address 2")
    sCityAlt = Column(String(100), nullable=True, comment="Alt contact city")
    sRegionAlt = Column(String(100), nullable=True, comment="Alt contact region")
    sPostalCodeAlt = Column(String(20), nullable=True, comment="Alt contact postal code")
    sCountryAlt = Column(String(100), nullable=True, comment="Alt contact country")
    sPhoneAlt = Column(String(100), nullable=True, comment="Alt contact phone")
    sEmailAlt = Column(String(255), nullable=True, comment="Alt contact email")
    sRelationshipAlt = Column(String(100), nullable=True, comment="Relationship to tenant")

    # =========================================================================
    # Business Contact
    # =========================================================================
    sMrMrsBus = Column(String(20), nullable=True, comment="Business contact title")
    sFNameBus = Column(String(100), nullable=True, comment="Business contact first name")
    sMIBus = Column(String(10), nullable=True, comment="Business contact middle initial")
    sLNameBus = Column(String(100), nullable=True, comment="Business contact last name")
    sCompanyBus = Column(String(255), nullable=True, comment="Business company")
    sAddr1Bus = Column(String(255), nullable=True, comment="Business address 1")
    sAddr2Bus = Column(String(255), nullable=True, comment="Business address 2")
    sCityBus = Column(String(100), nullable=True, comment="Business city")
    sRegionBus = Column(String(100), nullable=True, comment="Business region")
    sPostalCodeBus = Column(String(20), nullable=True, comment="Business postal code")
    sCountryBus = Column(String(100), nullable=True, comment="Business country")
    sPhoneBus = Column(String(100), nullable=True, comment="Business phone")
    sEmailBus = Column(String(255), nullable=True, comment="Business email")

    # =========================================================================
    # Additional Contact
    # =========================================================================
    sMrMrsAdd = Column(String(20), nullable=True, comment="Additional contact title")
    sFNameAdd = Column(String(100), nullable=True, comment="Additional contact first name")
    sMIAdd = Column(String(10), nullable=True, comment="Additional contact middle initial")
    sLNameAdd = Column(String(100), nullable=True, comment="Additional contact last name")
    sAddr1Add = Column(String(255), nullable=True, comment="Additional contact address 1")
    sAddr2Add = Column(String(255), nullable=True, comment="Additional contact address 2")
    sCityAdd = Column(String(100), nullable=True, comment="Additional contact city")
    sRegionAdd = Column(String(100), nullable=True, comment="Additional contact region")
    sPostalCodeAdd = Column(String(20), nullable=True, comment="Additional contact postal code")
    sCountryAdd = Column(String(100), nullable=True, comment="Additional contact country")
    sPhoneAdd = Column(String(100), nullable=True, comment="Additional contact phone")
    sEmailAdd = Column(String(255), nullable=True, comment="Additional contact email")

    # =========================================================================
    # Access & Security
    # =========================================================================
    sAccessCode = Column(String(100), nullable=True, comment="Primary access code")
    sAccessCode2 = Column(String(100), nullable=True, comment="Secondary access code")
    iAccessCode2Type = Column(Integer, nullable=True, comment="Secondary access code type")

    # =========================================================================
    # Identification
    # =========================================================================
    sLicense = Column(String(100), nullable=True, comment="License number")
    sLicRegion = Column(String(100), nullable=True, comment="License region")
    sSSN = Column(String(100), nullable=True, comment="SSN (encrypted)")
    sTaxID = Column(String(100), nullable=True, comment="Tax ID")
    sTaxExemptCode = Column(String(100), nullable=True, comment="Tax exempt code")
    dDOB = Column(DateTime, nullable=True, comment="Date of birth")
    iGender = Column(Integer, nullable=True, comment="Gender code")

    # =========================================================================
    # Status Flags
    # =========================================================================
    bCommercial = Column(Boolean, nullable=True, comment="Commercial flag")
    bCompanyIsTenant = Column(Boolean, nullable=True, comment="Company is tenant")
    bDisabledWebAccess = Column(Boolean, nullable=True, comment="Web access disabled")
    bExcludeFromInsurance = Column(Boolean, nullable=True, comment="Exclude from insurance")
    bInvoice = Column(Boolean, nullable=True, comment="Invoice flag")
    bNeverLockOut = Column(Boolean, nullable=True, comment="Never lock out")
    bNoChecks = Column(Boolean, nullable=True, comment="No checks accepted")
    bOnWaitingList = Column(Boolean, nullable=True, comment="On waiting list")
    bOverlocked = Column(Boolean, nullable=True, comment="Overlocked flag")
    bPermanent = Column(Boolean, nullable=True, comment="Permanent ledger")
    bPermanentGateLockout = Column(Boolean, nullable=True, comment="Permanent gate lockout")
    bSMSOptIn = Column(Boolean, nullable=True, comment="SMS opt-in")
    bSpecial = Column(Boolean, nullable=True, comment="Special flag")
    bSpecialAlert = Column(Boolean, nullable=True, comment="Special alert")
    bTaxExempt = Column(Boolean, nullable=True, comment="Tax exempt")
    bWalkInPOS = Column(Boolean, nullable=True, comment="Walk-in POS")

    # =========================================================================
    # Lease & Dates
    # =========================================================================
    iLeaseNum = Column(Integer, nullable=True, comment="Lease number")
    iDefLeaseNum = Column(Integer, nullable=True, comment="Default lease number")
    dMovedIn = Column(DateTime, nullable=True, comment="Move-in date")
    dPaidThru = Column(DateTime, nullable=True, comment="Paid through date")
    dSchedOut = Column(DateTime, nullable=True, comment="Scheduled move-out date")
    dAnniv = Column(DateTime, nullable=True, comment="Anniversary date")
    dCreated = Column(DateTime, nullable=True, comment="Record creation date")
    dUpdated = Column(DateTime, nullable=True, comment="Record last update date")

    # =========================================================================
    # Financial
    # =========================================================================
    dcRent = Column(Numeric(14, 4), nullable=True, comment="Current rent")
    dcInsurPremium = Column(Numeric(14, 4), nullable=True, comment="Insurance premium")
    dcChargeBalance = Column(Numeric(14, 4), nullable=True, comment="Charge balance")
    dcTotalDue = Column(Numeric(14, 4), nullable=True, comment="Total due")
    dcTaxRateRent = Column(Numeric(8, 4), nullable=True, comment="Rent tax rate")
    dcTaxRateInsurance = Column(Numeric(8, 4), nullable=True, comment="Insurance tax rate")
    sBillingFrequency = Column(String(50), nullable=True, comment="Billing frequency")

    # =========================================================================
    # Billing
    # =========================================================================
    iAutoBillType = Column(Integer, nullable=True, comment="Auto-bill type")
    iInvoiceDeliveryType = Column(Integer, nullable=True, comment="Invoice delivery type")

    # =========================================================================
    # Marketing
    # =========================================================================
    iHowManyOtherStorageCosDidYouContact = Column(Integer, nullable=True, comment="Other storage cos contacted")
    iUsedSelfStorageInThePast = Column(Integer, nullable=True, comment="Used self storage before")
    iMktg_DidYouVisitWebSite = Column(Integer, nullable=True, comment="Visited website")

    # =========================================================================
    # Exit Survey
    # =========================================================================
    bExit_OnEmailOfferList = Column(Boolean, nullable=True, comment="On email offer list after exit")
    iExitSat_Cleanliness = Column(Integer, nullable=True, comment="Exit satisfaction: cleanliness")
    iExitSat_Price = Column(Integer, nullable=True, comment="Exit satisfaction: price")
    iExitSat_Safety = Column(Integer, nullable=True, comment="Exit satisfaction: safety")
    iExitSat_Services = Column(Integer, nullable=True, comment="Exit satisfaction: services")
    iExitSat_Staff = Column(Integer, nullable=True, comment="Exit satisfaction: staff")

    # =========================================================================
    # Blacklist / Events
    # =========================================================================
    iBlackListRating = Column(Integer, nullable=True, comment="Blacklist rating")
    iTenEvents_OptOut = Column(Integer, nullable=True, comment="Tenant events opt-out")

    # =========================================================================
    # Geographic
    # =========================================================================
    dcLatitude = Column(Numeric(14, 10), nullable=True, comment="Latitude")
    dcLongitude = Column(Numeric(14, 10), nullable=True, comment="Longitude")

    # =========================================================================
    # Notes & Icons
    # =========================================================================
    sTenNote = Column(Text, nullable=True, comment="Tenant note")
    sIconList = Column(String(255), nullable=True, comment="Icon list for UI")

    # =========================================================================
    # Pictures
    # =========================================================================
    iPrimaryPic = Column(Integer, nullable=True, comment="Primary picture index")
    sPicFileN1 = Column(String(255), nullable=True, comment="Picture file 1")
    sPicFileN2 = Column(String(255), nullable=True, comment="Picture file 2")
    sPicFileN3 = Column(String(255), nullable=True, comment="Picture file 3")
    sPicFileN4 = Column(String(255), nullable=True, comment="Picture file 4")
    sPicFileN5 = Column(String(255), nullable=True, comment="Picture file 5")
    sPicFileN6 = Column(String(255), nullable=True, comment="Picture file 6")
    sPicFileN7 = Column(String(255), nullable=True, comment="Picture file 7")
    sPicFileN8 = Column(String(255), nullable=True, comment="Picture file 8")
    sPicFileN9 = Column(String(255), nullable=True, comment="Picture file 9")

    # =========================================================================
    # Source Timestamps
    # =========================================================================
    uTS = Column(String(100), nullable=True, comment="Timestamp string from API")
    uTSbigint = Column(BigInteger, nullable=True, comment="Timestamp as bigint from API")

    # =========================================================================
    # ETL Tracking
    # =========================================================================
    extract_date = Column(Date, nullable=True, comment="Date when data was extracted")

    __table_args__ = (
        Index('idx_ccws_ledger_site', 'SiteID'),
        Index('idx_ccws_ledger_tenant', 'TenantID'),
        Index('idx_ccws_ledger_unit', 'UnitID'),
        Index('idx_ccws_ledger_paid_thru', 'dPaidThru'),
        Index('idx_ccws_ledger_moved_in', 'dMovedIn'),
        Index('idx_ccws_ledger_sched_out', 'dSchedOut'),
    )


class CcwsCharge(Base, BaseModel, TimestampMixin):
    """
    Charge data from CallCenterWs ChargesAllByLedgerID SOAP API endpoint.

    Data Source: CallCenterWs ChargesAllByLedgerID endpoint (16 fields)
    Composite unique key: SiteID + ChargeID + dcPmtAmt
    """
    __tablename__ = 'ccws_charges'

    # =========================================================================
    # Primary Keys
    # =========================================================================
    ChargeID = Column(Integer, primary_key=True, nullable=False, comment="Unique charge identifier")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    dcPmtAmt = Column(Numeric(14, 4), primary_key=True, nullable=False, default=0, comment="Payment amount (PK for partial payments)")

    # =========================================================================
    # Foreign Keys
    # =========================================================================
    ChargeDescID = Column(Integer, nullable=True, comment="Charge description ID")
    LedgerID = Column(Integer, nullable=False, index=True, comment="Ledger ID (from ETL context)")

    # =========================================================================
    # Amounts
    # =========================================================================
    dcAmt = Column(Numeric(14, 4), nullable=True, comment="Charge amount")
    dcPrice = Column(Numeric(14, 4), nullable=True, comment="Price")
    dcQty = Column(Numeric(10, 4), nullable=True, comment="Quantity")
    dcTax1 = Column(Numeric(14, 4), nullable=True, comment="Tax 1 amount")
    dcTax2 = Column(Numeric(14, 4), nullable=True, comment="Tax 2 amount")

    # =========================================================================
    # Dates
    # =========================================================================
    dChgStrt = Column(DateTime, nullable=True, comment="Charge start date")
    dChgEnd = Column(DateTime, nullable=True, comment="Charge end date")

    # =========================================================================
    # Flags
    # =========================================================================
    bMoveIn = Column(Boolean, nullable=True, comment="Move-in charge flag")
    bMoveOut = Column(Boolean, nullable=True, comment="Move-out charge flag")

    # =========================================================================
    # Descriptions
    # =========================================================================
    sChgCategory = Column(String(50), nullable=True, comment="Charge category")
    sChgDesc = Column(String(255), nullable=True, comment="Charge description")
    sDefChgDesc = Column(String(255), nullable=True, comment="Default charge description")

    # =========================================================================
    # ETL Tracking
    # =========================================================================
    extract_date = Column(Date, nullable=True, comment="Date when data was extracted")

    __table_args__ = (
        Index('idx_ccws_charge_site', 'SiteID'),
        Index('idx_ccws_charge_ledger', 'LedgerID'),
        Index('idx_ccws_charge_category', 'sChgCategory'),
        Index('idx_ccws_charge_date', 'dChgStrt'),
    )


class CcwsDiscount(Base, BaseModel, TimestampMixin):
    """
    Discount/Concession plan data from DiscountPlansRetrieve endpoint.

    Data Source: CallCenterWs DiscountPlansRetrieve SOAP endpoint
    Contains concession plan definitions with joined ChargeDesc fields.

    Composite unique key: SiteID + ConcessionID
    """
    __tablename__ = 'ccws_discount'

    # =========================================================================
    # Primary Key
    # =========================================================================
    id = Column(Integer, primary_key=True, autoincrement=True, comment="Auto-increment primary key")

    # =========================================================================
    # Core Identifiers
    # =========================================================================
    ConcessionID = Column(Integer, nullable=False, index=True, comment="Concession plan ID")
    SiteID = Column(Integer, nullable=False, index=True, comment="Site/Location ID")
    iConcessionGlobalNum = Column(Integer, comment="Global concession number")
    QTTouchDiscPlanID = Column(String(50), comment="QT Touch discount plan ID")
    PlanName_TermID = Column(String(50), comment="Plan name term ID")
    OldPK = Column(String(50), comment="Legacy primary key")

    # =========================================================================
    # Plan Info
    # =========================================================================
    sDefPlanName = Column(String(255), comment="Default plan name")
    sPlanName = Column(String(255), comment="Plan name")
    sDescription = Column(Text, comment="Plan description")
    sComment = Column(Text, comment="Plan comment")
    sCouponCode = Column(String(100), comment="Coupon code")

    # =========================================================================
    # Plan Dates
    # =========================================================================
    dPlanStrt = Column(DateTime, comment="Plan start date")
    dPlanEnd = Column(DateTime, comment="Plan end date")
    dCreated = Column(DateTime, comment="Record created date")
    dUpdated = Column(DateTime, comment="Record updated date")
    dArchived = Column(DateTime, comment="Record archived date")
    dDisabled = Column(DateTime, comment="Record disabled date")
    dDeleted = Column(DateTime, comment="Record deleted date")

    # =========================================================================
    # Plan Configuration
    # =========================================================================
    iShowOn = Column(Integer, comment="Show-on flag")
    bNeverExpires = Column(Boolean, comment="Never expires flag")
    iExpirMonths = Column(Integer, comment="Expiration months")
    bPrepay = Column(Boolean, comment="Prepay flag")
    bOnPmt = Column(Boolean, comment="On payment flag")
    bManualCredit = Column(Boolean, comment="Manual credit flag")
    iPrePaidMonths = Column(Integer, comment="Prepaid months")
    iInMonth = Column(Integer, comment="In-month value")
    bPermanent = Column(Boolean, comment="Permanent flag")

    # =========================================================================
    # Discount Amounts
    # =========================================================================
    iAmtType = Column(Integer, comment="Amount type")
    dcChgAmt = Column(Numeric(14, 4), comment="Charge amount")
    dcFixedDiscount = Column(Numeric(14, 4), comment="Fixed discount amount")
    dcPCDiscount = Column(Numeric(14, 4), comment="Percentage discount")
    bRound = Column(Boolean, comment="Round flag")
    dcRoundTo = Column(Numeric(14, 4), comment="Round-to amount")
    dcMaxAmountOff = Column(Numeric(14, 4), comment="Maximum amount off")

    # =========================================================================
    # Charge Reference
    # =========================================================================
    ChargeDescID = Column(Integer, comment="Charge description ID")
    iQty = Column(Integer, comment="Quantity")
    iOfferItemAction = Column(Integer, comment="Offer item action")

    # =========================================================================
    # Corporate / Occupancy Rules
    # =========================================================================
    bForCorp = Column(Boolean, comment="For corporate flag")
    dcMaxOccPct = Column(Numeric(14, 4), comment="Max occupancy percentage")
    bForAllUnits = Column(Boolean, comment="For all units flag")
    iExcludeIfLessThanUnitsTotal = Column(Integer, comment="Exclude if less than units total")
    dcMaxOccPctExcludeIfMoreThanUnitsTotal = Column(Numeric(14, 4), comment="Max occ pct exclude if more than units total")
    iExcludeIfMoreThanUnitsTotal = Column(Integer, comment="Exclude if more than units total")
    iAvailableAt = Column(Integer, comment="Available-at flag")
    bEligibleToRemoveIfPastDue = Column(Boolean, comment="Eligible to remove if past due")
    iRestrictionFlags = Column(Integer, comment="Restriction flags bitmask")
    iOccupancyPctUnitCountMethod = Column(Integer, comment="Occupancy pct unit count method")

    # =========================================================================
    # ChargeDesc Joined Fields (suffix "1" from API join)
    # =========================================================================
    ChargeDescID1 = Column(Integer, comment="Joined ChargeDesc ID")
    SiteID1 = Column(Integer, comment="Joined ChargeDesc SiteID")
    ChartOfAcctID = Column(Integer, comment="Chart of account ID")
    ChgDesc_TermID = Column(Integer, comment="Charge description term ID")
    sDefChgDesc = Column(String(255), comment="Default charge description")
    sChgDesc = Column(String(255), comment="Charge description")
    sVendor = Column(String(255), comment="Vendor name")
    sVendorPhone = Column(String(50), comment="Vendor phone")
    sReorderPartNum = Column(String(100), comment="Reorder part number")
    sChgCategory = Column(String(100), comment="Charge category")
    bApplyAtMoveIn = Column(Boolean, comment="Apply at move-in flag")
    bProrateAtMoveIn = Column(Boolean, comment="Prorate at move-in flag")
    bPermanent1 = Column(Boolean, comment="ChargeDesc permanent flag")
    dcPrice = Column(Numeric(14, 4), comment="Price")
    dcTax1Rate = Column(Numeric(14, 6), comment="Tax 1 rate")
    dcTax2Rate = Column(Numeric(14, 6), comment="Tax 2 rate")
    dcCost = Column(Numeric(14, 4), comment="Cost")
    dcInStock = Column(Numeric(14, 4), comment="In stock quantity")
    dcOrderPt = Column(Numeric(14, 4), comment="Order point")
    dChgStrt = Column(DateTime, comment="Charge start date")
    dChgDisabled = Column(DateTime, comment="Charge disabled date")
    bUseMileageRate = Column(Boolean, comment="Use mileage rate flag")
    dcMileageRate = Column(Numeric(14, 4), comment="Mileage rate")
    iIncludedMiles = Column(Integer, comment="Included miles")
    dDisabled1 = Column(DateTime, comment="ChargeDesc disabled date")
    dDeleted1 = Column(DateTime, comment="ChargeDesc deleted date")
    dUpdated1 = Column(DateTime, comment="ChargeDesc updated date")
    OldPK1 = Column(Integer, comment="ChargeDesc legacy PK")
    sCorpCategory = Column(String(255), comment="Corporate category")
    sBarCode = Column(String(100), comment="Barcode")
    iPriceType = Column(Integer, comment="Price type")
    dcPCRate = Column(Numeric(14, 4), comment="PC rate")
    dcMinPriceIfPC = Column(Numeric(14, 4), comment="Min price if PC")
    bRound1 = Column(Boolean, comment="ChargeDesc round flag")
    dcRoundTo1 = Column(Numeric(14, 4), comment="ChargeDesc round-to amount")

    __table_args__ = (
        Index('idx_ccws_discount_site_concession', 'SiteID', 'ConcessionID', unique=True),
        Index('idx_ccws_discount_site', 'SiteID'),
        Index('idx_ccws_discount_plan_name', 'sPlanName'),
    )


class FXRateMonthly(Base, BaseModel, TimestampMixin):
    """
    Monthly average FX rates calculated from daily FX rates.

    Composite unique key: year_month + target_currency
    Refresh: Calculated during daily refresh script
    """
    __tablename__ = 'fx_rates_monthly'

    # ========================================================================
    # Composite Primary Key
    # ========================================================================
    year_month = Column(String(7), primary_key=True, nullable=False,
                       comment="YYYY-MM format")
    target_currency = Column(String(3), primary_key=True, nullable=False,
                            comment="Target currency code")

    # ========================================================================
    # Date Components
    # ========================================================================
    year = Column(Integer, nullable=False, index=True,
                  comment="Year extracted from year_month")
    month = Column(Integer, nullable=False, index=True,
                   comment="Month extracted from year_month (1-12)")

    # ========================================================================
    # Monthly Statistics
    # ========================================================================
    base_currency = Column(String(3), nullable=False, default='SGD',
                          comment="Base currency (always SGD)")
    avg_rate = Column(Numeric(18, 10), nullable=False,
                     comment="Monthly average rate")
    min_rate = Column(Numeric(18, 10), nullable=False,
                     comment="Minimum rate in month")
    max_rate = Column(Numeric(18, 10), nullable=False,
                     comment="Maximum rate in month")
    first_rate = Column(Numeric(18, 10), nullable=False,
                       comment="First rate of month (opening)")
    last_rate = Column(Numeric(18, 10), nullable=False,
                      comment="Last rate of month (closing)")
    trading_days = Column(Integer, nullable=False,
                         comment="Number of actual trading days")
    total_days = Column(Integer, nullable=False,
                       comment="Total calendar days in data")

    # ========================================================================
    # Indexes for Performance
    # ========================================================================
    __table_args__ = (
        Index('idx_fx_monthly_ym', 'year_month'),
        Index('idx_fx_monthly_currency', 'target_currency'),
        Index('idx_fx_monthly_year', 'year'),
    )


# ============================================================================
# Management Summary Models (ManagementSummary endpoint)
# ============================================================================


class MSDeposits(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Deposits data.
    Contains daily, month-to-date, and year-to-date deposit amounts by payment type.

    Data Source: SOAP API ManagementSummary endpoint (Deposits table)
    """
    __tablename__ = 'ms_deposits'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")

    # Daily Deposits
    DCash = Column(Numeric(14, 4), comment="Daily cash deposits")
    DCheck = Column(Numeric(14, 4), comment="Daily check deposits")
    DCharge = Column(Numeric(14, 4), comment="Daily credit card deposits")
    DACH = Column(Numeric(14, 4), comment="Daily ACH deposits")
    DDebit = Column(Numeric(14, 4), comment="Daily debit deposits")
    DDepTot = Column(Numeric(14, 4), comment="Daily total deposits")
    DMiscDep = Column(Numeric(14, 4), comment="Daily misc deposits")
    DNet = Column(Numeric(14, 4), comment="Daily net deposits")

    # Month-to-Date Deposits
    MCash = Column(Numeric(14, 4), comment="MTD cash deposits")
    MCheck = Column(Numeric(14, 4), comment="MTD check deposits")
    MCharge = Column(Numeric(14, 4), comment="MTD credit card deposits")
    MACH = Column(Numeric(14, 4), comment="MTD ACH deposits")
    MDebit = Column(Numeric(14, 4), comment="MTD debit deposits")
    MDepTot = Column(Numeric(14, 4), comment="MTD total deposits")
    MMiscDep = Column(Numeric(14, 4), comment="MTD misc deposits")
    MNet = Column(Numeric(14, 4), comment="MTD net deposits")

    # Year-to-Date Deposits
    YCash = Column(Numeric(14, 4), comment="YTD cash deposits")
    YCheck = Column(Numeric(14, 4), comment="YTD check deposits")
    YCharge = Column(Numeric(14, 4), comment="YTD credit card deposits")
    YACH = Column(Numeric(14, 4), comment="YTD ACH deposits")
    YDebit = Column(Numeric(14, 4), comment="YTD debit deposits")
    YDepTot = Column(Numeric(14, 4), comment="YTD total deposits")
    YMiscDep = Column(Numeric(14, 4), comment="YTD misc deposits")
    YNet = Column(Numeric(14, 4), comment="YTD net deposits")

    __table_args__ = (
        Index('idx_ms_deposits_composite', 'extract_date', 'SiteID'),
        Index('idx_ms_deposits_extract_date', 'extract_date'),
    )


class MSReceipts(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Receipts by category.
    Contains daily, month-to-date, and year-to-date receipts by category (Rent, Insurance, etc.).

    Data Source: SOAP API ManagementSummary endpoint (Receipts table)
    """
    __tablename__ = 'ms_receipts'

    # Composite Primary Key (SortID differentiates receipt types)
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    SortID = Column(Integer, primary_key=True, nullable=False, comment="Sort order / category ID")

    # Category Info
    sDesc = Column(String(100), comment="Receipt category description (Rent, Insurance, Late Fee, etc.)")

    # Amounts
    dcD = Column(Numeric(14, 4), comment="Daily amount")
    dcM = Column(Numeric(14, 4), comment="Month-to-date amount")
    dcY = Column(Numeric(14, 4), comment="Year-to-date amount")

    __table_args__ = (
        Index('idx_ms_receipts_composite', 'extract_date', 'SiteID', 'SortID'),
        Index('idx_ms_receipts_extract_date', 'extract_date'),
    )


class MSConcessions(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Concessions by category.
    Contains daily, month-to-date, and year-to-date concession amounts.

    Data Source: SOAP API ManagementSummary endpoint (Concessions table)
    """
    __tablename__ = 'ms_concessions'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    iSortOrder = Column(Integer, primary_key=True, nullable=False, comment="Sort order")

    # Category Info
    sCatName = Column(String(100), comment="Concession category name (Rent, Late Fees, Taxes, Other)")

    # Amounts
    DAmt = Column(Numeric(14, 4), comment="Daily concession amount")
    MAmt = Column(Numeric(14, 4), comment="Month-to-date concession amount")
    YAmt = Column(Numeric(14, 4), comment="Year-to-date concession amount")

    __table_args__ = (
        Index('idx_ms_concessions_composite', 'extract_date', 'SiteID', 'iSortOrder'),
        Index('idx_ms_concessions_extract_date', 'extract_date'),
    )


class MSDiscounts(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Discounts by category.
    Contains daily, month-to-date, and year-to-date discount amounts.

    Data Source: SOAP API ManagementSummary endpoint (Discounts table)
    """
    __tablename__ = 'ms_discounts'

    # Composite Primary Key (iSortOrder + bNeverExpires to differentiate expiring vs non-expiring)
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    iSortOrder = Column(Integer, primary_key=True, nullable=False, comment="Sort order")
    bNeverExpires = Column(Boolean, primary_key=True, nullable=False, comment="Never expires flag")

    # Category Info
    sCatName = Column(String(100), comment="Discount category name")

    # Amounts
    DAmt = Column(Numeric(14, 4), comment="Daily discount amount")
    MAmt = Column(Numeric(14, 4), comment="Month-to-date discount amount")
    YAmt = Column(Numeric(14, 4), comment="Year-to-date discount amount")

    __table_args__ = (
        Index('idx_ms_discounts_composite', 'extract_date', 'SiteID', 'iSortOrder'),
        Index('idx_ms_discounts_extract_date', 'extract_date'),
    )


class MSLiabilities(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Liabilities data.
    Contains counts and amounts for rent, insurance, recurring, and deposit liabilities.

    Data Source: SOAP API ManagementSummary endpoint (Liabilities table)
    """
    __tablename__ = 'ms_liabilities'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")

    # Rent Liabilities
    iCountRent = Column(Integer, comment="Count of rent liabilities")
    dcAmtRent = Column(Numeric(14, 4), comment="Amount of rent liabilities")

    # Insurance Liabilities
    iCountInsurance = Column(Integer, comment="Count of insurance liabilities")
    dcAmtInsurance = Column(Numeric(14, 4), comment="Amount of insurance liabilities")

    # Recurring Liabilities
    iCountRecurring = Column(Integer, comment="Count of recurring liabilities")
    dcAmtRecurring = Column(Numeric(14, 4), comment="Amount of recurring liabilities")

    # Deposit Liabilities
    iCountDeposit = Column(Integer, comment="Count of deposit liabilities")
    dcAmtDeposit = Column(Numeric(14, 4), comment="Amount of deposit liabilities")

    __table_args__ = (
        Index('idx_ms_liabilities_composite', 'extract_date', 'SiteID'),
        Index('idx_ms_liabilities_extract_date', 'extract_date'),
    )


class MSMisc(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Miscellaneous metrics.
    Contains various operational metrics including rent receipts, NSF, bad debts, move activity, etc.

    Data Source: SOAP API ManagementSummary endpoint (Misc table)
    """
    __tablename__ = 'ms_misc'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")

    # Date Range Labels
    sDaily = Column(String(50), comment="Daily label")
    sMTD = Column(String(50), comment="Month-to-date label")
    sYTD = Column(String(50), comment="Year-to-date label")

    # Date Ranges
    dDStrt = Column(DateTime, nullable=True, comment="Daily start date")
    dDEnd = Column(DateTime, nullable=True, comment="Daily end date")
    dMStrt = Column(DateTime, nullable=True, comment="MTD start date")
    dMEnd = Column(DateTime, nullable=True, comment="MTD end date")
    dYStrt = Column(DateTime, nullable=True, comment="YTD start date")
    dYEnd = Column(DateTime, nullable=True, comment="YTD end date")

    # Date Range Strings
    sDSDate = Column(String(20), comment="Daily start date string")
    sDEDate = Column(String(20), comment="Daily end date string")
    sMSDate = Column(String(20), comment="MTD start date string")
    sMEDate = Column(String(20), comment="MTD end date string")
    sYSDate = Column(String(20), comment="YTD start date string")
    sYEDate = Column(String(20), comment="YTD end date string")

    # Rent Receipts - Prepaid/Current/Past
    DPreR = Column(Numeric(14, 4), comment="Daily prepaid rent")
    MPreR = Column(Numeric(14, 4), comment="MTD prepaid rent")
    DCurrR = Column(Numeric(14, 4), comment="Daily current rent")
    MCurrR = Column(Numeric(14, 4), comment="MTD current rent")
    DPastR = Column(Numeric(14, 4), comment="Daily past rent")
    MPastR = Column(Numeric(14, 4), comment="MTD past rent")
    DTotR = Column(Numeric(14, 4), comment="Daily total rent")
    MTotR = Column(Numeric(14, 4), comment="MTD total rent")

    # Late Fees
    DCurrLF = Column(Numeric(14, 4), comment="Daily current late fees")
    MCurrLF = Column(Numeric(14, 4), comment="MTD current late fees")
    DPastLF = Column(Numeric(14, 4), comment="Daily past late fees")
    MPastLF = Column(Numeric(14, 4), comment="MTD past late fees")
    DTotLF = Column(Numeric(14, 4), comment="Daily total late fees")
    MTotLF = Column(Numeric(14, 4), comment="MTD total late fees")

    # NSF
    DNNSF = Column(Integer, comment="Daily NSF count")
    MNNSF = Column(Integer, comment="MTD NSF count")
    YNNSF = Column(Integer, comment="YTD NSF count")
    DNSFTot = Column(Numeric(14, 4), comment="Daily NSF total")
    MNSFTot = Column(Numeric(14, 4), comment="MTD NSF total")
    YNSFTot = Column(Numeric(14, 4), comment="YTD NSF total")

    # Bad Debts
    dcDBadDebts = Column(Numeric(14, 4), comment="Daily bad debts")
    dcMBadDebts = Column(Numeric(14, 4), comment="MTD bad debts")
    dcYBadDebts = Column(Numeric(14, 4), comment="YTD bad debts")

    # Move-Ins
    DIns = Column(Integer, comment="Daily move-ins (insurance)")
    MIns = Column(Integer, comment="MTD move-ins (insurance)")
    YIns = Column(Integer, comment="YTD move-ins (insurance)")
    DInsN = Column(Integer, comment="Daily move-ins (new)")
    MInsN = Column(Integer, comment="MTD move-ins (new)")
    YInsN = Column(Integer, comment="YTD move-ins (new)")

    # Move-Outs
    DOuts = Column(Integer, comment="Daily move-outs")
    MOuts = Column(Integer, comment="MTD move-outs")
    YOuts = Column(Integer, comment="YTD move-outs")

    # Transfers
    DXFers = Column(Integer, comment="Daily transfers")
    MXFers = Column(Integer, comment="MTD transfers")
    YXFers = Column(Integer, comment="YTD transfers")

    # Calls In
    DCallsIn = Column(Integer, comment="Daily calls in")
    MCallsIn = Column(Integer, comment="MTD calls in")
    YCallsIn = Column(Integer, comment="YTD calls in")

    # Walk-Ins
    DWalkIns = Column(Integer, comment="Daily walk-ins")
    MWalkIns = Column(Integer, comment="MTD walk-ins")
    YWalkIns = Column(Integer, comment="YTD walk-ins")

    # Walk-In Conversions
    DWInsConv = Column(Integer, comment="Daily walk-in conversions")
    MWInsConv = Column(Integer, comment="MTD walk-in conversions")
    YWInsConv = Column(Integer, comment="YTD walk-in conversions")

    # Letters
    DLetters = Column(Integer, comment="Daily letters")
    MLetters = Column(Integer, comment="MTD letters")
    YLetters = Column(Integer, comment="YTD letters")

    # Calls
    DCalls = Column(Integer, comment="Daily calls")
    MCalls = Column(Integer, comment="MTD calls")
    YCalls = Column(Integer, comment="YTD calls")

    # Payments & Fees
    DPmts = Column(Integer, comment="Daily payments")
    MPmts = Column(Integer, comment="MTD payments")
    DFeesChg = Column(Integer, comment="Daily fees charged")
    MFeesChg = Column(Integer, comment="MTD fees charged")

    # Merchandise
    DMerch = Column(Integer, comment="Daily merchandise")
    MMerch = Column(Integer, comment="MTD merchandise")

    # Misc
    sRHSCap = Column(String(100), comment="RHS caption (As of date)")
    WaitNum = Column(Integer, comment="Waiting list count")
    Overlocks = Column(Integer, comment="Overlock count")
    AutoBilled = Column(Integer, comment="Auto-billed count")
    Insurance = Column(Integer, comment="Insurance count")

    # Prepaid Rent
    PrepaidRentUnits = Column(Integer, comment="Prepaid rent units")
    PrepaidAmt = Column(Numeric(14, 4), comment="Prepaid rent amount")

    # Prepaid Insurance
    PrepaidInsurUnits = Column(Integer, comment="Prepaid insurance units")
    PrepaidInsurAmt = Column(Numeric(14, 4), comment="Prepaid insurance amount")

    # Prepaid Recurring
    PrepaidRecUnits = Column(Integer, comment="Prepaid recurring units")
    PrepaidRecAmt = Column(Numeric(14, 4), comment="Prepaid recurring amount")

    # Security Deposit Liability
    SecDepLiabilityUnits = Column(Integer, comment="Security deposit liability units")
    SecDepLiabilityAmt = Column(Numeric(14, 4), comment="Security deposit liability amount")

    __table_args__ = (
        Index('idx_ms_misc_composite', 'extract_date', 'SiteID'),
        Index('idx_ms_misc_extract_date', 'extract_date'),
    )


class MSRentalActivity(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Rental Activity metrics.
    Contains occupancy, area, standard rate, and gross potential metrics.

    Data Source: SOAP API ManagementSummary endpoint (RentalActivity table)
    """
    __tablename__ = 'ms_rental_activity'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")

    # Unit Counts
    Occupied = Column(Integer, comment="Occupied units")
    Vacant = Column(Integer, comment="Vacant units")
    Unrentable = Column(Integer, comment="Unrentable units")
    Complimentary = Column(Integer, comment="Complimentary units")
    TotalUnits = Column(Integer, comment="Total units")

    # Unit Percentages
    OccupiedPC = Column(Numeric(10, 4), comment="Occupied percentage")
    VacantPC = Column(Numeric(10, 4), comment="Vacant percentage")
    UnrentablePC = Column(Numeric(10, 4), comment="Unrentable percentage")
    ComplimentaryPC = Column(Numeric(10, 4), comment="Complimentary percentage")
    TotalUnitsPC = Column(Numeric(10, 4), comment="Total units percentage")

    # Area
    OccupiedArea = Column(Numeric(14, 4), comment="Occupied area")
    VacantArea = Column(Numeric(14, 4), comment="Vacant area")
    UnrentableArea = Column(Numeric(14, 4), comment="Unrentable area")
    ComplimentaryArea = Column(Numeric(14, 4), comment="Complimentary area")
    TotalArea = Column(Numeric(14, 4), comment="Total area")

    # Area Percentages (C prefix = calculated)
    COccupiedAreaPC = Column(Numeric(10, 4), comment="Occupied area percentage")
    VacantAreaPC = Column(Numeric(10, 4), comment="Vacant area percentage")
    UnrentableAreaPC = Column(Numeric(10, 4), comment="Unrentable area percentage")
    ComplimentaryAreaPC = Column(Numeric(10, 4), comment="Complimentary area percentage")
    TotalAreaPC = Column(Numeric(10, 4), comment="Total area percentage")

    # Standard Rates
    StdRateOccupied = Column(Numeric(14, 4), comment="Standard rate occupied")
    StdRateVacant = Column(Numeric(14, 4), comment="Standard rate vacant")
    StdRateUnrentable = Column(Numeric(14, 4), comment="Standard rate unrentable")
    StdRateComplimentary = Column(Numeric(14, 4), comment="Standard rate complimentary")
    StdRateTotal = Column(Numeric(14, 4), comment="Standard rate total")

    # Standard Rate Percentages
    StdRateOccupiedPC = Column(Numeric(10, 4), comment="Standard rate occupied percentage")
    StdRateVacantPC = Column(Numeric(10, 4), comment="Standard rate vacant percentage")
    StdRateUnrentablePC = Column(Numeric(10, 4), comment="Standard rate unrentable percentage")
    StdRateComplimentaryPC = Column(Numeric(10, 4), comment="Standard rate complimentary percentage")
    StdTotalPC = Column(Numeric(10, 4), comment="Standard rate total percentage")

    # Averages - Occupied
    OccAvgAreaOverUnit = Column(Numeric(10, 4), comment="Occupied avg area per unit")
    OccAvgRentOverUnit = Column(Numeric(14, 4), comment="Occupied avg rent per unit")
    OccAvgRentOverArea = Column(Numeric(10, 4), comment="Occupied avg rent per area")

    # Averages - Vacant
    VacAvgAreaOverUnit = Column(Numeric(10, 4), comment="Vacant avg area per unit")
    VacAvgRentOverUnit = Column(Numeric(14, 4), comment="Vacant avg rent per unit")
    VacAvgRentOverArea = Column(Numeric(10, 4), comment="Vacant avg rent per area")

    # Averages - Unrentable
    UnRAvgAreaOverUnit = Column(Numeric(10, 4), comment="Unrentable avg area per unit")
    UnRAvgRentOverUnit = Column(Numeric(14, 4), comment="Unrentable avg rent per unit")
    UnRAvgRentOverArea = Column(Numeric(10, 4), comment="Unrentable avg rent per area")

    # Averages - Complimentary
    CompAvgAreaOverUnit = Column(Numeric(10, 4), comment="Complimentary avg area per unit")
    CompAvgRentOverUnit = Column(Numeric(14, 4), comment="Complimentary avg rent per unit")
    CompAvgRentOverArea = Column(Numeric(10, 4), comment="Complimentary avg rent per area")

    # Averages - Total
    TotAvgAreaOverUnit = Column(Numeric(10, 4), comment="Total avg area per unit")
    TotAvgRentOverUnit = Column(Numeric(14, 4), comment="Total avg rent per unit")
    TotAvgRentOverArea = Column(Numeric(10, 4), comment="Total avg rent per area")

    # Gross Potential
    GrossPotential = Column(Numeric(14, 4), comment="Gross potential revenue")
    GrossComplimentary = Column(Numeric(14, 4), comment="Gross complimentary")
    GrossOccupied = Column(Numeric(14, 4), comment="Gross occupied")
    GrossVacant = Column(Numeric(14, 4), comment="Gross vacant")
    GrossUnrentable = Column(Numeric(14, 4), comment="Gross unrentable")

    # Actual & Variance
    ActualOccupied = Column(Numeric(14, 4), comment="Actual occupied revenue")
    OccupiedRateVariance = Column(Numeric(14, 4), comment="Occupied rate variance")
    EffectiveRate = Column(Numeric(14, 4), comment="Effective rate")

    # Gross Percentages
    GrossPotentialPC = Column(Numeric(10, 4), comment="Gross potential percentage")
    GrossComplimentaryPC = Column(Numeric(10, 4), comment="Gross complimentary percentage")
    GrossOccupiedPC = Column(Numeric(10, 4), comment="Gross occupied percentage")
    GrossVacantPC = Column(Numeric(10, 4), comment="Gross vacant percentage")
    GrossUnrentablePC = Column(Numeric(10, 4), comment="Gross unrentable percentage")
    ActualOccupiedPC = Column(Numeric(10, 4), comment="Actual occupied percentage")
    OccupiedRateVariancePC = Column(Numeric(10, 4), comment="Occupied rate variance percentage")
    EffectiveRatePC = Column(Numeric(10, 4), comment="Effective rate percentage")

    # Per Area
    GrossPotentialPerArea = Column(Numeric(10, 4), comment="Gross potential per area")
    GrossComplimentaryPerArea = Column(Numeric(10, 4), comment="Gross complimentary per area")
    GrossOccupiedPerArea = Column(Numeric(10, 4), comment="Gross occupied per area")
    GrossVacantPerArea = Column(Numeric(10, 4), comment="Gross vacant per area")
    GrossUnrentablePerArea = Column(Numeric(10, 4), comment="Gross unrentable per area")
    ActualOccupiedPerArea = Column(Numeric(10, 4), comment="Actual occupied per area")
    OccupiedRateVariancePerArea = Column(Numeric(10, 4), comment="Occupied rate variance per area")
    EffectiveRatePerArea = Column(Numeric(10, 4), comment="Effective rate per area")

    # Occupancy Percentages
    IncomePC = Column(Numeric(10, 4), comment="Income percentage")
    UnitPC = Column(Numeric(10, 4), comment="Unit occupancy percentage")
    AreaPC = Column(Numeric(10, 4), comment="Area occupancy percentage")
    EconomicPC = Column(Numeric(10, 4), comment="Economic occupancy percentage")

    __table_args__ = (
        Index('idx_ms_rental_activity_composite', 'extract_date', 'SiteID'),
        Index('idx_ms_rental_activity_extract_date', 'extract_date'),
    )


class MSDelinquency(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Delinquency by period type.
    Contains delinquency amounts and unit counts by period (Rent, Other, Taxes).

    Data Source: SOAP API ManagementSummary endpoint (Delinquency table)
    """
    __tablename__ = 'ms_delinquency'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    Period = Column(String(50), primary_key=True, nullable=False, comment="Period type (Rent, Other, Taxes)")

    # Amounts
    dcDlqntTot = Column(Numeric(14, 4), comment="Delinquent total amount")
    iDelUnits = Column(Integer, comment="Delinquent units count")
    dcPctUnits = Column(Numeric(10, 4), comment="Percentage of units")
    dcPctGrossPot = Column(Numeric(10, 4), comment="Percentage of gross potential")
    dcPctActOcc = Column(Numeric(10, 4), comment="Percentage of actual occupied")
    iDatePeriod = Column(Integer, comment="Date period identifier")

    __table_args__ = (
        Index('idx_ms_delinquency_composite', 'extract_date', 'SiteID', 'Period'),
        Index('idx_ms_delinquency_extract_date', 'extract_date'),
    )


class MSUnpaid(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Unpaid by aging bucket.
    Contains unpaid amounts and unit counts by aging period (0-10, 11-30, 31-60, etc.).

    Data Source: SOAP API ManagementSummary endpoint (Unpaid table)
    """
    __tablename__ = 'ms_unpaid'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    Period = Column(String(50), primary_key=True, nullable=False, comment="Aging bucket (0-10, 11-30, etc.)")

    # Amounts
    dcDlqntTot = Column(Numeric(14, 4), comment="Unpaid total amount")
    iDelUnits = Column(Integer, comment="Unpaid units count")
    dcPctUnits = Column(Numeric(10, 4), comment="Percentage of units")
    dcPctGrossPot = Column(Numeric(10, 4), comment="Percentage of gross potential")
    dcPctActOcc = Column(Numeric(10, 4), comment="Percentage of actual occupied")
    iDatePeriod = Column(Integer, comment="Date period identifier")

    __table_args__ = (
        Index('idx_ms_unpaid_composite', 'extract_date', 'SiteID', 'Period'),
        Index('idx_ms_unpaid_extract_date', 'extract_date'),
    )


class MSRentLastChanged(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Rent Last Changed categories.
    Contains counts of units by rent change recency (0-6 Months, 6-12 Months, etc.).

    Data Source: SOAP API ManagementSummary endpoint (RentLastChanged table)
    """
    __tablename__ = 'ms_rent_last_changed'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    SortID = Column(Integer, primary_key=True, nullable=False, comment="Sort order")

    # Category Info
    sRentLastChangedCat = Column(String(50), comment="Category (0-6 Months, 6-12 Months, etc.)")
    RentLastChangedCount = Column(Integer, comment="Count of units")

    __table_args__ = (
        Index('idx_ms_rent_last_changed_composite', 'extract_date', 'SiteID', 'SortID'),
        Index('idx_ms_rent_last_changed_extract_date', 'extract_date'),
    )


class MSVarFromStdRate(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Variance from Standard Rate categories.
    Contains counts of units by variance percentage category.

    Data Source: SOAP API ManagementSummary endpoint (VarFromStdRate table)
    """
    __tablename__ = 'ms_var_from_std_rate'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    SortID = Column(Integer, primary_key=True, nullable=False, comment="Sort order")

    # Category Info
    sVarFromStdRateCat = Column(String(50), comment="Category (< 0%, 0-15%, 15-30%, etc.)")
    VarFromStdRateCount = Column(Integer, comment="Count of units")

    __table_args__ = (
        Index('idx_ms_var_from_std_rate_composite', 'extract_date', 'SiteID', 'SortID'),
        Index('idx_ms_var_from_std_rate_extract_date', 'extract_date'),
    )


class MSUnitActivity(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Unit Activity by type.
    Contains daily, MTD, YTD counts by activity type (Move-Ins, Move-Outs, Leads, etc.).

    Data Source: SOAP API ManagementSummary endpoint (UnitActivity table)
    """
    __tablename__ = 'ms_unit_activity'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    SortID = Column(Integer, primary_key=True, nullable=False, comment="Sort order")

    # Activity Info
    sDesc = Column(String(100), comment="Activity description (Move-Ins, Insurance, Move-Outs, etc.)")

    # Counts
    iDCount = Column(Integer, comment="Daily count")
    iMCount = Column(Integer, comment="Month-to-date count")
    iYCount = Column(Integer, comment="Year-to-date count")

    __table_args__ = (
        Index('idx_ms_unit_activity_composite', 'extract_date', 'SiteID', 'SortID'),
        Index('idx_ms_unit_activity_extract_date', 'extract_date'),
    )


class MSAlerts(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Alerts by category.
    Contains alert counts by category (rates unchanged, backdated items, etc.).

    Data Source: SOAP API ManagementSummary endpoint (Alerts table)
    """
    __tablename__ = 'ms_alerts'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")
    iSortOrder = Column(Integer, primary_key=True, nullable=False, comment="Sort order")

    # Alert Info
    sCatName = Column(String(255), comment="Alert category name")
    iCnt = Column(Integer, comment="Alert count")

    __table_args__ = (
        Index('idx_ms_alerts_composite', 'extract_date', 'SiteID', 'iSortOrder'),
        Index('idx_ms_alerts_extract_date', 'extract_date'),
    )


class MSTenantStats(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Tenant Statistics.
    Contains tenant statistics including insurance, auto-pay, and web payment metrics.

    Data Source: SOAP API ManagementSummary endpoint (TenantStats table)
    """
    __tablename__ = 'ms_tenant_stats'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")

    # Occupancy
    iOccupied = Column(Integer, comment="Occupied units count")

    # Insurance
    iInsurance = Column(Integer, comment="Insurance count")
    pctInsurancePer = Column(Numeric(10, 6), comment="Insurance percentage")

    # Auto ACH
    iAutoACH = Column(Integer, comment="Auto ACH count")
    pctAutoACHPer = Column(Numeric(10, 6), comment="Auto ACH percentage")

    # Auto CC
    iAutoCC = Column(Integer, comment="Auto CC count")
    pctAutoCCPer = Column(Numeric(10, 6), comment="Auto CC percentage")

    # Web Payment
    iWebPmt = Column(Integer, comment="Web payment count")
    pctWebPmtPer = Column(Numeric(10, 6), comment="Web payment percentage")

    __table_args__ = (
        Index('idx_ms_tenant_stats_composite', 'extract_date', 'SiteID'),
        Index('idx_ms_tenant_stats_extract_date', 'extract_date'),
    )


class MSInsuranceStats(Base, BaseModel, TimestampMixin):
    """
    Management Summary - Insurance Statistics.
    Contains insurance premium and coverage totals.

    Data Source: SOAP API ManagementSummary endpoint (InsuranceStats table)
    """
    __tablename__ = 'ms_insurance_stats'

    # Composite Primary Key
    extract_date = Column(Date, primary_key=True, nullable=False, comment="Date when data was extracted")
    SiteID = Column(Integer, primary_key=True, nullable=False, comment="Site/Location ID")

    # Insurance Stats
    Premiums = Column(Numeric(14, 4), comment="Total premiums")
    Coverage = Column(Numeric(14, 4), comment="Total coverage amount")

    __table_args__ = (
        Index('idx_ms_insurance_stats_composite', 'extract_date', 'SiteID'),
        Index('idx_ms_insurance_stats_extract_date', 'extract_date'),
    )


# ============================================================================
# ECRI (Existing Customer Rate Increase) Models
# ============================================================================


class ECRIBatch(Base, BaseModel):
    """
    ECRI batch metadata — groups ledgers for a single rate increase run.

    Data Source: Created by ECRI app when user configures a new batch.
    """
    __tablename__ = 'ecri_batches'

    batch_id = Column(UUID(as_uuid=True), primary_key=True, comment="Batch UUID")
    name = Column(String(255), nullable=True, comment="Batch name/label")
    batch_type = Column(String(16), nullable=False, default='standard',
                        comment="standard=monthly ECRI; advance=pre-load (prepayer/recent move-in)")
    site_ids = Column(ARRAY(Integer), nullable=False, comment="Sites included in batch")
    target_increase_pct = Column(Numeric(5, 2), nullable=True, comment="Target increase percentage")
    control_group_enabled = Column(Boolean, nullable=False, default=False, comment="A/B testing mode")
    group_config = Column(JSONB, nullable=True, comment="Control group percentages config")
    total_ledgers = Column(Integer, nullable=False, default=0, comment="Count of ledgers in batch")
    status = Column(String(20), nullable=False, default='draft', comment="draft/site_review/rev_approved/executing/executed/cancelled")
    created_by = Column(String(255), nullable=True, comment="Username who created batch")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, comment="Batch creation time")
    executed_at = Column(DateTime, nullable=True, comment="When pushed to SiteLink")
    cancelled_at = Column(DateTime, nullable=True, comment="When cancelled")
    # Workflow fields (migration 036)
    site_review_deadline = Column(Date, nullable=True, comment="Deadline for ops exclusion requests")
    submitted_for_review_at = Column(DateTime, nullable=True)
    submitted_for_review_by = Column(String(255), nullable=True)

    # Configuration snapshot
    min_tenure_months = Column(Integer, nullable=False, default=12)
    notice_period_days = Column(Integer, nullable=False, default=14)
    discount_reference_pct = Column(Numeric(5, 2), nullable=False, default=40.00)
    attribution_window_days = Column(Integer, nullable=False, default=90)
    notes = Column(Text, nullable=True)

    # Relationships
    ledgers = relationship('ECRIBatchLedger', backref='batch', lazy='dynamic',
                           cascade='all, delete-orphan')
    outcomes = relationship('ECRIOutcome', backref='batch', lazy='dynamic',
                            cascade='all, delete-orphan')

    __table_args__ = (
        Index('idx_ecri_batches_status', 'status'),
        Index('idx_ecri_batches_created', 'created_at'),
    )

    def to_dict(self):
        return {
            'batch_id': str(self.batch_id),
            'name': self.name,
            'batch_type': self.batch_type or 'standard',
            'site_ids': self.site_ids,
            'target_increase_pct': float(self.target_increase_pct) if self.target_increase_pct else None,
            'control_group_enabled': self.control_group_enabled,
            'group_config': self.group_config,
            'total_ledgers': self.total_ledgers,
            'status': self.status,
            'created_by': self.created_by,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'executed_at': self.executed_at.isoformat() if self.executed_at else None,
            'cancelled_at': self.cancelled_at.isoformat() if self.cancelled_at else None,
            'min_tenure_months': self.min_tenure_months,
            'notice_period_days': self.notice_period_days,
            'discount_reference_pct': float(self.discount_reference_pct) if self.discount_reference_pct else None,
            'attribution_window_days': self.attribution_window_days,
            'notes': self.notes,
            'site_review_deadline': self.site_review_deadline.isoformat() if self.site_review_deadline else None,
            'submitted_for_review_at': self.submitted_for_review_at.isoformat() if self.submitted_for_review_at else None,
            'submitted_for_review_by': self.submitted_for_review_by,
        }


class ECRIBatchLedger(Base, BaseModel):
    """
    Per-ledger detail within an ECRI batch.

    Tracks old/new rent, control group assignment, benchmarks, and API status.
    """
    __tablename__ = 'ecri_batch_ledgers'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    batch_id = Column(UUID(as_uuid=True), ForeignKey('ecri_batches.batch_id', ondelete='CASCADE'), nullable=False)
    site_id = Column(Integer, nullable=False)
    ledger_id = Column(Integer, nullable=False)
    tenant_id = Column(Integer, nullable=False)
    unit_id = Column(Integer, nullable=True)
    unit_name = Column(String(100), nullable=True)
    tenant_name = Column(String(255), nullable=True)

    # Control group
    control_group = Column(Integer, nullable=False, default=0)

    # Rent details
    old_rent = Column(Numeric(14, 4), nullable=False)
    new_rent = Column(Numeric(14, 4), nullable=False)
    increase_pct = Column(Numeric(5, 2), nullable=False)
    increase_amt = Column(Numeric(14, 4), nullable=False)

    # Plan-vs-delivered snapshot (migration 043). Set once at batch creation,
    # never mutated by objections. new_rent/increase_pct/increase_amt may be
    # overwritten when an objection is applied.
    planned_new_rent = Column(Numeric(14, 4), nullable=True)
    planned_increase_pct = Column(Numeric(5, 2), nullable=True)
    planned_increase_amt = Column(Numeric(14, 4), nullable=True)

    # Dates
    notice_date = Column(Date, nullable=True)
    effective_date = Column(Date, nullable=True)

    # Benchmarking
    in_place_median_site = Column(Numeric(14, 4), nullable=True)
    in_place_median_country = Column(Numeric(14, 4), nullable=True)
    market_rate = Column(Numeric(14, 4), nullable=True)
    std_rate = Column(Numeric(14, 4), nullable=True)
    variance_vs_site = Column(Numeric(5, 2), nullable=True)
    variance_vs_market = Column(Numeric(5, 2), nullable=True)

    # Tenure info
    moved_in_date = Column(Date, nullable=True)
    last_increase_date = Column(Date, nullable=True)
    tenure_months = Column(Integer, nullable=True)

    # Currency of the native rent amounts stored in old_rent/new_rent/increase_amt
    # (3-letter ISO code, e.g. SGD/MYR/KRW/HKD). SGD-equivalent is computed
    # at display time using fx_rates.
    currency = Column(String(3), nullable=True)

    # Billing-cycle-aware scheduling (added migration 032)
    paid_thru_date = Column(Date, nullable=True)
    next_lad = Column(Date, nullable=True)
    bucket = Column(String(10), nullable=True)

    # Advance-scheduling snapshot (migration 055). Only populated when the
    # parent batch has batch_type='advance'.
    segment = Column(String(20), nullable=True,
                     comment="advance-batch segment: recent_movein / heavy_prepayer")
    projected_paid_thru = Column(Date, nullable=True,
                                 comment="paid_thru used to compute effective_date on advance batches")
    discount_expires = Column(Date, nullable=True,
                              comment="derived move-in discount expiration (recent_movein segment only)")

    # API execution
    api_status = Column(String(20), nullable=False, default='pending', comment="pending/success/failed/skipped")
    api_response = Column(JSONB, nullable=True)
    api_executed_at = Column(DateTime, nullable=True)

    # Site-review exclusion fields (migration 037)
    exclusion_status = Column(String(12), nullable=False, default='none',
                              comment="none/requested/approved/rejected")
    exclusion_reason_code = Column(String(40), nullable=True)
    exclusion_notes = Column(Text, nullable=True)
    exclusion_requested_by = Column(Integer, nullable=True)
    exclusion_requested_at = Column(DateTime, nullable=True)
    exclusion_decided_by = Column(Integer, nullable=True)
    exclusion_decided_at = Column(DateTime, nullable=True)
    exclusion_decision_notes = Column(Text, nullable=True)

    __table_args__ = (
        Index('idx_ecri_bl_batch', 'batch_id'),
        Index('idx_ecri_bl_site_ledger', 'site_id', 'ledger_id'),
        Index('idx_ecri_bl_api_status', 'api_status'),
        Index('idx_ecri_bl_control_group', 'batch_id', 'control_group'),
        Index('idx_ecri_bl_exclusion_status', 'batch_id', 'exclusion_status'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'batch_id': str(self.batch_id),
            'site_id': self.site_id,
            'ledger_id': self.ledger_id,
            'tenant_id': self.tenant_id,
            'unit_id': self.unit_id,
            'unit_name': self.unit_name,
            'tenant_name': self.tenant_name,
            'control_group': self.control_group,
            'old_rent': float(self.old_rent) if self.old_rent else None,
            'new_rent': float(self.new_rent) if self.new_rent else None,
            'increase_pct': float(self.increase_pct) if self.increase_pct else None,
            'increase_amt': float(self.increase_amt) if self.increase_amt else None,
            'planned_new_rent': float(self.planned_new_rent) if self.planned_new_rent is not None else None,
            'planned_increase_pct': float(self.planned_increase_pct) if self.planned_increase_pct is not None else None,
            'planned_increase_amt': float(self.planned_increase_amt) if self.planned_increase_amt is not None else None,
            'currency': self.currency,
            'notice_date': self.notice_date.isoformat() if self.notice_date else None,
            'effective_date': self.effective_date.isoformat() if self.effective_date else None,
            'paid_thru_date': self.paid_thru_date.isoformat() if self.paid_thru_date else None,
            'next_lad': self.next_lad.isoformat() if self.next_lad else None,
            'bucket': self.bucket,
            'segment': self.segment,
            'projected_paid_thru': self.projected_paid_thru.isoformat() if self.projected_paid_thru else None,
            'discount_expires': self.discount_expires.isoformat() if self.discount_expires else None,
            'in_place_median_site': float(self.in_place_median_site) if self.in_place_median_site else None,
            'in_place_median_country': float(self.in_place_median_country) if self.in_place_median_country else None,
            'market_rate': float(self.market_rate) if self.market_rate else None,
            'std_rate': float(self.std_rate) if self.std_rate else None,
            'variance_vs_site': float(self.variance_vs_site) if self.variance_vs_site else None,
            'variance_vs_market': float(self.variance_vs_market) if self.variance_vs_market else None,
            'moved_in_date': self.moved_in_date.isoformat() if self.moved_in_date else None,
            'last_increase_date': self.last_increase_date.isoformat() if self.last_increase_date else None,
            'tenure_months': self.tenure_months,
            'api_status': self.api_status,
            'api_response': self.api_response,
            'api_executed_at': self.api_executed_at.isoformat() if self.api_executed_at else None,
            'exclusion_status': self.exclusion_status or 'none',
            'exclusion_reason_code': self.exclusion_reason_code,
            'exclusion_notes': self.exclusion_notes,
            'exclusion_requested_by': self.exclusion_requested_by,
            'exclusion_requested_at': self.exclusion_requested_at.isoformat() if self.exclusion_requested_at else None,
            'exclusion_decided_by': self.exclusion_decided_by,
            'exclusion_decided_at': self.exclusion_decided_at.isoformat() if self.exclusion_decided_at else None,
            'exclusion_decision_notes': self.exclusion_decision_notes,
        }


class ECRIOutcome(Base, BaseModel):
    """
    Churn/stay tracking post-ECRI.

    Populated by scheduled outcome tracking job within the attribution window.
    """
    __tablename__ = 'ecri_outcomes'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    batch_id = Column(UUID(as_uuid=True), ForeignKey('ecri_batches.batch_id', ondelete='CASCADE'), nullable=False)
    site_id = Column(Integer, nullable=False)
    ledger_id = Column(Integer, nullable=False)
    outcome_date = Column(Date, nullable=False)
    outcome_type = Column(String(20), nullable=False, comment="stayed/moved_out/scheduled_out")
    days_after_notice = Column(Integer, nullable=True)
    months_at_new_rent = Column(Integer, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_ecri_outcomes_batch', 'batch_id'),
        Index('idx_ecri_outcomes_ledger', 'site_id', 'ledger_id'),
        Index('idx_ecri_outcomes_type', 'outcome_type'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'batch_id': str(self.batch_id),
            'site_id': self.site_id,
            'ledger_id': self.ledger_id,
            'outcome_date': self.outcome_date.isoformat() if self.outcome_date else None,
            'outcome_type': self.outcome_type,
            'days_after_notice': self.days_after_notice,
            'months_at_new_rent': self.months_at_new_rent,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


class ECRIObjection(Base, BaseModel):
    """
    Post-push rate objection raised by ops after tenant calls in.

    Allows modifying new_rent/increase_pct for a pushed ledger.
    Auto-approved if within user's ecri_max_pct_reduction + ecri_max_abs_reduction,
    otherwise routes to a senior approver.
    """
    __tablename__ = 'ecri_objections'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    batch_ledger_id = Column(BigInteger, ForeignKey('ecri_batch_ledgers.id', ondelete='CASCADE'), nullable=False)
    batch_id = Column(UUID(as_uuid=True), nullable=False)
    site_id = Column(Integer, nullable=False)
    ledger_id = Column(Integer, nullable=False)

    original_increase_pct = Column(Numeric(5, 2), nullable=False)
    original_new_rent = Column(Numeric(14, 4), nullable=False)
    currency = Column(String(3), nullable=False, default='SGD')

    new_increase_pct = Column(Numeric(5, 2), nullable=True)
    new_new_rent = Column(Numeric(14, 4), nullable=True)

    reason_code = Column(String(40), nullable=False)
    reason_notes = Column(Text, nullable=True)

    status = Column(String(20), nullable=False, default='pending_approval',
                    comment="pending_approval/approved/rejected/applied/cancelled")
    requires_approval = Column(Boolean, nullable=False, default=True)

    raised_by_user_id = Column(Integer, nullable=False)
    raised_by_username = Column(String(100), nullable=False)
    raised_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    approver_user_id = Column(Integer, nullable=True)
    approver_username = Column(String(100), nullable=True)
    approved_at = Column(DateTime, nullable=True)
    approval_notes = Column(Text, nullable=True)

    applied_at = Column(DateTime, nullable=True)
    applied_ret_code = Column(String(20), nullable=True)
    applied_ret_msg = Column(Text, nullable=True)

    __table_args__ = (
        Index('idx_ecri_obj_batch_ledger', 'batch_ledger_id'),
        Index('idx_ecri_obj_batch_id', 'batch_id'),
        Index('idx_ecri_obj_site_ledger', 'site_id', 'ledger_id'),
        Index('idx_ecri_obj_status', 'status'),
        Index('idx_ecri_obj_raised_by', 'raised_by_user_id'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'batch_ledger_id': self.batch_ledger_id,
            'batch_id': str(self.batch_id),
            'site_id': self.site_id,
            'ledger_id': self.ledger_id,
            'original_increase_pct': float(self.original_increase_pct) if self.original_increase_pct is not None else None,
            'original_new_rent': float(self.original_new_rent) if self.original_new_rent is not None else None,
            'currency': self.currency,
            'new_increase_pct': float(self.new_increase_pct) if self.new_increase_pct is not None else None,
            'new_new_rent': float(self.new_new_rent) if self.new_new_rent is not None else None,
            'reason_code': self.reason_code,
            'reason_notes': self.reason_notes,
            'status': self.status,
            'requires_approval': self.requires_approval,
            'raised_by_user_id': self.raised_by_user_id,
            'raised_by_username': self.raised_by_username,
            'raised_at': self.raised_at.isoformat() if self.raised_at else None,
            'approver_user_id': self.approver_user_id,
            'approver_username': self.approver_username,
            'approved_at': self.approved_at.isoformat() if self.approved_at else None,
            'approval_notes': self.approval_notes,
            'applied_at': self.applied_at.isoformat() if self.applied_at else None,
            'applied_ret_code': self.applied_ret_code,
            'applied_ret_msg': self.applied_ret_msg,
        }


# ============================================================================
# EmbedSocial Reviews Model
# ============================================================================


class EmbedSocialReview(Base, BaseModel):
    """
    EmbedSocial review data for tracking Google review scores over time per location.

    Data Source: EmbedSocial GetItems API
    Unique key: review_id (EmbedSocial item ID)
    """
    __tablename__ = 'embedsocial_reviews'

    id = Column(Integer, primary_key=True, autoincrement=True)
    review_id = Column(String(50), nullable=False, unique=True,
                       comment="EmbedSocial item ID")
    source_id = Column(String(50), nullable=False, index=True,
                       comment="Links to listing/location")
    source_name = Column(String(255), nullable=True,
                         comment="Location/listing name")
    source_address = Column(String(500), nullable=True,
                            comment="Location address")
    author_name = Column(String(255), nullable=True,
                         comment="Review author name")
    rating = Column(Integer, nullable=False,
                    comment="1-5 star rating")
    caption_text = Column(Text, nullable=True,
                          comment="Review text content")
    review_link = Column(String(500), nullable=True,
                         comment="Link to original review")
    original_created_on = Column(DateTime, nullable=False,
                                 comment="When the review was posted")
    reply_text = Column(Text, nullable=True,
                        comment="First reply text")
    reply_created_on = Column(DateTime, nullable=True,
                              comment="When the reply was posted")
    synced_at = Column(DateTime, nullable=False, default=datetime.utcnow,
                       comment="When the review was synced to DB")

    __table_args__ = (
        Index('ix_es_reviews_source_date', 'source_id', 'original_created_on'),
        Index('ix_es_reviews_rating', 'rating'),
    )


# =============================================================================
# Google Ads Models (BigQuery → PostgreSQL ETL)
# =============================================================================

class GadsAccountMap(Base, BaseModel):
    """Static mapping of Google Ads customer accounts to country/currency."""
    __tablename__ = 'gads_account_map'

    customer_id = Column(BigInteger, primary_key=True)
    country = Column(String(2), nullable=False)
    currency = Column(String(3), nullable=False)


class GadsCampaign(Base, BaseModel):
    """Google Ads campaign dimension (daily snapshot, full overwrite)."""
    __tablename__ = 'gads_campaigns'

    campaign_id = Column(BigInteger, primary_key=True)
    customer_id = Column(BigInteger, nullable=False, index=True)
    campaign_name = Column(String(255))
    campaign_status = Column(String(20), index=True)
    channel_type = Column(String(30))
    channel_sub_type = Column(String(30))
    bidding_strategy_type = Column(String(50))
    budget_amount_micros = Column(BigInteger)
    start_date = Column(Date)
    end_date = Column(Date)
    _data_date = Column(Date)
    synced_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class GadsAdGroup(Base, BaseModel):
    """Google Ads ad group dimension (daily snapshot, full overwrite)."""
    __tablename__ = 'gads_ad_groups'

    ad_group_id = Column(BigInteger, primary_key=True)
    campaign_id = Column(BigInteger, nullable=False, index=True)
    customer_id = Column(BigInteger, nullable=False, index=True)
    ad_group_name = Column(String(255))
    ad_group_status = Column(String(20))
    ad_group_type = Column(String(30))
    _data_date = Column(Date)
    synced_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class GadsKeyword(Base, BaseModel):
    """Google Ads keyword dimension (latest snapshot)."""
    __tablename__ = 'gads_keywords'

    criterion_id = Column(BigInteger, primary_key=True)
    ad_group_id = Column(BigInteger, primary_key=True)
    campaign_id = Column(BigInteger, nullable=False, index=True)
    keyword_text = Column(String(500))
    match_type = Column(String(20))
    is_negative = Column(Boolean, default=False)
    status = Column(String(20))
    quality_score = Column(Integer)
    _data_date = Column(Date)
    synced_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class GadsCampaignDaily(Base, BaseModel):
    """Google Ads campaign daily stats (upsert on composite PK)."""
    __tablename__ = 'gads_campaign_daily'

    campaign_id = Column(BigInteger, primary_key=True)
    segments_date = Column(Date, primary_key=True)
    device = Column(String(20), primary_key=True)
    ad_network_type = Column(String(30), primary_key=True)
    impressions = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    cost_micros = Column(BigInteger, default=0)
    conversions = Column(Numeric(12, 6), default=0)
    conversions_value = Column(Numeric(14, 2), default=0)
    interactions = Column(Integer, default=0)
    synced_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_gads_campaign_daily_date', 'segments_date'),
        Index('idx_gads_campaign_daily_campaign', 'campaign_id'),
    )


class GadsCampaignConversions(Base, BaseModel):
    """Google Ads campaign conversions by action (upsert on composite PK)."""
    __tablename__ = 'gads_campaign_conversions'

    campaign_id = Column(BigInteger, primary_key=True)
    segments_date = Column(Date, primary_key=True)
    conversion_action_name = Column(String(255), primary_key=True)
    ad_network_type = Column(String(30), primary_key=True)
    conversion_action_category = Column(String(50))
    conversions = Column(Numeric(12, 6), default=0)
    conversions_value = Column(Numeric(14, 2), default=0)
    value_per_conversion = Column(Numeric(14, 2), default=0)
    synced_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_gads_conv_date', 'segments_date'),
    )


class GadsAdGroupDaily(Base, BaseModel):
    """Google Ads ad group daily stats (upsert on composite PK)."""
    __tablename__ = 'gads_ad_group_daily'

    ad_group_id = Column(BigInteger, primary_key=True)
    campaign_id = Column(BigInteger, nullable=False)
    segments_date = Column(Date, primary_key=True)
    device = Column(String(20), primary_key=True)
    ad_network_type = Column(String(30), primary_key=True)
    impressions = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    cost_micros = Column(BigInteger, default=0)
    conversions = Column(Numeric(12, 6), default=0)
    conversions_value = Column(Numeric(14, 2), default=0)
    interactions = Column(Integer, default=0)
    synced_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_gads_ag_daily_date', 'segments_date'),
        Index('idx_gads_ag_daily_campaign', 'campaign_id'),
    )


class GadsKeywordDaily(Base, BaseModel):
    """Google Ads keyword daily stats (upsert on composite PK)."""
    __tablename__ = 'gads_keyword_daily'

    criterion_id = Column(BigInteger, primary_key=True)
    ad_group_id = Column(BigInteger, primary_key=True)
    campaign_id = Column(BigInteger, nullable=False)
    segments_date = Column(Date, primary_key=True)
    device = Column(String(20), primary_key=True)
    ad_network_type = Column(String(30), primary_key=True)
    impressions = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    cost_micros = Column(BigInteger, default=0)
    conversions = Column(Numeric(12, 6), default=0)
    conversions_value = Column(Numeric(14, 2), default=0)
    interactions = Column(Integer, default=0)
    synced_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_gads_kw_daily_date', 'segments_date'),
        Index('idx_gads_kw_daily_campaign', 'campaign_id'),
        Index('idx_gads_kw_daily_ad_group', 'ad_group_id'),
    )


# ============================================================================
# Igloo Smart Lock Models
# ============================================================================


class IglooProperty(Base, BaseModel, TimestampMixin):
    """Igloo property (site/location) synced from Igloo API."""
    __tablename__ = 'igloo_properties'

    id = Column(Integer, primary_key=True, autoincrement=True)
    propertyId = Column(String(50), unique=True, nullable=False, comment="Igloo property ID (API field: id)")
    name = Column(String(100), nullable=False, comment="Property name (API field: name)")
    timezone = Column(String(50), comment="Property timezone (API field: timezone)")
    totalLock = Column(Integer, comment="Total lock count (API field: totalLock)")
    site_id = Column(Integer, comment="ESA SiteID (mapped via config)")
    raw_json = Column(JSONB, comment="Full API response")

    __table_args__ = (
        Index('idx_igloo_prop_site_id', 'site_id'),
    )


class IglooDevice(Base, BaseModel, TimestampMixin):
    """Igloo device (lock, keypad, bridge, keybox) synced from Igloo API."""
    __tablename__ = 'igloo_devices'

    id = Column(Integer, primary_key=True, autoincrement=True)
    deviceId = Column(String(30), unique=True, nullable=False, comment="Bluetooth ID e.g. SP2X2916499b (API field: deviceId)")
    deviceName = Column(String(50), nullable=False, comment="Short name e.g. 6499b — JOIN KEY to padlock_id/keypad_id")
    type = Column(String(20), nullable=False, comment="Lock, Keypad, Bridge, KeyBox (API field: type)")
    igloo_id = Column(String(50), comment="Igloo internal record ID (API field: id)")
    batteryLevel = Column(Integer, comment="0-100 (API field: batteryLevel)")
    pairedAt = Column(DateTime(timezone=True), comment="API field: pairedAt")
    lastSync = Column(DateTime(timezone=True), comment="API field: lastSync")
    properties = Column(JSONB, comment="Array of property refs (API field: properties)")
    linkedDevices = Column(JSONB, comment="API field: linkedDevices")
    linkedAccessories = Column(JSONB, comment="API field: linkedAccessories")
    propertyId = Column(String(50), comment="Resolved from properties[0].id")
    propertyName = Column(String(100), comment="Resolved from properties[0].name")
    departmentId = Column(String(50), comment="From department lookup")
    departmentName = Column(String(100), comment="From department lookup")
    site_id = Column(Integer, comment="ESA SiteID (mapped via config)")
    raw_json = Column(JSONB, comment="Full API response for future fields")

    __table_args__ = (
        Index('idx_igloo_dev_device_name', 'deviceName'),
        Index('idx_igloo_dev_site_id', 'site_id'),
        Index('idx_igloo_dev_property_id', 'propertyId'),
    )


# ============================================================================
# Zoom Phone Sync Models
# ============================================================================

class ZoomContactSync(Base, BaseModel, TimestampMixin):
    """SugarCRM <-> Zoom External Contact mapping"""
    __tablename__ = 'zoom_contact_sync'

    id = Column(Integer, primary_key=True, autoincrement=True)
    sugar_id = Column(String(36), nullable=False)
    sugar_module = Column(String(20), nullable=False)
    zoom_contact_id = Column(String(50))
    phone_numbers = Column(JSONB)
    name_pushed = Column(String(200))
    sync_status = Column(String(20), nullable=False, default='pending')
    error_message = Column(Text)
    last_synced_at = Column(DateTime)

    __table_args__ = (
        Index('idx_zoom_contact_sync_unique', 'sugar_id', 'sugar_module', unique=True),
        Index('idx_zoom_contact_sync_status', 'sync_status'),
        Index('idx_zoom_contact_sync_zoom_id', 'zoom_contact_id'),
    )


class ZoomCallLog(Base, BaseModel, TimestampMixin):
    """Zoom call record cache + CRM matching state"""
    __tablename__ = 'zoom_call_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    zoom_call_id = Column(String(100), unique=True, nullable=False)
    direction = Column(String(10))
    caller_number = Column(String(30))
    callee_number = Column(String(30))
    caller_name = Column(String(200))
    callee_name = Column(String(200))
    duration = Column(Integer)
    answer_start = Column(DateTime)
    call_end = Column(DateTime)
    has_recording = Column(Boolean, default=False)
    recording_id = Column(String(100))
    download_url = Column(Text)
    transcript = Column(Text)  # legacy free-form (for Zoom-provided transcripts)
    transcript_status = Column(String(20), nullable=False, default='none')
    transcript_original = Column(Text)
    transcript_en = Column(Text)
    detected_language = Column(String(20))
    transcript_model = Column(String(50))
    transcript_processed_at = Column(DateTime)
    # Scoring fields (populated by call_scorer; flat columns mirror scores_json keys)
    score_status = Column(String(20), nullable=False, default='none')
    score_processed_at = Column(DateTime)
    score_model = Column(String(50))
    score_confidence = Column(Integer)
    score_error = Column(Text)
    quality_overall = Column(Integer)
    call_category = Column(String(30))
    call_subcategory = Column(String(100))
    sentiment = Column(String(20))
    scores_json = Column(JSONB)
    matched_sugar_id = Column(String(36))
    matched_sugar_module = Column(String(20))
    sugar_call_id = Column(String(36))
    sync_status = Column(String(20), nullable=False, default='pending')
    error_message = Column(Text)
    raw_json = Column(JSONB)

    __table_args__ = (
        Index('idx_zoom_call_logs_status', 'sync_status'),
        Index('idx_zoom_call_logs_caller', 'caller_number'),
        Index('idx_zoom_call_logs_callee', 'callee_number'),
        Index('idx_zoom_call_logs_call_end', 'call_end'),
    )


class ZoomSyncState(Base, BaseModel):
    """High-water marks for Zoom sync operations"""
    __tablename__ = 'zoom_sync_state'

    sync_name = Column(String(50), primary_key=True)
    last_sync_at = Column(DateTime)
    last_success_at = Column(DateTime)
    records_processed = Column(Integer, default=0)
    sync_metadata = Column('metadata', JSONB)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class CallScoringConfig(Base, BaseModel):
    """Editable LLM rubric for Zoom call scoring (single-row table, name='default')."""
    __tablename__ = 'call_scoring_config'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True, nullable=False, default='default')
    config_json = Column(JSONB, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    version = Column(Integer, nullable=False, default=1)
    updated_by = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class CcwsChargeDescription(Base, BaseModel, TimestampMixin):
    """
    Per-site charge type configuration from ChargeDescriptionsRetrieve.

    Provides per-charge-type tax rates and default prices needed by the
    internal MoveInCost calculator (admin fee amount, insurance tax rate).

    Composite unique key: ChargeDescID + SiteID
    """
    __tablename__ = 'ccws_charge_descriptions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ChargeDescID = Column(Integer, nullable=False, index=True)
    SiteID = Column(Integer, nullable=False, index=True)
    SiteCode = Column(String(20))
    sChgDesc = Column(String(255))
    sChgCategory = Column(String(100), index=True)
    dcPrice = Column(Numeric(14, 4), default=0)
    dcTax1Rate = Column(Numeric(14, 6), default=0)
    dcTax2Rate = Column(Numeric(14, 6), default=0)
    bApplyAtMoveIn = Column(Boolean, default=False)
    bProrateAtMoveIn = Column(Boolean, default=False)
    bPermanent = Column(Boolean, default=False)
    dDisabled = Column(DateTime)

    __table_args__ = (
        UniqueConstraint('ChargeDescID', 'SiteID', name='ccws_charge_descriptions_ChargeDescID_SiteID_key'),
    )


class CcwsInsuranceCoverage(Base, BaseModel, TimestampMixin):
    """
    Per-site insurance coverage plans from InsuranceCoverageRetrieve_V2.

    V3 returns 0 results on LSETUP — V2 is the working endpoint.
    Provides coverage amounts and premiums for the booking engine.

    Composite unique key: InsurCoverageID + SiteID
    """
    __tablename__ = 'ccws_insurance_coverage'

    id = Column(Integer, primary_key=True, autoincrement=True)
    InsurCoverageID = Column(Integer, nullable=False, index=True)
    SiteID = Column(Integer, nullable=False, index=True)
    SiteCode = Column(String(20))
    dcCoverage = Column(Numeric(14, 4), default=0)
    dcPremium = Column(Numeric(14, 4), default=0)
    dcPCTheft = Column(Numeric(14, 4), default=0)
    sCoverageDesc = Column(String(255))
    sProvidor = Column(String(255))
    sBrochureUrl = Column(Text)
    sCertificateUrl = Column(Text)

    __table_args__ = (
        UniqueConstraint('InsurCoverageID', 'SiteID', name='ccws_insurance_coverage_InsurCoverageID_SiteID_key'),
    )


class StripeWebhookEvent(Base, BaseModel):
    """
    Idempotency log for inbound Stripe webhook events.

    event_id (evt_xxx) is Stripe's unique identifier and acts as the
    idempotency key — enforced by UNIQUE constraint.  On INSERT conflict
    the handler returns 200 immediately without reprocessing.

    status lifecycle: received → processed | failed
    """
    __tablename__ = 'stripe_webhook_events'

    id                = Column(BigInteger, primary_key=True, autoincrement=True)
    event_id          = Column(Text, nullable=False, unique=True)
    event_type        = Column(Text, nullable=False)
    payment_intent_id = Column(Text, nullable=True)
    received_at       = Column(DateTime, nullable=False, default=datetime.utcnow)
    processed_at      = Column(DateTime, nullable=True)
    status            = Column(String(20), nullable=False, default='received')
    error_message     = Column(Text, nullable=True)

    __table_args__ = (
        Index('idx_swe_payment_intent_id', 'payment_intent_id'),
    )


class CcwsSiteBillingConfig(Base, BaseModel, TimestampMixin):
    """
    Per-site proration / billing-mode config for the MoveInCost calculator.

    Source: extracted from MoveInCostRetrieveWithDiscount_v4 SOAP response.
    Sync preserves manual overrides — rows with overridden_by set are
    skipped during pipeline runs.
    """
    __tablename__ = 'ccws_site_billing_config'

    id = Column(Integer, primary_key=True, autoincrement=True)
    SiteCode = Column(String(20), nullable=False, unique=True, index=True)
    SiteID = Column(Integer)
    b_anniv_date_leasing = Column(Boolean, nullable=False, default=False)
    i_day_strt_prorating = Column(Integer, nullable=False, default=1)
    i_day_strt_prorate_plus_next = Column(Integer, nullable=False, default=17)
    synced_from_soap_at = Column(DateTime)
    overridden_by = Column(String(100))
    overridden_at = Column(DateTime)
    notes = Column(Text)


class CcwsReservation(Base, BaseModel, TimestampMixin):
    """
    Raw reservation records from CallCenterWs ReservationList_v3.

    One row per SiteCode + WaitingID. PascalCase field names mirror
    the SOAP response shape.

    Stored in esa_middleware (orchestrator-managed).
    """
    __tablename__ = 'ccws_reservations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    SiteCode = Column(String(20), nullable=False, index=True)
    WaitingID = Column(Integer, nullable=False, index=True)
    iGlobalWaitingNum = Column(Integer)
    TenantID = Column(Integer)
    UnitID = Column(Integer)

    sFName = Column(String(100))
    sLName = Column(String(100))
    sEmail = Column(String(255))
    sPhone = Column(String(50))
    sMobile = Column(String(50))

    dcRate_Quoted = Column(Numeric(14, 4))
    ConcessionID = Column(Integer)
    iInquiryType = Column(Integer)
    QTRentalTypeID = Column(Integer)
    dcPaidReserveFee = Column(Numeric(14, 4))
    iReserveFeeReceiptID = Column(Integer)
    iWaitingStatus = Column(Integer)

    dNeeded = Column(DateTime)
    dExpires = Column(DateTime)
    dFollowup = Column(DateTime)
    dCreated = Column(DateTime)
    dPlaced = Column(DateTime)
    dUpdated = Column(DateTime)
    dConverted_ToMoveIn = Column(DateTime)

    sComment = Column(Text)
    sSource = Column(String(100))

    __table_args__ = (
        Index('idx_ccws_reservations_site_waiting', 'SiteCode', 'WaitingID', unique=True),
        Index('idx_ccws_reservations_status', 'iWaitingStatus'),
    )


class CcwsRentTaxRate(Base, BaseModel, TimestampMixin):
    """
    Per-site rent tax rates from RentTaxRatesRetrieve.

    SOAP returns only tax1/tax2 rates; we annotate with SiteCode from the
    request so rows are addressable by location.
    """
    __tablename__ = 'ccws_rent_tax_rates'

    id = Column(Integer, primary_key=True, autoincrement=True)
    SiteCode = Column(String(20), nullable=False, unique=True, index=True)
    dcTax1Rate = Column(Numeric(14, 6))
    dcTax2Rate = Column(Numeric(14, 6))


class CcwsAvailableUnit(Base, BaseModel, TimestampMixin):
    """
    Currently-available units from UnitsInformationAvailableUnitsOnly_v2.

    Snapshot of unrented, rentable inventory. Natural key (SiteID, UnitID).
    """
    __tablename__ = 'ccws_available_units'

    id = Column(Integer, primary_key=True, autoincrement=True)
    SiteID = Column(Integer, nullable=False, index=True)
    UnitID = Column(Integer, nullable=False, index=True)
    sLocationCode = Column(String(20), index=True)
    UnitTypeID = Column(Integer, index=True)
    sTypeName = Column(String(100))
    sUnitName = Column(String(100))
    sUnitNote = Column(String(500))
    sUnitDesc = Column(String(500))
    dcWidth = Column(Numeric(10, 4))
    dcLength = Column(Numeric(10, 4))
    iFloor = Column(Integer)
    dcMapTop = Column(Numeric(10, 4))
    dcMapLeft = Column(Numeric(10, 4))
    dcMapTheta = Column(Numeric(10, 4))
    bMapReversWL = Column(Boolean)
    iEntryLoc = Column(Integer)
    iDoorType = Column(Integer)
    iADA = Column(Integer)
    bClimate = Column(Boolean)
    bPower = Column(Boolean)
    bInside = Column(Boolean)
    bAlarm = Column(Boolean)
    bRentable = Column(Boolean)
    bMobile = Column(Boolean)
    bServiceRequired = Column(Boolean)
    bExcludeFromWebsite = Column(Boolean)
    bRented = Column(Boolean)
    bWaitingListReserved = Column(Boolean)
    bCorporate = Column(Boolean)
    iDaysVacant = Column(Integer)
    iDaysRented = Column(Integer)
    iDefLeaseNum = Column(Integer)
    DefaultCoverageID = Column(Integer)
    dcStdRate = Column(Numeric(14, 4))
    dcBoardRate = Column(Numeric(14, 4))
    dcPushRate = Column(Numeric(14, 4))
    dcPushRate_NotRounded = Column(Numeric(14, 4))
    dcRM_RoundTo = Column(Numeric(14, 4))
    dcStdSecDep = Column(Numeric(14, 4))
    dcStdWeeklyRate = Column(Numeric(14, 4))
    dcWebRate = Column(Numeric(14, 4))
    dcPreferredRate = Column(Numeric(14, 4))
    iPreferredChannelType = Column(Integer)
    bPreferredIsPushRate = Column(Boolean)
    dcTax1Rate = Column(Numeric(14, 6))
    dcTax2Rate = Column(Numeric(14, 6))

    __table_args__ = (
        UniqueConstraint('SiteID', 'UnitID', name='ccws_available_units_SiteID_UnitID_key'),
        Index('idx_ccws_avail_units_loc', 'sLocationCode'),
    )


class CcwsGateAccess(Base, BaseModel, TimestampMixin):
    """
    Raw gate-access codes from CallCenterWs GateAccessData.

    Middleware copy — schema mirrors esa_backend.gate_access_data.
    Access codes are Fernet-encrypted at rest (same crypto as legacy).
    Natural key: (location_code, unit_id).
    """
    __tablename__ = 'ccws_gate_access'

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_code = Column(String(10), nullable=False, index=True)
    site_id = Column(Integer, nullable=False, index=True)
    unit_id = Column(Integer, nullable=False)
    unit_name = Column(String(50), nullable=False)
    is_rented = Column(Boolean, nullable=False, default=False)
    access_code_enc = Column(Text)
    access_code2_enc = Column(Text)
    is_gate_locked = Column(Boolean, nullable=False, default=False)
    is_overlocked = Column(Boolean, nullable=False, default=False)
    keypad_zone = Column(Integer, nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint('location_code', 'unit_id', name='ccws_gate_access_loc_unit_key'),
    )


class CcwsUnitInfo(Base, BaseModel, TimestampMixin):
    """
    Full unit catalog from CallCenterWs UnitsInformation_v3.

    Lives in esa_middleware as `ccws_units`; populated by the orchestrator
    pipeline (parallel to the legacy scheduler job that writes
    esa_pbi.units_info, which stays untouched).
    """
    __tablename__ = 'ccws_units'

    id = Column(Integer, primary_key=True, autoincrement=True)
    SiteID = Column(Integer, nullable=False, index=True)
    UnitID = Column(Integer, nullable=False, index=True)
    UnitTypeID = Column(Integer, index=True)
    sLocationCode = Column(String(20), index=True)
    sUnitName = Column(String(100))
    sTypeName = Column(String(100), index=True)
    sUnitNote = Column(String(500))
    sUnitDesc = Column(String(500))
    dcWidth = Column(Numeric(10, 4))
    dcLength = Column(Numeric(10, 4))
    iFloor = Column(Integer)
    dcMapTheta = Column(Numeric(10, 4))
    bMapReversWL = Column(Boolean)
    iEntryLoc = Column(Integer)
    iDoorType = Column(Integer)
    iADA = Column(Integer)
    bClimate = Column(Boolean)
    bPower = Column(Boolean)
    bInside = Column(Boolean)
    bAlarm = Column(Boolean)
    bRentable = Column(Boolean)
    bMobile = Column(Boolean)
    bServiceRequired = Column(Boolean)
    bExcludeFromWebsite = Column(Boolean)
    bRented = Column(Boolean, index=True)
    bWaitingListReserved = Column(Boolean)
    bCorporate = Column(Boolean)
    iDaysVacant = Column(Integer)
    iDaysRented = Column(Integer)
    dMovedIn = Column(DateTime)
    iDefLeaseNum = Column(Integer)
    DefaultCoverageID = Column(Integer)
    dcStdRate = Column(Numeric(14, 4))
    dcWebRate = Column(Numeric(14, 4))
    dcPushRate = Column(Numeric(14, 4))
    dcPushRate_NotRounded = Column(Numeric(14, 4))
    dcBoardRate = Column(Numeric(14, 4))
    dcPreferredRate = Column(Numeric(14, 4))
    dcStdWeeklyRate = Column(Numeric(14, 4))
    dcStdSecDep = Column(Numeric(14, 4))
    dcRM_RoundTo = Column(Numeric(10, 4))
    dcTax1Rate = Column(Numeric(10, 4))
    dcTax2Rate = Column(Numeric(10, 4))
    iPreferredChannelType = Column(Integer)
    bPreferredIsPushRate = Column(Boolean)
    deleted_at = Column(Date)

    __table_args__ = (
        UniqueConstraint('SiteID', 'UnitID', name='ccws_units_SiteID_UnitID_key'),
        Index('idx_ccws_units_loc', 'sLocationCode'),
    )


class UnitDiscountCandidate(Base, BaseModel):
    """
    Per-unit candidate discount plans.

    One row per (site_id, unit_id, plan_id, concession_id). Built by the
    UnitDiscountCandidatesPipeline — combines `ccws_units`, `ccws_discount`
    and `mw_discount_plans.linked_concessions` into a snapshot the
    recommendation engine can query cheaply.

    sTypeName is decomposed via common.stype_name_parser into the six
    SOP COM01 dim columns (+ case_count for wine units).
    """
    __tablename__ = 'mw_unit_discount_candidates'

    # Composite primary key
    site_id = Column(Integer, primary_key=True)
    unit_id = Column(Integer, primary_key=True)
    plan_id = Column(Integer, primary_key=True)
    concession_id = Column(Integer, primary_key=True)

    # Identity
    site_code = Column(String(20))
    unit_type_id = Column(Integer)
    stype_name = Column(String(100))

    # Decomposed unit attributes (SOP COM01)
    size_category = Column(String(5))
    size_range = Column(String(10))
    unit_type = Column(String(10))
    climate_type = Column(String(5))
    unit_shape = Column(String(5))
    pillar = Column(String(5))
    case_count = Column(Integer)
    parse_ok = Column(Boolean, default=False, nullable=False)

    # Pricing (from ccws_units)
    std_rate = Column(Numeric(14, 4))
    web_rate = Column(Numeric(14, 4))
    push_rate = Column(Numeric(14, 4))
    board_rate = Column(Numeric(14, 4))
    preferred_rate = Column(Numeric(14, 4))

    # Concession params (from ccws_discount)
    amt_type = Column(Integer)
    fixed_discount = Column(Numeric(14, 4))
    pct_discount = Column(Numeric(7, 4))
    max_amount_off = Column(Numeric(14, 4))
    plan_start = Column(DateTime)
    plan_end = Column(DateTime)
    never_expires = Column(Boolean)
    in_month = Column(Integer)
    prepay = Column(Boolean)
    prepaid_months = Column(Integer)
    b_for_all_units = Column(Boolean)
    b_for_corp = Column(Boolean)
    restriction_flags = Column(Integer)
    exclude_if_less_than = Column(Integer)
    exclude_if_more_than = Column(Integer)
    max_occ_pct = Column(Numeric(7, 4))

    # Rules (from mw_discount_plans)
    plan_type = Column(String(40))
    plan_name = Column(String(200))
    promo_period_start = Column(Date)
    promo_period_end = Column(Date)
    booking_period_start = Column(Date)
    booking_period_end = Column(Date)
    move_in_range = Column(String(80))
    lock_in_period = Column(String(80))
    payment_terms = Column(String(80))
    # Duration limits from plan.restrictions. Metadata only — the candidate
    # pipeline does NOT filter on these; the recommender applies them at
    # query time against the booking's requested tenure.
    min_duration_months = Column(Integer)
    max_duration_months = Column(Integer)
    # Distribution channel — comma-separated list from the plan's Restrictions
    # block (Direct Mail, Chatbot, etc.). Surfaced so the recommender can
    # filter candidates by the channel a booking originates from.
    distribution_channel = Column(String(255))
    # Hidden-rate flag from plan — when true, the plan shouldn't be exposed to
    # public-facing channels.
    hidden_rate = Column(Boolean)
    # Coupon code that unlocks the plan when hidden_rate=True. NULL on
    # public plans. The recommender requires the booking input to provide
    # a matching coupon_code before emitting hidden candidates.
    coupon_code = Column(String(100))
    discount_type = Column(String(40))
    discount_numeric = Column(Numeric(14, 4))
    discount_segmentation = Column(String(80))
    is_active = Column(Boolean)

    # Smart-lock assignment (only when this unit has one). Shape:
    #   {"keypad_ids": [12345, 67890], "padlock_id": 999}
    # NULL when the unit has no assignment. Never filtered on — display only.
    smart_lock = Column(JSONB)

    # Computed
    effective_rate = Column(Numeric(14, 4))
    computed_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Phase 3.6 — NL-friendly fields the recommender returns to chatbots.
    concession_name = Column(Text)              # ccws_discount.sPlanName
    size_sqft = Column(Numeric(10, 2))          # dcWidth × dcLength
    lock_in_months = Column(Integer)            # parsed from lock_in_period
    promo_valid_until = Column(Date)            # earliest expiry across plan windows

    __table_args__ = (
        Index('idx_mudc_site_unit', 'site_id', 'unit_id'),
        Index('idx_mudc_plan', 'plan_id'),
        Index('idx_mudc_concession', 'concession_id'),
        Index('idx_mudc_climate_size', 'climate_type', 'size_category'),
    )


class RecommenderExcludedUnitType(Base, BaseModel):
    """
    Unit type codes that must be permanently excluded from the
    recommendation engine's candidate pool.

    Codes match `mw_dim_unit_type.code` (e.g., MB, BZ, PR). The
    UnitDiscountCandidatesPipeline reads this table on every run and
    drops any candidate whose parsed unit_type is in this set.

    Scope is global for now — a single row per unit_type. Per-site
    overrides can be layered on later with a separate table.
    """
    __tablename__ = 'mw_recommender_excluded_unit_types'

    unit_type = Column(String(10), primary_key=True)
    reason = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_by = Column(String(120))
    updated_by = Column(String(120))


class LeaseFollowupJob(Base, BaseModel):
    """
    Durable queue of post-MoveIn SOAP follow-up calls.

    The /api/reservations/move-in handler enqueues rows here after a
    successful SOAP MoveIn — one per follow-up action that needs to fire
    (PaymentSimpleCash for prepay surplus, ScheduleTenantRateChange_v2 for
    the future ECRI). The handler then attempts to execute pending rows
    inline before responding to the bot, so the happy path completes in a
    single request. Rows that fail or aren't reached inline are picked up
    by the worker in backend-scheduler with exponential backoff.

    After 5 failed attempts a job moves to status='failed_permanent' and
    operations gets alerted via the existing alert_manager. Manual retry
    is available on /admin/recommendation-engine/lease-followups.
    """
    __tablename__ = 'mw_lease_followup_jobs'

    id              = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at      = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at      = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    ledger_id       = Column(Integer, nullable=False, index=True)
    site_code       = Column(String(20), nullable=False)
    tenant_id       = Column(Integer)
    unit_id         = Column(Integer)
    action_type     = Column(String(40), nullable=False)
    payload         = Column(JSONB, nullable=False)
    status          = Column(String(20), nullable=False, default='pending')
    attempts        = Column(Integer, nullable=False, default=0)
    next_attempt_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    last_attempt_at = Column(DateTime(timezone=True))
    last_error      = Column(Text)
    soap_response   = Column(JSONB)
    related_request_id  = Column(String(64))
    related_session_id  = Column(String(64))
    related_customer_id = Column(String(120))
