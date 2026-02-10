"""
Expanded SQLAlchemy ORM models for Tenant, Ledger, and Charge tables.
Includes ALL columns from local SQL Server (sldbclnt database).

This file contains the expanded model definitions.
Copy these classes to replace the existing ones in models.py
"""

from sqlalchemy import Column, String, Integer, DateTime, Date, Boolean, Numeric, Text, BigInteger, Float, Index
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class TimestampMixin:
    """Mixin for automatic timestamp tracking"""
    from datetime import datetime
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class BaseModel:
    """Base model with common functionality"""
    pass


# =============================================================================
# TENANT MODEL - 129 columns from local SQL + tracking fields
# =============================================================================

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
    sAccessCode = Column(String(50), nullable=True, comment="Primary gate access code")
    sAccessCode2 = Column(String(50), nullable=True, comment="Secondary access code")
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
    sPhone = Column(String(50), nullable=True, comment="Phone number")
    sFax = Column(String(50), nullable=True, comment="Fax number")
    sEmail = Column(String(255), nullable=True, comment="Email address")
    sPager = Column(String(50), nullable=True, comment="Pager number")
    sMobile = Column(String(50), nullable=True, comment="Mobile phone")
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
    sPhoneAlt = Column(String(50), nullable=True, comment="Alt contact phone")
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
    sPhoneBus = Column(String(50), nullable=True, comment="Business phone")
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
    sPhoneAdd = Column(String(50), nullable=True, comment="Additional contact phone")
    sEmailAdd = Column(String(255), nullable=True, comment="Additional contact email")

    # =========================================================================
    # Identification & License
    # =========================================================================
    sLicense = Column(String(100), nullable=True, comment="Driver's license number")
    sLicRegion = Column(String(50), nullable=True, comment="License issuing state/region")
    sSSN = Column(String(50), nullable=True, comment="Social Security Number (encrypted)")
    sTaxID = Column(String(100), nullable=True, comment="Tax ID number")
    sTaxExemptCode = Column(String(50), nullable=True, comment="Tax exemption code")
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


# =============================================================================
# LEDGER MODEL - 186 columns from local SQL + tracking fields
# =============================================================================

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


# =============================================================================
# CHARGE MODEL - 33 columns from local SQL + tracking fields
# =============================================================================

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
