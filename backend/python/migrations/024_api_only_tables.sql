-- Migration 024: Create CallCenterWs tables for SOAP API data
-- These tables are separate from cc_* tables (which contain local SQL dump data)
-- to avoid NULL overwrites and maintain clean API-sourced data.

-- ============================================================================
-- ccws_tenants: TenantList endpoint (17 fields)
-- ============================================================================
CREATE TABLE IF NOT EXISTS ccws_tenants (
    "TenantID"      INTEGER NOT NULL,
    "SiteID"        INTEGER NOT NULL,
    "sAccessCode"   VARCHAR(100),
    "sFName"        VARCHAR(100),
    "sMI"           VARCHAR(10),
    "sLName"        VARCHAR(100),
    "sCompany"      VARCHAR(255),
    "sAddr1"        VARCHAR(255),
    "sAddr2"        VARCHAR(255),
    "sCity"         VARCHAR(100),
    "sRegion"       VARCHAR(100),
    "sPostalCode"   VARCHAR(20),
    "sEmail"        VARCHAR(255),
    "sPhone"        VARCHAR(100),
    "sMobile"       VARCHAR(100),
    "sLicense"      VARCHAR(100),
    "sLocationCode" VARCHAR(10),
    extract_date    DATE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY ("TenantID", "SiteID")
);

CREATE INDEX IF NOT EXISTS idx_ccws_tenant_site ON ccws_tenants ("SiteID");
CREATE INDEX IF NOT EXISTS idx_ccws_tenant_location ON ccws_tenants ("sLocationCode");
CREATE INDEX IF NOT EXISTS idx_ccws_tenant_name ON ccws_tenants ("sLName", "sFName");

-- ============================================================================
-- ccws_ledgers: LedgersByTenantID_v3 endpoint (141 fields)
-- ============================================================================
CREATE TABLE IF NOT EXISTS ccws_ledgers (
    -- Primary Keys
    "LedgerID"      INTEGER NOT NULL,
    "SiteID"        INTEGER NOT NULL,

    -- Foreign Keys / IDs
    "TenantID"      INTEGER,
    "EmployeeID"    INTEGER,
    "UnitID"        INTEGER,
    "MarketingID"   INTEGER,
    "MktgDistanceID" INTEGER,
    "MktgReasonID"  INTEGER,
    "MktgTypeID"    INTEGER,
    "MktgWhatID"    INTEGER,
    "MktgWhyID"     INTEGER,
    "TimeZoneID"    INTEGER,

    -- Tenant Name (denormalized)
    "TenantName"    VARCHAR(255),
    "sUnitName"     VARCHAR(100),

    -- Primary Contact
    "sMrMrs"        VARCHAR(20),
    "sFName"        VARCHAR(100),
    "sMI"           VARCHAR(10),
    "sLName"        VARCHAR(100),
    "sCompany"      VARCHAR(255),
    "sAddr1"        VARCHAR(255),
    "sAddr2"        VARCHAR(255),
    "sCity"         VARCHAR(100),
    "sRegion"       VARCHAR(100),
    "sPostalCode"   VARCHAR(20),
    "sCountry"      VARCHAR(100),
    "sPhone"        VARCHAR(100),
    "sFax"          VARCHAR(100),
    "sEmail"        VARCHAR(255),
    "sPager"        VARCHAR(100),
    "sMobile"       VARCHAR(100),
    "sCountryCodeMobile" VARCHAR(10),

    -- Alternate Contact
    "sMrMrsAlt"     VARCHAR(20),
    "sFNameAlt"     VARCHAR(100),
    "sMIAlt"        VARCHAR(10),
    "sLNameAlt"     VARCHAR(100),
    "sAddr1Alt"     VARCHAR(255),
    "sAddr2Alt"     VARCHAR(255),
    "sCityAlt"      VARCHAR(100),
    "sRegionAlt"    VARCHAR(100),
    "sPostalCodeAlt" VARCHAR(20),
    "sCountryAlt"   VARCHAR(100),
    "sPhoneAlt"     VARCHAR(100),
    "sEmailAlt"     VARCHAR(255),
    "sRelationshipAlt" VARCHAR(100),

    -- Business Contact
    "sMrMrsBus"     VARCHAR(20),
    "sFNameBus"     VARCHAR(100),
    "sMIBus"        VARCHAR(10),
    "sLNameBus"     VARCHAR(100),
    "sCompanyBus"   VARCHAR(255),
    "sAddr1Bus"     VARCHAR(255),
    "sAddr2Bus"     VARCHAR(255),
    "sCityBus"      VARCHAR(100),
    "sRegionBus"    VARCHAR(100),
    "sPostalCodeBus" VARCHAR(20),
    "sCountryBus"   VARCHAR(100),
    "sPhoneBus"     VARCHAR(100),
    "sEmailBus"     VARCHAR(255),

    -- Additional Contact
    "sMrMrsAdd"     VARCHAR(20),
    "sFNameAdd"     VARCHAR(100),
    "sMIAdd"        VARCHAR(10),
    "sLNameAdd"     VARCHAR(100),
    "sAddr1Add"     VARCHAR(255),
    "sAddr2Add"     VARCHAR(255),
    "sCityAdd"      VARCHAR(100),
    "sRegionAdd"    VARCHAR(100),
    "sPostalCodeAdd" VARCHAR(20),
    "sCountryAdd"   VARCHAR(100),
    "sPhoneAdd"     VARCHAR(100),
    "sEmailAdd"     VARCHAR(255),

    -- Access & Security
    "sAccessCode"   VARCHAR(100),
    "sAccessCode2"  VARCHAR(100),
    "iAccessCode2Type" INTEGER,

    -- Identification
    "sLicense"      VARCHAR(100),
    "sLicRegion"    VARCHAR(100),
    "sSSN"          VARCHAR(100),
    "sTaxID"        VARCHAR(100),
    "sTaxExemptCode" VARCHAR(100),
    "dDOB"          TIMESTAMP,
    "iGender"       INTEGER,

    -- Status Flags
    "bCommercial"   BOOLEAN,
    "bCompanyIsTenant" BOOLEAN,
    "bDisabledWebAccess" BOOLEAN,
    "bExcludeFromInsurance" BOOLEAN,
    "bInvoice"      BOOLEAN,
    "bNeverLockOut"  BOOLEAN,
    "bNoChecks"     BOOLEAN,
    "bOnWaitingList" BOOLEAN,
    "bOverlocked"   BOOLEAN,
    "bPermanent"    BOOLEAN,
    "bPermanentGateLockout" BOOLEAN,
    "bSMSOptIn"     BOOLEAN,
    "bSpecial"      BOOLEAN,
    "bSpecialAlert"  BOOLEAN,
    "bTaxExempt"    BOOLEAN,
    "bWalkInPOS"    BOOLEAN,

    -- Lease & Dates
    "iLeaseNum"     INTEGER,
    "iDefLeaseNum"  INTEGER,
    "dMovedIn"      TIMESTAMP,
    "dPaidThru"     TIMESTAMP,
    "dSchedOut"     TIMESTAMP,
    "dAnniv"        TIMESTAMP,
    "dCreated"      TIMESTAMP,
    "dUpdated"      TIMESTAMP,

    -- Financial
    "dcRent"        NUMERIC(14,4),
    "dcInsurPremium" NUMERIC(14,4),
    "dcChargeBalance" NUMERIC(14,4),
    "dcTotalDue"    NUMERIC(14,4),
    "dcTaxRateRent" NUMERIC(8,4),
    "dcTaxRateInsurance" NUMERIC(8,4),
    "sBillingFrequency" VARCHAR(50),

    -- Billing
    "iAutoBillType" INTEGER,
    "iInvoiceDeliveryType" INTEGER,

    -- Marketing
    "iHowManyOtherStorageCosDidYouContact" INTEGER,
    "iUsedSelfStorageInThePast" INTEGER,
    "iMktg_DidYouVisitWebSite" INTEGER,

    -- Exit Survey
    "bExit_OnEmailOfferList" BOOLEAN,
    "iExitSat_Cleanliness" INTEGER,
    "iExitSat_Price" INTEGER,
    "iExitSat_Safety" INTEGER,
    "iExitSat_Services" INTEGER,
    "iExitSat_Staff" INTEGER,

    -- Blacklist / Events
    "iBlackListRating" INTEGER,
    "iTenEvents_OptOut" INTEGER,

    -- Geographic
    "dcLatitude"    NUMERIC(14,10),
    "dcLongitude"   NUMERIC(14,10),

    -- Notes & Icons
    "sTenNote"      TEXT,
    "sIconList"     VARCHAR(255),

    -- Pictures
    "iPrimaryPic"   INTEGER,
    "sPicFileN1"    VARCHAR(255),
    "sPicFileN2"    VARCHAR(255),
    "sPicFileN3"    VARCHAR(255),
    "sPicFileN4"    VARCHAR(255),
    "sPicFileN5"    VARCHAR(255),
    "sPicFileN6"    VARCHAR(255),
    "sPicFileN7"    VARCHAR(255),
    "sPicFileN8"    VARCHAR(255),
    "sPicFileN9"    VARCHAR(255),

    -- Source Timestamps
    "uTS"           VARCHAR(100),
    "uTSbigint"     BIGINT,

    -- ETL Tracking
    extract_date    DATE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY ("LedgerID", "SiteID")
);

CREATE INDEX IF NOT EXISTS idx_ccws_ledger_site ON ccws_ledgers ("SiteID");
CREATE INDEX IF NOT EXISTS idx_ccws_ledger_tenant ON ccws_ledgers ("TenantID");
CREATE INDEX IF NOT EXISTS idx_ccws_ledger_unit ON ccws_ledgers ("UnitID");
CREATE INDEX IF NOT EXISTS idx_ccws_ledger_paid_thru ON ccws_ledgers ("dPaidThru");
CREATE INDEX IF NOT EXISTS idx_ccws_ledger_moved_in ON ccws_ledgers ("dMovedIn");
CREATE INDEX IF NOT EXISTS idx_ccws_ledger_sched_out ON ccws_ledgers ("dSchedOut");

-- ============================================================================
-- ccws_charges: ChargesAllByLedgerID endpoint (16 fields + LedgerID from context)
-- ============================================================================
CREATE TABLE IF NOT EXISTS ccws_charges (
    "ChargeID"      INTEGER NOT NULL,
    "SiteID"        INTEGER NOT NULL,
    "dcPmtAmt"      NUMERIC(14,4) NOT NULL DEFAULT 0,
    "ChargeDescID"  INTEGER,
    "LedgerID"      INTEGER NOT NULL,
    "dcAmt"         NUMERIC(14,4),
    "dcPrice"       NUMERIC(14,4),
    "dcQty"         NUMERIC(10,4),
    "dcTax1"        NUMERIC(14,4),
    "dcTax2"        NUMERIC(14,4),
    "dChgStrt"      TIMESTAMP,
    "dChgEnd"       TIMESTAMP,
    "bMoveIn"       BOOLEAN,
    "bMoveOut"      BOOLEAN,
    "sChgCategory"  VARCHAR(50),
    "sChgDesc"      VARCHAR(255),
    "sDefChgDesc"   VARCHAR(255),
    extract_date    DATE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY ("ChargeID", "SiteID", "dcPmtAmt")
);

CREATE INDEX IF NOT EXISTS idx_ccws_charge_site ON ccws_charges ("SiteID");
CREATE INDEX IF NOT EXISTS idx_ccws_charge_ledger ON ccws_charges ("LedgerID");
CREATE INDEX IF NOT EXISTS idx_ccws_charge_category ON ccws_charges ("sChgCategory");
CREATE INDEX IF NOT EXISTS idx_ccws_charge_date ON ccws_charges ("dChgStrt");
