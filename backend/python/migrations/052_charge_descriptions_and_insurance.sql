-- 052_charge_descriptions_and_insurance.sql
--
-- New tables for the MoveInCost internal calculator:
-- 1. ccws_charge_descriptions — per-site charge type config (tax rates, prices)
--    Source: ChargeDescriptionsRetrieve SOAP per site
--    Solves: insurance tax rate (8% vs 9% GST), admin fee amount
--
-- 2. ccws_insurance_coverage — per-site insurance plans
--    Source: InsuranceCoverageRetrieve_V2 SOAP per site (V3 returns 0 results)
--    Solves: insurance premium amount lookup

CREATE TABLE IF NOT EXISTS ccws_charge_descriptions (
    id SERIAL PRIMARY KEY,
    "ChargeDescID" INTEGER NOT NULL,
    "SiteID" INTEGER NOT NULL,
    "SiteCode" VARCHAR(20),
    "sChgDesc" VARCHAR(255),
    "sChgCategory" VARCHAR(100),
    "dcPrice" NUMERIC(14,4) DEFAULT 0,
    "dcTax1Rate" NUMERIC(14,6) DEFAULT 0,
    "dcTax2Rate" NUMERIC(14,6) DEFAULT 0,
    "bApplyAtMoveIn" BOOLEAN DEFAULT FALSE,
    "bProrateAtMoveIn" BOOLEAN DEFAULT FALSE,
    "bPermanent" BOOLEAN DEFAULT FALSE,
    "dDisabled" TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE ("ChargeDescID", "SiteID")
);

CREATE INDEX IF NOT EXISTS idx_ccws_charge_desc_site
    ON ccws_charge_descriptions ("SiteID");
CREATE INDEX IF NOT EXISTS idx_ccws_charge_desc_category
    ON ccws_charge_descriptions ("sChgCategory");

CREATE TABLE IF NOT EXISTS ccws_insurance_coverage (
    id SERIAL PRIMARY KEY,
    "InsurCoverageID" INTEGER NOT NULL,
    "SiteID" INTEGER NOT NULL,
    "SiteCode" VARCHAR(20),
    "dcCoverage" NUMERIC(14,4) DEFAULT 0,
    "dcPremium" NUMERIC(14,4) DEFAULT 0,
    "dcPCTheft" NUMERIC(14,4) DEFAULT 0,
    "sCoverageDesc" VARCHAR(255),
    "sProvidor" VARCHAR(255),
    "sBrochureUrl" TEXT,
    "sCertificateUrl" TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE ("InsurCoverageID", "SiteID")
);

CREATE INDEX IF NOT EXISTS idx_ccws_insurance_site
    ON ccws_insurance_coverage ("SiteID");
