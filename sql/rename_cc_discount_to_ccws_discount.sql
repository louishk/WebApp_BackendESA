-- Rename cc_discount table to ccws_discount
-- Aligns with ccws_ naming convention (ccws_tenant, ccws_ledger, ccws_charge)
-- Run against: esa_pbi database

ALTER TABLE cc_discount RENAME TO ccws_discount;
ALTER INDEX idx_cc_discount_site_concession RENAME TO idx_ccws_discount_site_concession;
ALTER INDEX idx_cc_discount_site RENAME TO idx_ccws_discount_site;
ALTER INDEX idx_cc_discount_plan_name RENAME TO idx_ccws_discount_plan_name;

-- Auto-generated indexes (from SQLAlchemy index=True on columns)
ALTER INDEX cc_discount_pkey RENAME TO ccws_discount_pkey;
ALTER INDEX "ix_cc_discount_ConcessionID" RENAME TO "ix_ccws_discount_ConcessionID";
ALTER INDEX "ix_cc_discount_SiteID" RENAME TO "ix_ccws_discount_SiteID";
