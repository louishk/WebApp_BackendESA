"""
Seed & Backfill script for ccws_tenants

Three modes:
  --mode seed     : Phase A — INSERT all known (SiteID, TenantID) pairs from
                    cc_tenants UNION rentroll into ccws_tenants (ON CONFLICT DO NOTHING).
                    Run once to populate IDs before cc_tenants is decommissioned.

  --mode backfill : Phase B — SELECT ccws_tenants rows missing full data (extract_date IS NULL
                    or dCreated IS NULL), map SiteID→sLocationCode via siteinfo,
                    call GetTenantInfoByTenantID for each, UPDATE with all fields.

  --mode probe    : Probe GetTenantInfoByTenantID on a single tenant to discover
                    field names and result_tag. Use for initial API exploration.

Usage:
    python seed_ccws_tenants.py --mode seed
    python seed_ccws_tenants.py --mode backfill
    python seed_ccws_tenants.py --mode backfill --location L001
    python seed_ccws_tenants.py --mode probe --location L001 --tenant-id 12345
"""

import argparse
import logging
import time
from datetime import date
from typing import Dict, Any, List, Optional

from tqdm import tqdm

from common import (
    DataLayerConfig,
    SOAPClient,
    REPORT_REGISTRY,
    create_engine_from_config,
    SessionManager,
    Base,
    CcwsTenant,
    SiteInfo,
    convert_to_bool,
    convert_to_int,
    convert_to_decimal,
    convert_to_datetime,
)

from sqlalchemy import text

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================

def get_callcenter_url(reporting_url: str) -> str:
    """Convert ReportingWs URL to CallCenterWs URL."""
    return reporting_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')


# =============================================================================
# API Functions
# =============================================================================

def call_soap_endpoint(soap_client: SOAPClient, report_name: str, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Make a SOAP call using the registered endpoint configuration."""
    config = REPORT_REGISTRY[report_name]
    return soap_client.call(
        operation=config.operation,
        parameters=parameters,
        soap_action=config.soap_action,
        namespace=config.namespace,
        result_tag=config.result_tag
    )


def fetch_tenant_info(soap_client: SOAPClient, location_code: str, tenant_id: int) -> Optional[Dict[str, Any]]:
    """Fetch full tenant info by TenantID. Returns first record or None."""
    try:
        results = call_soap_endpoint(
            soap_client,
            'tenant_info_by_tenant_id',
            {
                'sLocationCode': location_code,
                'iTenantID': tenant_id
            }
        )
        if results:
            return results[0]
        return None
    except Exception as e:
        logger.warning(f"Failed to fetch tenant info for {location_code}/TenantID={tenant_id}: {e}")
        return None


# =============================================================================
# Transform: GetTenantInfoByTenantID → ccws_tenants
# =============================================================================

def transform_tenant_info(record: Dict[str, Any], location_code: str, extract_date: date) -> Dict[str, Any]:
    """Transform GetTenantInfoByTenantID API record for ccws_tenants update."""
    return {
        'SiteID': convert_to_int(record.get('SiteID')),
        'TenantID': convert_to_int(record.get('TenantID')),

        # Access & Security
        'sAccessCode': record.get('sAccessCode'),
        'sAccessCode2': record.get('sAccessCode2'),
        'iAccessCode2Type': convert_to_int(record.get('iAccessCode2Type')),
        'sWebPassword': record.get('sWebPassword'),
        'bAllowedFacilityAccess': convert_to_bool(record.get('bAllowedFacilityAccess')),

        # Primary Contact
        'sMrMrs': record.get('sMrMrs'),
        'sFName': record.get('sFName'),
        'sMI': record.get('sMI'),
        'sLName': record.get('sLName'),
        'sCompany': record.get('sCompany'),
        'sAddr1': record.get('sAddr1'),
        'sAddr2': record.get('sAddr2'),
        'sCity': record.get('sCity'),
        'sRegion': record.get('sRegion'),
        'sPostalCode': record.get('sPostalCode'),
        'sCountry': record.get('sCountry'),
        'sPhone': record.get('sPhone'),
        'sFax': record.get('sFax'),
        'sEmail': record.get('sEmail'),
        'sPager': record.get('sPager'),
        'sMobile': record.get('sMobile'),
        'sCountryCodeMobile': record.get('sCountryCodeMobile'),

        # Alternate Contact
        'sMrMrsAlt': record.get('sMrMrsAlt'),
        'sFNameAlt': record.get('sFNameAlt'),
        'sMIAlt': record.get('sMIAlt'),
        'sLNameAlt': record.get('sLNameAlt'),
        'sAddr1Alt': record.get('sAddr1Alt'),
        'sAddr2Alt': record.get('sAddr2Alt'),
        'sCityAlt': record.get('sCityAlt'),
        'sRegionAlt': record.get('sRegionAlt'),
        'sPostalCodeAlt': record.get('sPostalCodeAlt'),
        'sCountryAlt': record.get('sCountryAlt'),
        'sPhoneAlt': record.get('sPhoneAlt'),
        'sEmailAlt': record.get('sEmailAlt'),
        'sRelationshipAlt': record.get('sRelationshipAlt'),

        # Business Contact
        'sMrMrsBus': record.get('sMrMrsBus'),
        'sFNameBus': record.get('sFNameBus'),
        'sMIBus': record.get('sMIBus'),
        'sLNameBus': record.get('sLNameBus'),
        'sCompanyBus': record.get('sCompanyBus'),
        'sAddr1Bus': record.get('sAddr1Bus'),
        'sAddr2Bus': record.get('sAddr2Bus'),
        'sCityBus': record.get('sCityBus'),
        'sRegionBus': record.get('sRegionBus'),
        'sPostalCodeBus': record.get('sPostalCodeBus'),
        'sCountryBus': record.get('sCountryBus'),
        'sPhoneBus': record.get('sPhoneBus'),
        'sEmailBus': record.get('sEmailBus'),

        # Additional Contact
        'sMrMrsAdd': record.get('sMrMrsAdd'),
        'sFNameAdd': record.get('sFNameAdd'),
        'sMIAdd': record.get('sMIAdd'),
        'sLNameAdd': record.get('sLNameAdd'),
        'sAddr1Add': record.get('sAddr1Add'),
        'sAddr2Add': record.get('sAddr2Add'),
        'sCityAdd': record.get('sCityAdd'),
        'sRegionAdd': record.get('sRegionAdd'),
        'sPostalCodeAdd': record.get('sPostalCodeAdd'),
        'sCountryAdd': record.get('sCountryAdd'),
        'sPhoneAdd': record.get('sPhoneAdd'),
        'sEmailAdd': record.get('sEmailAdd'),

        # Identification
        'sLicense': record.get('sLicense'),
        'sLicRegion': record.get('sLicRegion'),
        'sSSN': record.get('sSSN'),
        'sTaxID': record.get('sTaxID'),
        'sTaxExemptCode': record.get('sTaxExemptCode'),
        'dDOB': convert_to_datetime(record.get('dDOB')),
        'iGender': convert_to_int(record.get('iGender')),
        'sEmployer': record.get('sEmployer'),

        # Status Flags
        'bCommercial': convert_to_bool(record.get('bCommercial')),
        'bTaxExempt': convert_to_bool(record.get('bTaxExempt')),
        'bSpecial': convert_to_bool(record.get('bSpecial')),
        'bNeverLockOut': convert_to_bool(record.get('bNeverLockOut')),
        'bCompanyIsTenant': convert_to_bool(record.get('bCompanyIsTenant')),
        'bOnWaitingList': convert_to_bool(record.get('bOnWaitingList')),
        'bNoChecks': convert_to_bool(record.get('bNoChecks')),
        'bPermanent': convert_to_bool(record.get('bPermanent')),
        'bWalkInPOS': convert_to_bool(record.get('bWalkInPOS')),
        'bSpecialAlert': convert_to_bool(record.get('bSpecialAlert')),
        'bPermanentGateLockout': convert_to_bool(record.get('bPermanentGateLockout')),
        'bSMSOptIn': convert_to_bool(record.get('bSMSOptIn')),
        'bDisabledWebAccess': convert_to_bool(record.get('bDisabledWebAccess')),
        'bHasActiveLedger': convert_to_bool(record.get('bHasActiveLedger')),
        'iBlackListRating': convert_to_int(record.get('iBlackListRating')),
        'iTenEvents_OptOut': convert_to_int(record.get('iTenEvents_OptOut')),

        # Marketing
        'MarketingID': convert_to_int(record.get('MarketingID')),
        'MktgDistanceID': convert_to_int(record.get('MktgDistanceID')),
        'MktgWhatID': convert_to_int(record.get('MktgWhatID')),
        'MktgReasonID': convert_to_int(record.get('MktgReasonID')),
        'MktgWhyID': convert_to_int(record.get('MktgWhyID')),
        'MktgTypeID': convert_to_int(record.get('MktgTypeID')),
        'iHowManyOtherStorageCosDidYouContact': convert_to_int(record.get('iHowManyOtherStorageCosDidYouContact')),
        'iUsedSelfStorageInThePast': convert_to_int(record.get('iUsedSelfStorageInThePast')),
        'iMktg_DidYouVisitWebSite': convert_to_int(record.get('iMktg_DidYouVisitWebSite')),

        # Exit Survey
        'bExit_OnEmailOfferList': convert_to_bool(record.get('bExit_OnEmailOfferList')),
        'iExitSat_Cleanliness': convert_to_int(record.get('iExitSat_Cleanliness')),
        'iExitSat_Safety': convert_to_int(record.get('iExitSat_Safety')),
        'iExitSat_Services': convert_to_int(record.get('iExitSat_Services')),
        'iExitSat_Staff': convert_to_int(record.get('iExitSat_Staff')),
        'iExitSat_Price': convert_to_int(record.get('iExitSat_Price')),

        # Geographic
        'dcLongitude': convert_to_decimal(record.get('dcLongitude')),
        'dcLatitude': convert_to_decimal(record.get('dcLatitude')),

        # Notes & Icons
        'sTenNote': record.get('sTenNote'),
        'sIconList': record.get('sIconList'),

        # Pictures
        'iPrimaryPic': convert_to_int(record.get('iPrimaryPic')),
        'sPicFileN1': record.get('sPicFileN1'),
        'sPicFileN2': record.get('sPicFileN2'),
        'sPicFileN3': record.get('sPicFileN3'),
        'sPicFileN4': record.get('sPicFileN4'),
        'sPicFileN5': record.get('sPicFileN5'),
        'sPicFileN6': record.get('sPicFileN6'),
        'sPicFileN7': record.get('sPicFileN7'),
        'sPicFileN8': record.get('sPicFileN8'),
        'sPicFileN9': record.get('sPicFileN9'),

        # Global/National Account
        'iGlobalNum_NationalMasterAccount': convert_to_int(record.get('iGlobalNum_NationalMasterAccount')),
        'iGlobalNum_NationalFranchiseAccount': convert_to_int(record.get('iGlobalNum_NationalFranchiseAccount')),

        # Source Timestamps
        'dCreated': convert_to_datetime(record.get('dCreated')),
        'dUpdated': convert_to_datetime(record.get('dUpdated')),
        'uTS': record.get('uTS'),

        # ETL Tracking
        'sLocationCode': location_code,
        'extract_date': extract_date,
    }


# =============================================================================
# Phase A: Seed — INSERT all known (SiteID, TenantID) pairs
# =============================================================================

def run_seed(engine):
    """
    Query cc_tenants UNION rentroll for all DISTINCT (SiteID, TenantID) pairs.
    INSERT INTO ccws_tenants ON CONFLICT DO NOTHING.
    """
    logger.info("=" * 70)
    logger.info("Phase A: Seeding ccws_tenants with all known (SiteID, TenantID) pairs")
    logger.info("=" * 70)

    with engine.connect() as conn:
        # Count existing
        existing = conn.execute(text('SELECT COUNT(*) FROM ccws_tenants')).scalar()
        logger.info(f"  Existing ccws_tenants rows: {existing}")

        # UNION of all known tenant IDs from cc_tenants and rentroll
        seed_sql = text("""
            INSERT INTO ccws_tenants ("TenantID", "SiteID")
            SELECT DISTINCT "TenantID", "SiteID"
            FROM (
                SELECT "TenantID", "SiteID" FROM cc_tenants
                WHERE "TenantID" IS NOT NULL AND "SiteID" IS NOT NULL
                UNION
                SELECT "TenantID", "SiteID" FROM rentroll
                WHERE "TenantID" IS NOT NULL AND "SiteID" IS NOT NULL
            ) AS all_tenants
            ON CONFLICT ("TenantID", "SiteID") DO NOTHING
        """)

        result = conn.execute(seed_sql)
        conn.commit()

        inserted = result.rowcount
        logger.info(f"  Inserted {inserted} new (SiteID, TenantID) pairs")

        # Final count
        final = conn.execute(text('SELECT COUNT(*) FROM ccws_tenants')).scalar()
        logger.info(f"  Total ccws_tenants rows: {final}")

    logger.info("Seed complete.")


# =============================================================================
# Phase B: Backfill — call GetTenantInfoByTenantID for rows missing full data
# =============================================================================

def build_site_to_location_map(engine) -> Dict[int, str]:
    """Build SiteID → sLocationCode mapping from siteinfo table."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            'SELECT "SiteID", "SiteCode" FROM siteinfo WHERE "SiteCode" IS NOT NULL'
        )).fetchall()
    return {row[0]: row[1] for row in rows}


def run_backfill(engine, soap_client: SOAPClient, location_filter: Optional[str] = None, batch_size: int = 100):
    """
    SELECT ccws_tenants rows missing full data (dCreated IS NULL as marker),
    map SiteID → location_code via siteinfo,
    call GetTenantInfoByTenantID → UPDATE with all fields.
    """
    logger.info("=" * 70)
    logger.info("Phase B: Backfilling ccws_tenants via GetTenantInfoByTenantID")
    logger.info("=" * 70)

    extract_date = date.today()
    site_to_location = build_site_to_location_map(engine)
    logger.info(f"  Loaded {len(site_to_location)} site→location mappings")

    # Find rows that need backfill (dCreated IS NULL means never fetched full data)
    with engine.connect() as conn:
        where_clause = 'WHERE "dCreated" IS NULL AND extract_date IS NULL'
        if location_filter:
            # If location filter, need to resolve SiteID from siteinfo
            site_ids = [sid for sid, loc in site_to_location.items() if loc == location_filter]
            if not site_ids:
                logger.error(f"  No SiteID found for location {location_filter}")
                return
            placeholders = ','.join(str(s) for s in site_ids)
            where_clause += f' AND "SiteID" IN ({placeholders})'

        rows = conn.execute(text(
            f'SELECT "SiteID", "TenantID" FROM ccws_tenants {where_clause}'
        )).fetchall()

    total = len(rows)
    logger.info(f"  Found {total} tenants needing backfill")

    if total == 0:
        logger.info("  Nothing to backfill.")
        return

    updated = 0
    failed = 0

    with engine.connect() as conn:
        with tqdm(total=total, desc="  Backfilling", unit="tenant") as pbar:
            for site_id, tenant_id in rows:
                location_code = site_to_location.get(site_id)
                if not location_code:
                    logger.warning(f"  No location code for SiteID={site_id}, skipping TenantID={tenant_id}")
                    failed += 1
                    pbar.update(1)
                    continue

                raw = fetch_tenant_info(soap_client, location_code, tenant_id)
                if raw is None:
                    failed += 1
                    pbar.update(1)
                    continue

                transformed = transform_tenant_info(raw, location_code, extract_date)

                # Build UPDATE SET clause (exclude PK columns)
                set_cols = {k: v for k, v in transformed.items() if k not in ('SiteID', 'TenantID')}
                set_clause = ', '.join(f'"{k}" = :{k}' for k in set_cols)
                params = {**set_cols, 'pk_site': site_id, 'pk_tenant': tenant_id}

                conn.execute(
                    text(f'UPDATE ccws_tenants SET {set_clause} WHERE "SiteID" = :pk_site AND "TenantID" = :pk_tenant'),
                    params
                )
                updated += 1

                # Commit in batches
                if updated % batch_size == 0:
                    conn.commit()

                pbar.update(1)

        conn.commit()

    logger.info(f"  Backfill complete: {updated} updated, {failed} failed/skipped")


# =============================================================================
# Probe mode — discover API response fields
# =============================================================================

def run_probe(soap_client: SOAPClient, location_code: str, tenant_id: int):
    """Call GetTenantInfoByTenantID once and print all returned fields."""
    import json

    logger.info("=" * 70)
    logger.info(f"Probing GetTenantInfoByTenantID: {location_code} / TenantID={tenant_id}")
    logger.info("=" * 70)

    raw = fetch_tenant_info(soap_client, location_code, tenant_id)
    if raw is None:
        logger.error("  No data returned. Check location_code and tenant_id.")
        logger.info("  Trying raw XML probe...")

        # Fall back to raw call to see XML structure
        config = REPORT_REGISTRY['tenant_info_by_tenant_id']
        try:
            response = soap_client.call_raw(
                operation=config.operation,
                parameters={'sLocationCode': location_code, 'iTenantID': tenant_id},
                soap_action=config.soap_action,
                namespace=config.namespace,
            )
            logger.info(f"  Raw XML response (first 3000 chars):\n{response[:3000]}")
        except AttributeError:
            logger.info("  soap_client.call_raw() not available — use call() with debug logging")
        except Exception as e:
            logger.error(f"  Raw probe also failed: {e}")
        return

    logger.info(f"  Received {len(raw)} fields:")
    for key in sorted(raw.keys()):
        val = raw[key]
        val_repr = repr(val)[:80]
        logger.info(f"    {key}: {val_repr}")

    logger.info(f"\n  Total fields: {len(raw)}")


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description='Seed & Backfill ccws_tenants',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python seed_ccws_tenants.py --mode seed
  python seed_ccws_tenants.py --mode backfill
  python seed_ccws_tenants.py --mode backfill --location L001
  python seed_ccws_tenants.py --mode probe --location L001 --tenant-id 12345
        """
    )

    parser.add_argument('--mode', choices=['seed', 'backfill', 'probe'], required=True)
    parser.add_argument('--location', type=str, default=None, help='Location code filter (e.g., L001)')
    parser.add_argument('--tenant-id', type=int, default=None, help='TenantID for probe mode')
    parser.add_argument('--batch-size', type=int, default=100, help='Commit batch size for backfill (default: 100)')

    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    args = parse_args()

    config = DataLayerConfig.from_env()

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    engine = create_engine_from_config(db_config)

    # Ensure ccws_tenants table exists (with new columns)
    Base.metadata.create_all(engine, tables=[CcwsTenant.__table__])

    if args.mode == 'seed':
        run_seed(engine)

    elif args.mode == 'backfill' or args.mode == 'probe':
        if not config.soap:
            raise ValueError("SOAP configuration not found in .env")

        cc_url = get_callcenter_url(config.soap.base_url)
        soap_client = SOAPClient(
            base_url=cc_url,
            corp_code=config.soap.corp_code,
            corp_user=config.soap.corp_user,
            api_key=config.soap.api_key,
            corp_password=config.soap.corp_password,
            timeout=config.soap.timeout,
            retries=config.soap.retries
        )

        if args.mode == 'probe':
            if not args.location or not args.tenant_id:
                raise ValueError("--location and --tenant-id required for probe mode")
            run_probe(soap_client, args.location, args.tenant_id)
        else:
            run_backfill(engine, soap_client, args.location, args.batch_size)

        soap_client.close()

    engine.dispose()


if __name__ == "__main__":
    main()
