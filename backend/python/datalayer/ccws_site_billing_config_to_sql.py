"""
CCWS Site Billing Config to SQL Pipeline

For each site, calls MoveInCostRetrieveWithDiscount_v4 on a known available
unit and extracts the proration/billing-mode flags from the response:
  - bAnnivDateLeasing
  - iDayStrtProrating
  - iDayStrtProratePlusNext

Upserts into ccws_site_billing_config but PRESERVES manual overrides — rows
where overridden_by IS NOT NULL are skipped (admin UI lets staff lock
specific values).

Usage:
    python ccws_site_billing_config_to_sql.py
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from common import (
    DataLayerConfig, SOAPClient,
    create_engine_from_config, SessionManager, Base,
    convert_to_bool, convert_to_int,
)
from common.models import CcwsSiteBillingConfig
from common.config import get_pipeline_config
from common.config_loader import get_database_url


CALL_CENTER_WS_URL = "https://api.smdservers.net/CCWs_3.5/CallCenterWs.asmx"
NAMESPACE = "http://tempuri.org/CallCenterWs/CallCenterWs"


def soap_call(client, op, params, result_tag="RT"):
    return client.call(operation=op, parameters=params,
                       soap_action=f"{NAMESPACE}/{op}",
                       namespace=NAMESPACE, result_tag=result_tag)


def fetch_site_config(client, site_code: str) -> Optional[Tuple[int, dict]]:
    """
    Fetch billing config for one site by calling MoveInCostRetrieve on
    any available unit. Returns (site_id, config_dict) or None on error.
    """
    try:
        units = soap_call(client,
            "UnitsInformationAvailableUnitsOnly_v2",
            {"sLocationCode": site_code}, result_tag="Table")
        if not units:
            return None
        unit = units[0]
        unit_id = unit.get("UnitID")
        if not unit_id:
            return None

        move_in = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%dT00:00:00")
        cost = soap_call(client,
            "MoveInCostRetrieveWithDiscount_v4", {
                "sLocationCode": site_code, "iUnitID": str(unit_id),
                "dMoveInDate": move_in,
                "InsuranceCoverageID": "0", "ConcessionPlanID": "0",
                "iPromoGlobalNum": "0", "ChannelType": "0",
                "bApplyInsuranceCredit": "false",
                "sCreditCardNum": "",
            }, result_tag="Table")

        if not cost:
            return None

        row = cost[0]
        site_id = convert_to_int(row.get("SiteID")) or convert_to_int(unit.get("SiteID"))
        return site_id, {
            "b_anniv_date_leasing": convert_to_bool(row.get("bAnnivDateLeasing")),
            "i_day_strt_prorating": convert_to_int(row.get("iDayStrtProrating")) or 1,
            "i_day_strt_prorate_plus_next": convert_to_int(row.get("iDayStrtProratePlusNext")) or 17,
        }
    except Exception as e:
        tqdm.write(f"  ✗ {site_code}: {e}")
        return None


def main():
    config = DataLayerConfig.from_env()
    if not config.soap:
        raise ValueError("SOAP configuration not found")

    location_codes = (
        get_pipeline_config('ccws_site_billing_config', 'location_codes', [])
        or get_pipeline_config('ccws_discount_plans', 'location_codes', [])
    )
    if not location_codes:
        raise ValueError("location_codes not configured")

    soap_client = SOAPClient(
        base_url=CALL_CENTER_WS_URL,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=120, retries=3,
    )

    pbi_engine = create_engine(get_database_url('pbi'))
    Base.metadata.create_all(pbi_engine, tables=[CcwsSiteBillingConfig.__table__])
    session_mgr = SessionManager(pbi_engine)

    print("=" * 70)
    print("CC Site Billing Config to SQL Pipeline")
    print("=" * 70)
    print(f"Endpoint: MoveInCostRetrieveWithDiscount_v4 (config extraction)")
    print(f"Locations: {len(location_codes)}")
    print("=" * 70)

    synced = 0
    skipped_override = 0
    failed = 0

    with tqdm(total=len(location_codes), desc="  Syncing sites", unit="site") as pbar:
        for site_code in location_codes:
            site_code = site_code.strip()
            with session_mgr.session_scope() as session:
                existing = session.query(CcwsSiteBillingConfig).filter_by(
                    SiteCode=site_code).first()
                if existing and existing.overridden_by:
                    pbar.set_postfix({"site": site_code, "status": "OVERRIDDEN"})
                    pbar.update(1)
                    skipped_override += 1
                    continue

            result = fetch_site_config(soap_client, site_code)
            if not result:
                failed += 1
                pbar.set_postfix({"site": site_code, "status": "FAIL"})
                pbar.update(1)
                continue

            site_id, cfg = result
            with session_mgr.session_scope() as session:
                existing = session.query(CcwsSiteBillingConfig).filter_by(
                    SiteCode=site_code).first()
                if existing:
                    existing.SiteID = site_id
                    existing.b_anniv_date_leasing = cfg["b_anniv_date_leasing"]
                    existing.i_day_strt_prorating = cfg["i_day_strt_prorating"]
                    existing.i_day_strt_prorate_plus_next = cfg["i_day_strt_prorate_plus_next"]
                    existing.synced_from_soap_at = datetime.utcnow()
                else:
                    session.add(CcwsSiteBillingConfig(
                        SiteCode=site_code, SiteID=site_id,
                        synced_from_soap_at=datetime.utcnow(),
                        **cfg,
                    ))
            synced += 1
            mode = "anniversary" if cfg["b_anniv_date_leasing"] else "1st-of-month"
            pbar.set_postfix({"site": site_code, "mode": mode,
                              "X": cfg["i_day_strt_prorate_plus_next"]})
            pbar.update(1)

    soap_client.close()
    print(f"\nSynced:           {synced}")
    print(f"Skipped (override): {skipped_override}")
    print(f"Failed:           {failed}")


if __name__ == "__main__":
    main()
