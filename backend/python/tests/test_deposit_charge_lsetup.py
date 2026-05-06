#!/usr/bin/env python3
"""
Test deposit charge orchestration against LSETUP.
Run from project root: python3 test_deposit_charge_lsetup.py

Steps:
  1. ChargeDescriptionsRetrieve — find deposit ChargeDescID
  2. LedgersByTenantID — find a tenant's ledger(s)
  3. ChargeAddToLedger — add deposit charge (DRY RUN by default)
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend', 'python'))

from common.config import DataLayerConfig
from common.soap_client import SOAPClient, SOAPFaultError

CC_NS = "http://tempuri.org/CallCenterWs/CallCenterWs"
SITE_CODE = "LSETUP"

# Set DRY_RUN=False to actually charge
DRY_RUN = True


def get_client():
    config = DataLayerConfig.from_env()
    cc_url = config.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
    return SOAPClient(
        base_url=cc_url,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=config.soap.timeout,
        retries=config.soap.retries,
    )


def soap_action(op):
    return f"{CC_NS}/{op}"


def separator(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def test_charge_descriptions(client):
    """Step 1: Get all charge descriptions for LSETUP site."""
    separator("Step 1: ChargeDescriptionsRetrieve")
    try:
        results = client.call(
            operation="ChargeDescriptionsRetrieve",
            parameters={"sLocationCode": SITE_CODE},
            soap_action=soap_action("ChargeDescriptionsRetrieve"),
            namespace=CC_NS,
            result_tag="Table",
        )
        if not results:
            print("  No charge descriptions returned (may use different result_tag)")
            # Try without result_tag
            results = client.call(
                operation="ChargeDescriptionsRetrieve",
                parameters={"sLocationCode": SITE_CODE},
                soap_action=soap_action("ChargeDescriptionsRetrieve"),
                namespace=CC_NS,
            )
        print(f"  OK — {len(results) if results else 0} charge descriptions")
        if results:
            for r in results[:20]:  # Print first 20
                print(f"     {r}")

        # Find deposit-related descriptions
        deposit_descs = []
        if results:
            for r in results:
                desc_text = ''
                if isinstance(r, dict):
                    desc_text = str(r.get('sChgDesc', r.get('Description', r.get('sDescription', ''))))
                else:
                    desc_text = str(r)
                if 'deposit' in desc_text.lower():
                    deposit_descs.append(r)

            if deposit_descs:
                print(f"\n  Deposit-related descriptions found:")
                for d in deposit_descs:
                    print(f"     {d}")
            else:
                print(f"\n  No descriptions with 'deposit' in name — review list above to identify correct charge type")

        return results
    except Exception as e:
        print(f"  FAIL — {e}")
        return None


def test_ledgers_by_tenant(client, tenant_id):
    """Step 2: Get ledgers for a specific tenant."""
    separator(f"Step 2: LedgersByTenantID (tenant={tenant_id})")
    try:
        results = client.call(
            operation="LedgersByTenantID",
            parameters={
                "sLocationCode": SITE_CODE,
                "sTenantID": str(tenant_id),
            },
            soap_action=soap_action("LedgersByTenantID"),
            namespace=CC_NS,
            result_tag="Ledgers",
        )
        if not results:
            # Try with Table result_tag
            results = client.call(
                operation="LedgersByTenantID",
                parameters={
                    "sLocationCode": SITE_CODE,
                    "sTenantID": str(tenant_id),
                },
                soap_action=soap_action("LedgersByTenantID"),
                namespace=CC_NS,
                result_tag="Table",
            )
        print(f"  OK — {len(results) if results else 0} ledgers")
        if results:
            for r in results:
                if isinstance(r, dict):
                    print(f"     LedgerID={r.get('LedgerID')}  Unit={r.get('sUnitName')}  "
                          f"Rent={r.get('dcRent')}  MovedIn={r.get('dMovedIn')}  "
                          f"Balance={r.get('dcChargeBalance')}")
                else:
                    print(f"     {r}")
        return results
    except Exception as e:
        print(f"  FAIL — {e}")
        return None


def test_charge_add(client, ledger_id, charge_desc_id, amount):
    """Step 3: Add a deposit charge to a ledger."""
    separator(f"Step 3: ChargeAddToLedger (ledger={ledger_id}, desc={charge_desc_id}, amt={amount})")

    if DRY_RUN:
        print(f"  DRY RUN — would call ChargeAddToLedger with:")
        print(f"     sLocationCode = {SITE_CODE}")
        print(f"     LedgerID      = {ledger_id}")
        print(f"     ChargeDescID  = {charge_desc_id}")
        print(f"     dcAmtPreTax   = {amount}")
        print(f"\n  Set DRY_RUN=False to execute")
        return "dry_run"

    try:
        result = client.call(
            operation="ChargeAddToLedger",
            parameters={
                "sLocationCode": SITE_CODE,
                "LedgerID": ledger_id,
                "ChargeDescID": charge_desc_id,
                "dcAmtPreTax": amount,
            },
            soap_action=soap_action("ChargeAddToLedger"),
            namespace=CC_NS,
        )
        print(f"  OK — Charge added successfully")
        print(f"  Response: {result}")
        return result
    except SOAPFaultError as e:
        print(f"  SOAP FAULT — {e}")
        return None
    except Exception as e:
        print(f"  FAIL — {e}")
        return None


def main():
    print("=" * 70)
    print("  Deposit Charge Orchestration Test — LSETUP")
    print(f"  DRY_RUN = {DRY_RUN}")
    print("=" * 70)

    client = get_client()

    try:
        # Step 1: Get charge descriptions
        charge_descs = test_charge_descriptions(client)

        # Step 2: Find a tenant from PBI database (rent_roll for LSETUP site)
        tenant_id = None
        ledger_id = None

        separator("Finding test tenant from PBI database")
        try:
            from sqlalchemy import create_engine, text as sa_text
            from common.config_loader import get_database_url
            pbi_url = get_database_url('pbi')
            engine = create_engine(pbi_url)
            with engine.connect() as conn:
                # LSETUP SiteID = 27525 (from ChargeDescriptionsRetrieve response)
                row = conn.execute(sa_text(
                    "SELECT \"TenantID\", \"LedgerID\", \"sUnitName\", \"dcRent\" "
                    "FROM rentroll "
                    "WHERE \"SiteID\" = 27525 AND \"bRented\" = true "
                    "LIMIT 5"
                )).fetchall()
                if row:
                    print(f"  Found {len(row)} active tenants at LSETUP (SiteID=27525)")
                    for r in row:
                        print(f"     TenantID={r[0]}  LedgerID={r[1]}  Unit={r[2]}  Rent={r[3]}")
                    tenant_id = str(row[0][0])
                    ledger_id = int(row[0][1])
                    print(f"\n  Using TenantID={tenant_id}, LedgerID={ledger_id}")
                else:
                    print("  No active tenants found at LSETUP in PBI")
            engine.dispose()
        except Exception as e:
            print(f"  PBI lookup failed: {e}")

        # Step 2b: If we found a tenant, get their ledgers
        if tenant_id:
            ledgers = test_ledgers_by_tenant(client, tenant_id)

        # Step 3: Attempt deposit charge (dry run by default)
        if ledger_id and charge_descs:
            # Try to find a deposit charge desc ID
            deposit_id = None
            if charge_descs:
                for cd in charge_descs:
                    if isinstance(cd, dict):
                        desc_text = str(cd.get('sChgDesc', cd.get('Description', cd.get('sDescription', ''))))
                        if 'deposit' in desc_text.lower():
                            deposit_id = cd.get('ChargeDescID', cd.get('iChargeDescID'))
                            break

            if deposit_id:
                test_charge_add(client, ledger_id, deposit_id, 100.00)
            else:
                print(f"\n  No deposit charge description found — using first available for test")
                if isinstance(charge_descs[0], dict):
                    first_id = charge_descs[0].get('ChargeDescID', charge_descs[0].get('iChargeDescID'))
                    if first_id:
                        test_charge_add(client, ledger_id, first_id, 100.00)
                    else:
                        print(f"  Could not extract ChargeDescID from: {charge_descs[0]}")
                else:
                    print(f"  Unexpected charge desc format: {charge_descs[0]}")
        else:
            print("\n  Skipping Step 3 — missing ledger_id or charge descriptions")

    finally:
        client.close()

    print(f"\n{'='*70}")
    print("  Done")
    print(f"{'='*70}")


if __name__ == '__main__':
    main()
