"""
Probe SOAP MoveInCostRetrieveWithDiscount_Reservation_v4 across a matrix of
move-in days to determine the exact rounding rule for rent tax vs insurance tax.

Hypothesis (from one observation on LSETUP day 17):
  - rent tax: ROUND_DOWN (truncates) — already validated
  - insurance tax: HALF_UP — newly suspected

Approach:
  1. Create one tenant + one reservation on LSETUP (no concession)
  2. Call MoveInCostRetrieveWithDiscount_Reservation_v4 for each move-in day
     in a sample matrix (1, 5, 10, 14, 17, 20, 23, 28)
  3. For each rent + insurance line, record SOAP's tax value
  4. Compute DOWN(charge × rate) and HALF_UP(charge × rate)
  5. Print matrix; the rule is whichever rounding always matches SOAP

No real lease created — no payment. Just probes the cost-retrieve operation,
then cleans up the reservation.

Run from backend/python on the VM:
    python3 scripts/probe_soap_tax_rounding.py
"""
import sys, os
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url
from common.soap_client import SOAPClient
from common.config import DataLayerConfig

CC_NS = "http://tempuri.org/CallCenterWs/CallCenterWs"
SITE = "LSETUP"


def get_client():
    cfg = DataLayerConfig.from_env()
    cc_url = cfg.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
    return SOAPClient(
        base_url=cc_url, corp_code=cfg.soap.corp_code,
        corp_user=cfg.soap.corp_user, api_key=cfg.soap.api_key,
        corp_password=cfg.soap.corp_password, timeout=60, retries=1,
    )


def soap_call(client, op, params, result_tag="RT"):
    return client.call(operation=op, parameters=params,
                       soap_action=f"{CC_NS}/{op}", namespace=CC_NS,
                       result_tag=result_tag)


def round_down(amount, rate_pct):
    return (Decimal(str(amount)) * Decimal(str(rate_pct)) / Decimal('100')) \
        .quantize(Decimal('0.01'), rounding=ROUND_DOWN)


def round_half_up(amount, rate_pct):
    return (Decimal(str(amount)) * Decimal(str(rate_pct)) / Decimal('100')) \
        .quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def main():
    client = get_client()
    engine = create_engine(get_database_url('pbi'))

    print(f"\nSOAP rounding probe — {SITE}\n")

    # Pick a unit + insurance
    units = soap_call(client, "UnitsInformationAvailableUnitsOnly_v2",
                      {"sLocationCode": SITE}, result_tag="Table")
    unit = next((u for u in units if 60 <= float(u.get('dcStdRate', 0)) <= 80), units[0])
    unit_id = unit['UnitID']
    rate = float(unit['dcStdRate'])
    print(f"Unit: {unit_id} @ ${rate}/mo  (rent tax 9%, insurance tax 8% on LSETUP)")

    # Smallest insurance plan
    with engine.connect() as conn:
        ins = conn.execute(text("""
            SELECT "InsurCoverageID", "dcPremium" FROM ccws_insurance_coverage
            WHERE "SiteCode" = :s ORDER BY "dcPremium" LIMIT 1
        """), {'s': SITE}).fetchone()
    ins_id = int(ins[0])
    ins_premium = float(ins[1])
    print(f"Insurance: id={ins_id} ${ins_premium}/mo\n")

    # Create tenant
    tenant = soap_call(client, "TenantNewDetailed_v3", {
        "sLocationCode": SITE, "sWebPassword": "", "sMrMrs": "",
        "sFName": "TaxProbe", "sMI": "",
        "sLName": f"T{datetime.now().strftime('%H%M%S')}",
        "sCompany": "", "sAddr1": "1 Test St", "sAddr2": "",
        "sCity": "Singapore", "sRegion": "", "sPostalCode": "000000",
        "sCountry": "SG", "sPhone": "99999999",
        "sMrMrsAlt": "", "sFNameAlt": "", "sMIAlt": "", "sLNameAlt": "",
        "sAddr1Alt": "", "sAddr2Alt": "", "sCityAlt": "", "sRegionAlt": "",
        "sPostalCodeAlt": "", "sCountryAlt": "", "sPhoneAlt": "",
        "sMrMrsBus": "", "sFNameBus": "", "sMIBus": "", "sLNameBus": "",
        "sCompanyBus": "", "sAddr1Bus": "", "sAddr2Bus": "", "sCityBus": "",
        "sRegionBus": "", "sPostalCodeBus": "", "sCountryBus": "", "sPhoneBus": "",
        "sEmail": "probe@test.com", "sEmailAlt": "", "sEmailBus": "",
        "sFax": "", "sFaxAlt": "", "sFaxBus": "", "sMobile": "",
        "dDOB": "1990-01-01T00:00:00", "sSSN": "", "sDriversLic": "",
        "bBusiness": "false", "sIDType": "", "sIDNum": "", "sIDIssuer": "",
        "bTestMode": "false",
    })
    tid = int(tenant[0]["TenantID"])

    # Create one reservation (any future date, we'll override per-call)
    res = soap_call(client, "ReservationNewWithSource_v6", {
        "sLocationCode": SITE, "sTenantID": str(tid), "sUnitID": str(unit_id),
        "dNeeded": "2026-06-01T00:00:00", "sComment": "Tax-rounding probe",
        "iSource": "0", "sSource": "TaxProbe",
        "QTRentalTypeID": "0", "iInquiryType": "0",
        "dcQuotedRate": str(rate),
        "dExpires": "2026-06-15T00:00:00", "dFollowUp": "2026-06-04T00:00:00",
        "sTrackingCode": "", "sCallerID": "",
        "ConcessionID": "0", "PromoGlobalNum": "0",
    })
    wid = int(res[0]["Ret_Code"])
    print(f"Tenant {tid} · Reservation {wid}\n")

    # Sample matrix — vary day in May 2026 (31-day month, simple math)
    print(f"{'Day':>3}  {'RentChg':>9}  {'RentTax_SOAP':>14}  {'DOWN':>7}  {'HALFUP':>7}  {'rule':>8}  | "
          f"{'InsChg':>7}  {'InsTax_SOAP':>13}  {'DOWN':>6}  {'HALFUP':>7}  {'rule':>8}")
    print("-" * 130)

    rent_rule_votes = {'DOWN': 0, 'HALFUP': 0, 'AMBIG': 0}
    ins_rule_votes  = {'DOWN': 0, 'HALFUP': 0, 'AMBIG': 0}

    for day in [1, 5, 10, 14, 17, 20, 23, 25, 28, 30]:
        move_in_iso = f"2026-05-{day:02d}T00:00:00"
        try:
            charges = soap_call(client,
                "MoveInCostRetrieveWithDiscount_Reservation_v4", {
                    "sLocationCode": SITE, "iUnitID": str(unit_id),
                    "dMoveInDate": move_in_iso,
                    "InsuranceCoverageID": str(ins_id),
                    "ConcessionPlanID": "0", "WaitingID": str(wid),
                    "iPromoGlobalNum": "0", "ChannelType": "0",
                    "bApplyInsuranceCredit": "false",
                }, result_tag="Table")
        except Exception as e:
            print(f"  day {day:2d}  SOAP ERROR: {e}")
            continue

        # Filter rent + insurance lines (skip second-month and admin/sec_dep)
        rent_lines = [r for r in charges if 'Rent' in (r.get('ChargeDescription') or '') and 'Sec' not in (r.get('ChargeDescription') or '')]
        ins_lines = [r for r in charges if 'Insurance' in (r.get('ChargeDescription') or '')]

        # Take FIRST rent line (move-in month) and FIRST insurance line
        if not rent_lines or not ins_lines:
            print(f"  day {day:2d}  missing line(s) — rent={len(rent_lines)} ins={len(ins_lines)}")
            continue
        r0 = rent_lines[0]
        i0 = ins_lines[0]

        rent_chg = Decimal(str(r0.get('ChargeAmount') or 0))
        rent_tax_soap = Decimal(str(r0.get('Tax1') or r0.get('dcTax1') or 0))
        rent_down = round_down(rent_chg, 9)
        rent_up   = round_half_up(rent_chg, 9)
        if rent_down == rent_up:
            rent_verdict = '(eq)'
        elif rent_tax_soap == rent_down:
            rent_verdict = 'DOWN'; rent_rule_votes['DOWN'] += 1
        elif rent_tax_soap == rent_up:
            rent_verdict = 'HALFUP'; rent_rule_votes['HALFUP'] += 1
        else:
            rent_verdict = '???'; rent_rule_votes['AMBIG'] += 1

        ins_chg = Decimal(str(i0.get('ChargeAmount') or 0))
        ins_tax_soap = Decimal(str(i0.get('Tax1') or i0.get('dcTax1') or 0))
        ins_down = round_down(ins_chg, 8)
        ins_up   = round_half_up(ins_chg, 8)
        if ins_down == ins_up:
            ins_verdict = '(eq)'
        elif ins_tax_soap == ins_down:
            ins_verdict = 'DOWN'; ins_rule_votes['DOWN'] += 1
        elif ins_tax_soap == ins_up:
            ins_verdict = 'HALFUP'; ins_rule_votes['HALFUP'] += 1
        else:
            ins_verdict = '???'; ins_rule_votes['AMBIG'] += 1

        print(f"{day:>3}  {rent_chg:>9}  {rent_tax_soap:>14}  {rent_down:>7}  {rent_up:>7}  {rent_verdict:>8}  | "
              f"{ins_chg:>7}  {ins_tax_soap:>13}  {ins_down:>6}  {ins_up:>7}  {ins_verdict:>8}")

    print("\nVerdict:")
    print(f"  rent tax (9%):      DOWN={rent_rule_votes['DOWN']} HALFUP={rent_rule_votes['HALFUP']} AMBIG={rent_rule_votes['AMBIG']}")
    print(f"  insurance tax (8%): DOWN={ins_rule_votes['DOWN']} HALFUP={ins_rule_votes['HALFUP']} AMBIG={ins_rule_votes['AMBIG']}")

    # Cleanup reservation
    try:
        soap_call(client, "ReservationUpdate_v4", {
            "sLocationCode": SITE, "iWaitingID": str(wid), "iStatus": "2",
            "dExpires": "2026-06-15T00:00:00", "dFollowUp": "2026-06-04T00:00:00",
            "sNote": "probe cleanup",
        })
        print(f"\nCleaned up reservation {wid}.")
    except Exception:
        pass


if __name__ == '__main__':
    main()
