"""
Quick SOAP-vs-internal-calculator alignment check for one (site, unit, concession).

Usage (from backend/python on VM):
    python3 scripts/compare_calc_vs_soap.py L003 79215 11878 6
                                            site  unit  concession duration

Pulls billing config + taxes + admin/deposit/insurance from middleware,
calls MoveInCostRetrieveWithDiscount_v4 for ground truth, then runs
calculate_movein_cost + calculate_duration_breakdown with the same inputs.
Prints both side-by-side so we can spot any divergence.
"""
import sys
import os
from datetime import datetime, date
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text

from common.config_loader import get_database_url
from common.soap_client import SOAPClient
from common.movein_cost_calculator import (
    calculate_movein_cost, calculate_duration_breakdown, ChargeTypeTax,
)
from common.config import DataLayerConfig

CC_NS = "http://tempuri.org/"

def _get_soap_client():
    config = DataLayerConfig.from_env()
    cc_url = config.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
    return SOAPClient(
        base_url=cc_url, corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user, api_key=config.soap.api_key,
        corp_password=config.soap.corp_password, timeout=60, retries=1,
    )


def section(title):
    print('\n' + '=' * 70)
    print(f'  {title}')
    print('=' * 70)


def soap_call(client, op, params, result_tag='Table'):
    return client.call(operation=op, parameters=params,
                       soap_action=f"{CC_NS}/{op}", namespace=CC_NS,
                       result_tag=result_tag)


def _norm_money(v):
    if v is None:
        return Decimal('0')
    return Decimal(str(v)).quantize(Decimal('0.01'))


def main(site, unit_id, concession_id, duration_months):
    move_in = date.today()
    move_in_iso = move_in.strftime('%Y-%m-%dT00:00:00')

    print(f"site={site}  unit={unit_id}  concession={concession_id}  duration={duration_months}m  move_in={move_in}")

    engine = create_engine(get_database_url('middleware'))
    with engine.connect() as conn:
        # Concession + std rate
        row = conn.execute(text("""
            SELECT c."dcStdRate", d."dcPCDiscount", d."dcFixedDiscount",
                   d."iInMonth", d."iAmtType", d."dcMaxAmountOff",
                   d."bPrepay", d."iPrePaidMonths", d."bNeverExpires", d."sPlanName"
            FROM ccws_units c
            JOIN ccws_discount d ON d."SiteID" = c."SiteID"
                                AND d."ConcessionID" = :cid
            WHERE c."sLocationCode" = :site AND c."UnitID" = :uid
            LIMIT 1
        """), {'site': site, 'uid': unit_id, 'cid': concession_id}).fetchone()
        if not row:
            print(f"[ERROR] no candidate row for {site}/{unit_id}/{concession_id}")
            return
        std_rate, pct, fixed, in_month, amt_type, max_off, prepay, prepaid_m, never_exp, plan_name = row
        print(f"\nConcession '{plan_name}': pct={pct} fixed={fixed} amt_type={amt_type} "
              f"iInMonth={in_month} prepay={prepay} prepaid={prepaid_m} neverExp={never_exp}")
        print(f"Unit std_rate={std_rate}")

        # Billing config
        bc = conn.execute(text("""
            SELECT b_anniv_date_leasing, i_day_strt_prorating, i_day_strt_prorate_plus_next
            FROM ccws_site_billing_config
            WHERE "SiteCode" = :site
        """), {'site': site}).fetchone()
        if not bc:
            print(f"[ERROR] no billing config for {site}")
            return
        anniv, prorate_day, prorate_plus_next = bc
        print(f"Billing: anniv={anniv} prorate_day={prorate_day} prorate_plus_next={prorate_plus_next}")

        # Charge descriptions (rent tax, admin, deposit, insurance)
        cd_rows = conn.execute(text("""
            SELECT "sChgCategory", "dcTax1Rate", "dcTax2Rate", "dcPrice"
            FROM ccws_charge_descriptions
            WHERE "SiteCode" = :site
              AND "sChgCategory" IN ('Rent','AdminFee','SecDep','Insurance')
        """), {'site': site}).fetchall()
        cd_map = {r[0]: r for r in cd_rows}
        print(f"\nCharge descriptions: {[r[0] for r in cd_rows]}")

        # Site security deposit (= std_rate by default for most sites)
        deposit = std_rate

        # Insurance premium (default first available coverage)
        ins = conn.execute(text("""
            SELECT "InsurCoverageID", "dcPremium"
            FROM ccws_insurance_coverage
            WHERE "SiteCode" = :site
            ORDER BY "dcPremium" ASC LIMIT 1
        """), {'site': site}).fetchone()
        if ins:
            ins_id, ins_premium = ins
            print(f"Insurance: id={ins_id} premium={ins_premium}")
        else:
            ins_id, ins_premium = 0, Decimal('0')

    def _ct(cat):
        row = cd_map.get(cat, (cat, 0, 0, 0))
        return ChargeTypeTax(
            category=cat,
            tax1_rate=Decimal(str(row[1] or 0)),
            tax2_rate=Decimal(str(row[2] or 0)),
            default_price=Decimal(str(row[3] or 0)),
        )
    rent_tax = _ct('Rent')
    admin_tax = _ct('AdminFee')
    dep_tax = _ct('SecDep')
    ins_tax = _ct('Insurance')
    admin_fee = admin_tax.default_price

    print(f"\nTax rates: rent={rent_tax.tax1_rate}% admin={admin_tax.tax1_rate}% "
          f"dep={dep_tax.tax1_rate}% ins={ins_tax.tax1_rate}%")
    print(f"Admin fee: {admin_fee}")

    # ==================== Internal calculator ====================
    section("INTERNAL CALCULATOR — month 1 (calculate_movein_cost)")
    m1_lines, conf = calculate_movein_cost(
        std_rate=std_rate, security_deposit=deposit, admin_fee=admin_fee,
        move_in_date=move_in,
        rent_tax=rent_tax, admin_tax=admin_tax, deposit_tax=dep_tax, insurance_tax=ins_tax,
        pc_discount=pct or 0, fixed_discount=fixed or 0,
        insurance_premium=ins_premium, anniversary_billing=bool(anniv),
        day_start_prorate_plus_next=prorate_plus_next or 17,
    )
    print(f"\n{'Line':<30} {'Charge':>10} {'Disc':>8} {'Tax1':>8} {'Tax2':>8} {'Total':>10}")
    m1_total = Decimal('0')
    for l in m1_lines:
        print(f"{l.description:<30} {_norm_money(l.charge_amount):>10} "
              f"{_norm_money(l.discount):>8} {_norm_money(l.tax1):>8} "
              f"{_norm_money(l.tax2):>8} {_norm_money(l.total):>10}")
        m1_total += _norm_money(l.total)
    print(f"{'INTERNAL TOTAL':<30} {'':>10} {'':>8} {'':>8} {'':>8} {m1_total:>10}")
    print(f"Confidence: {conf}")

    section(f"INTERNAL CALCULATOR — full duration ({duration_months} months)")
    quote = calculate_duration_breakdown(
        std_rate=std_rate, security_deposit=deposit, admin_fee=admin_fee,
        move_in_date=move_in,
        rent_tax=rent_tax, admin_tax=admin_tax, deposit_tax=dep_tax, insurance_tax=ins_tax,
        pc_discount=pct or 0, fixed_discount=fixed or 0,
        insurance_premium=ins_premium, anniversary_billing=bool(anniv),
        duration_months=duration_months,
        concession_in_month=int(in_month or 1),
        max_amount_off=Decimal(str(max_off)) if max_off else None,
        unit_id=unit_id, plan_id=0, concession_id=concession_id,
    )
    print(f"first_month: {quote.first_month_total}")
    print(f"monthly_avg: {quote.monthly_average}")
    print(f"total_contract: {quote.total_contract}")
    print(f"confidence: {quote.confidence} ({quote.confidence_reason})")

    # ==================== SOAP call ====================
    section("SOAP MoveInCostRetrieveWithDiscount_v4 — ground truth")
    try:
        client = _get_soap_client()
        result = soap_call(client, "MoveInCostRetrieveWithDiscount_v4", {
            "sLocationCode": site, "iUnitID": str(unit_id),
            "dMoveInDate": move_in_iso,
            "InsuranceCoverageID": str(ins_id),
            "ConcessionPlanID": str(concession_id),
            "iPromoGlobalNum": "0", "ChannelType": "0",
            "bApplyInsuranceCredit": "false",
        })
        soap_total = Decimal('0')
        if isinstance(result, list):
            print(f"\n{'Description':<35} {'Charge':>10} {'Disc':>8} {'Tax1':>8} {'Total':>10}")
            for r in result:
                desc = r.get('Description', '?')
                charge = _norm_money(r.get('Charge'))
                disc = _norm_money(r.get('Discount') or 0)
                tax = _norm_money(r.get('Tax1') or 0) + _norm_money(r.get('Tax2') or 0)
                tot = _norm_money(r.get('Total') or (charge - disc + tax))
                print(f"{desc:<35} {charge:>10} {disc:>8} {tax:>8} {tot:>10}")
                soap_total += tot
            print(f"\nSOAP TOTAL: {soap_total}")
        else:
            print(f"unexpected SOAP response shape: {result}")

        # Diff
        section("DIFF")
        print(f"Internal m1: {m1_total}")
        print(f"SOAP m1   : {soap_total}")
        diff = m1_total - soap_total
        print(f"DELTA     : {diff} ({'MATCH' if abs(diff) < Decimal('0.05') else 'MISMATCH'})")
    except Exception as exc:
        print(f"[ERROR] SOAP call failed: {exc}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    if len(sys.argv) < 5:
        print("usage: compare_calc_vs_soap.py SITE UNIT_ID CONCESSION_ID DURATION_MONTHS")
        sys.exit(1)
    main(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]))
