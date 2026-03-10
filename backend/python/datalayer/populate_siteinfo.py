"""
Populate SiteInfo Dimension Table

Creates and populates the siteinfo dimension table with site metadata.
SiteID values are mapped from the rentroll extraction data.

Usage:
    python Scripts/datalayer/populate_siteinfo.py
"""

from decimal import Decimal
from sqlalchemy import text
from common import (
    DataLayerConfig,
    create_engine_from_config,
    SessionManager,
    Base,
    SiteInfo
)


# Site data with SiteID mapped from rentroll extraction
# Includes InternalLabel, Longitude, and Latitude
SITE_DATA = [
    {"SiteID": 48, "SiteCode": "L001", "Name": "Extra Space Jurong Pte Ltd", "InternalLabel": "IMM", "Country": "Singapore", "CityDistrict": "Jurong East", "Street": "2 Jurong East Street 21, Unit #02-71 IMM Building", "PostalCode": "609601", "PrimaryEmail": "imm@extraspaceasia.com", "Longitude": Decimal("103.7466569"), "Latitude": Decimal("1.3355903"), "google_place_id": "ChIJqzjMdA4Q2jERtzVI_xKtL4c", "embedsocial_source_id": "53220059a04d3f49b781fdbb1f7f75d8"},
    {"SiteID": 49, "SiteCode": "L002", "Name": "Extra Space Pte Ltd", "InternalLabel": "BKR", "Country": "Singapore", "CityDistrict": "Boon Keng", "Street": "301 Boon Keng Road", "PostalCode": "339779", "PrimaryEmail": "boonkeng@extraspaceasia.com", "Longitude": Decimal("103.8651647"), "Latitude": Decimal("1.3153397"), "google_place_id": "ChIJ0ZdZgQ4Z2jEROSCUE8O-eiQ", "embedsocial_source_id": "8d291258987f38d8bb3f0bc5c705d0fa"},
    {"SiteID": 63, "SiteCode": "L003", "Name": "Extra Space Eunos Link", "InternalLabel": "ELK", "Country": "Singapore", "CityDistrict": "Kaki Bukit", "Street": "7 Kaki Bukit Road 2", "PostalCode": "417840", "PrimaryEmail": "eunoslink@extraspaceasia.com", "Longitude": Decimal("103.8977703"), "Latitude": Decimal("1.3371325"), "google_place_id": "ChIJq6qqeu4X2jERcJuZmk-bjd0", "embedsocial_source_id": "40407c9c08713b838390ef6a3176532e"},
    {"SiteID": 26486, "SiteCode": "L004", "Name": "Extra Space West Coast Pte Ltd", "InternalLabel": "WCT", "Country": "Singapore", "CityDistrict": "Toh Tuck", "Street": "2 Toh Tuck Link, #01-03 & #04-01", "PostalCode": "596225", "PrimaryEmail": "westcoast@extraspaceasia.com", "Longitude": Decimal("103.758696"), "Latitude": Decimal("1.3314914"), "google_place_id": "ChIJq6qqMp0a2jERsmG4rQQXn2Y", "embedsocial_source_id": "34918437817936e8a0245d8bbae1bc74"},
    {"SiteID": 1910, "SiteCode": "L005", "Name": "Extra Space Marymount", "InternalLabel": "MMR", "Country": "Singapore", "CityDistrict": "Sin Ming", "Street": "9 Sin Ming Industrial Estate Sector B", "PostalCode": "575654", "PrimaryEmail": "marymount@extraspaceasia.com", "Longitude": Decimal("103.8409345"), "Latitude": Decimal("1.3574508"), "google_place_id": "ChIJAQAAsCMX2jER3pbtmDol7ww", "embedsocial_source_id": "942936bccb8d35bd94692e83602d6d84"},
    {"SiteID": 2276, "SiteCode": "L006", "Name": "Extra Space - Yangjae", "InternalLabel": "ESKY", "Country": "South Korea", "CityDistrict": "Seocho-gu, Seoul", "Street": "B2, 16 Maehun-ro Seocho-gu", "PostalCode": None, "PrimaryEmail": "yangjae@extraspaceasia.com", "Longitude": Decimal("127.0312101"), "Latitude": Decimal("37.4732933"), "google_place_id": "ChIJqWAuiyqhfDURsrLQM-NZmbE", "embedsocial_source_id": "31addf25923339d48652c56355b4a83a"},
    {"SiteID": 4183, "SiteCode": "L007", "Name": "Extra Space Malaysia Sdn Bhd", "InternalLabel": "CSL", "Country": "Malaysia", "CityDistrict": "Chan Sow Lin, Kuala Lumpur", "Street": "Lot 271, Jalan Dua, off Jalan Chan Sow Lin", "PostalCode": "55200", "PrimaryEmail": "chansowlin@extraspaceasia.com", "Longitude": Decimal("101.7126301"), "Latitude": Decimal("3.1227428"), "google_place_id": "ChIJ4VxT5wQ2zDEReiTIHxfXfg8", "embedsocial_source_id": "b93fd396a2333f92ac302632c00bb1f8"},
    {"SiteID": 9415, "SiteCode": "L008", "Name": "Extra Space Kallang Way", "InternalLabel": "KWY", "Country": "Singapore", "CityDistrict": "Kallang", "Street": "12 Kallang Sector", "PostalCode": "349281", "PrimaryEmail": "kallangway@extraspaceasia.com", "Longitude": Decimal("103.8744395"), "Latitude": Decimal("1.323517"), "google_place_id": "ChIJ8baDeikY2jERKiAFA-22Tgs", "embedsocial_source_id": "ff100921bb523926a40e5e1dc2bd036f"},
    {"SiteID": 10419, "SiteCode": "L009", "Name": "Extra Space Segambut Sdn Bhd", "InternalLabel": "SEG", "Country": "Malaysia", "CityDistrict": "Segambut, Kuala Lumpur", "Street": "No. 144, Jalan Batu Estate, Segambut", "PostalCode": "51200", "PrimaryEmail": "segambut@extraspaceasia.com", "Longitude": Decimal("101.677144"), "Latitude": Decimal("3.180597"), "google_place_id": "ChIJE0B8-oVIzDERWJuvN3g8Doo", "embedsocial_source_id": "e560bb522a4134338086e1ea622f6db5"},
    {"SiteID": 10777, "SiteCode": "L010", "Name": "Extra Space S51A Sdn Bhd", "InternalLabel": "S51A", "Country": "Malaysia", "CityDistrict": "Sunway, Selangor", "Street": "M-G, Sunway PJ@51A", "PostalCode": "47300", "PrimaryEmail": "section51a@extraspaceasia.com", "Longitude": Decimal("101.6244002"), "Latitude": Decimal("3.0868581"), "google_place_id": "ChIJbdGA9o5LzDERK-OpkIeQwAc", "embedsocial_source_id": "c180c97c007d3070ad5d591d501b421e"},
    {"SiteID": 24411, "SiteCode": "L011", "Name": "Extra Space - Bundang", "InternalLabel": "ESKB", "Country": "South Korea", "CityDistrict": "Bundang-gu, Seongnam", "Street": "24 Yatap-ro, Bundang-gu", "PostalCode": None, "PrimaryEmail": "bundang@extraspaceasia.com", "Longitude": Decimal("127.121617"), "Latitude": Decimal("37.4085396"), "google_place_id": "ChIJVdM-Q-GnfDURaEfnAQfdQys", "embedsocial_source_id": "fddb2708a890320b982bafec8901aca9"},
    {"SiteID": 25675, "SiteCode": "L013", "Name": "Extra Space - Gasan", "InternalLabel": "ESKG", "Country": "South Korea", "CityDistrict": "Geumcheon-gu, Seoul", "Street": "02, 648, Seobusaet-gil, Geumcheon-gu", "PostalCode": None, "PrimaryEmail": "gasan@extraspaceasia.com", "Longitude": Decimal("126.8760629"), "Latitude": Decimal("37.4812005"), "google_place_id": "ChIJ6SF3qwKefDURVIS3MEhTEYk", "embedsocial_source_id": "f69c631e74853959ad57af2268852ac0"},
    {"SiteID": 26710, "SiteCode": "L015", "Name": "Extra Space Island (HK) Limited", "InternalLabel": "SW", "Country": "Hong Kong", "CityDistrict": "Kwai Chung", "Street": "11/F, Hong Kong Industrial Building No. 444-452 Des Voeux Road West", "PostalCode": None, "PrimaryEmail": None, "Longitude": Decimal("114.1332813"), "Latitude": Decimal("22.2870237"), "google_place_id": "ChIJJeswXoP_AzQRTsmG9C_Iuto", "embedsocial_source_id": "b35bbadd1a6437db906af9a6f1547a33"},
    {"SiteID": 27903, "SiteCode": "L017", "Name": "Extra Space Woodlands", "InternalLabel": "WDL", "Country": "Singapore", "CityDistrict": "Woodlands", "Street": "No. 11 Woodlands Close #03-47", "PostalCode": "737853", "PrimaryEmail": "woodlands@extraspaceasia.com", "Longitude": Decimal("103.8026989"), "Latitude": Decimal("1.4346244"), "google_place_id": "ChIJdzi-Em8T2jERGYYLqb-wnTM", "embedsocial_source_id": "0d67c1cec3b33017af1ce5a83e54bd4b"},
    {"SiteID": 29197, "SiteCode": "L018", "Name": "Extra Space AMK Pte Ltd", "InternalLabel": "AMK", "Country": "Singapore", "CityDistrict": "Ang Mo Kio", "Street": "14 Ang Mo Kio Industrial Park 2", "PostalCode": "569503", "PrimaryEmail": "amk@extraspaceasia.com", "Longitude": Decimal("103.8642441"), "Latitude": Decimal("1.372534"), "google_place_id": "ChIJjVgox_kW2jERonQJTi_Nhqw", "embedsocial_source_id": "9f09368e3a6c3127bb2c6e4d87b5e89b"},
    {"SiteID": 29064, "SiteCode": "L019", "Name": "Extra Space - Apgujeong", "InternalLabel": "ESKA", "Country": "South Korea", "CityDistrict": "Gangnam-gu, Seoul", "Street": "B1, 823 Seolleung-ro Gangnam-gu", "PostalCode": None, "PrimaryEmail": "apgujeong@extraspaceasia.com", "Longitude": Decimal("127.0392909"), "Latitude": Decimal("37.5256089"), "google_place_id": "ChIJKUOpkWajfDURioedmBWZWjQ", "embedsocial_source_id": "f6cf0636865b3178a4f254ccfe8d62a4"},
    {"SiteID": 32663, "SiteCode": "L020", "Name": "Extra Space Hung Hom", "InternalLabel": "HH", "Country": "Hong Kong", "CityDistrict": "Hung Hom", "Street": "Heng Ngai Jewelry Centre 4 Hok Yuen Street", "PostalCode": None, "PrimaryEmail": None, "Longitude": Decimal("114.190845"), "Latitude": Decimal("22.3102551"), "google_place_id": "ChIJkUwZW98ABDQRh4WC2Pg01dY", "embedsocial_source_id": "c57d71261af832e0b7095ba7dbff40f5"},
    {"SiteID": 33881, "SiteCode": "L021", "Name": "Extra Space - Yeongdeungpo", "InternalLabel": "ESKYP", "Country": "South Korea", "CityDistrict": "Yeongdeungpo-gu, Seoul", "Street": "B1 #05,06,07 Acro Tower Square 13-dong", "PostalCode": None, "PrimaryEmail": "yeongdeungpo@extraspaceasia.com", "Longitude": Decimal("126.9072522"), "Latitude": Decimal("37.523105"), "google_place_id": "ChIJD0jwDLWffDUR5op02XQDbyc", "embedsocial_source_id": "77d5e93a9d5031668ca4f22f5f3631a8"},
    {"SiteID": 38782, "SiteCode": "L022", "Name": "Extra Space Toa Payoh", "InternalLabel": "TPY", "Country": "Singapore", "CityDistrict": "Toa Payoh", "Street": "11 Lor 3 Toa Payoh, Block D #01-42, Jackson Square", "PostalCode": "319579", "PrimaryEmail": "toapayoh@extraspaceasia.com", "Longitude": Decimal("103.8487515"), "Latitude": Decimal("1.3370568"), "google_place_id": "ChIJ6cLM5GgX2jERJqdhT35HHQg", "embedsocial_source_id": "e2d208a7da8e327094d00f3249ed6284"},
    {"SiteID": 39284, "SiteCode": "L023", "Name": "Extra Space - Yongsan", "InternalLabel": "ESKYS", "Country": "South Korea", "CityDistrict": "Yongsan-gu, Seoul", "Street": "16 Hyochangwon-ro 62 gil", "PostalCode": None, "PrimaryEmail": "yongsan@extraspaceasia.com", "Longitude": Decimal("126.9625896"), "Latitude": Decimal("37.5397603"), "google_place_id": "ChIJZc28iwejfDURzoaHsvYXQHM", "embedsocial_source_id": "3560e2d4cf48339f810f7add6fb58d65"},
    {"SiteID": 40100, "SiteCode": "L024", "Name": "Extra Space - Banpo", "InternalLabel": "ESKBP", "Country": "South Korea", "CityDistrict": "Seocho-gu, Seoul", "Street": "209, Seochojungang-ro, Seocho-gu", "PostalCode": None, "PrimaryEmail": "banpo@extraspaceasia.com", "Longitude": Decimal("127.0117761"), "Latitude": Decimal("37.5003122"), "google_place_id": "ChIJCaeZ43ehfDUR9tAUwGbW0so", "embedsocial_source_id": "942bc8f3b60c305188c57b11714fdfe0"},
    {"SiteID": 43344, "SiteCode": "L025", "Name": "Extra Space Hillview Pte Ltd", "InternalLabel": "HV", "Country": "Singapore", "CityDistrict": "Hillview", "Street": "63 Hillview Ave #04-17", "PostalCode": "669569", "PrimaryEmail": "hillview@extraspaceasia.com", "Longitude": Decimal("103.763132"), "Latitude": Decimal("1.3570307"), "google_place_id": "ChIJt1ogGXkR2jER6jc7vtcP_tY", "embedsocial_source_id": "ba12e225f55d324a986f379330fa7ba1"},
    {"SiteID": 44449, "SiteCode": "L026", "Name": "Extra Space Kota Damansara Sdn Bhd", "InternalLabel": "KD", "Country": "Malaysia", "CityDistrict": "Kota Damansara, Selangor", "Street": "R-03A-03, Emporis, Persiaran Surian", "PostalCode": "47810", "PrimaryEmail": "kotadamansara@extraspaceasia.com", "Longitude": Decimal("101.5714338"), "Latitude": Decimal("3.1564258"), "google_place_id": "ChIJb-8hWyxPzDERCcgDJettSEY", "embedsocial_source_id": "8202096c95923ffbaff40326755ee324"},
    {"SiteID": 52421, "SiteCode": "L028", "Name": "Extra Space Commonwealth Pte Ltd", "InternalLabel": "CW", "Country": "Singapore", "CityDistrict": "Commonwealth", "Street": "115A & 115B Commonwealth Drive", "PostalCode": "149596", "PrimaryEmail": None, "Longitude": Decimal("103.7981889"), "Latitude": Decimal("1.304547"), "google_place_id": "ChIJWxA0eucb2jERa-gjIbx-Nv4", "embedsocial_source_id": "40cf59fd1e2831d0a5ee06ef4eafd5bf"},
    {"SiteID": 54219, "SiteCode": "L029", "Name": "Extra Space Commonwealth Pte Ltd", "InternalLabel": "CW", "Country": "Singapore", "CityDistrict": "Commonwealth", "Street": "115A & 115B Commonwealth Drive", "PostalCode": "149596", "PrimaryEmail": "commonwealth@extraspaceasia.com", "Longitude": Decimal("103.7981889"), "Latitude": Decimal("1.304547"), "google_place_id": None, "embedsocial_source_id": None},
    {"SiteID": 57451, "SiteCode": "L030", "Name": "Extra Space Tai Seng Pte Ltd", "InternalLabel": "TS", "Country": "Singapore", "CityDistrict": "Tai Seng", "Street": "14 Little Road", "PostalCode": "536987", "PrimaryEmail": "taiseng@extraspaceasia.com", "Longitude": Decimal("103.8847211"), "Latitude": Decimal("1.3384956"), "google_place_id": "ChIJqyALTVYX2jERxmybLU6Edws", "embedsocial_source_id": "49abc3e1141c3dba8f56eddacf87129a"},
    {"SiteID": 27525, "SiteCode": "LSETUP", "Name": "Extra Space Setup/Test Site", "InternalLabel": "SETUP", "Country": "Singapore", "CityDistrict": None, "Street": None, "PostalCode": None, "PrimaryEmail": None, "Longitude": None, "Latitude": None, "google_place_id": None, "embedsocial_source_id": None},
]


def main():
    """Create and populate siteinfo table."""

    # Load configuration
    config = DataLayerConfig.from_env()

    # Get PostgreSQL configuration
    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    # Create engine
    engine = create_engine_from_config(db_config)

    # Ensure table exists (first run), then truncate + re-insert atomically.
    # Using TRUNCATE instead of DROP CASCADE preserves dependent views
    # (vw_units_inventory, vw_reviews, etc.).
    Base.metadata.create_all(engine, tables=[SiteInfo.__table__])

    session_manager = SessionManager(engine)

    with session_manager.session_scope() as session:
        session.execute(text("TRUNCATE TABLE siteinfo"))
        for site in SITE_DATA:
            session.add(SiteInfo(**site))
        print(f"  ✓ Inserted {len(SITE_DATA)} site records")

    print("\nSiteInfo table populated successfully!")


if __name__ == "__main__":
    main()
