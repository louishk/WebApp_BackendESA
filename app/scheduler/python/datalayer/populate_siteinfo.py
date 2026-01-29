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
    {"SiteID": 48, "SiteCode": "L001", "Name": "Extra Space Jurong Pte Ltd", "InternalLabel": "IMM", "Country": "Singapore", "CityDistrict": "Jurong East", "Street": "2 Jurong East Street 21, Unit #02-71 IMM Building", "Longitude": Decimal("103.7466569"), "Latitude": Decimal("1.3355903")},
    {"SiteID": 49, "SiteCode": "L002", "Name": "Extra Space Pte Ltd", "InternalLabel": "BKR", "Country": "Singapore", "CityDistrict": "Boon Keng", "Street": "301 Boon Keng Road", "Longitude": Decimal("103.8651647"), "Latitude": Decimal("1.3153397")},
    {"SiteID": 63, "SiteCode": "L003", "Name": "Extra Space Eunos Link", "InternalLabel": "ELK", "Country": "Singapore", "CityDistrict": "Kaki Bukit", "Street": "7 Kaki Bukit Road 2", "Longitude": Decimal("103.8977703"), "Latitude": Decimal("1.3371325")},
    {"SiteID": 26486, "SiteCode": "L004", "Name": "Extra Space West Coast Pte Ltd", "InternalLabel": "WCT", "Country": "Singapore", "CityDistrict": "Toh Tuck", "Street": "2 Toh Tuck Link, #01-03 & #04-01", "Longitude": Decimal("103.758696"), "Latitude": Decimal("1.3314914")},
    {"SiteID": 1910, "SiteCode": "L005", "Name": "Extra Space Marymount", "InternalLabel": "MMR", "Country": "Singapore", "CityDistrict": "Sin Ming", "Street": "9 Sin Ming Industrial Estate Sector B", "Longitude": Decimal("103.8409345"), "Latitude": Decimal("1.3574508")},
    {"SiteID": 2276, "SiteCode": "L006", "Name": "Extra Space - Yangjae", "InternalLabel": "ESKY", "Country": "South Korea", "CityDistrict": "Seocho-gu, Seoul", "Street": "B2, 16 Maehun-ro Seocho-gu", "Longitude": Decimal("127.0312101"), "Latitude": Decimal("37.4732933")},
    {"SiteID": 4183, "SiteCode": "L007", "Name": "Extra Space Malaysia Sdn Bhd", "InternalLabel": "CSL", "Country": "Malaysia", "CityDistrict": "Chan Sow Lin, Kuala Lumpur", "Street": "Lot 271, Jalan Dua, off Jalan Chan Sow Lin", "Longitude": Decimal("101.7126301"), "Latitude": Decimal("3.1227428")},
    {"SiteID": 9415, "SiteCode": "L008", "Name": "Extra Space Kallang Way", "InternalLabel": "KWY", "Country": "Singapore", "CityDistrict": "Kallang", "Street": "12 Kallang Sector", "Longitude": Decimal("103.8744395"), "Latitude": Decimal("1.323517")},
    {"SiteID": 10419, "SiteCode": "L009", "Name": "Extra Space Segambut Sdn Bhd", "InternalLabel": "SEG", "Country": "Malaysia", "CityDistrict": "Segambut, Kuala Lumpur", "Street": "No. 144, Jalan Batu Estate, Segambut", "Longitude": Decimal("101.677144"), "Latitude": Decimal("3.180597")},
    {"SiteID": 10777, "SiteCode": "L010", "Name": "Extra Space S51A Sdn Bhd", "InternalLabel": "S51A", "Country": "Malaysia", "CityDistrict": "Sunway, Selangor", "Street": "L-G, Sunway PJ@51A", "Longitude": Decimal("101.6244002"), "Latitude": Decimal("3.0868581")},
    {"SiteID": 24411, "SiteCode": "L011", "Name": "Extra Space - Bundang", "InternalLabel": "ESKB", "Country": "South Korea", "CityDistrict": "Bundang-gu, Seongnam", "Street": "24 Yatap-ro, Bundang-gu", "Longitude": Decimal("127.121617"), "Latitude": Decimal("37.4085396")},
    {"SiteID": 25675, "SiteCode": "L013", "Name": "Extra Space - Gasan", "InternalLabel": "ESKG", "Country": "South Korea", "CityDistrict": "Geumcheon-gu, Seoul", "Street": "02, 648, Seobusaet-gil, Geumcheon-gu", "Longitude": Decimal("126.8760629"), "Latitude": Decimal("37.4812005")},
    {"SiteID": 26710, "SiteCode": "L015", "Name": "Extra Space Island (HK) Limited", "InternalLabel": "SW", "Country": "Hong Kong", "CityDistrict": "Kwai Chung", "Street": "11/F, Hong Kong Industrial Building No. 444-452 Des Voeux Road West", "Longitude": Decimal("114.1332813"), "Latitude": Decimal("22.2870237")},
    {"SiteID": 27903, "SiteCode": "L017", "Name": "Extra Space Woodlands", "InternalLabel": "WDL", "Country": "Singapore", "CityDistrict": "Woodlands", "Street": "No. 11 Woodlands Close #03-47", "Longitude": Decimal("103.8026989"), "Latitude": Decimal("1.4346244")},
    {"SiteID": 29197, "SiteCode": "L018", "Name": "Extra Space AMK Pte Ltd", "InternalLabel": "AMK", "Country": "Singapore", "CityDistrict": "Ang Mo Kio", "Street": "14 Ang Mo Kio Industrial Park 2", "Longitude": Decimal("103.8642441"), "Latitude": Decimal("1.372534")},
    {"SiteID": 29064, "SiteCode": "L019", "Name": "Extra Space - Apgujeong", "InternalLabel": "ESKA", "Country": "South Korea", "CityDistrict": "Gangnam-gu, Seoul", "Street": "B1, 823 Seolleung-ro Gangnam-gu", "Longitude": Decimal("127.0392909"), "Latitude": Decimal("37.5256089")},
    {"SiteID": 32663, "SiteCode": "L020", "Name": "Extra Space Hung Hom", "InternalLabel": "HH", "Country": "Hong Kong", "CityDistrict": "Hung Hom", "Street": "Heng Ngai Jewelry Centre 4 Hok Yuen Street", "Longitude": Decimal("114.190845"), "Latitude": Decimal("22.3102551")},
    {"SiteID": 33881, "SiteCode": "L021", "Name": "Extra Space - Yeongdeungpo", "InternalLabel": "ESKYP", "Country": "South Korea", "CityDistrict": "Yeongdeungpo-gu, Seoul", "Street": "B1 #05,06,07 Acro Tower Square 13-dong", "Longitude": Decimal("126.9072522"), "Latitude": Decimal("37.523105")},
    {"SiteID": 38782, "SiteCode": "L022", "Name": "Extra Space Toa Payoh", "InternalLabel": "TPY", "Country": "Singapore", "CityDistrict": "Toa Payoh", "Street": "11 Lor 3 Toa Payoh, Block D #01-42, Jackson Square", "Longitude": Decimal("103.8487515"), "Latitude": Decimal("1.3370568")},
    {"SiteID": 39284, "SiteCode": "L023", "Name": "Extra Space - Yongsan", "InternalLabel": "ESKYS", "Country": "South Korea", "CityDistrict": "Yongsan-gu, Seoul", "Street": "16 Hyochangwon-ro 62 gil", "Longitude": Decimal("126.9625896"), "Latitude": Decimal("37.5397603")},
    {"SiteID": 40100, "SiteCode": "L024", "Name": "Extra Space - Banpo", "InternalLabel": "ESKBP", "Country": "South Korea", "CityDistrict": "Seocho-gu, Seoul", "Street": "209, Seochojungang-ro, Seocho-gu", "Longitude": Decimal("127.0117761"), "Latitude": Decimal("37.5003122")},
    {"SiteID": 43344, "SiteCode": "L025", "Name": "Extra Space Hillview Pte Ltd", "InternalLabel": "HV", "Country": "Singapore", "CityDistrict": "Hillview", "Street": "63 Hillview Ave #04-17", "Longitude": Decimal("103.763132"), "Latitude": Decimal("1.3570307")},
    {"SiteID": 44449, "SiteCode": "L026", "Name": "Extra Space Kota Damansara Sdn Bhd", "InternalLabel": "KD", "Country": "Malaysia", "CityDistrict": "Kota Damansara, Selangor", "Street": "R-03A-03, Emporis, Persiaran Surian", "Longitude": Decimal("101.5714338"), "Latitude": Decimal("3.1564258")},
    {"SiteID": 52421, "SiteCode": "L028", "Name": "Extra Space Commonwealth Pte Ltd", "InternalLabel": "CW", "Country": "Singapore", "CityDistrict": "Commonwealth", "Street": "115A & 115B Commonwealth Drive", "Longitude": Decimal("103.7981889"), "Latitude": Decimal("1.304547")},
    {"SiteID": 54219, "SiteCode": "L029", "Name": "Extra Space Commonwealth Pte Ltd", "InternalLabel": "CW", "Country": "Singapore", "CityDistrict": "Commonwealth", "Street": "115A & 115B Commonwealth Drive", "Longitude": Decimal("103.7981889"), "Latitude": Decimal("1.304547")},
    {"SiteID": 57451, "SiteCode": "L030", "Name": "Extra Space Tai Seng Pte Ltd", "InternalLabel": "TS", "Country": "Singapore", "CityDistrict": "Tai Seng", "Street": "14 Little Road", "Longitude": Decimal("103.8847211"), "Latitude": Decimal("1.3384956")},
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

    # Drop and recreate table to apply schema changes
    # Use CASCADE to drop dependent views (they will be recreated by their own scripts)
    print("Recreating siteinfo table...")
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS siteinfo CASCADE"))
        conn.commit()
    Base.metadata.create_all(engine, tables=[SiteInfo.__table__])
    print("  ✓ Table 'siteinfo' ready")

    # Initialize session
    session_manager = SessionManager(engine)

    with session_manager.session_scope() as session:
        # Insert site data
        for site in SITE_DATA:
            site_record = SiteInfo(**site)
            session.add(site_record)

        print(f"  ✓ Inserted {len(SITE_DATA)} site records")

    print("\nSiteInfo table populated successfully!")


if __name__ == "__main__":
    main()
