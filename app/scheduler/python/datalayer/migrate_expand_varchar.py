"""
Migration: Expand VARCHAR(50) columns to VARCHAR(100) for Tenant table.

This fixes the "value too long for type character varying(50)" error.

Run from Scripts directory:
    python -m datalayer.migrate_expand_varchar
"""

from sqlalchemy import create_engine, text
from decouple import config as env_config

# Import vault-aware config for sensitive values
try:
    from common.secrets_vault import vault_config as secure_config
except ImportError:
    secure_config = env_config


def get_postgres_url() -> str:
    """Build PostgreSQL connection URL."""
    host = env_config('POSTGRESQL_HOST')
    port = env_config('POSTGRESQL_PORT', default='5432')
    database = env_config('POSTGRESQL_DATABASE')
    user = env_config('POSTGRESQL_USERNAME')
    password = secure_config('POSTGRESQL_PASSWORD')  # From vault
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


# Columns to expand - use TEXT for potentially long fields
TENANT_COLUMNS_TO_TEXT = [
    'sAccessCode', 'sAccessCode2', 'sWebPassword',
    'sFName', 'sLName', 'sCompany',
    'sAddr1', 'sAddr2', 'sCity', 'sRegion', 'sCountry',
    'sPhone', 'sFax', 'sEmail', 'sPager', 'sMobile',
    'sFNameAlt', 'sLNameAlt', 'sAddr1Alt', 'sAddr2Alt', 'sCityAlt', 'sRegionAlt', 'sCountryAlt',
    'sPhoneAlt', 'sEmailAlt', 'sRelationshipAlt',
    'sEmployer', 'sFNameBus', 'sLNameBus', 'sCompanyBus',
    'sAddr1Bus', 'sAddr2Bus', 'sCityBus', 'sRegionBus', 'sCountryBus',
    'sPhoneBus', 'sEmailBus',
    'sFNameAdd', 'sLNameAdd', 'sAddr1Add', 'sAddr2Add', 'sCityAdd', 'sRegionAdd', 'sCountryAdd',
    'sPhoneAdd', 'sEmailAdd',
    'sLicense', 'sLicRegion', 'sSSN', 'sTaxID', 'sTaxExemptCode',
    'sWebSecurityQ', 'sWebSecurityQA',
    'sIconList',
    'sPicFileN1', 'sPicFileN2', 'sPicFileN3', 'sPicFileN4', 'sPicFileN5',
    'sPicFileN6', 'sPicFileN7', 'sPicFileN8', 'sPicFileN9',
]

LEDGER_COLUMNS_TO_TEXT = [
    'sLicPlate', 'sVehicleDesc', 'sReasonComplimentary', 'sCompanySub',
    'sPurchOrderCode',
    'sCreditCardNum', 'sCreditCardHolderName', 'sCreditCardCVV2',
    'sCreditCardStreet', 'sCreditCardZip',
    'sACH_CheckWriterAcctNum', 'sACH_CheckWriterAcctName',
    'sACH_ABA_RoutingNum', 'sACH_RDFI', 'sACH_Check_SavingsCode',
]

CHARGE_COLUMNS_TO_TEXT = [
    'sChgCategory', 'sChgDesc', 'sDefChgDesc',
]


def run_migration():
    """Run the migration to expand VARCHAR columns to TEXT."""
    engine = create_engine(get_postgres_url())

    print("=" * 70)
    print("Migration: Expand VARCHAR columns to TEXT")
    print("=" * 70)

    with engine.connect() as conn:
        # Expand tenant columns
        print("\n[1/3] Expanding tenant columns to TEXT...")
        for col in TENANT_COLUMNS_TO_TEXT:
            try:
                sql = f'ALTER TABLE tenants ALTER COLUMN "{col}" TYPE TEXT;'
                conn.execute(text(sql))
                print(f"  {col}: -> TEXT")
            except Exception as e:
                if "does not exist" in str(e):
                    print(f"  {col}: Column doesn't exist, skipping")
                else:
                    print(f"  {col}: ERROR - {e}")

        # Expand ledger columns
        print("\n[2/3] Expanding ledger columns to TEXT...")
        for col in LEDGER_COLUMNS_TO_TEXT:
            try:
                sql = f'ALTER TABLE ledgers ALTER COLUMN "{col}" TYPE TEXT;'
                conn.execute(text(sql))
                print(f"  {col}: -> TEXT")
            except Exception as e:
                if "does not exist" in str(e):
                    print(f"  {col}: Column doesn't exist, skipping")
                else:
                    print(f"  {col}: ERROR - {e}")

        # Expand charge columns
        print("\n[3/3] Expanding charge columns to TEXT...")
        for col in CHARGE_COLUMNS_TO_TEXT:
            try:
                sql = f'ALTER TABLE charges ALTER COLUMN "{col}" TYPE TEXT;'
                conn.execute(text(sql))
                print(f"  {col}: -> TEXT")
            except Exception as e:
                if "does not exist" in str(e):
                    print(f"  {col}: Column doesn't exist, skipping")
                else:
                    print(f"  {col}: ERROR - {e}")

        conn.commit()

    print("\nMigration completed!")
    engine.dispose()


if __name__ == "__main__":
    run_migration()
