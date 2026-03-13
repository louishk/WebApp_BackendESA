-- 1. Remove unit_type and unit_range from mimo_enriched
DROP VIEW IF EXISTS mimo_enriched;

CREATE OR REPLACE VIEW mimo_enriched AS
 SELECT m."SiteID",
    m."TenantID",
    m."MoveDate",
    m.extract_date,
    m."MoveIn",
    m."MoveOut",
    m."Transfer",
    m."UnitName",
    m."UnitSize",
    m."Width",
    m."Length",
    m."sUnitType",
    m."TenantName",
    m."sCompany",
    m."sEmail",
    lower(TRIM(BOTH FROM m."sEmail")) AS tenant_email,
    m."Address",
    m."City",
    m."Region",
    m."PostalCode",
    m."Country",
    m."StandardRate",
    m."MovedInRentalRate",
    m."MovedInVariance",
    m."MovedInDaysVacant",
    m."MovedOutRentalRate",
    m."MovedOutVariance",
    m."MovedOutDaysRented",
    m."iLeaseNum",
    m."dRentLastChanged",
    m."sLicPlate",
    m."sEmpInitials",
    m."sPlanTerm",
    m."dcInsurPremium",
    m."dcDiscount",
    m."sDiscountPlan",
    m."iAuctioned",
    m."sAuctioned",
    m."iDaysSinceMoveOut",
    m."dcAmtPaid",
    m."sSource",
    m."bPower",
    m."bClimate",
    m."bAlarm",
    m."bInside",
    m."dcPushRateAtMoveIn",
    m."dcStdRateAtMoveIn",
    m."dcInsurPremiumAtMoveIn",
    m."sDiscountPlanAtMoveIn",
    m."WaitingID",
    m."InquiryEmployeeID",
    m."sInquiryPlacedBy",
    m."CorpUserID_Placed",
    m."CorpUserID_ConvertedToMoveIn",
    m.created_at,
    m.updated_at,
    m."MoveDate"::date AS date_fixed,
        CASE
            WHEN m."SiteID" = ANY (ARRAY[2276, 24411, 25675, 29064, 33881, 39284, 40100]) THEN m."MovedInArea" * 10.7639
            ELSE m."MovedInArea"
        END AS "MovedInArea_fixed",
        CASE
            WHEN m."SiteID" = ANY (ARRAY[2276, 24411, 25675, 29064, 33881, 39284, 40100]) THEN m."MovedOutArea" * 10.7639
            ELSE m."MovedOutArea"
        END AS "MovedOutArea_fixed",
        CASE
            WHEN m."SiteID" = ANY (ARRAY[26710, 32663]) THEN m."MovedInRentalRate" / COALESCE(fx_hkd.avg_rate, 1::numeric)
            WHEN m."SiteID" = ANY (ARRAY[4183, 10419, 10777, 44449]) THEN m."MovedInRentalRate" / COALESCE(fx_myr.avg_rate, 1::numeric)
            WHEN m."SiteID" = ANY (ARRAY[2276, 24411, 25675, 29064, 33881, 39284, 40100]) THEN m."MovedInRentalRate" / COALESCE(fx_krw.avg_rate, 1::numeric)
            ELSE m."MovedInRentalRate"
        END AS "MovedInRentalRate_SGD",
        CASE
            WHEN m."SiteID" = ANY (ARRAY[26710, 32663]) THEN m."MovedOutRentalRate" / COALESCE(fx_hkd.avg_rate, 1::numeric)
            WHEN m."SiteID" = ANY (ARRAY[4183, 10419, 10777, 44449]) THEN m."MovedOutRentalRate" / COALESCE(fx_myr.avg_rate, 1::numeric)
            WHEN m."SiteID" = ANY (ARRAY[2276, 24411, 25675, 29064, 33881, 39284, 40100]) THEN m."MovedOutRentalRate" / COALESCE(fx_krw.avg_rate, 1::numeric)
            ELSE m."MovedOutRentalRate"
        END AS "MovedOutRentalRate_SGD",
    m."MovedInArea",
    m."MovedOutArea",
    m."SiteID"::text || '_' || m."UnitName"::text AS mimo_id
   FROM mimo m
     LEFT JOIN fx_rates_monthly fx_hkd ON to_char(m."MoveDate", 'YYYY-MM') = fx_hkd.year_month::text AND fx_hkd.target_currency::text = 'HKD'
     LEFT JOIN fx_rates_monthly fx_myr ON to_char(m."MoveDate", 'YYYY-MM') = fx_myr.year_month::text AND fx_myr.target_currency::text = 'MYR'
     LEFT JOIN fx_rates_monthly fx_krw ON to_char(m."MoveDate", 'YYYY-MM') = fx_krw.year_month::text AND fx_krw.target_currency::text = 'KRW';


-- 2. Drop old unit_type table
DROP TABLE IF EXISTS unit_type;


-- 3. Create dimension tables for label fields

-- dim_size_category: S/M/L/XL
CREATE TABLE IF NOT EXISTS dim_size_category (
    code VARCHAR(5) PRIMARY KEY,
    description VARCHAR(50) NOT NULL,
    range_min_sqft NUMERIC,
    range_max_sqft NUMERIC,
    sort_order INTEGER NOT NULL
);
TRUNCATE dim_size_category;
INSERT INTO dim_size_category (code, description, range_min_sqft, range_max_sqft, sort_order) VALUES
    ('S',  'Small',   0,  30, 1),
    ('M',  'Medium', 30,  60, 2),
    ('L',  'Large',  60,  90, 3),
    ('XL', 'X-Large', 90, NULL, 4);


-- dim_size_range: 0-6, 6-8, ..., 250+
CREATE TABLE IF NOT EXISTS dim_size_range (
    range_code VARCHAR(10) PRIMARY KEY,
    description VARCHAR(50) NOT NULL,
    range_min_sqft NUMERIC NOT NULL,
    range_max_sqft NUMERIC,
    size_category VARCHAR(5) REFERENCES dim_size_category(code),
    sort_order INTEGER NOT NULL
);
TRUNCATE dim_size_range;
INSERT INTO dim_size_range (range_code, description, range_min_sqft, range_max_sqft, size_category, sort_order) VALUES
    ('0-6',     '0 to 6 sqft',     0,   6, 'S',  1),
    ('6-8',     '6 to 8 sqft',     6,   8, 'S',  2),
    ('8-10',    '8 to 10 sqft',    8,  10, 'S',  3),
    ('10-12',   '10 to 12 sqft',  10,  12, 'S',  4),
    ('12-14',   '12 to 14 sqft',  12,  14, 'S',  5),
    ('14-16',   '14 to 16 sqft',  14,  16, 'S',  6),
    ('16-18',   '16 to 18 sqft',  16,  18, 'S',  7),
    ('18-20',   '18 to 20 sqft',  18,  20, 'S',  8),
    ('20-22',   '20 to 22 sqft',  20,  22, 'S',  9),
    ('22-24',   '22 to 24 sqft',  22,  24, 'S', 10),
    ('24-26',   '24 to 26 sqft',  24,  26, 'S', 11),
    ('26-28',   '26 to 28 sqft',  26,  28, 'S', 12),
    ('28-30',   '28 to 30 sqft',  28,  30, 'S', 13),
    ('30-35',   '30 to 35 sqft',  30,  35, 'M', 14),
    ('35-40',   '35 to 40 sqft',  35,  40, 'M', 15),
    ('40-45',   '40 to 45 sqft',  40,  45, 'M', 16),
    ('45-50',   '45 to 50 sqft',  45,  50, 'M', 17),
    ('50-60',   '50 to 60 sqft',  50,  60, 'M', 18),
    ('60-70',   '60 to 70 sqft',  60,  70, 'L', 19),
    ('70-80',   '70 to 80 sqft',  70,  80, 'L', 20),
    ('80-90',   '80 to 90 sqft',  80,  90, 'L', 21),
    ('90-110',  '90 to 110 sqft', 90, 110, 'XL', 22),
    ('110-130', '110 to 130 sqft', 110, 130, 'XL', 23),
    ('130-150', '130 to 150 sqft', 130, 150, 'XL', 24),
    ('150-175', '150 to 175 sqft', 150, 175, 'XL', 25),
    ('175-200', '175 to 200 sqft', 175, 200, 'XL', 26),
    ('200-225', '200 to 225 sqft', 200, 225, 'XL', 27),
    ('225-250', '225 to 250 sqft', 225, 250, 'XL', 28),
    ('250+',    '250+ sqft',      250, NULL, 'XL', 29);


-- dim_unit_type: W, U, M, L, WN, etc.
CREATE TABLE IF NOT EXISTS dim_unit_type (
    code VARCHAR(10) PRIMARY KEY,
    description VARCHAR(50) NOT NULL,
    type_group VARCHAR(20) NOT NULL,
    sort_order INTEGER NOT NULL
);
TRUNCATE dim_unit_type;
INSERT INTO dim_unit_type (code, description, type_group, sort_order) VALUES
    ('W',    'Walk-In',                'Walk-In',  1),
    ('E',    'Executive Walk-In',      'Walk-In',  2),
    ('S',    'Smart Walk-In',          'Walk-In',  3),
    ('U',    'Locker Upper',           'Locker',   4),
    ('M',    'Locker Middle',          'Locker',   5),
    ('L',    'Locker Lower',           'Locker',   6),
    ('SU',   'Smart Locker Upper',     'Locker',   7),
    ('SM',   'Smart Locker Middle',    'Locker',   8),
    ('SL',   'Smart Locker Lower',     'Locker',   9),
    ('EU',   'Executive Locker Upper', 'Locker',  10),
    ('EM',   'Executive Locker Middle','Locker',  11),
    ('EL',   'Executive Locker Lower', 'Locker',  12),
    ('WN',   'Wine Walk-In',           'Wine',    13),
    ('WNU',  'Wine Locker Upper',      'Wine',    14),
    ('WNM',  'Wine Locker Middle',     'Wine',    15),
    ('WNL',  'Wine Locker Lower',      'Wine',    16),
    ('SWN',  'Smart Wine Walk-In',     'Wine',    17),
    ('SWNU', 'Smart Wine Locker Upper','Wine',    18),
    ('SWNM', 'Smart Wine Locker Middle','Wine',   19),
    ('SWNL', 'Smart Wine Locker Lower','Wine',    20),
    ('DV',   'Drive-Up',               'Others',  21),
    ('RB',   'Wardrobe',               'Others',  22),
    ('MB',   'Mailbox',                'Others',  23),
    ('BZ',   'BizPlus',                'Others',  24),
    ('SC',   'Showcase',               'Others',  25),
    ('SB',   'SubTenant',              'Others',  26),
    ('PR',   'Parking',                'Others',  27);


-- dim_climate_type: NC, A, D, AD, RF
CREATE TABLE IF NOT EXISTS dim_climate_type (
    code VARCHAR(5) PRIMARY KEY,
    description VARCHAR(50) NOT NULL,
    sort_order INTEGER NOT NULL
);
TRUNCATE dim_climate_type;
INSERT INTO dim_climate_type (code, description, sort_order) VALUES
    ('NC', 'No Climate Control',      1),
    ('A',  'Aircon Only',             2),
    ('D',  'Dehumidifier Only',       3),
    ('AD', 'Aircon + Dehumidifier',   4),
    ('RF', 'Refrigerated',            5);


-- dim_unit_shape: SS, WR, NR, OS
CREATE TABLE IF NOT EXISTS dim_unit_shape (
    code VARCHAR(5) PRIMARY KEY,
    description VARCHAR(50) NOT NULL,
    rule TEXT,
    sort_order INTEGER NOT NULL
);
TRUNCATE dim_unit_shape;
INSERT INTO dim_unit_shape (code, description, rule, sort_order) VALUES
    ('SS', 'Square',            'Ratio L/W ≥0.8 to ≤1.2', 1),
    ('WR', 'Wide Rectangle',    'Ratio not 0.8-1.2, both dimensions ≥1500mm', 2),
    ('NR', 'Narrow Rectangle',  'Ratio not 0.8-1.2, one dimension <1500mm', 3),
    ('OS', 'Odd Shape',         'Manual verification', 4);


-- dim_pillar: P, NP
CREATE TABLE IF NOT EXISTS dim_pillar (
    code VARCHAR(5) PRIMARY KEY,
    description VARCHAR(50) NOT NULL,
    sort_order INTEGER NOT NULL
);
TRUNCATE dim_pillar;
INSERT INTO dim_pillar (code, description, sort_order) VALUES
    ('P',  'Pillar in unit',    1),
    ('NP', 'No pillar in unit', 2);
