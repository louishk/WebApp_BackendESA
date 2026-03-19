-- View: vw_units_inventory
-- Classifies each unit with physical attributes (pillar, odd shape, deck, cases)
-- parsed from sUnitDesc/sUnitNote, plus climate/NOKE info from unit_range_mappings.
--
-- Uses DISTINCT ON to resolve overlapping ranges: prefers the narrowest
-- matching range, then suffix-specific over generic.
-- Target database: esa_pbi

CREATE OR REPLACE VIEW vw_units_inventory AS
WITH unit_parsed AS (
    SELECT
        u.*,
        -- Extract letter prefix from unit name (everything before the first digit)
        regexp_replace("sUnitName", '[^A-Za-z].*', '') AS unit_prefix,
        -- Extract first contiguous digits
        (regexp_match("sUnitName", '(\d+)'))[1]::integer AS unit_num,
        -- Extract trailing letters after digits as suffix
        COALESCE(upper((regexp_match("sUnitName", '\d+([A-Za-z]+)$'))[1]), '') AS unit_suffix,
        -- Combined text for pattern matching
        lower(coalesce("sUnitDesc",'') || ' ' || coalesce("sUnitNote",'')) AS combined_text
    FROM units_info u
)
SELECT DISTINCT ON (up."UnitID")
    s."SiteCode"           AS site_code,
    s."InternalLabel"      AS internal_label,
    s."Country"            AS country,
    up."SiteID"            AS site_id,
    up."UnitID"            AS unit_id,
    up."sUnitName"         AS unit_name,
    up."sTypeName"         AS type_name,
    up."dcWidth"           AS width,
    up."dcLength"          AS length,
    up."iFloor"            AS floor,
    up."bClimate"          AS is_climate_controlled,
    up."bRentable"         AS is_rentable,
    up."bRented"           AS is_rented,
    up."dcStdRate"         AS std_rate,

    -- Physical attributes from text
    (up.combined_text ~ '(pillar|pililar|column)')          AS has_pillar,
    CASE
        WHEN up.combined_text ~ '(big|full)\s*(pillar|column)' THEN 'Big'
        WHEN up.combined_text ~ 'small\s*(pillar|column)'      THEN 'Small'
        WHEN up.combined_text ~ '(pillar|pililar|column)'      THEN 'Standard'
    END                                                       AS pillar_size,
    (up.combined_text ~ '(odd|irregular|chamfer)')            AS is_odd_shape,
    CASE
        WHEN up.combined_text ~ 'upper'  THEN 'Upper'
        WHEN up.combined_text ~ 'lower'  THEN 'Lower'
        WHEN up.combined_text ~ 'middle' THEN 'Middle'
        WHEN up.combined_text ~ 'single' THEN 'Single'
        WHEN up.combined_text ~ '1st'    THEN '1st'
        WHEN up.combined_text ~ '2nd'    THEN '2nd'
        WHEN up.combined_text ~ '3rd'    THEN '3rd'
        WHEN up.combined_text ~ '4th'    THEN '4th'
    END                                                       AS deck_position,
    (regexp_match(lower(up."sUnitDesc"), '(\d+)\s*cases?'))[1]::integer AS case_count,

    -- Climate/NOKE from Excel mapping
    rm.climate_type,
    rm.has_dehumidifier,
    rm.noke_status,
    rm.storage_type,

    -- Raw fields for reference
    up."sUnitDesc"         AS unit_desc,
    up."sUnitNote"         AS unit_note,

    -- Published category labels from Inventory Checker
    cl.size_category       AS label_size_category,
    cl.size_range          AS label_size_range,
    cl.unit_type_code      AS label_type_code,
    cl.climate_code        AS label_climate_code,
    cl.shape               AS label_shape,
    cl.pillar              AS label_pillar,
    cl.final_label         AS category_label,
    cl.published_at        AS label_published_at,

    up.deleted_at

FROM unit_parsed up
JOIN siteinfo s ON s."SiteID" = up."SiteID"
LEFT JOIN unit_category_labels cl
    ON cl.site_id = up."SiteID"
    AND cl.unit_id = up."UnitID"
LEFT JOIN unit_range_mappings rm
    ON rm.site_code = s."SiteCode"
    AND rm.unit_prefix = up.unit_prefix
    AND up.unit_num BETWEEN rm.range_start AND rm.range_end
    AND (
        rm.suffix_start IS NULL
        OR (
            up.unit_suffix >= rm.suffix_start
            AND up.unit_suffix <= COALESCE(rm.suffix_end, rm.suffix_start)
        )
    )
ORDER BY
    up."UnitID",
    CASE WHEN rm.id IS NULL THEN 1 ELSE 0 END,           -- matched first
    (rm.range_end - rm.range_start),                       -- narrower range first
    CASE WHEN rm.suffix_start IS NOT NULL THEN 0 ELSE 1 END;  -- suffix-specific first
