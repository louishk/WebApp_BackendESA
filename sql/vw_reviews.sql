CREATE OR REPLACE VIEW vw_reviews AS
SELECT
    s."SiteCode"                       AS site_code,
    s."InternalLabel"                  AS internal_label,
    s."Country"                        AS country,
    d::date                            AS date,
    COUNT(*) FILTER (WHERE e.type = 'review')   AS review_count,
    COUNT(*) FILTER (WHERE e.type = 'reply')    AS reply_count,
    ROUND(AVG(e.rating) FILTER (WHERE e.type = 'review'), 2) AS avg_rating,
    COUNT(*) FILTER (WHERE e.type = 'review' AND e.rating = 1) AS rating_1,
    COUNT(*) FILTER (WHERE e.type = 'review' AND e.rating = 2) AS rating_2,
    COUNT(*) FILTER (WHERE e.type = 'review' AND e.rating = 3) AS rating_3,
    COUNT(*) FILTER (WHERE e.type = 'review' AND e.rating = 4) AS rating_4,
    COUNT(*) FILTER (WHERE e.type = 'review' AND e.rating = 5) AS rating_5
FROM siteinfo s
JOIN (
    -- Reviews: one row per review on its creation date
    SELECT
        source_id,
        original_created_on::date AS d,
        rating,
        'review' AS type
    FROM embedsocial_reviews

    UNION ALL

    -- Replies: one row per reply on its reply date
    SELECT
        source_id,
        reply_created_on::date AS d,
        NULL AS rating,
        'reply' AS type
    FROM embedsocial_reviews
    WHERE reply_created_on IS NOT NULL
) e ON s.embedsocial_source_id = e.source_id
GROUP BY s."SiteCode", s."InternalLabel", s."Country", d
ORDER BY d DESC, s."SiteCode";
