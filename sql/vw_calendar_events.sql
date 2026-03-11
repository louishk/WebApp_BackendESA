-- View: vw_calendar_events
-- Customer-facing calendar events only: bookings, viewings, move-ins, move-outs,
-- reservations, tenant appointments, and storage unit appointments.
-- Excludes: holidays, staff schedules, admin tasks, training, cleaning, maintenance, etc.
-- Times converted to local timezone per country (SG/MY = UTC+8, KR = UTC+9).

CREATE OR REPLACE VIEW vw_calendar_events AS
SELECT
    id,
    event_id,
    mailbox,
    country,
    subject,
    categories,
    organizer,
    -- Convert to local time
    start_time AT TIME ZONE local_tz AS start_time,
    end_time AT TIME ZONE local_tz AS end_time,
    (start_time AT TIME ZONE local_tz)::time AS time_of_day,
    make_time(EXTRACT(HOUR FROM start_time AT TIME ZONE local_tz)::int, 0, 0) AS hour_bucket,
    CASE
        WHEN is_all_day THEN NULL
        ELSE EXTRACT(EPOCH FROM (end_time - start_time)) / 60
    END AS duration_minutes,
    location,
    is_all_day,
    is_cancelled,
    response_status,
    sensitivity,
    show_as,
    attendees_count,
    is_recurring,
    body_preview,
    web_link,
    created_at,
    updated_at,
    synced_at,
    event_type
FROM (
    SELECT
        *,
        -- Derive country from mailbox
        CASE
            WHEN mailbox IN ('section51a@extraspaceasia.com','chansowlin@extraspaceasia.com',
                             'segambut@extraspaceasia.com','kotadamansara@extraspaceasia.com')
                THEN 'MY'
            WHEN mailbox IN ('yangjae@extraspaceasia.com','bundang@extraspaceasia.com',
                             'apgujeong@extraspaceasia.com','gasan@extraspaceasia.com',
                             'yeongdeungpo@extraspaceasia.com','yongsan@extraspaceasia.com',
                             'banpo@extraspaceasia.com')
                THEN 'KR'
            ELSE 'SG'
        END AS country,
        -- Local timezone per country
        CASE
            WHEN mailbox IN ('yangjae@extraspaceasia.com','bundang@extraspaceasia.com',
                             'apgujeong@extraspaceasia.com','gasan@extraspaceasia.com',
                             'yeongdeungpo@extraspaceasia.com','yongsan@extraspaceasia.com',
                             'banpo@extraspaceasia.com')
                THEN 'Asia/Seoul'
            ELSE 'Asia/Singapore'  -- SG and MY are both UTC+8
        END AS local_tz,
        -- Event type classification
        CASE
            WHEN subject ILIKE '[BOOKING]%'
              OR subject ILIKE '[SIGN UP]%' THEN 'booking'
            WHEN subject ILIKE '%viewing%'
              OR subject ILIKE '%방문%'
              OR subject ILIKE '%SU appt%' OR subject ILIKE '% SU %'
              OR subject ~ '^[0-9]{5,}'
              THEN 'visit'
            WHEN subject ILIKE '%move in%' OR subject ILIKE '%move-in%'
              OR subject ILIKE '%movein%' THEN 'move_in'
            WHEN subject ILIKE '%move out%' OR subject ILIKE '%move-out%'
              OR subject ILIKE '%moveout%' THEN 'move_out'
            WHEN subject ILIKE '%rsvn%'
              OR subject ILIKE '%reservation%' THEN 'reservation'
            WHEN subject ILIKE '%cancel%' THEN 'cancellation'
            ELSE NULL
        END AS event_type
    FROM calendar_events
    WHERE
        -- Exclude public holidays
        NOT (
            COALESCE(categories, '') ILIKE '%Holiday%'
            OR COALESCE(categories, '') ILIKE '%법정공휴일%'
            OR subject IN (
                '설날', '추석', '크리스마스', '석가탄신일 대체휴일', '근로자의 날',
                '개천절 대체휴일', '광복절 대체휴일', '삼일절 대체휴무',
                '제헌절', '한글날', '현충일', '어린이날'
            )
        )
) classified
WHERE event_type IS NOT NULL;
