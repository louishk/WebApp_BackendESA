-- 047_zoom_call_logs_transcript.sql
-- Add transcript-processing columns to zoom_call_logs for self-hosted STT.
-- Target: esa_pbi

ALTER TABLE zoom_call_logs
    ADD COLUMN IF NOT EXISTS download_url        TEXT,
    ADD COLUMN IF NOT EXISTS transcript_status   VARCHAR(20) NOT NULL DEFAULT 'none',
    ADD COLUMN IF NOT EXISTS transcript_original TEXT,
    ADD COLUMN IF NOT EXISTS transcript_en       TEXT,
    ADD COLUMN IF NOT EXISTS detected_language   VARCHAR(20),
    ADD COLUMN IF NOT EXISTS transcript_model    VARCHAR(50),
    ADD COLUMN IF NOT EXISTS transcript_processed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_zoom_call_logs_transcript_status
    ON zoom_call_logs(transcript_status);

-- Backfill transcript_status: rows that already have transcripts should be 'done'
UPDATE zoom_call_logs
   SET transcript_status = 'done'
 WHERE transcript IS NOT NULL AND transcript != '' AND transcript_status = 'none';
