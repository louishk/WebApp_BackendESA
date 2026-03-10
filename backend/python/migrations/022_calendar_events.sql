-- Migration 022: Calendar Events table
-- Target database: esa_pbi
-- Stores Outlook calendar events extracted via Microsoft Graph API

CREATE TABLE IF NOT EXISTS calendar_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(512) NOT NULL,
    mailbox VARCHAR(255) NOT NULL,
    subject TEXT,
    organizer VARCHAR(255),
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    location TEXT,
    is_all_day BOOLEAN DEFAULT FALSE,
    is_cancelled BOOLEAN DEFAULT FALSE,
    response_status VARCHAR(50),
    sensitivity VARCHAR(50),
    show_as VARCHAR(50),
    categories TEXT,
    attendees_count INTEGER DEFAULT 0,
    is_recurring BOOLEAN DEFAULT FALSE,
    body_preview TEXT,
    web_link TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    synced_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(event_id, mailbox)
);

CREATE INDEX IF NOT EXISTS idx_cal_events_mailbox ON calendar_events(mailbox);
CREATE INDEX IF NOT EXISTS idx_cal_events_start ON calendar_events(start_time);
