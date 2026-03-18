-- Visit sessions and shortlist items for walk-in / guided visit workflow
-- Database: esa_backend

CREATE TABLE IF NOT EXISTS visit_sessions (
    id SERIAL PRIMARY KEY,
    lead_id VARCHAR(36),                -- SugarCRM lead UUID (nullable initially)
    site_code VARCHAR(10) NOT NULL,
    staff_user_id INTEGER NOT NULL,     -- references users.id
    flow_type VARCHAR(20) NOT NULL DEFAULT 'walk_in',  -- walk_in / guided
    status VARCHAR(30) NOT NULL DEFAULT 'active',
    outcome VARCHAR(30),                -- reserved / converted / visit_completed / lost
    outcome_notes TEXT,
    lost_reason VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS visit_shortlist_items (
    id SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES visit_sessions(id) ON DELETE CASCADE,
    site_id INTEGER NOT NULL,
    unit_id INTEGER NOT NULL,
    unit_name VARCHAR(50),
    category_label VARCHAR(100),
    area NUMERIC(10,2),
    floor INTEGER,
    climate_code VARCHAR(5),
    std_rate NUMERIC(10,2),
    indicative_rate NUMERIC(10,2),
    discount_plan_id INTEGER,
    concession_id INTEGER DEFAULT 0,
    notes TEXT,
    sort_order INTEGER DEFAULT 0,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(session_id, site_id, unit_id)
);

-- Index for fast lookup of active sessions per user
CREATE INDEX IF NOT EXISTS idx_visit_sessions_staff_status
    ON visit_sessions(staff_user_id, status);

-- Index for session shortlist items
CREATE INDEX IF NOT EXISTS idx_visit_shortlist_session
    ON visit_shortlist_items(session_id);
