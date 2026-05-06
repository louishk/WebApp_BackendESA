-- 049_call_scoring_config.sql
-- Editable LLM scoring rubric for Zoom call quality scoring.
-- The pipeline reads its prompt + dimensions from this table at runtime so
-- managers can tweak the rubric without code changes / redeploys.
-- Target: esa_pbi

CREATE TABLE IF NOT EXISTS call_scoring_config (
    id           SERIAL PRIMARY KEY,
    name         VARCHAR(50) UNIQUE NOT NULL DEFAULT 'default',
    config_json  JSONB NOT NULL,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    version      INTEGER NOT NULL DEFAULT 1,
    updated_by   VARCHAR(100),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS call_scoring_config_history (
    id              SERIAL PRIMARY KEY,
    config_id       INTEGER NOT NULL,
    name            VARCHAR(50) NOT NULL,
    config_json     JSONB NOT NULL,
    version         INTEGER NOT NULL,
    updated_by      VARCHAR(100),
    archived_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_call_scoring_config_active ON call_scoring_config(is_active);
CREATE INDEX IF NOT EXISTS idx_call_scoring_history_config ON call_scoring_config_history(config_id);

-- Seed the default rubric. Uses ON CONFLICT to be re-runnable.
INSERT INTO call_scoring_config (name, config_json, is_active, version, updated_by)
VALUES (
    'default',
    $${
  "model": "grok-3-mini",
  "temperature": 0.2,
  "max_tokens": 8000,
  "system_prompt": "You are a QA analyst for Extra Space Asia, a self-storage company operating in Singapore, Malaysia, Korea, Japan, Hong Kong, and Taiwan. You are scoring an internal phone call between an Extra Space Asia agent and a customer (or prospect). The call transcript is given to you formatted as a conversation with [Agent (Name)] and [Customer] turns. Score the call across the dimensions listed below. Use the rubric strictly. For dimensions marked applies_to=sales or applies_to=support, only fill them when the call category matches; otherwise return null for those keys. Return ONLY a single JSON object with keys exactly matching the dimension keys plus the metadata keys quality_overall, score_confidence, and score_summary. Do not wrap in markdown.",
  "context_hints": {
    "company": "Extra Space Asia (self-storage)",
    "common_topics": ["unit size", "monthly rate", "discount", "lock", "access hours", "deposit", "move-in date", "kaki bukit", "innoslink"]
  },
  "dimensions": [
    {
      "key": "quality_politeness",
      "label": "Politeness",
      "type": "int",
      "min": 1,
      "max": 10,
      "applies_to": "all",
      "sugar_field": "es_zoom_quality_politeness_c",
      "rubric": "1=rude or dismissive; 5=neutral professional minimum; 10=exemplary courtesy with greetings, please/thanks, no interruptions",
      "enabled": true
    },
    {
      "key": "quality_tone",
      "label": "Tone",
      "type": "int",
      "min": 1,
      "max": 10,
      "applies_to": "all",
      "sugar_field": "es_zoom_quality_tone_c",
      "rubric": "1=cold or hostile; 5=flat but acceptable; 10=warm, friendly, empathetic. Inferred from word choice and sentence structure.",
      "enabled": true
    },
    {
      "key": "quality_listening",
      "label": "Active Listening",
      "type": "int",
      "min": 1,
      "max": 10,
      "applies_to": "all",
      "sugar_field": "es_zoom_quality_listening_c",
      "rubric": "1=agent steamrolls or ignores customer concerns; 5=acknowledges but doesn't engage; 10=actively reflects back, asks clarifying questions, addresses real concerns",
      "enabled": true
    },
    {
      "key": "quality_clarity",
      "label": "Clarity",
      "type": "int",
      "min": 1,
      "max": 10,
      "applies_to": "all",
      "sugar_field": "es_zoom_quality_clarity_c",
      "rubric": "1=confusing, jargon-heavy, contradictory; 5=clear enough; 10=plain language, confirms customer understanding, summarizes key points",
      "enabled": true
    },
    {
      "key": "quality_deescalation",
      "label": "De-escalation",
      "type": "int",
      "min": 1,
      "max": 10,
      "applies_to": "all",
      "sugar_field": "es_zoom_quality_deescalation_c",
      "rubric": "Only score when tension or complaint is detected; otherwise null. 1=agent escalated/argued; 5=neutral handling; 10=calm, validating, resolution-focused",
      "enabled": true
    },
    {
      "key": "sales_pitch_quality",
      "label": "Sales Pitch Quality",
      "type": "int",
      "min": 1,
      "max": 10,
      "applies_to": "sales",
      "sugar_field": "es_zoom_sales_pitch_quality_c",
      "rubric": "Only on Sales calls. 1=vague or wrong info; 5=basic correct info; 10=clear value prop, product knowledge, tailored to customer need",
      "enabled": true
    },
    {
      "key": "sales_cta_clarity",
      "label": "Sales CTA Clarity",
      "type": "int",
      "min": 1,
      "max": 10,
      "applies_to": "sales",
      "sugar_field": "es_zoom_sales_cta_clarity_c",
      "rubric": "Only on Sales calls. 1=no next step proposed; 5=vague follow-up; 10=clear concrete next step (visit booked, deposit asked, deadline given)",
      "enabled": true
    },
    {
      "key": "sales_objection_handling",
      "label": "Sales Objection Handling",
      "type": "int",
      "min": 1,
      "max": 10,
      "applies_to": "sales",
      "sugar_field": "es_zoom_sales_objection_hand_c",
      "rubric": "Only on Sales calls. 1=ignored or argued; 5=acknowledged but didn't address; 10=acknowledged, clarified, offered alternatives or addressed root cause. Null if no objection raised.",
      "enabled": true
    },
    {
      "key": "sales_outcome",
      "label": "Sales Outcome",
      "type": "enum",
      "values": ["interested", "not_interested", "follow_up_needed", "closed_won", "closed_lost", "no_decision"],
      "applies_to": "sales",
      "sugar_field": "es_zoom_sales_outcome_c",
      "rubric": "Only on Sales calls. Best classification of where the call left the deal.",
      "enabled": true
    },
    {
      "key": "support_resolution_quality",
      "label": "Support Resolution Quality",
      "type": "int",
      "min": 1,
      "max": 10,
      "applies_to": "support",
      "sugar_field": "es_zoom_support_resolution_q_c",
      "rubric": "Only on Support calls. 1=issue not addressed; 5=partial fix or workaround; 10=fully resolved on call",
      "enabled": true
    },
    {
      "key": "support_csat_signal",
      "label": "Support CSAT Signal",
      "type": "int",
      "min": 1,
      "max": 10,
      "applies_to": "support",
      "sugar_field": "es_zoom_support_csat_signal_c",
      "rubric": "Only on Support calls. Inferred from customer's verbal cues (thanks, perfect, sigh, frustration). 1=clearly unhappy; 5=neutral; 10=delighted",
      "enabled": true
    },
    {
      "key": "support_first_call_resolution",
      "label": "Support First-Call Resolution",
      "type": "bool",
      "applies_to": "support",
      "sugar_field": "es_zoom_support_first_call_r_c",
      "rubric": "Only on Support calls. true if the issue was resolved without needing a callback or transfer; false otherwise",
      "enabled": true
    },
    {
      "key": "call_category",
      "label": "Call Category",
      "type": "enum",
      "values": ["Sales", "Support", "Billing", "Complaint", "General Enquiry", "Reservation", "Move-In", "Move-Out", "Other"],
      "applies_to": "all",
      "sugar_field": "es_call_category_c",
      "rubric": "Pick the single best category that captures the dominant intent of the call",
      "enabled": true
    },
    {
      "key": "call_subcategory",
      "label": "Call Subcategory",
      "type": "text",
      "max_length": 100,
      "applies_to": "all",
      "sugar_field": "es_call_subcategory_c",
      "rubric": "Free-text 3-5 word phrase: e.g. 'Pricing inquiry', 'Lock issue', 'Late payment', 'Site visit booking'",
      "enabled": true
    },
    {
      "key": "sentiment",
      "label": "Sentiment",
      "type": "enum",
      "values": ["positive", "neutral", "negative"],
      "applies_to": "all",
      "sugar_field": "es_sentiment_c",
      "rubric": "Overall tone of the customer at the END of the call",
      "enabled": true
    },
    {
      "key": "customer_intent_summary",
      "label": "Customer Intent",
      "type": "text",
      "max_length": 300,
      "applies_to": "all",
      "sugar_field": "es_customer_intent_c",
      "rubric": "One sentence summarizing what the customer wants/needs",
      "enabled": true
    },
    {
      "key": "action_items",
      "label": "Action Items",
      "type": "text",
      "max_length": 500,
      "applies_to": "all",
      "sugar_field": "es_action_items_c",
      "rubric": "Concrete actions the agent committed to. One per line. Empty string if none.",
      "enabled": true
    },
    {
      "key": "red_flags",
      "label": "Red Flags",
      "type": "text",
      "max_length": 500,
      "applies_to": "all",
      "sugar_field": "es_red_flags_c",
      "rubric": "Only populate if you detect: complaint about staff, mention of a competitor, churn intent, threat to leave, escalation request, legal threat. Otherwise null/empty.",
      "enabled": true
    }
  ]
}$$::jsonb,
    TRUE,
    1,
    'system'
)
ON CONFLICT (name) DO NOTHING;
