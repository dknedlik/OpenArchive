CREATE TABLE oa_review_decision (
    review_decision_id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL,
    item_kind TEXT NOT NULL CHECK (
        item_kind IN (
            'artifact_needs_attention',
            'artifact_missing_summary',
            'object_low_confidence',
            'candidate_key_collision',
            'object_missing_evidence'
        )
    ),
    artifact_id TEXT NOT NULL REFERENCES oa_artifact (artifact_id) ON DELETE CASCADE,
    derived_object_id TEXT REFERENCES oa_derived_object (derived_object_id) ON DELETE CASCADE,
    decision_status TEXT NOT NULL CHECK (decision_status IN ('noted', 'dismissed', 'resolved')),
    note_text TEXT,
    decided_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX ix_oa_review_decision_item_id
    ON oa_review_decision (item_id, created_at DESC);

CREATE INDEX ix_oa_review_decision_item_status
    ON oa_review_decision (item_id, decision_status);

CREATE INDEX ix_oa_review_decision_artifact_id
    ON oa_review_decision (artifact_id);

CREATE INDEX ix_oa_review_decision_derived_object_id
    ON oa_review_decision (derived_object_id);
