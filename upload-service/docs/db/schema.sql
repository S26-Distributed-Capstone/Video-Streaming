CREATE TABLE IF NOT EXISTS video_upload (
    id SERIAL PRIMARY KEY,
    video_id UUID NOT NULL UNIQUE,
    video_name VARCHAR(256) NOT NULL DEFAULT '',
    total_segments INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    machine_id VARCHAR(128),
    container_id VARCHAR(128)
);

ALTER TABLE video_upload
    ADD COLUMN IF NOT EXISTS container_id VARCHAR(128);

ALTER TABLE video_upload
    ADD COLUMN IF NOT EXISTS video_name VARCHAR(256) NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS segment_upload (
    id SERIAL PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES video_upload(video_id) ON DELETE CASCADE,
    segment_number INTEGER NOT NULL,
    UNIQUE (video_id, segment_number)
);

CREATE INDEX IF NOT EXISTS idx_segment_upload_video_id
    ON segment_upload(video_id);

CREATE TABLE IF NOT EXISTS transcoded_segment_status (
    id SERIAL PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES video_upload(video_id) ON DELETE CASCADE,
    profile VARCHAR(32) NOT NULL,
    segment_number INTEGER NOT NULL,
    state VARCHAR(32) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (video_id, profile, segment_number)
);

CREATE INDEX IF NOT EXISTS idx_transcoded_segment_status_video_profile_state
    ON transcoded_segment_status(video_id, profile, state);

CREATE TABLE IF NOT EXISTS processing_upload_task (
    id SERIAL PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES video_upload(video_id) ON DELETE CASCADE,
    profile VARCHAR(32) NOT NULL,
    segment_number INTEGER NOT NULL,
    chunk_key VARCHAR(512) NOT NULL,
    output_key VARCHAR(512) NOT NULL,
    spool_path VARCHAR(1024) NOT NULL,
    size_bytes BIGINT NOT NULL,
    output_ts_offset_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
    state VARCHAR(32) NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    claimed_by VARCHAR(128),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (video_id, profile, segment_number)
);

CREATE INDEX IF NOT EXISTS idx_processing_upload_task_state_updated
    ON processing_upload_task(state, updated_at);

CREATE TABLE IF NOT EXISTS processing_task_claim (
    id SERIAL PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES video_upload(video_id) ON DELETE CASCADE,
    profile VARCHAR(32) NOT NULL,
    segment_number INTEGER NOT NULL,
    stage VARCHAR(32) NOT NULL,
    claimed_by VARCHAR(128) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (video_id, profile, segment_number)
);

CREATE INDEX IF NOT EXISTS idx_processing_task_claim_claimed_by
    ON processing_task_claim(claimed_by, updated_at);
