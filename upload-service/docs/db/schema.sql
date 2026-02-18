CREATE TABLE IF NOT EXISTS job_tasks (
    job_id TEXT PRIMARY KEY,
    num_tasks INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS video_upload (
    id SERIAL PRIMARY KEY,
    video_id UUID NOT NULL UNIQUE,
    total_segments INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    machine_id VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS segment_upload (
    id SERIAL PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES video_upload(video_id) ON DELETE CASCADE,
    segment_number INTEGER NOT NULL,
    UNIQUE (video_id, segment_number)
);

CREATE INDEX IF NOT EXISTS idx_segment_upload_video_id
    ON segment_upload(video_id);
