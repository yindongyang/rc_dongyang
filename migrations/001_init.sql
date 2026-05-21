-- 通知任务表
CREATE TABLE IF NOT EXISTS notifications (
    id              TEXT PRIMARY KEY,        -- UUID
    biz_system      TEXT NOT NULL,
    biz_id          TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,           -- 透传到外部 Header 的去重 key

    url             TEXT NOT NULL,
    method          TEXT NOT NULL,
    headers         TEXT NOT NULL DEFAULT '{}', -- JSON
    body            BLOB,

    status          TEXT NOT NULL DEFAULT 'pending', -- pending|running|success|dead_letter
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 8,

    next_retry_at   INTEGER NOT NULL,        -- unix nano；可立即调度则为 0
    lease_until     INTEGER NOT NULL DEFAULT 0,

    last_error      TEXT NOT NULL DEFAULT '',
    last_status_code INTEGER NOT NULL DEFAULT 0,

    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,

    -- 入口去重：同一业务的同一个 biz_id 只允许一条
    UNIQUE (biz_system, biz_id)
);

-- Worker 抢占用：拉取 status='pending' AND next_retry_at <= now
CREATE INDEX IF NOT EXISTS idx_notify_status_next ON notifications(status, next_retry_at);

-- 看门狗用：找已过期 lease 的 running
CREATE INDEX IF NOT EXISTS idx_notify_lease ON notifications(status, lease_until);
