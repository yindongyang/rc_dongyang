package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite" // 纯 Go SQLite，无需 cgo

	"rc_dongyang/internal/model"
)

// SQLiteStore Store 接口的 SQLite 实现。
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore 打开 / 创建 DB 并执行 migrations。
func NewSQLiteStore(dsn string) (*SQLiteStore, error) {
	if dir := filepath.Dir(dsn); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}

	// _busy_timeout：SQLite 在并发写时会返回 SQLITE_BUSY，让驱动自动等待
	// _journal_mode=WAL：并发读 + 单写不阻塞读
	dsnWithPragma := dsn + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsnWithPragma)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// SQLite 写串行化，连接池开太大没意义，反而增加 BUSY 概率
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)

	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &SQLiteStore{db: db}, nil
}

// migrate 内嵌建表 SQL（避免运行时依赖 migrations 文件）。
func migrate(db *sql.DB) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS notifications (
    id              TEXT PRIMARY KEY,
    biz_system      TEXT NOT NULL,
    biz_id          TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    url             TEXT NOT NULL,
    method          TEXT NOT NULL,
    headers         TEXT NOT NULL DEFAULT '{}',
    body            BLOB,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 8,
    next_retry_at   INTEGER NOT NULL,
    lease_until     INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT NOT NULL DEFAULT '',
    last_status_code INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    UNIQUE (biz_system, biz_id)
);
CREATE INDEX IF NOT EXISTS idx_notify_status_next ON notifications(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_notify_lease ON notifications(status, lease_until);
`
	_, err := db.Exec(ddl)
	return err
}

func (s *SQLiteStore) Close() error { return s.db.Close() }

// Insert 利用 UNIQUE 约束做入口去重。
func (s *SQLiteStore) Insert(ctx context.Context, n *model.Notification) (*model.Notification, error) {
	headersJSON, err := json.Marshal(n.Headers)
	if err != nil {
		return nil, fmt.Errorf("marshal headers: %w", err)
	}
	now := time.Now()
	if n.CreatedAt.IsZero() {
		n.CreatedAt = now
	}
	n.UpdatedAt = now

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO notifications
		(id, biz_system, biz_id, idempotency_key, url, method, headers, body,
		 status, attempts, max_attempts, next_retry_at, lease_until,
		 last_error, last_status_code, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?, ?,?,?,?,?, ?,?,?,?)`,
		n.ID, n.BizSystem, n.BizID, n.IdempotencyKey,
		n.URL, n.Method, string(headersJSON), n.Body,
		string(n.Status), n.Attempts, n.MaxAttempts, n.NextRetryAt.UnixNano(), n.LeaseUntil.UnixNano(),
		n.LastError, n.LastStatusCode, n.CreatedAt.UnixNano(), n.UpdatedAt.UnixNano(),
	)
	if err != nil {
		// modernc/sqlite 错误信息里包含 "UNIQUE constraint failed"
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			existing, getErr := s.getByBiz(ctx, n.BizSystem, n.BizID)
			if getErr != nil {
				return nil, fmt.Errorf("duplicate, but failed to load existing: %w", getErr)
			}
			return existing, ErrDuplicate
		}
		return nil, fmt.Errorf("insert: %w", err)
	}
	return nil, nil
}

func (s *SQLiteStore) getByBiz(ctx context.Context, bizSystem, bizID string) (*model.Notification, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+columns+` FROM notifications WHERE biz_system=? AND biz_id=?`,
		bizSystem, bizID,
	)
	return scan(row)
}

func (s *SQLiteStore) Get(ctx context.Context, id string) (*model.Notification, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+columns+` FROM notifications WHERE id=?`, id,
	)
	n, err := scan(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return n, err
}

// AcquirePending 抢占一条任务。
//
// 关键技巧：先 SELECT 一条候选 ID，再用 UPDATE WHERE status='pending' 做 CAS。
// UPDATE 影响行数=1 才算抢到，否则说明被其它 worker 抢先了，重试或返回空。
func (s *SQLiteStore) AcquirePending(ctx context.Context, leaseTimeout time.Duration, now time.Time) (*model.Notification, error) {
	// 在 SQLite 中，这里偶尔与 reaper / 其它 worker 竞争，最多重试 3 次。
	for attempt := 0; attempt < 3; attempt++ {
		var id string
		err := s.db.QueryRowContext(ctx, `
			SELECT id FROM notifications
			WHERE status='pending' AND next_retry_at <= ?
			ORDER BY next_retry_at ASC
			LIMIT 1`, now.UnixNano(),
		).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("select pending: %w", err)
		}

		leaseUntil := now.Add(leaseTimeout)
		res, err := s.db.ExecContext(ctx, `
			UPDATE notifications
			SET status='running',
			    lease_until=?,
			    attempts=attempts+1,
			    updated_at=?
			WHERE id=? AND status='pending'`,
			leaseUntil.UnixNano(), now.UnixNano(), id,
		)
		if err != nil {
			return nil, fmt.Errorf("update to running: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			// 被别人抢先了，重试
			continue
		}
		return s.Get(ctx, id)
	}
	return nil, nil
}

func (s *SQLiteStore) MarkSuccess(ctx context.Context, id string, statusCode int, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE notifications
		SET status='success', last_status_code=?, last_error='', updated_at=?
		WHERE id=?`, statusCode, now.UnixNano(), id)
	return err
}

func (s *SQLiteStore) ScheduleRetry(ctx context.Context, id string, nextAt time.Time, lastErr string, statusCode int, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE notifications
		SET status='pending',
		    next_retry_at=?,
		    last_error=?,
		    last_status_code=?,
		    updated_at=?
		WHERE id=?`,
		nextAt.UnixNano(), truncate(lastErr, 1024), statusCode, now.UnixNano(), id)
	return err
}

func (s *SQLiteStore) MarkDeadLetter(ctx context.Context, id string, lastErr string, statusCode int, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE notifications
		SET status='dead_letter',
		    last_error=?,
		    last_status_code=?,
		    updated_at=?
		WHERE id=?`,
		truncate(lastErr, 1024), statusCode, now.UnixNano(), id)
	return err
}

func (s *SQLiteStore) ReapExpiredLeases(ctx context.Context, now time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE notifications
		SET status='pending', updated_at=?
		WHERE status='running' AND lease_until <= ?`,
		now.UnixNano(), now.UnixNano())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *SQLiteStore) Requeue(ctx context.Context, id string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE notifications
		SET status='pending', attempts=0, next_retry_at=?, last_error='', updated_at=?
		WHERE id=? AND status='dead_letter'`,
		now.UnixNano(), now.UnixNano(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ---------------- 行扫描 ----------------

const columns = `id, biz_system, biz_id, idempotency_key,
url, method, headers, body,
status, attempts, max_attempts, next_retry_at, lease_until,
last_error, last_status_code, created_at, updated_at`

type rowScanner interface {
	Scan(dest ...any) error
}

func scan(r rowScanner) (*model.Notification, error) {
	var (
		n           model.Notification
		headersJSON string
		nextRetryNs int64
		leaseUntilNs int64
		createdNs   int64
		updatedNs   int64
		statusStr   string
	)
	if err := r.Scan(
		&n.ID, &n.BizSystem, &n.BizID, &n.IdempotencyKey,
		&n.URL, &n.Method, &headersJSON, &n.Body,
		&statusStr, &n.Attempts, &n.MaxAttempts, &nextRetryNs, &leaseUntilNs,
		&n.LastError, &n.LastStatusCode, &createdNs, &updatedNs,
	); err != nil {
		return nil, err
	}
	n.Status = model.Status(statusStr)
	if headersJSON != "" {
		if err := json.Unmarshal([]byte(headersJSON), &n.Headers); err != nil {
			return nil, fmt.Errorf("unmarshal headers: %w", err)
		}
	}
	n.NextRetryAt = time.Unix(0, nextRetryNs)
	n.LeaseUntil = time.Unix(0, leaseUntilNs)
	n.CreatedAt = time.Unix(0, createdNs)
	n.UpdatedAt = time.Unix(0, updatedNs)
	return &n, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
