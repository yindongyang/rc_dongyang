// Package store 抽象持久化层。
//
// 设计取舍：
//   - MVP 仅实现 SQLite，但接口刻意不暴露 SQL 细节，便于将来切到 MySQL/Postgres
//   - Worker 抢占采用乐观锁（UPDATE WHERE status='pending'），SQLite 没有 SKIP LOCKED
//   - 入口去重靠 (biz_system, biz_id) 唯一索引，让 DB 兜底
package store

import (
	"context"
	"errors"
	"time"

	"rc_dongyang/internal/model"
)

// 业务级错误（store 层向上暴露的语义化错误）。
var (
	ErrDuplicate = errors.New("duplicate biz_system/biz_id")
	ErrNotFound  = errors.New("notification not found")
)

// Store 持久化接口。
type Store interface {
	// Insert 插入一条新任务；若 (biz_system, biz_id) 已存在，返回 ErrDuplicate 并附带已存在的记录。
	Insert(ctx context.Context, n *model.Notification) (existing *model.Notification, err error)

	// Get 按 ID 获取。
	Get(ctx context.Context, id string) (*model.Notification, error)

	// AcquirePending 乐观锁抢占一条到期的 pending 任务。
	// 抢到则把 status 置为 running、lease_until 置为 now+leaseTimeout、attempts+1。
	// 没有可抢任务返回 (nil, nil)。
	AcquirePending(ctx context.Context, leaseTimeout time.Duration, now time.Time) (*model.Notification, error)

	// MarkSuccess 标记成功。
	MarkSuccess(ctx context.Context, id string, statusCode int, now time.Time) error

	// ScheduleRetry 失败但仍需重试：状态回到 pending，next_retry_at = nextAt。
	ScheduleRetry(ctx context.Context, id string, nextAt time.Time, lastErr string, statusCode int, now time.Time) error

	// MarkDeadLetter 进入死信。
	MarkDeadLetter(ctx context.Context, id string, lastErr string, statusCode int, now time.Time) error

	// ReapExpiredLeases 看门狗：把 status=running 且 lease_until<=now 的回滚为 pending。
	// 返回回收的条数。
	ReapExpiredLeases(ctx context.Context, now time.Time) (int64, error)

	// Requeue 把死信重新置为 pending，attempts 重置为 0。
	Requeue(ctx context.Context, id string, now time.Time) error

	// Close 释放底层资源。
	Close() error
}
