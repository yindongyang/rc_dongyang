// Package model 定义任务数据模型与状态机。
package model

import "time"

// Status 通知任务的生命周期状态。
type Status string

const (
	StatusPending    Status = "pending"
	StatusRunning    Status = "running"
	StatusSuccess    Status = "success"
	StatusDeadLetter Status = "dead_letter"
)

// Notification 一条通知任务（与 DB 行 1:1）。
type Notification struct {
	ID             string
	BizSystem      string
	BizID          string
	IdempotencyKey string

	URL     string
	Method  string
	Headers map[string]string
	Body    []byte

	Status         Status
	Attempts       int
	MaxAttempts    int
	NextRetryAt    time.Time
	LeaseUntil     time.Time
	LastError      string
	LastStatusCode int

	CreatedAt time.Time
	UpdatedAt time.Time
}
