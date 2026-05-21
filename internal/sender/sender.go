// Package sender 负责真正调用外部 HTTP API 并解释结果。
//
// 设计取舍：
//   - 不感知具体供应商协议；URL/Header/Body 都由业务方提供
//   - 只关注一件事：这次调用应该被视为"成功 / 可重试失败 / 不可重试失败"
//   - 透传 Idempotency-Key 头给外部供应商配合去重（外部即使不支持也无伤大雅）
package sender

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"rc_dongyang/internal/model"
)

// Outcome 一次发送的结果分类。
type Outcome int

const (
	// OutcomeSuccess 业务上视为送达：HTTP 2xx
	OutcomeSuccess Outcome = iota
	// OutcomeRetryable 5xx / 408 / 429 / 网络错误 / 超时
	OutcomeRetryable
	// OutcomePermanent 4xx（除 408/429）—— 重试无意义
	OutcomePermanent
)

// Result 发送结果详情。
type Result struct {
	Outcome    Outcome
	StatusCode int
	Err        error
	// RetryAfter 来自外部的 Retry-After 头（429/503 才会有），为 0 表示让本服务自决定退避
	RetryAfter time.Duration
}

// Sender 真正发请求的执行体。
type Sender struct {
	client *http.Client
}

// New 构造一个 Sender，单次调用强制超时由调用方在 ctx 控制 + Client.Timeout 兜底。
func New(timeout time.Duration) *Sender {
	return &Sender{
		client: &http.Client{Timeout: timeout},
	}
}

// Send 调用外部 API 并解释结果。
func (s *Sender) Send(ctx context.Context, n *model.Notification) Result {
	req, err := http.NewRequestWithContext(ctx, n.Method, n.URL, bytes.NewReader(n.Body))
	if err != nil {
		// 请求构造失败几乎只会因为 method/url 有问题，归类为不可重试
		return Result{Outcome: OutcomePermanent, Err: fmt.Errorf("build request: %w", err)}
	}
	for k, v := range n.Headers {
		req.Header.Set(k, v)
	}
	// 透传 Idempotency-Key（业务方未自带时由我们补上）
	if req.Header.Get("Idempotency-Key") == "" && n.IdempotencyKey != "" {
		req.Header.Set("Idempotency-Key", n.IdempotencyKey)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		// 网络错误 / DNS 失败 / 超时 / 连接重置 —— 全部可重试
		// 注意 ctx.Err() == context.Canceled 也应可重试（worker 优雅停机时）
		if errors.Is(err, context.Canceled) {
			return Result{Outcome: OutcomeRetryable, Err: err}
		}
		return Result{Outcome: OutcomeRetryable, Err: err}
	}
	defer resp.Body.Close()

	// 把 body 读完丢弃，让连接可以复用（即使我们不关心返回值，连接复用也是基本操守）
	_, _ = io.Copy(io.Discard, resp.Body)

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return Result{Outcome: OutcomeSuccess, StatusCode: resp.StatusCode}

	case resp.StatusCode == http.StatusRequestTimeout, // 408
		resp.StatusCode == http.StatusTooManyRequests, // 429
		resp.StatusCode >= 500:
		return Result{
			Outcome:    OutcomeRetryable,
			StatusCode: resp.StatusCode,
			Err:        fmt.Errorf("upstream status %d", resp.StatusCode),
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}

	default:
		// 4xx（除 408/429）= 永久失败：请求本身有问题，重试只会浪费容量
		return Result{
			Outcome:    OutcomePermanent,
			StatusCode: resp.StatusCode,
			Err:        fmt.Errorf("upstream status %d (non-retryable)", resp.StatusCode),
		}
	}
}

// parseRetryAfter 仅支持秒数格式（HTTP-date 这种边角不在 MVP 范围）。
func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}
