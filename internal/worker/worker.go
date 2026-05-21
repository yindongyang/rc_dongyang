// Package worker 负责从 Store 抢占任务、调用 sender、写回状态。
//
// 设计要点：
//   - N 个 worker goroutine + 1 个 reaper goroutine
//   - 没有 in-process 队列：抢占基于 DB，天然支持横向扩展（同一个 DB 多个进程一起抢）
//   - 内置 vendor（按 host）维度的简单熔断：连续失败 N 次的 host 短路 M 分钟
package worker

import (
	"context"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/url"
	"sync"
	"time"

	"rc_dongyang/internal/config"
	"rc_dongyang/internal/model"
	"rc_dongyang/internal/sender"
	"rc_dongyang/internal/store"
)

// Worker 调度器。
type Worker struct {
	cfg    *config.Config
	store  store.Store
	sender *sender.Sender
	log    *slog.Logger

	circuit *circuitBreaker
}

// New 构造。
func New(cfg *config.Config, st store.Store, sd *sender.Sender, log *slog.Logger) *Worker {
	return &Worker{
		cfg:     cfg,
		store:   st,
		sender:  sd,
		log:     log,
		circuit: newCircuitBreaker(cfg.Circuit.FailThreshold, cfg.Circuit.Cooldown),
	}
}

// Run 启动调度（阻塞直到 ctx 取消）。
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup

	// reaper：清理过期 lease
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.runReaper(ctx)
	}()

	// N 个 worker
	for i := 0; i < w.cfg.Worker.Concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w.runLoop(ctx, id)
		}(i)
	}

	wg.Wait()
	w.log.Info("worker stopped")
}

func (w *Worker) runLoop(ctx context.Context, id int) {
	log := w.log.With("worker", id)
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := w.store.AcquirePending(ctx, w.cfg.Worker.LeaseTimeout, time.Now())
		if err != nil {
			log.Error("acquire pending failed", "err", err)
			sleepCtx(ctx, w.cfg.Worker.PollInterval)
			continue
		}
		if n == nil {
			sleepCtx(ctx, w.cfg.Worker.PollInterval)
			continue
		}

		// 检查 vendor 熔断；命中则直接安排稍后重试
		host := hostOf(n.URL)
		if delay, tripped := w.circuit.shouldSkip(host); tripped {
			next := time.Now().Add(delay)
			_ = w.store.ScheduleRetry(ctx, n.ID, next, "circuit open for "+host, 0, time.Now())
			log.Info("circuit open, postpone", "id", n.ID, "host", host, "delay", delay)
			continue
		}

		w.process(ctx, log, n)
	}
}

// process 执行一次发送 + 写回结果。
func (w *Worker) process(ctx context.Context, log *slog.Logger, n *model.Notification) {
	// 单次 HTTP 调用强制超时（与 client.Timeout 双重保险）
	sendCtx, cancel := context.WithTimeout(ctx, w.cfg.Retry.HTTPTimeout)
	defer cancel()

	res := w.sender.Send(sendCtx, n)
	host := hostOf(n.URL)
	now := time.Now()

	switch res.Outcome {
	case sender.OutcomeSuccess:
		w.circuit.onSuccess(host)
		if err := w.store.MarkSuccess(ctx, n.ID, res.StatusCode, now); err != nil {
			log.Error("mark success failed", "id", n.ID, "err", err)
			return
		}
		log.Info("delivered", "id", n.ID, "biz", n.BizSystem+"/"+n.BizID, "status", res.StatusCode, "attempts", n.Attempts)

	case sender.OutcomePermanent:
		// 4xx：直接死信，不再重试
		w.circuit.onSuccess(host) // 4xx 是请求本身的问题，不算 vendor 故障
		errStr := errString(res.Err)
		if err := w.store.MarkDeadLetter(ctx, n.ID, errStr, res.StatusCode, now); err != nil {
			log.Error("mark dead letter failed", "id", n.ID, "err", err)
			return
		}
		log.Warn("dead-letter (permanent)", "id", n.ID, "status", res.StatusCode, "err", errStr)

	case sender.OutcomeRetryable:
		w.circuit.onFailure(host)
		errStr := errString(res.Err)
		// 已用完重试次数 → 死信
		if n.Attempts >= n.MaxAttempts {
			if err := w.store.MarkDeadLetter(ctx, n.ID, errStr, res.StatusCode, now); err != nil {
				log.Error("mark dead letter failed", "id", n.ID, "err", err)
				return
			}
			log.Warn("dead-letter (max attempts)", "id", n.ID, "attempts", n.Attempts, "err", errStr)
			return
		}
		// 继续重试
		backoff := w.calcBackoff(n.Attempts, res.RetryAfter)
		next := now.Add(backoff)
		if err := w.store.ScheduleRetry(ctx, n.ID, next, errStr, res.StatusCode, now); err != nil {
			log.Error("schedule retry failed", "id", n.ID, "err", err)
			return
		}
		log.Info("retry scheduled", "id", n.ID, "attempts", n.Attempts, "backoff", backoff, "err", errStr)
	}
}

// calcBackoff 指数退避 + jitter，并尊重外部 Retry-After（取较大值，避免短于服务方要求）。
func (w *Worker) calcBackoff(attempt int, retryAfter time.Duration) time.Duration {
	base := w.cfg.Retry.BaseBackoff
	maxBO := w.cfg.Retry.MaxBackoff
	// attempt 在调用前已 +1，因此第一次失败后 attempt=1 → 退避 base
	exp := time.Duration(math.Pow(2, float64(attempt-1))) * base
	if exp <= 0 || exp > maxBO {
		exp = maxBO
	}
	// jitter: [0, base)
	jitter := time.Duration(rand.Int64N(int64(base)))
	d := exp + jitter
	if d > maxBO {
		d = maxBO
	}
	if retryAfter > d {
		d = retryAfter
	}
	return d
}

func (w *Worker) runReaper(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.Worker.ReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := w.store.ReapExpiredLeases(ctx, time.Now())
			if err != nil {
				w.log.Error("reaper failed", "err", err)
				continue
			}
			if n > 0 {
				w.log.Info("reaper reclaimed expired leases", "count", n)
			}
		}
	}
}

// ---------- helpers ----------

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func hostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return "unknown"
	}
	return u.Host
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// ---------------- circuit breaker ----------------

type circuitBreaker struct {
	threshold int
	cooldown  time.Duration

	mu     sync.Mutex
	states map[string]*hostState
}

type hostState struct {
	consecutiveFails int
	openedUntil      time.Time
}

func newCircuitBreaker(threshold int, cooldown time.Duration) *circuitBreaker {
	return &circuitBreaker{
		threshold: threshold,
		cooldown:  cooldown,
		states:    make(map[string]*hostState),
	}
}

// shouldSkip 当前是否应跳过该 host；返回剩余冷却时间。
func (c *circuitBreaker) shouldSkip(host string) (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.states[host]
	if st == nil {
		return 0, false
	}
	now := time.Now()
	if st.openedUntil.After(now) {
		return st.openedUntil.Sub(now), true
	}
	return 0, false
}

func (c *circuitBreaker) onSuccess(host string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.states, host)
}

func (c *circuitBreaker) onFailure(host string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.states[host]
	if st == nil {
		st = &hostState{}
		c.states[host] = st
	}
	st.consecutiveFails++
	if st.consecutiveFails >= c.threshold {
		st.openedUntil = time.Now().Add(c.cooldown)
		st.consecutiveFails = 0
	}
}
