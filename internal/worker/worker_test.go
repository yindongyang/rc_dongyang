package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"rc_dongyang/internal/api"
	"rc_dongyang/internal/config"
	"rc_dongyang/internal/sender"
	"rc_dongyang/internal/store"
)

// TestEndToEnd_RetryThenSuccess 验证：
//  1. API 接收任务并入库
//  2. Worker 拉到任务调用外部
//  3. 前两次 500 → 重试；第三次 200 → 成功
//  4. 状态查询接口返回 success
func TestEndToEnd_RetryThenSuccess(t *testing.T) {
	// 模拟外部供应商：前 2 次 500，之后 200
	var hits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	st, cfg, log := setupTestEnv(t)
	defer st.Close()

	sd := sender.New(cfg.Retry.HTTPTimeout)
	wk := New(cfg, st, sd, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go wk.Run(ctx)

	apiSrv := api.New(cfg, st, log)
	srv := httptest.NewServer(apiSrv.Routes())
	defer srv.Close()

	id := submit(t, srv.URL, map[string]any{
		"biz_system":   "test",
		"biz_id":       "e2e-1",
		"url":          upstream.URL,
		"method":       "POST",
		"body":         map[string]string{"hello": "world"},
		"max_attempts": 5,
	})

	if got := waitStatus(t, srv.URL, id, "success", 8*time.Second); got != "success" {
		t.Fatalf("expected success, got %s", got)
	}
	if h := atomic.LoadInt32(&hits); h != 3 {
		t.Fatalf("expected 3 upstream hits (2 fail + 1 ok), got %d", h)
	}
}

// TestEndToEnd_PermanentFailureGoesDeadLetter 验证 4xx 不重试，直接进死信。
func TestEndToEnd_PermanentFailureGoesDeadLetter(t *testing.T) {
	var hits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusBadRequest) // 400 永久失败
	}))
	defer upstream.Close()

	st, cfg, log := setupTestEnv(t)
	defer st.Close()

	sd := sender.New(cfg.Retry.HTTPTimeout)
	wk := New(cfg, st, sd, log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go wk.Run(ctx)

	apiSrv := api.New(cfg, st, log)
	srv := httptest.NewServer(apiSrv.Routes())
	defer srv.Close()

	id := submit(t, srv.URL, map[string]any{
		"biz_system": "test", "biz_id": "perm-1",
		"url": upstream.URL, "method": "POST",
		"body": map[string]string{"x": "y"}, "max_attempts": 5,
	})

	if got := waitStatus(t, srv.URL, id, "dead_letter", 5*time.Second); got != "dead_letter" {
		t.Fatalf("expected dead_letter, got %s", got)
	}
	if h := atomic.LoadInt32(&hits); h != 1 {
		t.Fatalf("expected exactly 1 hit (no retry on 4xx), got %d", h)
	}
}

// TestSubmit_DuplicateReturnsExisting 验证入口去重：同 (biz_system, biz_id) 返回已存在记录。
func TestSubmit_DuplicateReturnsExisting(t *testing.T) {
	st, cfg, log := setupTestEnv(t)
	defer st.Close()

	apiSrv := api.New(cfg, st, log)
	srv := httptest.NewServer(apiSrv.Routes())
	defer srv.Close()

	payload := map[string]any{
		"biz_system": "test", "biz_id": "dup-1",
		"url": "https://example.com/x", "method": "POST",
		"body": map[string]string{"a": "b"},
	}
	id1 := submit(t, srv.URL, payload)
	body, _ := json.Marshal(payload)
	resp2, err := http.Post(srv.URL+"/api/v1/notifications",
		"application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("second submit: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("dup submit status=%d", resp2.StatusCode)
	}
	var got struct {
		ID        string `json:"id"`
		Duplicate bool   `json:"duplicate"`
	}
	_ = json.NewDecoder(resp2.Body).Decode(&got)
	if !got.Duplicate {
		t.Fatalf("expected duplicate=true")
	}
	if got.ID != id1 {
		t.Fatalf("expected same id %s, got %s", id1, got.ID)
	}
}

// ---------- helpers ----------

func setupTestEnv(t *testing.T) (store.Store, *config.Config, *slog.Logger) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.NewSQLiteStore(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("init store: %v", err)
	}
	cfg := config.Default()
	cfg.Worker.PollInterval = 50 * time.Millisecond
	cfg.Worker.LeaseTimeout = 5 * time.Second
	cfg.Worker.ReaperInterval = 1 * time.Second
	cfg.Worker.Concurrency = 2
	cfg.Retry.BaseBackoff = 50 * time.Millisecond
	cfg.Retry.MaxBackoff = 200 * time.Millisecond
	cfg.Retry.HTTPTimeout = 2 * time.Second
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return st, cfg, log
}

func submit(t *testing.T, baseURL string, payload map[string]any) string {
	t.Helper()
	body, _ := json.Marshal(payload)
	resp, err := http.Post(baseURL+"/api/v1/notifications",
		"application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("submit status=%d body=%s", resp.StatusCode, raw)
	}
	var r struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return r.ID
}

func waitStatus(t *testing.T, baseURL, id, want string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/api/v1/notifications/" + id)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		var r struct {
			Status string `json:"status"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&r)
		resp.Body.Close()
		last = r.Status
		if r.Status == want {
			return r.Status
		}
		time.Sleep(50 * time.Millisecond)
	}
	return last
}