// Package api 暴露内部业务方使用的 HTTP 接口。
//
// 仅 4 个端点：
//   POST /api/v1/notifications              提交一条通知
//   GET  /api/v1/notifications/{id}         查询状态
//   POST /admin/notifications/{id}/requeue  重投死信（无鉴权，假设内部可信）
//   GET  /healthz                            健康检查
package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"rc_dongyang/internal/config"
	"rc_dongyang/internal/model"
	"rc_dongyang/internal/store"
)

// Server HTTP 服务包装。
type Server struct {
	cfg   *config.Config
	store store.Store
	log   *slog.Logger
}

func New(cfg *config.Config, st store.Store, log *slog.Logger) *Server {
	return &Server{cfg: cfg, store: st, log: log}
}

// Routes 注册所有路由。
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/api/v1/notifications", s.handleSubmit)            // POST
	mux.HandleFunc("/api/v1/notifications/", s.handleGet)              // GET /{id}
	mux.HandleFunc("/admin/notifications/", s.handleAdmin)             // POST /{id}/requeue
	return logRequest(s.log, mux)
}

// ---------------- handlers ----------------

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// SubmitRequest 业务方提交通知的入参。
type SubmitRequest struct {
	BizSystem      string            `json:"biz_system"`
	BizID          string            `json:"biz_id"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"` // 不传则用 biz_system+biz_id
	URL            string            `json:"url"`
	Method         string            `json:"method"`
	Headers        map[string]string `json:"headers,omitempty"`
	Body           json.RawMessage   `json:"body,omitempty"` // 透传任意 JSON；非 JSON 内容业务方自行 base64 后再 stringify 或直接传 string
	MaxAttempts    int               `json:"max_attempts,omitempty"`
}

// SubmitResponse 提交结果。
type SubmitResponse struct {
	ID        string         `json:"id"`
	Status    model.Status   `json:"status"`
	Duplicate bool           `json:"duplicate,omitempty"` // 命中入口去重时为 true
	CreatedAt time.Time      `json:"created_at"`
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req SubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if err := validate(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// 默认值
	if req.MaxAttempts <= 0 {
		req.MaxAttempts = s.cfg.Retry.MaxAttempts
	}
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = req.BizSystem + ":" + req.BizID
	}
	if req.Method == "" {
		req.Method = http.MethodPost
	}
	req.Method = strings.ToUpper(req.Method)

	bodyBytes := []byte(req.Body)
	// json.RawMessage 为空时长度为 0，注意区分 null
	if len(bodyBytes) == 0 || string(bodyBytes) == "null" {
		bodyBytes = nil
	}

	now := time.Now()
	n := &model.Notification{
		ID:             newID(),
		BizSystem:      req.BizSystem,
		BizID:          req.BizID,
		IdempotencyKey: req.IdempotencyKey,
		URL:            req.URL,
		Method:         req.Method,
		Headers:        req.Headers,
		Body:           bodyBytes,
		Status:         model.StatusPending,
		Attempts:       0,
		MaxAttempts:    req.MaxAttempts,
		NextRetryAt:    now, // 立即可调度
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	existing, err := s.store.Insert(r.Context(), n)
	if errors.Is(err, store.ErrDuplicate) {
		writeJSON(w, http.StatusOK, SubmitResponse{
			ID:        existing.ID,
			Status:    existing.Status,
			Duplicate: true,
			CreatedAt: existing.CreatedAt,
		})
		return
	}
	if err != nil {
		s.log.Error("insert failed", "err", err)
		writeError(w, http.StatusInternalServerError, "insert failed")
		return
	}

	writeJSON(w, http.StatusAccepted, SubmitResponse{
		ID:        n.ID,
		Status:    n.Status,
		CreatedAt: n.CreatedAt,
	})
}

// GetResponse 状态查询返回。
type GetResponse struct {
	ID             string            `json:"id"`
	BizSystem      string            `json:"biz_system"`
	BizID          string            `json:"biz_id"`
	URL            string            `json:"url"`
	Method         string            `json:"method"`
	Status         model.Status      `json:"status"`
	Attempts       int               `json:"attempts"`
	MaxAttempts    int               `json:"max_attempts"`
	NextRetryAt    time.Time         `json:"next_retry_at"`
	LastError      string            `json:"last_error,omitempty"`
	LastStatusCode int               `json:"last_status_code,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/notifications/")
	id = strings.Trim(id, "/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing id")
		return
	}
	n, err := s.store.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get failed")
		return
	}
	writeJSON(w, http.StatusOK, GetResponse{
		ID: n.ID, BizSystem: n.BizSystem, BizID: n.BizID,
		URL: n.URL, Method: n.Method, Status: n.Status,
		Attempts: n.Attempts, MaxAttempts: n.MaxAttempts,
		NextRetryAt: n.NextRetryAt,
		LastError:   n.LastError, LastStatusCode: n.LastStatusCode,
		CreatedAt: n.CreatedAt, UpdatedAt: n.UpdatedAt,
	})
}

// handleAdmin 处理 /admin/notifications/{id}/requeue
func (s *Server) handleAdmin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/admin/notifications/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) != 2 || parts[1] != "requeue" {
		writeError(w, http.StatusNotFound, "unknown admin endpoint")
		return
	}
	id := parts[0]
	if err := s.store.Requeue(r.Context(), id, time.Now()); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "dead-letter not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "requeue failed: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": id, "status": string(model.StatusPending)})
}

// ---------------- helpers ----------------

func validate(r *SubmitRequest) error {
	if r.BizSystem == "" {
		return fmt.Errorf("biz_system is required")
	}
	if r.BizID == "" {
		return fmt.Errorf("biz_id is required")
	}
	if r.URL == "" {
		return fmt.Errorf("url is required")
	}
	if !strings.HasPrefix(r.URL, "http://") && !strings.HasPrefix(r.URL, "https://") {
		return fmt.Errorf("url must start with http(s)://")
	}
	return nil
}

func newID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// logRequest 极简结构化日志中间件
func logRequest(log *slog.Logger, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: 200}
		h.ServeHTTP(sw, r)
		log.Info("http",
			"method", r.Method, "path", r.URL.Path,
			"status", sw.code, "elapsed", time.Since(start),
		)
	})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(c int) {
	s.code = c
	s.ResponseWriter.WriteHeader(c)
}
