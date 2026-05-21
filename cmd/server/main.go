// Command server 启动 API + Worker（同进程）。
//
// 使用：
//   go run ./cmd/server                 使用默认配置
//   go run ./cmd/server -c config.yaml  使用配置文件
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rc_dongyang/internal/api"
	"rc_dongyang/internal/config"
	"rc_dongyang/internal/sender"
	"rc_dongyang/internal/store"
	"rc_dongyang/internal/worker"
)

func main() {
	cfgPath := flag.String("c", "config.yaml", "config file path")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Error("load config failed", "err", err)
		os.Exit(1)
	}
	log.Info("config loaded", "addr", cfg.Server.Addr, "dsn", cfg.Storage.DSN)

	st, err := store.NewSQLiteStore(cfg.Storage.DSN)
	if err != nil {
		log.Error("init store failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	sd := sender.New(cfg.Retry.HTTPTimeout)
	wk := worker.New(cfg, st, sd, log.With("component", "worker"))
	apiSrv := api.New(cfg, st, log.With("component", "api"))

	// 用一个 root ctx 控制所有 goroutine 的优雅停机
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启 worker
	workerDone := make(chan struct{})
	go func() {
		wk.Run(rootCtx)
		close(workerDone)
	}()

	// 启 HTTP server
	httpSrv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           apiSrv.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("http server starting", "addr", cfg.Server.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http serve failed", "err", err)
			cancel()
		}
	}()

	// 等待信号
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutdown signal received")

	// 1. 先停 HTTP（不再接收新任务）
	shutdownCtx, sCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer sCancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown error", "err", err)
	}

	// 2. 再停 worker（取消 rootCtx）；worker 不再拉新任务，in-flight 由 HTTP 超时兜底
	cancel()

	select {
	case <-workerDone:
		log.Info("graceful shutdown done")
	case <-time.After(cfg.Server.ShutdownTimeout):
		log.Warn("worker shutdown timeout")
	}
}
