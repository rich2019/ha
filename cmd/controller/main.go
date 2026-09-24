package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/ddchencm/ha/internal/api"
	"github.com/ddchencm/ha/internal/config"
	"github.com/ddchencm/ha/internal/ha"
	"github.com/ddchencm/ha/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}
	stateStore, err := store.NewEtcdStore(cfg.EtcdEndpoints, cfg.ClusterName)
	if err != nil {
		logger.Warn("etcd unavailable, using in-memory state; persistence is disabled", "error", err)
		stateStore = store.NewMemoryStore(cfg.ClusterName)
	}
	controller, err := ha.NewController(cfg, stateStore, logger)
	if err != nil {
		logger.Error("create controller", "error", err)
		os.Exit(1)
	}
	defer controller.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := controller.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("controller stopped", "error", err)
		}
	}()
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: api.NewServer(controller).Handler()}
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()
	logger.Info("HA controller started", "addr", cfg.HTTPAddr, "cluster", cfg.ClusterName)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("HTTP server stopped", "error", err)
		os.Exit(1)
	}
}
