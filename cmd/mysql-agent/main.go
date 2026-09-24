package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ddchencm/ha/internal/agent"
	"github.com/ddchencm/ha/internal/config"
	"github.com/ddchencm/ha/internal/model"
	mysqlclient "github.com/ddchencm/ha/internal/mysql"
	"github.com/ddchencm/ha/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.LoadAgent()
	if err != nil {
		logger.Error("load agent config", "error", err)
		os.Exit(1)
	}
	caPEM, err := os.ReadFile(cfg.AgentCAFile)
	if err != nil {
		logger.Error("read agent CA", "error", err)
		os.Exit(1)
	}
	clientCAPool := x509.NewCertPool()
	if !clientCAPool.AppendCertsFromPEM(caPEM) {
		logger.Error("agent CA has no certificates")
		os.Exit(1)
	}
	cert, err := tls.LoadX509KeyPair(cfg.AgentCertFile, cfg.AgentKeyFile)
	if err != nil {
		logger.Error("load agent TLS certificate", "error", err)
		os.Exit(1)
	}
	db, err := mysqlclient.NewClient(model.MySQLNodeConfig{ID: cfg.NodeID, Address: cfg.NodeAddress, DSN: cfg.MySQLDSN, ReplicationUser: cfg.ReplicationUser, ReplicationPass: cfg.ReplicationPass})
	if err != nil {
		logger.Error("create local MySQL client", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	stateStore, err := store.NewEtcdStore(cfg.EtcdEndpoints, cfg.ClusterName)
	if err != nil {
		logger.Error("Etcd is required for agent write authorization", "error", err)
		os.Exit(1)
	}
	defer stateStore.Close()
	service, err := agent.NewServer(cfg.NodeID, db, stateStore)
	if err != nil {
		logger.Error("create agent", "error", err)
		os.Exit(1)
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAPool}
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: service.Handler(), TLSConfig: tlsConfig, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 90 * time.Second, IdleTimeout: 60 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	logger.Info("mysql agent started", "node", cfg.NodeID, "addr", cfg.HTTPAddr)
	if err := server.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("agent server stopped", "error", err)
		os.Exit(1)
	}
}
