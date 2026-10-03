package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pageobserver/internal/api"
	"pageobserver/internal/observer"
)

func main() {
	if err := run(); err != nil {
		slog.Error("service stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	cfg := observer.DefaultConfig()
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	dbPath := flag.String("db", "data/observer.db", "SQLite database file")
	flag.IntVar(&cfg.Workers, "workers", cfg.Workers, "maximum simultaneous checks")
	flag.IntVar(&cfg.QueueSize, "queue-size", cfg.QueueSize, "maximum waiting jobs")
	flag.DurationVar(&cfg.RequestTimeout, "timeout", cfg.RequestTimeout, "request timeout")
	flag.Int64Var(&cfg.MaxHTMLSize, "max-html", cfg.MaxHTMLSize, "maximum HTML size in bytes")
	flag.IntVar(&cfg.VersionRetention, "versions", cfg.VersionRetention, "retained versions per page")
	flag.DurationVar(&cfg.SchedulerTick, "tick", cfg.SchedulerTick, "scheduler polling interval")
	flag.Parse()
	store, err := observer.OpenStore(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	engine, err := observer.NewEngine(store, cfg, nil)
	if err != nil {
		return err
	}
	server := &http.Server{Addr: *addr, Handler: api.New(engine), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	served := make(chan error, 1)
	go func() {
		slog.Info("Page Observer started", "address", *addr, "docs", "http://"+*addr+"/docs")
		served <- server.ListenAndServe()
	}()
	select {
	case <-signals.Done():
		slog.Info("shutting down")
	case err = <-served:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	engineDone := make(chan error, 1)
	go func() { engineDone <- engine.Stop(ctx) }()
	if shutErr := server.Shutdown(ctx); shutErr != nil {
		server.Close()
		if err == nil {
			err = shutErr
		}
	}
	if stopErr := <-engineDone; stopErr != nil {
		// Never close SQLite underneath a worker transaction.
		engine.Stop(context.Background())
		if err == nil {
			err = stopErr
		}
	}
	return err
}
