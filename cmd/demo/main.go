// Demo serves controllable HTML fixtures for the assignment presentation.
package main

import (
	"context"
	"flag"
	"fmt"
	"html"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8090", "demo listen address")
	flag.Parse()
	var version atomic.Int64
	version.Store(1)
	mux := http.NewServeMux()
	page := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, `<!doctype html><html><body><h1>Новости</h1><div class="clock">%s</div><p>Version %d</p><p>Hello from %s</p></body></html>`, time.Now().Format("15:04:05.000"), version.Load(), html.EscapeString(r.URL.Path))
	}
	mux.HandleFunc("GET /page", page)
	mux.HandleFunc("GET /static", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, "<h1>Stable page</h1><p>Version %d</p>", version.Load())
	})
	mux.HandleFunc("POST /change", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, "Version %d\n", version.Add(1)) })
	mux.HandleFunc("GET /slow", func(w http.ResponseWriter, r *http.Request) {
		delay := 10 * time.Second
		if value := r.URL.Query().Get("delay"); value != "" {
			d, err := time.ParseDuration(value)
			if err != nil || d < 0 || d > time.Minute {
				http.Error(w, "delay must be 0..1m", 400)
				return
			}
			delay = d
		}
		select {
		case <-time.After(delay):
			page(w, r)
		case <-r.Context().Done():
			return
		}
	})
	mux.HandleFunc("GET /error", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "demo HTTP 500", 500) })
	mux.HandleFunc("GET /pdf", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		fmt.Fprint(w, "%PDF-1.4 demo")
	})
	mux.HandleFunc("GET /large", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, "<p>"+strings.Repeat("x", 3<<20)+"</p>")
	})
	mux.HandleFunc("GET /redirect", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/page", 302) })
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, `<h1>Demo fixtures</h1><p>GET /page, /static, /slow?delay=10s, /error, /pdf, /large, /redirect</p><p>POST /change increments the content version.</p>`)
	})
	server := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		server.Shutdown(shutdown)
	}()
	slog.Info("demo server", "address", *addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("demo stopped", "error", err)
		os.Exit(1)
	}
}
