package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pageobserver/internal/observer"
)

func TestAPIWorkflowAndValidation(t *testing.T) {
	s, err := observer.OpenStore(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := observer.DefaultConfig()
	cfg.SchedulerTick = time.Hour
	e, err := observer.NewEngine(s, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { e.Stop(context.Background()); s.Close() }()
	h := New(e)
	request := func(method, path, body string, want int) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s: got %d want %d: %s", method, path, w.Code, want, w.Body.String())
		}
		return w
	}
	request("POST", "/pages", `{"url":"file:///bad","interval":"1s"}`, 400)
	request("POST", "/pages", `{"url":"http://example.com","interval":"1s","unknown":true}`, 400)
	request("POST", "/pages", `{} {}`, 400)
	request("POST", "/pages", `null`, 400)
	request("PATCH", "/pages/999", `null`, 400)
	request("POST", "/pages", strings.Repeat(" ", 65537), 400)
	request("GET", "/pages/nope", "", 400)
	request("GET", "/pages/999", "", 404)
	request("GET", "/pages/999/history?limit=0", "", 400)
	request("GET", "/pages/999/history", "", 404)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		io.WriteString(w, "<p>Hello</p>")
	}))
	defer target.Close()
	w := request("POST", "/pages", `{"url":"`+target.URL+`","interval":"1h","ignore":[".clock"]}`, 201)
	if w.Header().Get("Location") != "/pages/1" {
		t.Fatal(w.Header())
	}
	var p observer.Page
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil || p.ID != 1 || p.State != "idle" {
		t.Fatal(p, err)
	}
	request("GET", "/pages", "", 200)
	request("GET", "/pages/1", "", 200)
	request("PATCH", "/pages/1", `{"ignore":["["]}`, 400)
	request("PATCH", "/pages/1", `{"interval":"2h"}`, 200)
	request("POST", "/pages/1/check", "", 202)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		p, _ = e.Get(1)
		if p.LastStatus == "initial" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if p.LastStatus != "initial" {
		t.Fatal(p)
	}
	for _, path := range []string{"/pages/1/history", "/pages/1/changes", "/pages/1/versions", "/stats", "/health"} {
		request("GET", path, "", 200)
	}
	request("DELETE", "/pages/1", "", 204)
	request("POST", "/pages/1/check", "", 404)
	request("PATCH", "/pages/1", `{"interval":"1s"}`, 404)
	request("GET", "/pages/1/history", "", 200)
	w = request("GET", "/pages", "", 200)
	if strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatal(w.Body.String())
	}
	w = request("GET", "/pages?includeDeleted=true", "", 200)
	if !strings.Contains(w.Body.String(), "deletedAt") {
		t.Fatal(w.Body.String())
	}
	request("DELETE", "/pages/1", "", 404)
	request("GET", "/docs", "", 200)
	request("GET", "/swagger/swagger-ui.css", "", 200)
	request("GET", "/swagger/swagger-ui-bundle.js", "", 200)
	w = request("GET", "/openapi.json", "", 200)
	var spec map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &spec); err != nil {
		t.Fatal(err)
	}
	if spec["openapi"] != "3.0.3" {
		t.Fatal(spec)
	}
	paths := spec["paths"].(map[string]any)
	if len(paths) != 8 {
		t.Fatalf("missing API paths: %v", paths)
	}
}

func TestAPIConflictAndQueueFull(t *testing.T) {
	s, err := observer.OpenStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	cfg := observer.DefaultConfig()
	cfg.Workers = 1
	cfg.QueueSize = 1
	cfg.SchedulerTick = time.Hour
	e, err := observer.NewEngine(s, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { e.Stop(context.Background()); s.Close() }()
	started := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { started <- struct{}{}; <-r.Context().Done() }))
	defer target.Close()
	for i := 0; i < 3; i++ {
		if _, err := e.Add(observer.PageInput{URL: target.URL, Interval: "1h"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Enqueue(1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker not running")
	}
	if err := e.Enqueue(2); err != nil {
		t.Fatal(err)
	}
	h := New(e)
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{{"POST", "/pages/1/check", "", 409}, {"POST", "/pages/2/check", "", 409}, {"POST", "/pages/3/check", "", 503}, {"PATCH", "/pages/1", `{"interval":"1s"}`, 409}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if w.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.path, w.Code, w.Body.String())
		}
	}
	e.Stop(context.Background())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/pages/3/check", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
}
