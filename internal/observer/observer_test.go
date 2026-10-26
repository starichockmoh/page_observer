package observer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func manualConfig() Config {
	c := DefaultConfig()
	c.SchedulerTick = time.Hour
	c.RequestTimeout = time.Second
	return c
}
func newTestEngine(t *testing.T, cfg Config, client *http.Client) *Engine {
	t.Helper()
	s, err := OpenStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := NewEngine(s, cfg, client)
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := e.Stop(ctx); err != nil {
			t.Error(err)
		}
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return e
}
func addPage(t *testing.T, e *Engine, url string, ignore ...string) Page {
	t.Helper()
	p, err := e.Add(PageInput{url, "1h", ignore})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
func runCheck(t *testing.T, e *Engine, p Page, want string) Check {
	t.Helper()
	before, err := e.History(context.Background(), p.ID, 500, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Enqueue(p.ID); err != nil {
		t.Fatal(err)
	}
	var result Check
	eventually(t, func() bool {
		h, err := e.History(context.Background(), p.ID, 500, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(h) > len(before) {
			result = h[0]
			current, err := e.Get(p.ID)
			return err == nil && current.State == "idle"
		}
		return false
	})
	if result.Status != want {
		t.Fatalf("status=%s want=%s error=%s", result.Status, want, result.Error)
	}
	return result
}

func TestInitialUnchangedChangedIgnoreAndBaseline(t *testing.T) {
	var body atomic.Value
	body.Store(`<h1>Version 1</h1><div class="clock">12:00</div><p>Hello</p>`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, body.Load().(string))
	}))
	defer srv.Close()
	e := newTestEngine(t, manualConfig(), nil)
	p := addPage(t, e, srv.URL)
	runCheck(t, e, p, "initial")
	runCheck(t, e, p, "unchanged")
	body.Store(`<h1>Version 1</h1><div class="clock">12:01</div><p>Hello</p>`)
	c := runCheck(t, e, p, "changed")
	if !strings.Contains(c.Diff, "-12:00") || !strings.Contains(c.Diff, "+12:01") {
		t.Fatal(c.Diff)
	}
	ignore := []string{".clock"}
	if _, err := e.Patch(p.ID, PagePatch{Ignore: &ignore}); err != nil {
		t.Fatal(err)
	}
	c = runCheck(t, e, p, "baseline")
	if c.Diff != "" {
		t.Fatal("settings change produced diff")
	}
	body.Store(`<h1>Version 1</h1><div class="clock">12:02</div><p>Hello</p>`)
	runCheck(t, e, p, "unchanged")
	body.Store(`<h1>Version 2</h1><div class="clock">12:03</div><p>Goodbye</p>`)
	c = runCheck(t, e, p, "changed")
	if !strings.Contains(c.Diff, "-Hello") || !strings.Contains(c.Diff, "+Goodbye") {
		t.Fatal(c.Diff)
	}
	versions, err := e.Versions(context.Background(), p.ID, 100, 0)
	if err != nil || len(versions) != 4 {
		t.Fatalf("versions=%v err=%v", versions, err)
	}
	st, err := e.Stats(context.Background())
	if err != nil || st.ChecksTotal != 6 || st.Changed != 2 || st.Errors != 0 {
		t.Fatalf("stats=%+v err=%v", st, err)
	}
}

func TestNormalize(t *testing.T) {
	cases := []struct{ a, b, want string }{
		{"<h1>\n Новости\n</h1>", "<h1>Новости</h1>", "Новости"},
		{"<p>Hello\n    world</p>", "<p>Hello world</p>", "Hello world"},
		{"<p>Hel<b>lo</b> &amp; goodbye</p>", "<p>Hello &amp; goodbye</p>", "Hello & goodbye"},
		{"<head><title>hidden</title></head><p>Hello</p><script>noise</script><style>noise</style>", "<p>Hello</p>", "Hello"},
	}
	for _, c := range cases {
		a, err := Normalize(strings.NewReader(c.a), nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := Normalize(strings.NewReader(c.b), nil)
		if err != nil || a != b || a != c.want {
			t.Fatalf("a=%q b=%q want=%q err=%v", a, b, c.want, err)
		}
	}
	text, err := Normalize(strings.NewReader(`<main><div class="ad"><p>noise</p></div><p id="counter">noise</p><p>Hello</p></main>`), []string{"main .ad", "#counter"})
	if err != nil || text != "Hello" {
		t.Fatalf("%q %v", text, err)
	}
}

func TestNormalizeIgnoreMatchesOriginalDOM(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
		rules            []string
	}{
		{"first-child", `<main><p class="ad">advert</p><p>news</p></main>`, "news", []string{".ad", "main > p:first-child"}},
		{"adjacent-sibling", `<main><p class="ad">advert</p><p>noise</p><p>news</p></main>`, "news", []string{".ad", ".ad + p"}},
		{"overlapping-subtrees", `<main><div class="ad"><p>noise</p></div><p>news</p></main>`, "news", []string{".ad", ".ad p", "div.ad"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, rules := range [][]string{tc.rules, reverseRules(tc.rules)} {
				got, err := Normalize(strings.NewReader(tc.body), rules)
				if err != nil || got != tc.want {
					t.Fatalf("rules=%q: got %q, want %q, err=%v", rules, got, tc.want, err)
				}
			}
		})
	}
}

func reverseRules(rules []string) []string {
	out := append([]string(nil), rules...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func TestCheckPreparationDoesNotBlockEngine(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/html"}}, Body: io.NopCloser(strings.NewReader("<p>news</p>")), Request: r}, nil
	})}
	e := newTestEngine(t, manualConfig(), client)
	p := addPage(t, e, "http://example.test")

	// Occupy the reader pool so the worker pauses in preparation, just
	// before comparison. WaitCount identifies this point without CPU timing.
	e.store.readers.SetMaxOpenConns(1)
	conn, err := e.store.readers.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waits := e.store.readers.Stats().WaitCount
	if err := e.Enqueue(p.ID); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return e.store.readers.Stats().WaitCount > waits })
	responsive := make(chan error, 1)
	go func() {
		current, err := e.Get(p.ID)
		if err == nil && current.State != "running" {
			err = fmt.Errorf("state=%s, want running", current.State)
		}
		if err == nil && !errors.Is(e.Enqueue(p.ID), ErrBusy) {
			err = errors.New("duplicate check accepted during preparation")
		}
		interval := "2h"
		if err == nil {
			if _, patchErr := e.Patch(p.ID, PagePatch{Interval: &interval}); !errors.Is(patchErr, ErrBusy) {
				err = fmt.Errorf("patch during preparation: %v", patchErr)
			}
		}
		responsive <- err
	}()
	select {
	case err := <-responsive:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("check preparation holds the engine mutex")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- e.Stop(ctx) }()
	select {
	case err := <-stopped:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop while preparation is blocked: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop cannot observe its deadline during preparation")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	h, err := e.History(context.Background(), p.ID, 100, 0, false)
	if err != nil || len(h) != 1 || h[0].Status != "initial" {
		t.Fatalf("shutdown lost the completed result: %v, err=%v", h, err)
	}
}

func TestDeleteDiscardsPreparedCheck(t *testing.T) {
	e := newTestEngine(t, manualConfig(), nil)
	p := addPage(t, e, "http://example.test")
	r := fetchResult{check: Check{PageID: p.ID, Status: "success", CheckedAt: time.Now().UTC(), OriginalURL: p.URL, RedirectChain: []string{}}, content: "news"}
	v, baseline, err := e.prepareCheck(p, &r, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Delete(p.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.complete(p.ID, &r.check, v, baseline); err != nil {
		t.Fatal(err)
	}
	h, err := e.History(context.Background(), p.ID, 100, 0, false)
	if err != nil || len(h) != 0 {
		t.Fatalf("deleted page received history: %v, err=%v", h, err)
	}
	versions, err := e.Versions(context.Background(), p.ID, 100, 0)
	if err != nil || len(versions) != 0 {
		t.Fatalf("deleted page received a version: %v, err=%v", versions, err)
	}
	current, err := e.Get(p.ID)
	if err != nil || current.LastCheckedAt != nil || !current.BaselinePending {
		t.Fatalf("deleted page state was changed: %+v, err=%v", current, err)
	}
}

func TestHTTPErrorRetryNonHTMLAndSize(t *testing.T) {
	var mode atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch mode.Load() {
		case 0:
			w.WriteHeader(500)
		case 1:
			w.Header().Set("Content-Type", "text/html")
			io.WriteString(w, "<p>OK</p>")
		case 2:
			w.Header().Set("Content-Type", "application/pdf")
			io.WriteString(w, "%PDF")
		case 3:
			w.Header().Set("Content-Type", "text/html")
			io.WriteString(w, strings.Repeat("a", 65))
		case 4:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	cfg := manualConfig()
	cfg.MaxHTMLSize = 64
	e := newTestEngine(t, cfg, nil)
	p := addPage(t, e, srv.URL)
	c := runCheck(t, e, p, "http_error")
	if c.HTTPStatus != 500 {
		t.Fatal(c)
	}
	mode.Store(1)
	runCheck(t, e, p, "initial")
	mode.Store(2)
	runCheck(t, e, p, "non_html")
	mode.Store(3)
	c = runCheck(t, e, p, "html_too_large")
	if c.HTTPStatus != 200 {
		t.Fatal(c)
	}
	mode.Store(4)
	runCheck(t, e, p, "http_error")
	mode.Store(1)
	runCheck(t, e, p, "unchanged")
	st, _ := e.Stats(context.Background())
	if st.Errors != 4 || st.HTTP4xx != 1 || st.HTTP5xx != 1 {
		t.Fatal(st)
	}
}

func TestTimeoutDoesNotBlockOtherPages(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			<-r.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "text/html")
		io.WriteString(w, "<p>fast</p>")
	}))
	defer srv.Close()
	cfg := manualConfig()
	cfg.Workers = 2
	cfg.RequestTimeout = 50 * time.Millisecond
	e := newTestEngine(t, cfg, nil)
	slow := addPage(t, e, srv.URL+"/slow")
	fast := addPage(t, e, srv.URL+"/fast")
	if err := e.Enqueue(slow.ID); err != nil {
		t.Fatal(err)
	}
	runCheck(t, e, fast, "initial")
	eventually(t, func() bool {
		h, _ := e.History(context.Background(), slow.ID, 100, 0, false)
		return len(h) == 1 && h[0].Status == "timeout"
	})
	runCheck(t, e, slow, "timeout")
}

func TestSlowPageHasNoDuplicateChecks(t *testing.T) {
	var active, peak, count atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		if n > peak.Load() {
			peak.Store(n)
		}
		count.Add(1)
		select {
		case <-time.After(90 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "text/html")
		io.WriteString(w, "<p>OK</p>")
	}))
	defer srv.Close()
	cfg := manualConfig()
	cfg.SchedulerTick = 5 * time.Millisecond
	e := newTestEngine(t, cfg, nil)
	p, err := e.Add(PageInput{srv.URL, "10ms", nil})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return count.Load() >= 3 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if peak.Load() != 1 {
		t.Fatalf("overlapping requests: %d", peak.Load())
	}
	h, _ := e.History(context.Background(), p.ID, 100, 0, false)
	if len(h) < 2 {
		t.Fatal("scheduler did not resume")
	}
}

func TestWorkerLimitBoundedQueueAndFairness(t *testing.T) {
	var active, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		w.Header().Set("Content-Type", "text/html")
		io.WriteString(w, "<p>OK</p>")
	}))
	defer srv.Close()
	cfg := manualConfig()
	cfg.Workers = 5
	cfg.QueueSize = 7
	cfg.SchedulerTick = 2 * time.Millisecond
	e := newTestEngine(t, cfg, nil)
	for i := 0; i < 100; i++ {
		addPage(t, e, srv.URL)
	}
	eventually(t, func() bool {
		st, err := e.Stats(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if st.Queued > 7 || st.Running > 5 {
			t.Fatalf("unbounded %+v", st)
		}
		return st.ChecksTotal == 100
	})
	if peak.Load() > 5 || peak.Load() < 2 {
		t.Fatalf("worker peak %d", peak.Load())
	}
	for _, p := range e.List(false) {
		if p.LastStatus != "initial" {
			t.Fatalf("starved page %d", p.ID)
		}
	}
}

func TestQueueFullBusyDeleteAndShutdown(t *testing.T) {
	started := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { started <- struct{}{}; <-r.Context().Done() }))
	defer srv.Close()
	cfg := manualConfig()
	cfg.Workers = 1
	cfg.QueueSize = 1
	e := newTestEngine(t, cfg, nil)
	p := addPage(t, e, srv.URL)
	q := addPage(t, e, srv.URL)
	r := addPage(t, e, srv.URL)
	if err := e.Enqueue(p.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("not started")
	}
	if err := e.Enqueue(q.ID); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(e.Enqueue(q.ID), ErrBusy) || !errors.Is(e.Enqueue(p.ID), ErrBusy) {
		t.Fatal("duplicate accepted")
	}
	if !errors.Is(e.Enqueue(r.ID), ErrQueueFull) {
		t.Fatal("queue not bounded")
	}
	rp, _ := e.Get(r.ID)
	if rp.State != "idle" {
		t.Fatal("queue full left state queued")
	}
	interval := "2s"
	if _, err := e.Patch(p.ID, PagePatch{Interval: &interval}); !errors.Is(err, ErrBusy) {
		t.Fatal("busy patch accepted")
	}
	if err := e.Delete(q.ID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	for _, p := range e.List(true) {
		if p.State != "idle" {
			t.Fatal(p)
		}
	}
	if !errors.Is(e.Enqueue(r.ID), ErrStopped) {
		t.Fatal("enqueue after shutdown")
	}
	h, _ := e.History(context.Background(), p.ID, 100, 0, false)
	if len(h) != 1 || h[0].Status != "cancelled" {
		t.Fatal(h)
	}
	h, _ = e.History(context.Background(), q.ID, 100, 0, false)
	if len(h) != 0 {
		t.Fatal("deleted queued page was checked")
	}
}

func TestConcurrentEnqueueIsAtomic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer srv.Close()
	e := newTestEngine(t, manualConfig(), nil)
	p := addPage(t, e, srv.URL)
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.Enqueue(p.ID); err == nil {
				accepted.Add(1)
			} else if !errors.Is(err, ErrBusy) {
				t.Errorf("enqueue: %v", err)
			}
			e.Get(p.ID)
			e.List(false)
			e.Stats(context.Background())
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("accepted %d duplicates", accepted.Load())
	}
}

func TestRedirectsHTTPToHTTPS(t *testing.T) {
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		io.WriteString(w, "<p>secure</p>")
	}))
	defer target.Close()
	for _, code := range []int{301, 302, 307, 308} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, code) }))
			defer origin.Close()
			e := newTestEngine(t, manualConfig(), target.Client())
			p := addPage(t, e, origin.URL)
			c := runCheck(t, e, p, "initial")
			if c.OriginalURL != origin.URL || c.FinalURL != target.URL || len(c.RedirectChain) != 1 {
				t.Fatal(c)
			}
		})
	}
}

func TestRestartPersistenceRetentionAndSoftDelete(t *testing.T) {
	var body atomic.Value
	body.Store("<p>1</p>")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		io.WriteString(w, body.Load().(string))
	}))
	defer srv.Close()
	path := filepath.Join(t.TempDir(), "persistent.db")
	cfg := manualConfig()
	cfg.VersionRetention = 2
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	e, err := NewEngine(s, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	p := addPage(t, e, srv.URL, ".clock")
	runCheck(t, e, p, "initial")
	for i := 2; i <= 4; i++ {
		body.Store(fmt.Sprintf("<p>%d</p>", i))
		runCheck(t, e, p, "changed")
	}
	q := addPage(t, e, srv.URL)
	runCheck(t, e, q, "initial")
	if err := e.Delete(q.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`DROP INDEX IF EXISTS checks_version`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.SchedulerTick = 5 * time.Millisecond
	e, err = NewEngine(s, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { e.Stop(context.Background()); s.Close() }()
	var planID, parent, unused int
	var plan string
	if err := s.db.QueryRow(`EXPLAIN QUERY PLAN SELECT rowid FROM checks WHERE version_id=?`, 1).Scan(&planID, &parent, &unused, &plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "checks_version") {
		t.Fatalf("version references require a full history scan after reopening: %s", plan)
	}
	loaded, _ := e.Get(p.ID)
	if loaded.Interval != "1h" || len(loaded.Ignore) != 1 {
		t.Fatal(loaded)
	}
	eventually(t, func() bool {
		h, _ := e.History(context.Background(), p.ID, 100, 0, false)
		return len(h) == 5 && h[0].Status == "unchanged"
	})
	v, err := e.Versions(context.Background(), p.ID, 100, 0)
	if err != nil || len(v) != 2 {
		t.Fatalf("retention %v %v", v, err)
	}
	h, err := e.History(context.Background(), p.ID, 100, 0, true)
	if err != nil || len(h) != 3 || h[2].Diff == "" {
		t.Fatalf("pruning lost history diff: %v, err=%v", h, err)
	}
	if h[2].VersionID != nil || h[0].VersionID == nil || h[1].VersionID == nil {
		t.Fatalf("pruning changed the wrong version references: %+v", h)
	}
	deleted, _ := e.Get(q.ID)
	if deleted.DeletedAt == nil {
		t.Fatal("soft delete lost")
	}
	qh, _ := e.History(context.Background(), q.ID, 100, 0, false)
	if len(qh) != 1 {
		t.Fatal("deleted page rechecked or history lost")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestNetworkErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status string
	}{
		{&net.DNSError{Err: "no such host", Name: "invalid.test"}, "dns_error"},
		{&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, "connection_error"},
		{context.DeadlineExceeded, "timeout"},
	} {
		t.Run(tc.status, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, tc.err })}
			r := fetch(context.Background(), client, Page{URL: "http://invalid.test"}, time.Second, 64)
			if r.check.Status != tc.status {
				t.Fatal(r.check)
			}
		})
	}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	r := fetch(context.Background(), http.DefaultClient, Page{URL: srv.URL}, time.Second, 64)
	if r.check.Status != "tls_error" {
		t.Fatal(r.check)
	}
}

func TestInvalidInput(t *testing.T) {
	e := newTestEngine(t, manualConfig(), nil)
	for _, in := range []PageInput{{"file:///etc/passwd", "1s", nil}, {"http://example.com", "0s", nil}, {"http://example.com", "bad", nil}, {"http://example.com", "1s", []string{"["}}, {"http://u:p@example.com", "1s", nil}} {
		if _, err := e.Add(in); !errors.Is(err, ErrInvalid) {
			t.Fatalf("accepted invalid input %+v: %v", in, err)
		}
	}
}

func TestBaselineFlagSurvivesErrorAndRestart(t *testing.T) {
	var broken atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if broken.Load() {
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		io.WriteString(w, `<p>Hello</p><span class="clock">12:00</span>`)
	}))
	defer srv.Close()
	path := filepath.Join(t.TempDir(), "baseline.db")
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	e, err := NewEngine(s, manualConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	p := addPage(t, e, srv.URL)
	runCheck(t, e, p, "initial")
	ignore := []string{".clock"}
	if _, err := e.Patch(p.ID, PagePatch{Ignore: &ignore}); err != nil {
		t.Fatal(err)
	}
	broken.Store(true)
	runCheck(t, e, p, "http_error")
	e.Stop(context.Background())
	s.Close()
	s, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	e, err = NewEngine(s, manualConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { e.Stop(context.Background()); s.Close() }()
	loaded, _ := e.Get(p.ID)
	if !loaded.BaselinePending {
		t.Fatal("failed check lost pending baseline")
	}
	broken.Store(false)
	runCheck(t, e, p, "baseline")
	runCheck(t, e, p, "unchanged")
}

func TestExactHTMLLimitAndChunkedOverflow(t *testing.T) {
	var overflow atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.(http.Flusher).Flush()
		io.WriteString(w, strings.Repeat("a", 64))
		if overflow.Load() {
			io.WriteString(w, "b")
		}
	}))
	defer srv.Close()
	cfg := manualConfig()
	cfg.MaxHTMLSize = 64
	e := newTestEngine(t, cfg, nil)
	p := addPage(t, e, srv.URL)
	runCheck(t, e, p, "initial")
	overflow.Store(true)
	runCheck(t, e, p, "html_too_large")
}

func TestSchedulerContinuesAfterHTTPError(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		io.WriteString(w, "<p>Recovered</p>")
	}))
	defer srv.Close()
	cfg := manualConfig()
	cfg.SchedulerTick = 5 * time.Millisecond
	e := newTestEngine(t, cfg, nil)
	p, err := e.Add(PageInput{URL: srv.URL, Interval: "25ms"})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { h, _ := e.History(context.Background(), p.ID, 100, 0, false); return len(h) >= 2 })
	h, _ := e.History(context.Background(), p.ID, 100, 0, false)
	if h[len(h)-1].Status != "http_error" || h[len(h)-2].Status != "initial" {
		t.Fatal(h)
	}
}
