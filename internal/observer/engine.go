package observer

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/pmezard/go-difflib/difflib"
)

type Config struct {
	Workers          int
	QueueSize        int
	RequestTimeout   time.Duration
	MaxHTMLSize      int64
	VersionRetention int
	SchedulerTick    time.Duration
}

func DefaultConfig() Config {
	return Config{20, 100, 5 * time.Second, 2 << 20, 20, 100 * time.Millisecond}
}

type Engine struct {
	mu      sync.Mutex
	store   *Store
	cfg     Config
	client  *http.Client
	pages   map[int64]*Page
	queue   chan int64
	wake    chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	active  map[int64]context.CancelFunc
	wg      sync.WaitGroup
	done    chan struct{}
	stopped bool
}

func NewEngine(store *Store, cfg Config, client *http.Client) (*Engine, error) {
	if cfg.Workers < 1 || cfg.QueueSize < 1 || cfg.RequestTimeout <= 0 || cfg.MaxHTMLSize < 1 || cfg.MaxHTMLSize > 1<<30 || cfg.VersionRetention < 1 || cfg.SchedulerTick <= 0 {
		return nil, fmt.Errorf("invalid engine configuration")
	}
	pages, err := store.loadPages()
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{store: store, cfg: cfg, client: client, pages: map[int64]*Page{}, queue: make(chan int64, cfg.QueueSize), wake: make(chan struct{}, cfg.Workers), ctx: ctx, cancel: cancel, active: map[int64]context.CancelFunc{}, done: make(chan struct{})}
	for i := range pages {
		p := pages[i]
		e.pages[p.ID] = &p
	}
	e.wg.Add(cfg.Workers + 1)
	go e.scheduler()
	for i := 0; i < cfg.Workers; i++ {
		go e.worker()
	}
	return e, nil
}

func (e *Engine) Add(in PageInput) (Page, error) {
	if err := validateInput(in); err != nil {
		return Page{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return Page{}, ErrStopped
	}
	p := Page{URL: in.URL, Interval: in.Interval, Ignore: append([]string{}, in.Ignore...), State: "idle", CreatedAt: time.Now().UTC(), NextCheckAt: time.Now().UTC(), BaselinePending: true}
	if err := e.store.create(&p); err != nil {
		return Page{}, err
	}
	e.pages[p.ID] = &p
	return clonePage(p), nil
}

func (e *Engine) List(includeDeleted bool) []Page {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := []Page{}
	for _, p := range e.pages {
		if p.DeletedAt == nil || includeDeleted {
			out = append(out, clonePage(*p))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (e *Engine) Get(id int64) (Page, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.pages[id]
	if !ok {
		return Page{}, ErrNotFound
	}
	return clonePage(*p), nil
}

func (e *Engine) Patch(id int64, in PagePatch) (Page, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return Page{}, ErrStopped
	}
	p, ok := e.pages[id]
	if !ok || p.DeletedAt != nil {
		return Page{}, ErrNotFound
	}
	if p.State != "idle" {
		return Page{}, ErrBusy
	}
	next := clonePage(*p)
	if in.URL != nil {
		next.URL = *in.URL
	}
	if in.Interval != nil {
		next.Interval = *in.Interval
	}
	if in.Ignore != nil {
		next.Ignore = append([]string{}, (*in.Ignore)...)
	}
	if err := validateInput(PageInput{next.URL, next.Interval, next.Ignore}); err != nil {
		return Page{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if next.URL != p.URL || !reflect.DeepEqual(next.Ignore, p.Ignore) {
		next.BaselinePending = true
	}
	next.NextCheckAt = time.Now().UTC()
	if err := e.store.update(next); err != nil {
		return Page{}, err
	}
	*p = next
	return clonePage(next), nil
}

func (e *Engine) Delete(id int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return ErrStopped
	}
	p, ok := e.pages[id]
	if !ok || p.DeletedAt != nil {
		return ErrNotFound
	}
	now := time.Now().UTC()
	if err := e.store.softDelete(id, now); err != nil {
		return err
	}
	p.DeletedAt = &now
	if cancel := e.active[id]; cancel != nil {
		cancel()
	}
	return nil
}

func (e *Engine) Enqueue(id int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return ErrStopped
	}
	p, ok := e.pages[id]
	if !ok || p.DeletedAt != nil {
		return ErrNotFound
	}
	return e.enqueueLocked(p)
}

func (e *Engine) enqueueLocked(p *Page) error {
	if p.State != "idle" {
		return ErrBusy
	}
	// The state and nonblocking send are protected by the same lock used by workers.
	p.State = "queued"
	select {
	case e.queue <- p.ID:
		select {
		case e.wake <- struct{}{}:
		default:
		}
		return nil
	default:
		p.State = "idle"
		return ErrQueueFull
	}
}

func (e *Engine) scheduler() {
	defer e.wg.Done()
	ticker := time.NewTicker(e.cfg.SchedulerTick)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case now := <-ticker.C:
			e.schedule(now)
		}
	}
}

func (e *Engine) schedule(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return
	}
	due := []*Page{}
	for _, p := range e.pages {
		if p.DeletedAt == nil && !p.NextCheckAt.After(now) {
			due = append(due, p)
		}
	}
	// Oldest due time first prevents queue saturation from starving later page IDs.
	sort.Slice(due, func(i, j int) bool {
		if due[i].NextCheckAt.Equal(due[j].NextCheckAt) {
			return due[i].ID < due[j].ID
		}
		return due[i].NextCheckAt.Before(due[j].NextCheckAt)
	})
	for _, p := range due {
		err := e.enqueueLocked(p)
		if err == ErrQueueFull {
			continue
		}
		d, _ := time.ParseDuration(p.Interval)
		// Skip missed slots, including slots that occurred while queued/running.
		slots := now.Sub(p.NextCheckAt)/d + 1
		p.NextCheckAt = p.NextCheckAt.Add(slots * d)
	}
}

func (e *Engine) worker() {
	defer e.wg.Done()
	for {
		e.mu.Lock()
		if e.stopped {
			e.mu.Unlock()
			return
		}
		var id int64
		// Dequeue and queued → running share the scheduler lock. A worker
		// must not free a queue slot while the page still looks queued.
		select {
		case id = <-e.queue:
		default:
			e.mu.Unlock()
			select {
			case <-e.ctx.Done():
				return
			case <-e.wake:
			}
			continue
		}
		p := e.pages[id]
		if p.DeletedAt != nil {
			p.State = "idle"
			e.mu.Unlock()
			continue
		}
		p.State = "running"
		snapshot := clonePage(*p)
		ctx, cancel := context.WithCancel(e.ctx)
		e.active[id] = cancel
		e.mu.Unlock()
		started := time.Now()
		r := fetch(ctx, e.client, snapshot, e.cfg.RequestTimeout, e.cfg.MaxHTMLSize)
		cancel()
		e.mu.Lock()
		if p.DeletedAt == nil {
			if err := e.complete(p, &r, started); err != nil {
				slog.Error("save check", "pageId", id, "error", err)
			}
		}
		delete(e.active, id)
		p.State = "idle"
		e.mu.Unlock()
	}
}

func (e *Engine) complete(p *Page, r *fetchResult, started time.Time) error {
	c := &r.check
	var v *Version
	baseline := p.BaselinePending
	if c.Status == "success" {
		previous, err := e.store.latestVersion(p.ID)
		if err != nil {
			return err
		}
		switch {
		case previous == nil:
			c.Status = "initial"
		case baseline:
			c.Status = "baseline"
		case previous.Content == r.content:
			c.Status = "unchanged"
			c.VersionID = &previous.ID
		default:
			c.Status = "changed"
			c.Diff, err = difflib.GetUnifiedDiffString(difflib.UnifiedDiff{A: difflib.SplitLines(previous.Content), B: difflib.SplitLines(r.content), FromFile: fmt.Sprintf("version-%d", previous.ID), ToFile: "current", Context: 3})
			if err != nil {
				return err
			}
		}
		if c.Status != "unchanged" {
			v = &Version{PageID: p.ID, CheckedAt: c.CheckedAt, HTTPStatus: c.HTTPStatus, Content: r.content, Hash: fmt.Sprintf("%x", sha256.Sum256([]byte(r.content))), ContentSize: r.size}
		}
		baseline = false
	}
	c.DurationMS = time.Since(started).Milliseconds()
	if err := e.store.saveCheck(c, v, e.cfg.VersionRetention, baseline); err != nil {
		return err
	}
	p.BaselinePending = baseline
	at := c.CheckedAt
	p.LastCheckedAt = &at
	p.LastStatus = c.Status
	p.FinalURL = c.FinalURL
	return nil
}

func (e *Engine) History(id int64, limit, offset int, changes bool) ([]Check, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.pages[id]; !ok {
		return nil, ErrNotFound
	}
	return e.store.history(id, limit, offset, changes)
}
func (e *Engine) Versions(id int64, limit, offset int) ([]Version, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.pages[id]; !ok {
		return nil, ErrNotFound
	}
	return e.store.versions(id, limit, offset)
}
func (e *Engine) Stats() (Stats, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, err := e.store.stats()
	if err != nil {
		return st, err
	}
	st.Workers = e.cfg.Workers
	st.QueueCapacity = e.cfg.QueueSize
	for _, p := range e.pages {
		if p.DeletedAt == nil {
			st.Pages++
		}
		if p.State == "queued" {
			st.Queued++
		}
		if p.State == "running" {
			st.Running++
		}
	}
	return st, nil
}

// Stop rejects new jobs immediately and cancels requests. Close the store only
// after this method succeeds, so workers can finish their final transactions.
func (e *Engine) Stop(ctx context.Context) error {
	e.mu.Lock()
	if !e.stopped {
		e.stopped = true
		e.cancel()
		go func() {
			e.wg.Wait()
			e.mu.Lock()
			for _, p := range e.pages {
				p.State = "idle"
			}
			e.mu.Unlock()
			close(e.done)
		}()
	}
	e.mu.Unlock()
	select {
	case <-e.done:
		e.client.CloseIdleConnections()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
