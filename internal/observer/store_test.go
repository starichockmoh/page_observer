package observer

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func TestStatsMigrationPersistenceAndRollback(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy # database.db")
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if s != nil {
			s.Close()
		}
	})
	p := Page{URL: "http://example.test", Interval: "1h", Ignore: []string{}, CreatedAt: time.Now()}
	if err := s.create(&p); err != nil {
		t.Fatal(err)
	}
	statuses := []string{"initial", "baseline", "unchanged", "changed", "timeout", "http_error", "http_error", "non_html", "html_too_large", "cancelled"}
	for i, status := range statuses {
		c := Check{PageID: p.ID, CheckedAt: time.Now(), Status: status, DurationMS: int64(i + 1)}
		if i == 5 {
			c.HTTPStatus = 404
		}
		if i == 6 {
			c.HTTPStatus = 500
		}
		if err := s.saveCheck(&c, nil, 20, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.softDelete(p.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Removing only the new table models a database from the previous release.
	if _, err := s.db.Exec(`DROP TABLE check_stats`); err != nil {
		t.Fatal(err)
	}
	want := Stats{ChecksTotal: 10, Changed: 1, Errors: 5, Timeouts: 1, HTTP4xx: 1, HTTP5xx: 1, AverageDurationMS: 5.5}
	for restart := 0; restart < 2; restart++ {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s, err = OpenStore(path)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := s.stats(ctx); err != nil || got != want {
			t.Fatalf("restart %d: stats=%+v want=%+v err=%v", restart, got, want, err)
		}
	}
	// Fail after the counter update to verify that the entire save rolls back.
	if _, err := s.db.Exec(`CREATE TRIGGER reject_check_save BEFORE UPDATE ON pages
 BEGIN SELECT RAISE(ABORT, 'injected failure'); END`); err != nil {
		t.Fatal(err)
	}
	c := Check{PageID: p.ID, CheckedAt: time.Now(), Status: "changed", DurationMS: 11}
	v := &Version{PageID: p.ID, CheckedAt: c.CheckedAt, Content: "new content", HTTPStatus: 200}
	if err := s.saveCheck(&c, v, 20, false); err == nil {
		t.Fatal("expected save to fail")
	}
	if got, err := s.stats(ctx); err != nil || got != want {
		t.Fatalf("rollback changed counters: %+v, err=%v", got, err)
	}
	if h, err := s.history(ctx, p.ID, 100, 0, false); err != nil || len(h) != 10 {
		t.Fatalf("rollback changed history: len=%d err=%v", len(h), err)
	}
	if versions, err := s.versions(ctx, p.ID, 100, 0); err != nil || len(versions) != 0 {
		t.Fatalf("rollback retained version: %v err=%v", versions, err)
	}
	if _, err := s.db.Exec(`DROP TRIGGER reject_check_save`); err != nil {
		t.Fatal(err)
	}
	if err := s.saveCheck(&c, v, 20, false); err != nil {
		t.Fatal(err)
	}
	want.ChecksTotal, want.Changed, want.AverageDurationMS = 11, 2, 6
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.stats(ctx); err != nil || got != want {
		t.Fatalf("increment after migration: %+v want=%+v err=%v", got, want, err)
	}
}

func TestBlockedReadsDoNotLockEngineAndCanBeCancelled(t *testing.T) {
	for _, operation := range []string{"history", "versions", "stats"} {
		t.Run(operation, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				<-r.Context().Done()
				return nil, r.Context().Err()
			})}
			e := newTestEngine(t, manualConfig(), client)
			p := addPage(t, e, "http://example.test")
			e.store.readers.SetMaxOpenConns(1)
			conn, err := e.store.readers.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waits := e.store.readers.Stats().WaitCount
			readDone := make(chan error, 1)
			go func() {
				var err error
				switch operation {
				case "history":
					_, err = e.History(ctx, p.ID, 100, 0, false)
				case "versions":
					_, err = e.Versions(ctx, p.ID, 100, 0)
				case "stats":
					_, err = e.Stats(ctx)
				}
				readDone <- err
			}()
			eventually(t, func() bool { return e.store.readers.Stats().WaitCount > waits })
			responsive := make(chan error, 1)
			go func() {
				_, err := e.Get(p.ID)
				if err == nil {
					_, err = e.Add(PageInput{URL: p.URL, Interval: "1h"})
				}
				if err == nil {
					err = e.Enqueue(p.ID)
				}
				responsive <- err
			}()
			select {
			case err := <-responsive:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("blocked SQL read holds the engine mutex")
			}
			cancel()
			select {
			case err := <-readDone:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled read returned %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("read ignored context cancellation")
			}
		})
	}
}

func TestWALReaderSnapshotDoesNotBlockWorkerSave(t *testing.T) {
	e := newTestEngine(t, manualConfig(), &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 500, Header: http.Header{}, Body: http.NoBody, Request: r}, nil
	})})
	p := addPage(t, e, "http://example.test")
	tx, err := e.store.readers.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var count int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM checks`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("initial snapshot: count=%d err=%v", count, err)
	}
	runCheck(t, e, p, "http_error")
	if err := tx.QueryRow(`SELECT COUNT(*) FROM checks`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("reader snapshot changed: count=%d err=%v", count, err)
	}
	if st, err := e.Stats(context.Background()); err != nil || st.ChecksTotal != 1 || st.HTTP5xx != 1 {
		t.Fatalf("worker could not commit beside reader: %+v err=%v", st, err)
	}
}

func TestSQLiteConnectionSettings(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "settings.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, pool := range []struct {
		db        *sql.DB
		count     int
		queryOnly int
	}{{s.db, 1, 0}, {s.readers, 4, 1}} {
		// Force replacement too, so settings cannot depend on the first connection.
		pool.db.SetMaxIdleConns(0)
		for round := 0; round < 2; round++ {
			func() {
				for i := 0; i < pool.count; i++ {
					conn, err := pool.db.Conn(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					for pragma, want := range map[string]int{"foreign_keys": 1, "busy_timeout": 5000, "query_only": pool.queryOnly} {
						var got int
						if err := conn.QueryRowContext(context.Background(), "PRAGMA "+pragma).Scan(&got); err != nil || got != want {
							t.Fatalf("%s=%d want=%d err=%v", pragma, got, want, err)
						}
					}
					var mode string
					if err := conn.QueryRowContext(context.Background(), "PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
						t.Fatalf("journal_mode=%s err=%v", mode, err)
					}
				}
			}()
		}
	}
}
