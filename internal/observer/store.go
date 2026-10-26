package observer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db      *sql.DB // One writer; SQLite serializes writes.
	readers *sql.DB
}

func OpenStore(path string) (*Store, error) {
	dsn := ":memory:?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	if path != ":memory:" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		path = absolute
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return nil, err
		}
		uriPath := filepath.ToSlash(path)
		if uriPath[0] != '/' {
			uriPath = "/" + uriPath
		}
		u := url.URL{Scheme: "file", Path: uriPath}
		dsn = u.String() + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	_, err = db.Exec(`
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS pages (
 id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT NOT NULL, interval TEXT NOT NULL,
 ignore_json TEXT NOT NULL, created_at TEXT NOT NULL, deleted_at TEXT,
 baseline_pending INTEGER NOT NULL DEFAULT 1, last_checked_at TEXT,
 last_status TEXT NOT NULL DEFAULT '', final_url TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS versions (
 id INTEGER PRIMARY KEY AUTOINCREMENT, page_id INTEGER NOT NULL REFERENCES pages(id),
 checked_at TEXT NOT NULL, http_status INTEGER NOT NULL, content TEXT NOT NULL,
 hash TEXT NOT NULL, content_size INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS versions_page ON versions(page_id, id DESC);
CREATE TABLE IF NOT EXISTS checks (
 id INTEGER PRIMARY KEY AUTOINCREMENT, page_id INTEGER NOT NULL REFERENCES pages(id),
 checked_at TEXT NOT NULL, status TEXT NOT NULL, http_status INTEGER NOT NULL,
 error TEXT NOT NULL, duration_ms INTEGER NOT NULL, original_url TEXT NOT NULL,
 final_url TEXT NOT NULL, redirects_json TEXT NOT NULL, diff TEXT NOT NULL,
 version_id INTEGER REFERENCES versions(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS checks_page ON checks(page_id, id DESC);
CREATE INDEX IF NOT EXISTS checks_version ON checks(version_id);
`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize database: %w", err)
	}
	if err := s.initializeStats(); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize statistics: %w", err)
	}
	// A private in-memory database cannot use a separate WAL reader pool.
	s.readers = db
	if path != ":memory:" {
		readers, err := sql.Open("sqlite", dsn+"&mode=ro&_pragma=query_only(1)")
		if err != nil {
			db.Close()
			return nil, err
		}
		readers.SetMaxOpenConns(4)
		readers.SetMaxIdleConns(4)
		if err := readers.Ping(); err != nil {
			readers.Close()
			db.Close()
			return nil, err
		}
		s.readers = readers
	}
	return s, nil
}

// Backfill once for databases created before persistent counters existed.
func (s *Store) initializeStats() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS check_stats (
 id INTEGER PRIMARY KEY CHECK(id=1), checks_total INTEGER NOT NULL,
 changed INTEGER NOT NULL, errors INTEGER NOT NULL, timeouts INTEGER NOT NULL,
 http4xx INTEGER NOT NULL, http5xx INTEGER NOT NULL, duration_sum_ms INTEGER NOT NULL
)`); err != nil {
		return err
	}
	var id int
	err = tx.QueryRow(`SELECT id FROM check_stats WHERE id=1`).Scan(&id)
	if err == sql.ErrNoRows {
		_, err = tx.Exec(`INSERT INTO check_stats
 SELECT 1, COUNT(*), COALESCE(SUM(status='changed'),0),
 COALESCE(SUM(status NOT IN ('initial','baseline','changed','unchanged','cancelled')),0),
 COALESCE(SUM(status='timeout'),0), COALESCE(SUM(http_status BETWEEN 400 AND 499),0),
 COALESCE(SUM(http_status BETWEEN 500 AND 599),0), COALESCE(SUM(duration_ms),0) FROM checks`)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Close() error {
	var readErr error
	if s.readers != s.db {
		readErr = s.readers.Close()
	}
	return errors.Join(readErr, s.db.Close())
}

func timestamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func (s *Store) loadPages() ([]Page, error) {
	rows, err := s.db.Query(`SELECT id,url,interval,ignore_json,created_at,deleted_at,baseline_pending,last_checked_at,last_status,final_url FROM pages`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	pages := []Page{}
	for rows.Next() {
		var p Page
		var ignore, created string
		var deleted, checked sql.NullString
		if err := rows.Scan(&p.ID, &p.URL, &p.Interval, &ignore, &created, &deleted, &p.BaselinePending, &checked, &p.LastStatus, &p.FinalURL); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(ignore), &p.Ignore); err != nil {
			return nil, err
		}
		p.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return nil, err
		}
		if deleted.Valid {
			t, e := time.Parse(time.RFC3339Nano, deleted.String)
			if e != nil {
				return nil, e
			}
			p.DeletedAt = &t
		}
		if checked.Valid {
			t, e := time.Parse(time.RFC3339Nano, checked.String)
			if e != nil {
				return nil, e
			}
			p.LastCheckedAt = &t
		}
		p.State = "idle"
		p.NextCheckAt = time.Now().UTC()
		pages = append(pages, p)
	}
	return pages, rows.Err()
}

func (s *Store) create(p *Page) error {
	ignore, _ := json.Marshal(p.Ignore)
	r, err := s.db.Exec(`INSERT INTO pages(url,interval,ignore_json,created_at) VALUES(?,?,?,?)`, p.URL, p.Interval, string(ignore), timestamp(p.CreatedAt))
	if err != nil {
		return err
	}
	p.ID, err = r.LastInsertId()
	return err
}

func (s *Store) update(p Page) error {
	ignore, _ := json.Marshal(p.Ignore)
	_, err := s.db.Exec(`UPDATE pages SET url=?,interval=?,ignore_json=?,baseline_pending=? WHERE id=?`, p.URL, p.Interval, string(ignore), p.BaselinePending, p.ID)
	return err
}

func (s *Store) softDelete(id int64, at time.Time) error {
	_, err := s.db.Exec(`UPDATE pages SET deleted_at=? WHERE id=?`, timestamp(at), id)
	return err
}

func (s *Store) latestVersion(ctx context.Context, id int64) (*Version, error) {
	var v Version
	var at string
	err := s.readers.QueryRowContext(ctx, `SELECT id,page_id,checked_at,http_status,content,hash,content_size FROM versions WHERE page_id=? ORDER BY id DESC LIMIT 1`, id).Scan(&v.ID, &v.PageID, &at, &v.HTTPStatus, &v.Content, &v.Hash, &v.ContentSize)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	v.CheckedAt, err = time.Parse(time.RFC3339Nano, at)
	return &v, err
}

// The version, history, counters, baseline flag and pruning commit atomically.
func (s *Store) saveCheck(c *Check, v *Version, retention int, baseline bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if v != nil {
		r, e := tx.Exec(`INSERT INTO versions(page_id,checked_at,http_status,content,hash,content_size) VALUES(?,?,?,?,?,?)`, v.PageID, timestamp(v.CheckedAt), v.HTTPStatus, v.Content, v.Hash, v.ContentSize)
		if e != nil {
			return e
		}
		v.ID, e = r.LastInsertId()
		if e != nil {
			return e
		}
		c.VersionID = &v.ID
	}
	chain, _ := json.Marshal(c.RedirectChain)
	r, err := tx.Exec(`INSERT INTO checks(page_id,checked_at,status,http_status,error,duration_ms,original_url,final_url,redirects_json,diff,version_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, c.PageID, timestamp(c.CheckedAt), c.Status, c.HTTPStatus, c.Error, c.DurationMS, c.OriginalURL, c.FinalURL, string(chain), c.Diff, c.VersionID)
	if err != nil {
		return err
	}
	c.ID, err = r.LastInsertId()
	if err != nil {
		return err
	}
	isError := c.Status != "initial" && c.Status != "baseline" && c.Status != "changed" && c.Status != "unchanged" && c.Status != "cancelled"
	_, err = tx.Exec(`UPDATE check_stats SET checks_total=checks_total+1,
 changed=changed+?, errors=errors+?, timeouts=timeouts+?, http4xx=http4xx+?,
 http5xx=http5xx+?, duration_sum_ms=duration_sum_ms+? WHERE id=1`,
		c.Status == "changed", isError, c.Status == "timeout", c.HTTPStatus >= 400 && c.HTTPStatus < 500,
		c.HTTPStatus >= 500 && c.HTTPStatus < 600, c.DurationMS)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE pages SET last_checked_at=?,last_status=?,final_url=?,baseline_pending=? WHERE id=?`, timestamp(c.CheckedAt), c.Status, c.FinalURL, baseline, c.PageID)
	if err != nil {
		return err
	}
	if v != nil {
		_, err = tx.Exec(`DELETE FROM versions WHERE page_id=? AND id NOT IN (SELECT id FROM versions WHERE page_id=? ORDER BY id DESC LIMIT ?)`, c.PageID, c.PageID, retention)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) history(ctx context.Context, id int64, limit, offset int, changes bool) ([]Check, error) {
	q := `SELECT id,page_id,checked_at,status,http_status,error,duration_ms,original_url,final_url,redirects_json,diff,version_id FROM checks WHERE page_id=?`
	if changes {
		q += ` AND status='changed'`
	}
	q += ` ORDER BY id DESC LIMIT ? OFFSET ?`
	rows, err := s.readers.QueryContext(ctx, q, id, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Check{}
	for rows.Next() {
		var c Check
		var at, chain string
		var vid sql.NullInt64
		if err := rows.Scan(&c.ID, &c.PageID, &at, &c.Status, &c.HTTPStatus, &c.Error, &c.DurationMS, &c.OriginalURL, &c.FinalURL, &chain, &c.Diff, &vid); err != nil {
			return nil, err
		}
		c.CheckedAt, err = time.Parse(time.RFC3339Nano, at)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(chain), &c.RedirectChain); err != nil {
			return nil, err
		}
		if vid.Valid {
			value := vid.Int64
			c.VersionID = &value
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) versions(ctx context.Context, id int64, limit, offset int) ([]Version, error) {
	rows, err := s.readers.QueryContext(ctx, `SELECT id,page_id,checked_at,http_status,content,hash,content_size FROM versions WHERE page_id=? ORDER BY id DESC LIMIT ? OFFSET ?`, id, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Version{}
	for rows.Next() {
		var v Version
		var at string
		if err := rows.Scan(&v.ID, &v.PageID, &at, &v.HTTPStatus, &v.Content, &v.Hash, &v.ContentSize); err != nil {
			return nil, err
		}
		v.CheckedAt, err = time.Parse(time.RFC3339Nano, at)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) stats(ctx context.Context) (Stats, error) {
	var st Stats
	err := s.readers.QueryRowContext(ctx, `SELECT checks_total,changed,errors,timeouts,http4xx,http5xx,
 CASE WHEN checks_total=0 THEN 0.0 ELSE duration_sum_ms*1.0/checks_total END
 FROM check_stats WHERE id=1`).Scan(&st.ChecksTotal, &st.Changed, &st.Errors, &st.Timeouts, &st.HTTP4xx, &st.HTTP5xx, &st.AverageDurationMS)
	return st, err
}
