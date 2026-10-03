package observer

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/andybalholm/cascadia"
)

var (
	ErrInvalid   = errors.New("invalid input")
	ErrNotFound  = errors.New("page not found")
	ErrBusy      = errors.New("page is already queued or running")
	ErrQueueFull = errors.New("check queue is full")
	ErrStopped   = errors.New("service is stopping")
)

type Page struct {
	ID              int64      `json:"id"`
	URL             string     `json:"url"`
	Interval        string     `json:"interval"`
	Ignore          []string   `json:"ignore"`
	State           string     `json:"state"`
	CreatedAt       time.Time  `json:"createdAt"`
	DeletedAt       *time.Time `json:"deletedAt,omitempty"`
	NextCheckAt     time.Time  `json:"nextCheckAt"`
	LastCheckedAt   *time.Time `json:"lastCheckedAt,omitempty"`
	LastStatus      string     `json:"lastStatus,omitempty"`
	FinalURL        string     `json:"finalUrl,omitempty"`
	BaselinePending bool       `json:"baselinePending"`
}

type PageInput struct {
	URL      string   `json:"url"`
	Interval string   `json:"interval"`
	Ignore   []string `json:"ignore"`
}

type PagePatch struct {
	URL      *string   `json:"url,omitempty"`
	Interval *string   `json:"interval,omitempty"`
	Ignore   *[]string `json:"ignore,omitempty"`
}

func validateInput(p PageInput) error {
	u, err := url.Parse(p.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return fmt.Errorf("url must be an absolute HTTP/HTTPS URL without credentials")
	}
	if d, err := time.ParseDuration(p.Interval); err != nil || d <= 0 || d > 365*24*time.Hour {
		return fmt.Errorf("interval must be a positive Go duration up to 8760h, e.g. 30s or 2m")
	}
	if len(p.Ignore) > 100 {
		return fmt.Errorf("at most 100 ignore selectors are allowed")
	}
	for _, selector := range p.Ignore {
		if len(selector) > 1024 {
			return fmt.Errorf("ignore selector is too long")
		}
		if _, err := cascadia.Compile(selector); err != nil {
			return fmt.Errorf("invalid CSS selector %q: %w", selector, err)
		}
	}
	return nil
}

type Version struct {
	ID          int64     `json:"id"`
	PageID      int64     `json:"pageId"`
	CheckedAt   time.Time `json:"checkedAt"`
	HTTPStatus  int       `json:"httpStatus"`
	Content     string    `json:"content"`
	Hash        string    `json:"hash"`
	ContentSize int64     `json:"contentSize"`
}

type Check struct {
	ID            int64     `json:"id"`
	PageID        int64     `json:"pageId"`
	CheckedAt     time.Time `json:"checkedAt"`
	Status        string    `json:"status"`
	HTTPStatus    int       `json:"httpStatus,omitempty"`
	Error         string    `json:"error,omitempty"`
	DurationMS    int64     `json:"durationMs"`
	OriginalURL   string    `json:"originalUrl"`
	FinalURL      string    `json:"finalUrl,omitempty"`
	RedirectChain []string  `json:"redirectChain"`
	Diff          string    `json:"diff,omitempty"`
	VersionID     *int64    `json:"versionId,omitempty"`
}

type Stats struct {
	Pages             int     `json:"pages"`
	Queued            int     `json:"queued"`
	Running           int     `json:"running"`
	ChecksTotal       int64   `json:"checksTotal"`
	Changed           int64   `json:"changed"`
	Errors            int64   `json:"errors"`
	Timeouts          int64   `json:"timeouts"`
	HTTP4xx           int64   `json:"http4xx"`
	HTTP5xx           int64   `json:"http5xx"`
	AverageDurationMS float64 `json:"averageDurationMs"`
	Workers           int     `json:"workers"`
	QueueCapacity     int     `json:"queueCapacity"`
}

func clonePage(p Page) Page {
	p.Ignore = append([]string{}, p.Ignore...)
	if p.DeletedAt != nil {
		at := *p.DeletedAt
		p.DeletedAt = &at
	}
	if p.LastCheckedAt != nil {
		at := *p.LastCheckedAt
		p.LastCheckedAt = &at
	}
	return p
}
