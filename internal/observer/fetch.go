package observer

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/html/charset"
)

type fetchResult struct {
	check   Check
	content string
	size    int64
}

func fetch(ctx context.Context, client *http.Client, p Page, timeout time.Duration, maxSize int64) fetchResult {
	started := time.Now()
	r := fetchResult{check: Check{PageID: p.ID, CheckedAt: started.UTC(), OriginalURL: p.URL, RedirectChain: []string{}}}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL, nil)
	if err != nil {
		r.check.Status = "network_error"
		r.check.Error = err.Error()
		return r
	}
	req.Header.Set("User-Agent", "PageObserver/1.0")
	req.Header.Set("Accept", "text/html, application/xhtml+xml")
	local := *client
	local.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		r.check.RedirectChain = append(r.check.RedirectChain, req.URL.String())
		if len(via) >= 10 {
			return fmt.Errorf("redirect limit exceeded")
		}
		if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
			return fmt.Errorf("redirect to unsupported scheme")
		}
		if client.CheckRedirect != nil {
			return client.CheckRedirect(req, via)
		}
		return nil
	}
	resp, err := local.Do(req)
	if resp != nil {
		r.check.HTTPStatus = resp.StatusCode
		r.check.FinalURL = resp.Request.URL.String()
		defer resp.Body.Close()
	}
	if err != nil {
		r.check.Status = classifyError(err)
		r.check.Error = err.Error()
		return r
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		r.check.Status = "http_error"
		r.check.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
		return r
	}
	typ, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || (typ != "text/html" && typ != "application/xhtml+xml") {
		r.check.Status = "non_html"
		r.check.Error = "response Content-Type is not HTML"
		return r
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSize+1))
	if err != nil {
		r.check.Status = classifyError(err)
		r.check.Error = err.Error()
		return r
	}
	r.size = int64(len(body))
	if r.size > maxSize {
		r.check.Status = "html_too_large"
		r.check.Error = fmt.Sprintf("HTML exceeds %d bytes", maxSize)
		return r
	}
	decoded, err := charset.NewReader(bytes.NewReader(body), resp.Header.Get("Content-Type"))
	if err != nil {
		r.check.Status = "parse_error"
		r.check.Error = err.Error()
		return r
	}
	// Bound decoded data too: legacy encodings may expand when converted to UTF-8.
	utf8Body, err := io.ReadAll(io.LimitReader(decoded, maxSize*4+1))
	if err == nil && int64(len(utf8Body)) > maxSize*4 {
		err = fmt.Errorf("decoded HTML exceeds limit")
	}
	if err != nil {
		r.check.Status = "parse_error"
		r.check.Error = err.Error()
		return r
	}
	r.content, err = Normalize(bytes.NewReader(utf8Body), p.Ignore)
	if err != nil {
		r.check.Status = "parse_error"
		r.check.Error = err.Error()
		return r
	}
	r.check.Status = "success"
	return r
}

func classifyError(err error) string {
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	var nerr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &nerr) && nerr.Timeout()) {
		return "timeout"
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "dns_error"
	}
	var cert *tls.CertificateVerificationError
	var unknown x509.UnknownAuthorityError
	var invalid x509.CertificateInvalidError
	var host x509.HostnameError
	var record tls.RecordHeaderError
	if errors.As(err, &cert) || errors.As(err, &unknown) || errors.As(err, &invalid) || errors.As(err, &host) || errors.As(err, &record) {
		return "tls_error"
	}
	var op *net.OpError
	if errors.As(err, &op) {
		return "connection_error"
	}
	return "network_error"
}
