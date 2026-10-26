package api

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"pageobserver/internal/observer"
)

//go:embed openapi.json docs.html swagger
var assets embed.FS

type API struct{ engine *observer.Engine }

func New(e *observer.Engine) http.Handler {
	a := &API{engine: e}
	mux := http.NewServeMux()
	mux.Handle("GET /swagger/", http.FileServerFS(assets))
	mux.HandleFunc("POST /pages", a.add)
	mux.HandleFunc("GET /pages", a.list)
	mux.HandleFunc("GET /pages/{id}", a.get)
	mux.HandleFunc("PATCH /pages/{id}", a.patch)
	mux.HandleFunc("DELETE /pages/{id}", a.delete)
	mux.HandleFunc("GET /pages/{id}/history", a.history)
	mux.HandleFunc("GET /pages/{id}/changes", a.history)
	mux.HandleFunc("GET /pages/{id}/versions", a.versions)
	mux.HandleFunc("POST /pages/{id}/check", a.check)
	mux.HandleFunc("GET /stats", a.stats)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /openapi.json", func(w http.ResponseWriter, r *http.Request) {
		b, _ := assets.ReadFile("openapi.json")
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	})
	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, r *http.Request) {
		b, _ := assets.ReadFile("docs.html")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(b)
	})
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/docs", http.StatusFound) })
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})
}

func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		json.NewEncoder(w).Encode(v)
	}
}
func fail(w http.ResponseWriter, status int, msg string) {
	write(w, status, map[string]string{"error": msg})
}
func serviceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, observer.ErrInvalid):
		fail(w, 400, err.Error())
	case errors.Is(err, observer.ErrNotFound):
		fail(w, 404, err.Error())
	case errors.Is(err, observer.ErrBusy):
		fail(w, 409, err.Error())
	case errors.Is(err, observer.ErrQueueFull), errors.Is(err, observer.ErrStopped):
		fail(w, 503, err.Error())
	default:
		slog.Error("API error", "error", err)
		fail(w, 500, "internal storage error")
	}
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	d := json.NewDecoder(r.Body)
	var raw json.RawMessage
	if err := d.Decode(&raw); err != nil {
		fail(w, 400, "invalid JSON: "+err.Error())
		return false
	}
	if err := d.Decode(new(any)); err != io.EOF {
		fail(w, 400, "body must contain exactly one JSON object")
		return false
	}
	if len(raw) == 0 || raw[0] != '{' {
		fail(w, 400, "body must be a JSON object")
		return false
	}
	object := json.NewDecoder(bytes.NewReader(raw))
	object.DisallowUnknownFields()
	if err := object.Decode(v); err != nil {
		fail(w, 400, "invalid JSON: "+err.Error())
		return false
	}
	return true
}
func pageID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		fail(w, 400, "invalid page ID")
		return 0, false
	}
	return id, true
}
func pagination(w http.ResponseWriter, r *http.Request) (int, int, bool) {
	limit, offset := 100, 0
	var err error
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil {
			fail(w, 400, "invalid limit")
			return 0, 0, false
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		offset, err = strconv.Atoi(v)
		if err != nil {
			fail(w, 400, "invalid offset")
			return 0, 0, false
		}
	}
	if limit < 1 || limit > 500 || offset < 0 {
		fail(w, 400, "limit must be 1..500 and offset nonnegative")
		return 0, 0, false
	}
	return limit, offset, true
}
func (a *API) add(w http.ResponseWriter, r *http.Request) {
	var in observer.PageInput
	if !decode(w, r, &in) {
		return
	}
	p, err := a.engine.Add(in)
	if err != nil {
		serviceError(w, err)
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/pages/%d", p.ID))
	write(w, 201, p)
}
func (a *API) list(w http.ResponseWriter, r *http.Request) {
	write(w, 200, a.engine.List(r.URL.Query().Get("includeDeleted") == "true"))
}
func (a *API) get(w http.ResponseWriter, r *http.Request) {
	id, ok := pageID(w, r)
	if !ok {
		return
	}
	p, err := a.engine.Get(id)
	if err != nil {
		serviceError(w, err)
		return
	}
	write(w, 200, p)
}
func (a *API) patch(w http.ResponseWriter, r *http.Request) {
	id, ok := pageID(w, r)
	if !ok {
		return
	}
	var in observer.PagePatch
	if !decode(w, r, &in) {
		return
	}
	p, err := a.engine.Patch(id, in)
	if err != nil {
		serviceError(w, err)
		return
	}
	write(w, 200, p)
}
func (a *API) delete(w http.ResponseWriter, r *http.Request) {
	id, ok := pageID(w, r)
	if !ok {
		return
	}
	if err := a.engine.Delete(id); err != nil {
		serviceError(w, err)
		return
	}
	write(w, 204, nil)
}
func (a *API) check(w http.ResponseWriter, r *http.Request) {
	id, ok := pageID(w, r)
	if !ok {
		return
	}
	if err := a.engine.Enqueue(id); err != nil {
		serviceError(w, err)
		return
	}
	write(w, 202, map[string]any{"pageId": id, "status": "accepted"})
}
func (a *API) history(w http.ResponseWriter, r *http.Request) {
	id, ok := pageID(w, r)
	if !ok {
		return
	}
	l, o, ok := pagination(w, r)
	if !ok {
		return
	}
	out, err := a.engine.History(r.Context(), id, l, o, r.Pattern == "GET /pages/{id}/changes")
	if err != nil {
		serviceError(w, err)
		return
	}
	write(w, 200, out)
}
func (a *API) versions(w http.ResponseWriter, r *http.Request) {
	id, ok := pageID(w, r)
	if !ok {
		return
	}
	l, o, ok := pagination(w, r)
	if !ok {
		return
	}
	out, err := a.engine.Versions(r.Context(), id, l, o)
	if err != nil {
		serviceError(w, err)
		return
	}
	write(w, 200, out)
}
func (a *API) stats(w http.ResponseWriter, r *http.Request) {
	st, err := a.engine.Stats(r.Context())
	if err != nil {
		serviceError(w, err)
		return
	}
	write(w, 200, st)
}
