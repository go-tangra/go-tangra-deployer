package bigip

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeIControl is a stateful iControl REST fake: it keeps uploaded files,
// sys/crypto cert and key objects and client-SSL profiles, answers like a
// BIG-IP (409 for an existing object, 404 for a missing one) and records
// every request so tests can assert method, path, headers and body.
type fakeIControl struct {
	mu       sync.Mutex
	calls    []call
	uploads  map[string]string         // file name → content
	certs    map[string]string         // full path → content
	keys     map[string]string         // full path → content
	profiles map[string]map[string]any // full path → profile
	failures map[string][]fakeAnswer   // "METHOD path" → queued forced answers
	sticky   map[string]fakeAnswer     // "METHOD path" → forced answer, every time
	version  int                       // status of /mgmt/tm/sys/version (0 = 200)
	srv      *httptest.Server
}

type call struct {
	method, path, body string
	contentType        string
	contentRange       string
	user, pass         string
	authOK             bool
}

type fakeAnswer struct {
	status int
	body   string
}

func newFake(t *testing.T) *fakeIControl {
	t.Helper()
	f := &fakeIControl{
		uploads:  map[string]string{},
		certs:    map[string]string{},
		keys:     map[string]string{},
		profiles: map[string]map[string]any{},
		failures: map[string][]fakeAnswer{},
		sticky:   map[string]fakeAnswer{},
	}
	f.srv = httptest.NewTLSServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

// creds are valid credentials for the fake.
func (f *fakeIControl) creds() map[string]any {
	return map[string]any{"host": strings.TrimPrefix(f.srv.URL, "https://"), "username": testUser, "password": testPass}
}

// failOnce queues a forced answer for the next matching request.
func (f *fakeIControl) failOnce(method, path string, status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := method + " " + path
	f.failures[k] = append(f.failures[k], fakeAnswer{status, body})
}

// failAlways forces the answer of every matching request.
func (f *fakeIControl) failAlways(method, path string, status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sticky[method+" "+path] = fakeAnswer{status, body}
}

func (f *fakeIControl) addProfile(full string, fields map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := map[string]any{"fullPath": full}
	for k, v := range fields {
		p[k] = v
	}
	f.profiles[full] = p
}

func (f *fakeIControl) profile(full string) map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.profiles[full]
}

func (f *fakeIControl) recorded() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

func (f *fakeIControl) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

// writes are the recorded non-GET calls.
func (f *fakeIControl) writes() []call {
	var out []call
	for _, c := range f.recorded() {
		if c.method != http.MethodGet {
			out = append(out, c)
		}
	}
	return out
}

// sig renders calls as "METHOD path" for sequence assertions.
func sig(calls []call) []string {
	out := make([]string, len(calls))
	for i, c := range calls {
		out[i] = c.method + " " + c.path
	}
	return out
}

func decodePath(p, prefix string) string {
	return strings.ReplaceAll(strings.TrimPrefix(p, prefix), "~", "/")
}

func (f *fakeIControl) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, _ := io.ReadAll(r.Body)
	u, pw, ok := r.BasicAuth()
	f.calls = append(f.calls, call{
		method: r.Method, path: r.URL.Path, body: string(raw),
		contentType: r.Header.Get("Content-Type"), contentRange: r.Header.Get("Content-Range"),
		user: u, pass: pw, authOK: ok,
	})
	if !ok || u != testUser || pw != testPass {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"code":401,"message":"Authorization failed"}`)
		return
	}
	k := r.Method + " " + r.URL.Path
	if q := f.failures[k]; len(q) > 0 {
		f.failures[k] = q[1:]
		w.WriteHeader(q[0].status)
		_, _ = io.WriteString(w, q[0].body)
		return
	}
	if a, ok := f.sticky[k]; ok {
		w.WriteHeader(a.status)
		_, _ = io.WriteString(w, a.body)
		return
	}
	var body map[string]any
	_ = json.Unmarshal(raw, &body)

	const (
		uploads  = "/mgmt/shared/file-transfer/uploads/"
		certObj  = "/mgmt/tm/sys/crypto/cert/"
		keyObj   = "/mgmt/tm/sys/crypto/key/"
		profiles = "/mgmt/tm/ltm/profile/client-ssl"
	)
	p := r.URL.Path
	switch {
	case r.Method == http.MethodGet && p == "/mgmt/tm/sys/version":
		if f.version != 0 {
			w.WriteHeader(f.version)
			return
		}
		_, _ = io.WriteString(w, `{"kind":"tm:sys:version:versionstats"}`)

	case r.Method == http.MethodPost && strings.HasPrefix(p, uploads):
		f.uploads[strings.TrimPrefix(p, uploads)] = string(raw)
		_, _ = fmt.Fprintf(w, `{"remainingByteCount":0,"usedChunks":{"0":%d}}`, len(raw))

	case r.Method == http.MethodPost && (p == "/mgmt/tm/sys/crypto/cert" || p == "/mgmt/tm/sys/crypto/key"):
		store := f.certs
		what := "Certificate"
		if p == "/mgmt/tm/sys/crypto/key" {
			store, what = f.keys, "Key"
		}
		name, _ := body["name"].(string)
		file, _ := body["from-local-file"].(string)
		content, uploaded := f.uploads[strings.TrimPrefix(file, downloadsDir+"/")]
		if body["command"] != "install" || !strings.HasPrefix(file, downloadsDir+"/") || !uploaded {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"code":400,"message":"bad install request"}`)
			return
		}
		if _, exists := store[name]; exists && body["overwrite"] != true {
			w.WriteHeader(http.StatusConflict)
			_, _ = fmt.Fprintf(w, `{"code":409,"message":"01020066:3: The requested %s File (%s) already exists in partition Common."}`, what, name)
			return
		}
		store[name] = content
		_, _ = fmt.Fprintf(w, `{"kind":"tm:sys:crypto:%s:%sstate","name":%q}`, strings.ToLower(what), strings.ToLower(what), name)

	case r.Method == http.MethodGet && strings.HasPrefix(p, certObj):
		if _, ok := f.certs[decodePath(p, certObj)]; !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"code":404,"message":"01020036:3: The requested Certificate File was not found."}`)
			return
		}
		_, _ = io.WriteString(w, `{"kind":"tm:sys:file:ssl-cert:ssl-certstate"}`)

	case r.Method == http.MethodDelete && (strings.HasPrefix(p, certObj) || strings.HasPrefix(p, keyObj)):
		store, prefix := f.certs, certObj
		if strings.HasPrefix(p, keyObj) {
			store, prefix = f.keys, keyObj
		}
		name := decodePath(p, prefix)
		if _, ok := store[name]; !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		delete(store, name)

	case r.Method == http.MethodPost && p == profiles:
		name, _ := body["name"].(string)
		if _, exists := f.profiles[name]; exists {
			w.WriteHeader(http.StatusConflict)
			_, _ = fmt.Fprintf(w, `{"code":409,"message":"01020066:3: The requested client-ssl profile (%s) already exists in partition Common."}`, name)
			return
		}
		prof := map[string]any{"fullPath": name, "defaultsFrom": "/Common/clientssl"}
		for k, v := range body {
			prof[k] = v
		}
		f.profiles[name] = prof
		_ = json.NewEncoder(w).Encode(prof)

	case strings.HasPrefix(p, profiles+"/"):
		name := decodePath(p, profiles+"/")
		prof, ok := f.profiles[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"code":404,"message":"01020036:3: The requested profile was not found."}`)
			return
		}
		switch r.Method {
		case http.MethodPatch:
			for k, v := range body {
				prof[k] = v
			}
		case http.MethodDelete:
			delete(f.profiles, name)
			return
		}
		_ = json.NewEncoder(w).Encode(prof)

	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"code":404}`)
	}
}
