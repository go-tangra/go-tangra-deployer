package httpapi

import (
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/go-freya/freya/internal/testrt"
	"github.com/go-freya/freya/internal/testutil"
)

// TestRemoteServing covers the federated-UI remote handler mounted under
// RemotePrefix: the manifest is served and hashed assets are cached.
func TestRemoteServing(t *testing.T) {
	rt := testrt.New(t, testutil.MustCA("example.org"), "deployer")
	s, err := NewHandler(rt, WithRemote(fstest.MapFS{
		"mf-manifest.json":       {Data: []byte(`{"id":"deployer"}`)},
		"assets/app-abcdef12.js": {Data: []byte("console.log(1)")},
		"index.html":             {Data: []byte("<!doctype html>")},
	}))
	if err != nil {
		t.Fatal(err)
	}
	get := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "https://localhost"+path, nil)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}

	if w := get(RemotePrefix + "/mf-manifest.json"); w.Code != 200 {
		t.Fatalf("manifest: %d", w.Code)
	}
	if w := get(RemotePrefix + "/assets/app-abcdef12.js"); w.Code != 200 || w.Header().Get("Cache-Control") == "" {
		t.Fatalf("asset: %d cache=%q", w.Code, w.Header().Get("Cache-Control"))
	}
}

// TestNoRemoteWithoutOption confirms the remote is absent when not configured.
func TestNoRemoteWithoutOption(t *testing.T) {
	rt := testrt.New(t, testutil.MustCA("example.org"), "deployer")
	s, err := NewHandler(rt)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "https://localhost"+RemotePrefix+"/mf-manifest.json", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code == 200 {
		t.Fatalf("remote served without WithRemote: %d", w.Code)
	}
}
