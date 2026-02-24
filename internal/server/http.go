package server

import (
	"net/http"
	"os"

	kratosHttp "github.com/go-kratos/kratos/v2/transport/http"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
)

// NewHTTPServer creates a simple HTTP server for serving the frontend assets.
func NewHTTPServer(ctx *bootstrap.Context) *kratosHttp.Server {
	l := ctx.NewLoggerHelper("deployer/http")

	addr := os.Getenv("DEPLOYER_HTTP_ADDR")
	if addr == "" {
		addr = "0.0.0.0:9201"
	}

	srv := kratosHttp.NewServer(kratosHttp.Address(addr))

	route := srv.Route("/")
	route.GET("/health", func(ctx kratosHttp.Context) error {
		return ctx.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})

	frontendDist := os.Getenv("FRONTEND_DIST_PATH")
	if frontendDist == "" {
		frontendDist = "/app/frontend-dist"
	}

	if info, err := os.Stat(frontendDist); err == nil && info.IsDir() {
		fileServer := http.FileServer(http.Dir(frontendDist))
		srv.HandlePrefix("/", fileServer)
		l.Infof("Serving frontend assets from %s", frontendDist)
	}

	l.Infof("HTTP server listening on %s", addr)
	return srv
}
