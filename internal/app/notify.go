package app

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	authv1 "github.com/go-tangra/go-tangra-auth/sdk/v4/api/proto/auth/v1"
	"github.com/go-tangra/go-tangra-notification/sdk/v4/pkg/notifyclient"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/notify"
)

// lazyConn dials a mesh peer (SPIFFE mTLS, pooled by Freya — never closed
// here) on first use, so the deployer starts while auth or notification is
// down; a failed dial is retried on the next use.
type lazyConn struct {
	app     *App
	service string
	mu      sync.Mutex
	conn    grpc.ClientConnInterface
}

func (l *lazyConn) get(ctx context.Context) (grpc.ClientConnInterface, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		c, err := l.app.Freya.Client(ctx, l.service)
		if err != nil {
			l.app.Log.WarnContext(ctx, "deployer: mesh client unavailable", "service", l.service, "err", err)
			return nil, status.Error(codes.Unavailable, l.service+" unreachable")
		}
		l.conn = c
	}
	return l.conn, nil
}

func fixedConn(c grpc.ClientConnInterface) *lazyConn { return &lazyConn{conn: c} }

// lazyProfiles is auth's Profiles client over a lazyConn.
type lazyProfiles struct{ c *lazyConn }

func (p lazyProfiles) Members(ctx context.Context, in *authv1.ListMemberDetailsRequest, opts ...grpc.CallOption) (*authv1.ListMemberDetailsResponse, error) {
	conn, err := p.c.get(ctx)
	if err != nil {
		return nil, err
	}
	return authv1.NewProfilesClient(conn).Members(ctx, in, opts...)
}

func (p lazyProfiles) Contacts(ctx context.Context, in *authv1.LookupContactsRequest, opts ...grpc.CallOption) (*authv1.LookupContactsResponse, error) {
	conn, err := p.c.get(ctx)
	if err != nil {
		return nil, err
	}
	return authv1.NewProfilesClient(conn).Contacts(ctx, in, opts...)
}

// lazySender is the notification module client over a lazyConn; an
// unreachable module is a retryable outcome.
type lazySender struct{ c *lazyConn }

func (s lazySender) SendKey(ctx context.Context, tenantID, key, recipient string, vars map[string]string, correlationID string) (notifyclient.Result, error) {
	conn, err := s.c.get(ctx)
	if err != nil {
		return notifyclient.Result{Retryable: true, Reason: "notification unreachable"}, nil
	}
	return notifyclient.New(conn).SendKey(ctx, tenantID, key, recipient, vars, correlationID)
}

// newNotifier builds the target status e-mail notifier (auth for the
// recipients, notification for delivery); its worker runs with the others.
func (a *App) newNotifier(o Options, aud notify.Auditor) *notify.Notifier {
	authC := &lazyConn{app: a, service: a.Cfg.Auth.Service}
	if o.AuthConn != nil {
		authC = fixedConn(o.AuthConn)
	}
	notC := &lazyConn{app: a, service: a.Cfg.Notification.Service}
	if o.NotificationConn != nil {
		notC = fixedConn(o.NotificationConn)
	}
	return notify.New(notify.Config{
		Profiles: lazyProfiles{authC}, Sender: lazySender{notC}, Auditor: aud, Log: a.Log,
		PortalURL: a.Cfg.Notification.PortalURL,
	})
}
