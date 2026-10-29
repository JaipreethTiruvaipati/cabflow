// Package notify sends a run's effect to whoever acts on it. We may send the
// same run twice after a failover; the receiver drops repeats by key, which
// is how at-least-once turns into exactly-once.
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type Notification struct {
	Key      string
	TenantID uuid.UUID
	Type     string
	Payload  json.RawMessage
}

type Sender interface {
	Send(ctx context.Context, n Notification) error
}

type HTTPSender struct {
	URL    string
	Client *http.Client
}

func NewHTTPSender(url string) *HTTPSender {
	return &HTTPSender{URL: url, Client: &http.Client{Timeout: 10 * time.Second}}
}

func (s *HTTPSender) Send(ctx context.Context, n Notification) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(n.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", n.Key)
	req.Header.Set("X-Tenant-ID", n.TenantID.String())
	req.Header.Set("X-Job-Type", n.Type)
	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 { // a retry is fine, the key makes it safe
		return fmt.Errorf("receiver returned %d", resp.StatusCode)
	}
	return nil
}

// LogSender is for running locally without a receiver. It logs the key only;
// payloads can have phone numbers in them.
type LogSender struct{ Logger *slog.Logger }

func (s LogSender) Send(_ context.Context, n Notification) error {
	s.Logger.Info("notification", "key", n.Key, "tenant_id", n.TenantID, "type", n.Type)
	return nil
}
