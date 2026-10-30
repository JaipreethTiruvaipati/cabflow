// Command receiver pretends to be the notification service. It remembers
// every Idempotency-Key and counts repeats, which is how we tell "sent twice"
// apart from "delivered twice".
package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `CREATE TABLE IF NOT EXISTS deliveries (
	idempotency_key TEXT PRIMARY KEY,
	tenant_id       UUID NOT NULL,
	received_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
	attempts        INTEGER NOT NULL DEFAULT 1);
CREATE INDEX IF NOT EXISTS deliveries_received ON deliveries (received_at)`

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err == nil {
		for range 30 {
			if _, err = pool.Exec(ctx, schema); err == nil {
				break
			}
			time.Sleep(time.Second)
		}
	}
	if err != nil {
		log.Error("database unavailable", "error", err)
		os.Exit(1)
	}
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			for {
				batchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				deleted, err := pruneDeliveries(batchCtx, pool)
				cancel()
				if err != nil {
					log.Error("prune deliveries", "error", err)
					break
				}
				if deleted == 0 {
					break
				}
			}
		}
	}()

	// DELAY_MS slows deliveries down so chaos.sh can stop a node mid-send.
	delay, _ := time.ParseDuration(os.Getenv("DELAY_MS") + "ms")
	mux := http.NewServeMux()
	mux.HandleFunc("POST /notify", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		key, tenant := r.Header.Get("Idempotency-Key"), r.Header.Get("X-Tenant-ID")
		body, _ := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if key == "" || !json.Valid(body) {
			http.Error(w, "need Idempotency-Key and a JSON body", http.StatusBadRequest)
			return
		}
		// Seen this key before: ack it, but don't deliver again.
		var first bool
		err := pool.QueryRow(r.Context(), `INSERT INTO deliveries (idempotency_key, tenant_id) VALUES ($1, $2)
			ON CONFLICT (idempotency_key) DO UPDATE SET attempts = deliveries.attempts + 1
			RETURNING attempts = 1`, key, tenant).Scan(&first)
		if err != nil {
			log.Error("store delivery", "error", err)
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]bool{"delivered": first})
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		var unique, repeats int64
		pool.QueryRow(r.Context(), `SELECT count(*), COALESCE(sum(attempts - 1), 0) FROM deliveries`).Scan(&unique, &repeats)
		json.NewEncoder(w).Encode(map[string]int64{"delivered": unique, "duplicates_ignored": repeats})
	})
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":9000"
	}
	log.Info("receiver listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error("receiver stopped", "error", err)
		os.Exit(1)
	}
}

func pruneDeliveries(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	tag, err := pool.Exec(ctx, `WITH cleanup AS MATERIALIZED (
		SELECT pg_try_advisory_xact_lock(4242, 2) AS acquired)
		DELETE FROM deliveries WHERE idempotency_key IN (
			SELECT idempotency_key FROM deliveries, cleanup
			WHERE acquired AND received_at < statement_timestamp() - interval '7 days'
			ORDER BY received_at LIMIT 5000 FOR UPDATE OF deliveries SKIP LOCKED)`)
	return tag.RowsAffected(), err
}
