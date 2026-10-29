// Command tripora runs a node (API + scheduler). Nodes are all the same, just
// point three or more of them at one PostgreSQL.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/JaipreethTiruvaipati/cabflow/internal/api"
	"github.com/JaipreethTiruvaipati/cabflow/internal/metrics"
	"github.com/JaipreethTiruvaipati/cabflow/internal/notify"
	"github.com/JaipreethTiruvaipati/cabflow/internal/scheduler"
	"github.com/JaipreethTiruvaipati/cabflow/internal/service"
	"github.com/JaipreethTiruvaipati/cabflow/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("node stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Fresh ID on every start. Otherwise a restarted node could renew the
	// leases of its dead self and sneak past the fencing check.
	nodeID := env("NODE_NAME", hostname()) + "-" + uuid.NewString()[:8]
	secret := os.Getenv("JWT_SECRET")
	if len(secret) < 16 {
		return errors.New("JWT_SECRET must be set (at least 16 characters)")
	}
	log = log.With("node", nodeID)

	config, err := store.PoolConfig(env("DATABASE_URL", "postgres://tripora:tripora@localhost:5432/tripora?sslmode=disable"), int32(envInt("DB_MAX_CONNS", 30)))
	if err != nil {
		return err
	}
	pool, err := connect(ctx, config)
	if err != nil {
		return err
	}
	defer pool.Close()
	db := store.New(pool)
	if err := db.Migrate(ctx); err != nil {
		return err
	}

	var sender notify.Sender = notify.LogSender{Logger: log}
	if url := os.Getenv("NOTIFY_URL"); url != "" {
		sender = notify.NewHTTPSender(url)
	}
	m := &metrics.Metrics{}
	node := scheduler.New(scheduler.DefaultConfig(nodeID), db, sender, m, log)

	server := api.NewServer(
		service.NewJobs(db, node.KnownType),
		api.Auth{Secret: []byte(secret), Issuer: env("JWT_ISSUER", "tripora"), Audience: env("JWT_AUDIENCE", "tripora-api")},
		log, m.Handler(db),
		func(r *http.Request) error { return db.Ping(r.Context()) },
	)
	server.OnServerError = func() { m.HTTP5xx.Add(1) }
	httpServer := &http.Server{Addr: env("HTTP_ADDR", ":8080"), Handler: server.Routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("listening", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "error", err)
			stop()
		}
	}()

	node.Run(ctx) // blocks until shutdown and in-flight runs are done
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdown)
}

// connect retries for a while since Postgres may still be booting in compose.
func connect(ctx context.Context, config *pgxpool.Config) (*pgxpool.Pool, error) {
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	for attempt := 1; ; attempt++ {
		if err = pool.Ping(ctx); err == nil || attempt == 30 {
			return pool, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(key)); err == nil && value > 0 {
		return value
	}
	return fallback
}

func hostname() string {
	name, _ := os.Hostname()
	return name
}
