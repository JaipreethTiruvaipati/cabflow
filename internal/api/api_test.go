package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/JaipreethTiruvaipati/cabflow/internal/api"
	"github.com/JaipreethTiruvaipati/cabflow/internal/domain"
	"github.com/JaipreethTiruvaipati/cabflow/internal/metrics"
	"github.com/JaipreethTiruvaipati/cabflow/internal/service"
	"github.com/JaipreethTiruvaipati/cabflow/internal/store"
	"github.com/JaipreethTiruvaipati/cabflow/internal/testdb"
)

const secret = "test-secret-0123456789"

func newServer(t *testing.T) http.Handler {
	h, _ := newServerWithStore(t)
	return h
}

func newServerWithStore(t *testing.T) (http.Handler, *store.Store) {
	t.Helper()
	db := store.New(testdb.Pool(t, testdb.New(t), nil))
	jobs := service.NewJobs(db, func(jobType string) bool { return jobType == "alert" })
	auth := api.Auth{Secret: []byte(secret), Issuer: "tripora", Audience: "tripora-api"}
	m := &metrics.Metrics{}
	m.Error("claim runs")
	server := api.NewServer(jobs, auth, slog.New(slog.NewTextHandler(io.Discard, nil)), m.Handler(db),
		func(*http.Request) error { return nil })
	server.OnServerError = func() { m.HTTP5xx.Add(1) }
	return server.Routes(), db
}

func token(t *testing.T, tenant uuid.UUID, issuer string, roles ...string) string {
	t.Helper()
	claims := api.Claims{TenantID: tenant.String(), Roles: roles, RegisteredClaims: jwt.RegisteredClaims{
		Issuer: issuer, Audience: jwt.ClaimStrings{"tripora-api"}, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func call(h http.Handler, method, path, bearer, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestTenantsAreIsolated(t *testing.T) {
	h := newServer(t)
	a, b := uuid.New(), uuid.New()
	created := call(h, "POST", "/v1/jobs", token(t, a, "tripora", "admin"), `{"type":"alert","payload":{"employee":"e1"}}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", created.Code, created.Body)
	}
	var job struct{ ID string }
	_ = json.Unmarshal(created.Body.Bytes(), &job)
	adminB := token(t, b, "tripora", "admin")
	if got := call(h, "GET", "/v1/jobs/"+job.ID, adminB, ""); got.Code != http.StatusNotFound {
		t.Errorf("tenant B read tenant A's job: %d", got.Code)
	}
	if got := call(h, "POST", "/v1/jobs/"+job.ID+"/trigger", adminB, ""); got.Code != http.StatusNotFound {
		t.Errorf("tenant B triggered tenant A's job: %d", got.Code)
	}
	if got := call(h, "POST", "/v1/tenants/"+a.String()+"/pause", adminB, ""); got.Code != http.StatusForbidden {
		t.Errorf("tenant B's admin paused tenant A: %d", got.Code)
	}
	if got := call(h, "POST", "/v1/tenants/"+a.String()+"/pause", token(t, b, "tripora", "operator"), ""); got.Code != http.StatusOK {
		t.Errorf("an operator should be able to pause any tenant: %d", got.Code)
	}
}

func TestRolesAndTokens(t *testing.T) {
	h := newServer(t)
	tenant := uuid.New()
	job := `{"type":"alert","payload":{}}`
	cases := []struct {
		name, bearer, method, path, body string
		want                             int
	}{
		{"employee can read", token(t, tenant, "tripora", "employee"), "GET", "/v1/jobs", "", 200},
		{"employee cannot create", token(t, tenant, "tripora", "employee"), "POST", "/v1/jobs", job, 403},
		{"employee cannot see dead letters", token(t, tenant, "tripora", "employee"), "GET", "/v1/dead-letters", "", 403},
		{"admin can create", token(t, tenant, "tripora", "admin"), "POST", "/v1/jobs", job, 201},
		{"wrong issuer", token(t, tenant, "someone-else", "admin"), "GET", "/v1/jobs", "", 401},
		{"no token", "", "GET", "/v1/jobs", "", 401},
		{"health needs no token", "", "GET", "/healthz", "", 200},
	}
	for _, tc := range cases {
		if got := call(h, tc.method, tc.path, tc.bearer, tc.body); got.Code != tc.want {
			t.Errorf("%s: got %d, want %d (%s)", tc.name, got.Code, tc.want, got.Body)
		}
	}
}

func TestBadInputIsA400WithASafeMessage(t *testing.T) {
	h := newServer(t)
	admin := token(t, uuid.New(), "tripora", "admin")
	for name, body := range map[string]string{
		"unknown type":      `{"type":"sms","payload":{}}`,
		"bad cron":          `{"type":"alert","payload":{},"cron":"every day"}`,
		"bad timezone":      `{"type":"alert","payload":{},"cron":"0 22 * * *","timezone":"Mars/Base"}`,
		"cron and run_at":   `{"type":"alert","payload":{},"cron":"0 22 * * *","run_at":"2030-01-01T00:00:00Z"}`,
		"bad policy":        `{"type":"alert","payload":{},"misfire_policy":"LATER"}`,
		"unknown field":     `{"type":"alert","payload":{},"crontab":"0 22 * * *"}`,
		"runtime too small": `{"type":"alert","payload":{},"max_runtime_ms":5}`,
	} {
		got := call(h, "POST", "/v1/jobs", admin, body)
		if got.Code != http.StatusBadRequest || bytes.Contains(got.Body.Bytes(), []byte("internal")) {
			t.Errorf("%s: got %d %s", name, got.Code, got.Body)
		}
	}
}

func TestNextFireTimesFollowTenantTimezone(t *testing.T) {
	h := newServer(t)
	admin := token(t, uuid.New(), "tripora", "admin")
	created := call(h, "POST", "/v1/jobs", admin, `{"type":"alert","payload":{},"cron":"0 22 * * *","timezone":"Asia/Kolkata"}`)
	var job struct{ ID string }
	_ = json.Unmarshal(created.Body.Bytes(), &job)
	got := call(h, "GET", "/v1/jobs/"+job.ID+"/next-fire-times?limit=3", admin, "")
	var body struct {
		Times []time.Time `json:"next_fire_times"`
	}
	_ = json.Unmarshal(got.Body.Bytes(), &body)
	if len(body.Times) != 3 {
		t.Fatalf("want 3 fire times, got %s", got.Body)
	}
	for _, at := range body.Times {
		if at.UTC().Hour() != 16 || at.UTC().Minute() != 30 {
			t.Fatalf("22:00 IST is 16:30 UTC, got %s", at.UTC())
		}
	}
}

// Employees can see a job exists and when it runs, just not the payload.
func TestEmployeesDoNotSeePayloads(t *testing.T) {
	h := newServer(t)
	tenant := uuid.New()
	created := call(h, "POST", "/v1/jobs", token(t, tenant, "tripora", "admin"), `{"type":"alert","payload":{"phone":"+91 98765 43210"}}`)
	var job struct{ ID string }
	_ = json.Unmarshal(created.Body.Bytes(), &job)
	employee := token(t, tenant, "tripora", "employee")
	for _, path := range []string{"/v1/jobs/" + job.ID, "/v1/jobs"} {
		got := call(h, "GET", path, employee, "")
		if got.Code != http.StatusOK || bytes.Contains(got.Body.Bytes(), []byte("98765")) {
			t.Errorf("employee GET %s: %d %s", path, got.Code, got.Body)
		}
	}
	if got := call(h, "GET", "/v1/jobs/"+job.ID, token(t, tenant, "tripora", "admin"), ""); !bytes.Contains(got.Body.Bytes(), []byte("98765")) {
		t.Errorf("admin should see the payload: %s", got.Body)
	}
}

// Whatever on-call would look at during an incident should be on /metrics.
func TestMetricsExposeOnCallCounters(t *testing.T) {
	got := call(newServer(t), "GET", "/metrics", "", "")
	for _, name := range []string{
		"tripora_fire_lag_seconds_bucket", "tripora_runs_retried_total", "tripora_runs_throttled_total",
		"tripora_stale_writes_rejected_total", `tripora_errors_total{operation="claim runs"} 1`, "tripora_http_5xx_total",
		`tripora_runs{state="due"}`, `tripora_runs{state="paused"}`, `tripora_runs{state="dead_lettered"}`,
	} {
		if !bytes.Contains(got.Body.Bytes(), []byte(name)) {
			t.Errorf("/metrics is missing %s", name)
		}
	}
}

// Pause should stop claims and resume should start them again.
func TestPauseThenResumeTenant(t *testing.T) {
	h, db := newServerWithStore(t)
	ctx := context.Background()
	tenant := uuid.New()
	admin := token(t, tenant, "tripora", "admin")
	if got := call(h, "POST", "/v1/jobs", admin, `{"type":"alert","payload":{}}`); got.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", got.Code, got.Body)
	}
	if _, err := db.AcquireLeases(ctx, "node-a", domain.ShardCount, time.Minute); err != nil {
		t.Fatal(err)
	}
	runNow := func(store.ClaimedRun) (bool, time.Time, error) { return true, time.Time{}, nil }
	claim := func() int {
		runs, _, _, err := db.Claim(ctx, "node-a", 10, 10, runNow)
		if err != nil {
			t.Fatal(err)
		}
		return len(runs)
	}

	if got := call(h, "POST", "/v1/tenants/"+tenant.String()+"/pause", admin, ""); got.Code != http.StatusOK {
		t.Fatalf("pause: %d", got.Code)
	}
	if n := claim(); n != 0 {
		t.Fatalf("paused tenant's run was claimed")
	}
	if got := call(h, "POST", "/v1/tenants/"+tenant.String()+"/resume", admin, ""); got.Code != http.StatusOK {
		t.Fatalf("resume: %d", got.Code)
	}
	if n := claim(); n != 1 {
		t.Fatalf("resumed tenant's run should be claimed, got %d", n)
	}
}
