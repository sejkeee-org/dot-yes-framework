package pgread

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"connection failure", &pgconn.PgError{Code: "08006"}, true},
		{"server shutting down", &pgconn.PgError{Code: "57P01"}, true},
		{"bad query", &pgconn.PgError{Code: "42P01"}, false},
		{"no rows", pgx.ErrNoRows, false},
		{"replica timeout", context.DeadlineExceeded, true},
		{"unrelated error", errors.New("scan failed"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryable(tc.err); got != tc.want {
				t.Fatalf("retryable(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestReplicaDSN(t *testing.T) {
	cases := []struct{ input, want string }{
		{"postgres://postgres:secret@postgres:5432/citadel?sslmode=disable", "postgres://postgres:secret@postgres-read:5432/citadel?sslmode=disable"},
		{"postgresql://user:secret@postgres.dot-yes.svc.cluster.local:5432/db", "postgresql://user:secret@postgres-read.dot-yes.svc.cluster.local:5432/db"},
		{"postgres://postgres:secret@citadel-db:5432/citadel", "postgres://postgres:secret@citadel-db-read:5432/citadel"},
		{"postgres://postgres:secret@admin-panel-db.dot-yes.svc.cluster.local:5432/admin_panel", "postgres://postgres:secret@admin-panel-db-read.dot-yes.svc.cluster.local:5432/admin_panel"},
		{"postgres://user:secret@localhost:5432/db", ""},
		{"postgres://user:secret@postgres:5432/db?host=other", ""},
	}
	for _, tc := range cases {
		if got := replicaDSN(tc.input); got != tc.want {
			t.Errorf("replicaDSN(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestReadFallsBackOnlyOnReplicaFailure(t *testing.T) {
	pools := &Pools{primary: &pgxpool.Pool{}, replica: &pgxpool.Pool{}}
	primaryCalls := 0
	value, err := Read(context.Background(), pools, func(_ context.Context, pool *pgxpool.Pool) (int, error) {
		if pool == pools.replica {
			return 0, &pgconn.PgError{Code: "08006"}
		}
		primaryCalls++
		return 42, nil
	})
	if err != nil || value != 42 || primaryCalls != 1 {
		t.Fatalf("fallback result = (%d, %v), primary calls = %d", value, err, primaryCalls)
	}

	primaryCalls = 0
	_, err = Read(context.Background(), pools, func(_ context.Context, pool *pgxpool.Pool) (int, error) {
		if pool == pools.primary {
			primaryCalls++
		}
		return 0, pgx.ErrNoRows
	})
	if !errors.Is(err, pgx.ErrNoRows) || primaryCalls != 0 {
		t.Fatalf("missing row should not be retried on primary: %v, calls %d", err, primaryCalls)
	}
}
