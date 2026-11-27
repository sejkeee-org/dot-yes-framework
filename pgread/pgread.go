// Package pgread routes selected reads to synchronous or asynchronous
// replicas and retries them on the primary when an endpoint is unavailable.
package pgread

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const replicaTimeout = 2 * time.Second

// Pools owns the primary pool and a read pool when the DSN points to the
// Kubernetes postgres Service. Patroni labels route each Service by role.
type Pools struct {
	primary      *pgxpool.Pool
	replica      *pgxpool.Pool
	asyncReplica *pgxpool.Pool
}

func New(ctx context.Context, primaryDSN string) (*Pools, error) {
	primary, err := pgxpool.New(ctx, primaryDSN)
	if err != nil {
		return nil, err
	}
	pools := &Pools{primary: primary}
	if readDSN := replicaDSN(primaryDSN); readDSN != "" {
		pools.replica, err = pgxpool.New(ctx, readDSN)
		if err != nil {
			primary.Close()
			return nil, err
		}
	}
	if readDSN := asyncReplicaDSN(primaryDSN); readDSN != "" {
		pools.asyncReplica, err = pgxpool.New(ctx, readDSN)
		if err != nil {
			pools.Close()
			return nil, err
		}
	}
	return pools, nil
}

// replicaDSN derives the stable read Service from the stable primary Service.
// Other hosts (including local development) use the primary for all reads.
func replicaDSN(primaryDSN string) string {
	return readDSN(primaryDSN, "-read")
}

func asyncReplicaDSN(primaryDSN string) string {
	return readDSN(primaryDSN, "-async-read")
}

func readDSN(primaryDSN, suffix string) string {
	u, err := url.Parse(primaryDSN)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Query().Get("host") != "" {
		return ""
	}
	host := u.Hostname()
	service, domain, _ := strings.Cut(host, ".")
	if service != "postgres" && !strings.HasSuffix(service, "-db") {
		return ""
	}
	readHost := service + suffix
	if domain != "" {
		readHost += "." + domain
	}
	if port := u.Port(); port != "" {
		u.Host = net.JoinHostPort(readHost, port)
	} else {
		u.Host = readHost
	}
	return u.String()
}

func (p *Pools) Primary() *pgxpool.Pool { return p.primary }

func (p *Pools) Close() {
	if p.replica != nil {
		p.replica.Close()
	}
	if p.asyncReplica != nil {
		p.asyncReplica.Close()
	}
	p.primary.Close()
}

// Read must receive a side-effect-free operation that consumes all rows before
// returning. It tries the synchronous replica, then the primary.
// Never use it for writes, transactions, authentication checks, or
// read-after-write flows.
func Read[T any](ctx context.Context, p *Pools, query func(context.Context, *pgxpool.Pool) (T, error)) (T, error) {
	return read(ctx, p.primary, query, p.replica)
}

// ReadAsync tries the asynchronous replica, then the synchronous replica,
// then the primary. Use it only when replication lag is acceptable.
func ReadAsync[T any](ctx context.Context, p *Pools, query func(context.Context, *pgxpool.Pool) (T, error)) (T, error) {
	return read(ctx, p.primary, query, p.asyncReplica, p.replica)
}

func read[T any](ctx context.Context, primary *pgxpool.Pool, query func(context.Context, *pgxpool.Pool) (T, error), replicas ...*pgxpool.Pool) (T, error) {
	for _, replica := range replicas {
		if replica == nil {
			continue
		}
		replicaCtx, cancel := context.WithTimeout(ctx, replicaTimeout)
		result, err := query(replicaCtx, replica)
		cancel()
		if err == nil || ctx.Err() != nil || !retryable(err) {
			return result, err
		}
	}
	return query(ctx, primary)
}

func retryable(err error) bool {
	if errors.Is(err, pgx.ErrNoRows) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return len(pgErr.Code) >= 2 && pgErr.Code[:2] == "08" ||
			pgErr.Code == "57P01" || pgErr.Code == "57P02" || pgErr.Code == "57P03"
	}
	var netErr net.Error
	return errors.As(err, &netErr) || errors.Is(err, io.EOF) ||
		errors.Is(err, context.DeadlineExceeded)
}
