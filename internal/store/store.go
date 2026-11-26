// Package store persists the control-plane state of the always-on agent:
// inbox, timers, outbox, approvals, and the single-worker lease.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // pure-Go database/sql driver, no cgo

	"github.com/google/uuid"

	"github.com/projanvil/aoa/internal/aoa"
)

const schemaSQL = `
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS inbox (
    id          TEXT PRIMARY KEY,
    source_id   TEXT NOT NULL UNIQUE,
    kind        TEXT NOT NULL,
    session_id  TEXT NOT NULL DEFAULT '',
    payload     TEXT NOT NULL DEFAULT '',
    resume      TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'pending',
    attempts    INTEGER NOT NULL DEFAULT 0,
    lease_until INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT NOT NULL DEFAULT '',
    last_error  TEXT NOT NULL DEFAULT '',
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS inbox_pending ON inbox(status, created_at);

CREATE TABLE IF NOT EXISTS timers (
    id           TEXT PRIMARY KEY,
    session_id   TEXT NOT NULL,
    kind         TEXT NOT NULL,
    due_at       INTEGER NOT NULL,
    interval_sec INTEGER NOT NULL DEFAULT 0,
    payload      TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'pending'
);
CREATE INDEX IF NOT EXISTS timers_due ON timers(status, due_at);

CREATE TABLE IF NOT EXISTS outbox (
    id          TEXT PRIMARY KEY,
    session_id  TEXT NOT NULL,
    kind        TEXT NOT NULL,
    payload     TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    attempts    INTEGER NOT NULL DEFAULT 0,
    created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(status, created_at);

CREATE TABLE IF NOT EXISTS approvals (
    id            TEXT PRIMARY KEY,
    session_id    TEXT NOT NULL UNIQUE,
    interrupt_id  TEXT NOT NULL DEFAULT '',
    request       TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT 'pending',
    resolved_at   INTEGER NOT NULL DEFAULT 0
);

-- Tombstones survive inbox pruning, so a redelivered old event stays a
-- duplicate. Prune removes them only after the dedup retention window.
CREATE TABLE IF NOT EXISTS dedup (
    source_id TEXT PRIMARY KEY,
    seen_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    name     TEXT PRIMARY KEY,
    owner    TEXT NOT NULL,
    until_ms INTEGER NOT NULL
);
`

// Store is a SQLite-backed control plane. One connection serializes writes,
// which keeps the lease CAS and event claims atomic without extra locking.
type Store struct {
	db *sql.DB
}

// Open creates the database at path and applies the schema. Use ":memory:"
// for tests. The caller must Close the Store.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store: open %q: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: apply schema: %w", err)
	}
	if err := ensureInboxColumns(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: migrate inbox: %w", err)
	}
	return &Store{db: db}, nil
}

// ensureInboxColumns upgrades databases created before the lease_owner and
// last_error columns existed. CREATE TABLE IF NOT EXISTS does not alter rows.
func ensureInboxColumns(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(inbox)`)
	if err != nil {
		return err
	}
	columns := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt any
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			_ = rows.Close()
			return err
		}
		columns[name] = true
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if !columns["lease_owner"] {
		if _, err := db.Exec(`ALTER TABLE inbox ADD COLUMN lease_owner TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if !columns["last_error"] {
		if _, err := db.Exec(`ALTER TABLE inbox ADD COLUMN last_error TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	approvalRows, err := db.Query(`PRAGMA table_info(approvals)`)
	if err != nil {
		return err
	}
	approvalCols := map[string]bool{}
	for approvalRows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt any
		var pk int
		if err := approvalRows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			_ = approvalRows.Close()
			return err
		}
		approvalCols[name] = true
	}
	if err := approvalRows.Close(); err != nil {
		return err
	}
	if !approvalCols["resolved_at"] {
		if _, err := db.Exec(`ALTER TABLE approvals ADD COLUMN resolved_at INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	return nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// Enqueue inserts an event once. A duplicate SourceID is silently ignored,
// which makes at-least-once delivery from a Knative Broker safe.
// It returns true when a new row was inserted.
func (s *Store) Enqueue(ctx context.Context, in aoa.Inbound) (bool, error) {
	if in.SourceID == "" {
		return false, errors.New("store: SourceID is required")
	}
	now := time.Now().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("store: begin enqueue tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// The dedup table is the authority. The inbox row alone cannot dedup
	// after Prune deletes terminal events.
	res, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO dedup(source_id, seen_at) VALUES(?, ?)`, in.SourceID, now)
	if err != nil {
		return false, fmt.Errorf("store: dedup %q: %w", in.SourceID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO inbox
    (id, source_id, kind, session_id, payload, resume, status, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)`,
		uuid.NewString(), in.SourceID, string(in.Kind), in.SessionID,
		in.Payload, in.ResumeJSON, now, now)
	if err != nil {
		return false, fmt.Errorf("store: enqueue %q: %w", in.SourceID, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: commit enqueue: %w", err)
	}
	return true, nil
}

// AcquireLease claims the right to be the only active inbox worker. A lease
// that has expired can be taken over by a replacement worker.
func (s *Store) AcquireLease(ctx context.Context, owner string, now time.Time, ttl time.Duration) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
INSERT INTO leases(name, owner, until_ms) VALUES('inbox', ?, ?)
ON CONFLICT(name) DO UPDATE SET owner=excluded.owner, until_ms=excluded.until_ms
WHERE leases.until_ms < ?`, owner, now.Add(ttl).UnixMilli(), now.UnixMilli())
	if err != nil {
		return false, fmt.Errorf("store: acquire lease: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: acquire lease rows affected: %w", err)
	}
	return n == 1, nil
}

// RenewLease extends a lease held by owner while a long run is in flight.
func (s *Store) RenewLease(ctx context.Context, owner string, until time.Time) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE leases SET until_ms=? WHERE name='inbox' AND owner=? AND until_ms>0`,
		until.UnixMilli(), owner)
	if err != nil {
		return fmt.Errorf("store: renew lease: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: renew lease rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("store: lease lost for %q", owner)
	}
	return nil
}

// ReleaseLease gives up the worker slot immediately after a batch finishes.
func (s *Store) ReleaseLease(ctx context.Context, owner string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE leases SET until_ms=0 WHERE name='inbox' AND owner=?`, owner)
	if err != nil {
		return fmt.Errorf("store: release lease: %w", err)
	}
	return nil
}

// Claim takes up to limit pending events as one atomic batch. The reclaim,
// select, and claim updates run in a single transaction so two workers can
// never observe the same pending row. owner is an optional fence token: when
// supplied, only that owner may Ack or Fail the row.
func (s *Store) Claim(ctx context.Context, now time.Time, ttl time.Duration, limit int, owner ...string) ([]aoa.Event, error) {
	if limit < 1 {
		limit = 1
	}
	fence := ""
	if len(owner) > 0 {
		fence = owner[0]
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: begin claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`UPDATE inbox SET status='pending', lease_owner='' WHERE status='claimed' AND lease_until < ?`,
		now.UnixMilli()); err != nil {
		return nil, fmt.Errorf("store: reclaim expired claims: %w", err)
	}
	rows, err := tx.QueryContext(ctx, `
SELECT id, source_id, kind, session_id, payload, resume, attempts, created_at
FROM inbox WHERE status='pending' ORDER BY created_at, id LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: select pending: %w", err)
	}
	var events []aoa.Event
	for rows.Next() {
		var e aoa.Event
		var created int64
		var kind string
		if err := rows.Scan(&e.ID, &e.SourceID, &kind, &e.SessionID,
			&e.Payload, &e.ResumeJSON, &e.Attempts, &created); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("store: scan pending: %w", err)
		}
		e.Kind = aoa.Kind(kind)
		e.CreatedAt = time.UnixMilli(created)
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("store: iterate pending: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("store: close pending rows: %w", err)
	}

	until := now.Add(ttl).UnixMilli()
	for i := range events {
		res, err := tx.ExecContext(ctx, `
UPDATE inbox SET status='claimed', attempts=attempts+1, lease_until=?,
    lease_owner=?, updated_at=? WHERE id=? AND status='pending'`,
			until, fence, now.UnixMilli(), events[i].ID)
		if err != nil {
			return nil, fmt.Errorf("store: claim %q: %w", events[i].ID, err)
		}
		n, err := res.RowsAffected()
		if err != nil || n != 1 {
			return nil, fmt.Errorf("store: claim %q lost to another worker", events[i].ID)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: commit claim: %w", err)
	}
	return events, nil
}

// ErrNotClaimed is returned when Ack or Fail targets a row the caller does
// not own. It prevents a stale worker from finishing a taken-over event.
var ErrNotClaimed = errors.New("store: event is not claimed by this worker")

// Ack marks an event done. It is the last step of a run: a crash between a
// side effect and Ack replays the event, so tool effects must be idempotent.
func (s *Store) Ack(ctx context.Context, id string, owner ...string) error {
	query := `UPDATE inbox SET status='done', lease_until=0, lease_owner='', updated_at=? WHERE id=? AND status='claimed'`
	args := []any{time.Now().UnixMilli(), id}
	if len(owner) > 0 && owner[0] != "" {
		query += ` AND lease_owner=?`
		args = append(args, owner[0])
	}
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("store: ack %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: ack %q rows affected: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: %q", ErrNotClaimed, id)
	}
	return nil
}

// Fail requeues an event for retry. The error text goes to last_error, never
// into payload, so retries cannot grow the model context without bound.
func (s *Store) Fail(ctx context.Context, id string, cause error, maxAttempts int, owner ...string) error {
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	if len(msg) > 1024 {
		msg = msg[:1024]
	}
	query := `
UPDATE inbox
SET status=CASE WHEN attempts >= ? THEN 'failed' ELSE 'pending' END,
    lease_until=0,
    lease_owner='',
    last_error=?,
    updated_at=?
WHERE id=? AND status='claimed'`
	args := []any{maxAttempts, msg, time.Now().UnixMilli(), id}
	if len(owner) > 0 && owner[0] != "" {
		query += ` AND lease_owner=?`
		args = append(args, owner[0])
	}
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("store: fail %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return fmt.Errorf("%w: %q", ErrNotClaimed, id)
	}
	return nil
}

// Timer is a durable wake-up request written by the agent during a run.
type Timer struct {
	ID          string
	SessionID   string
	Kind        aoa.TimerKind
	DueAt       time.Time
	IntervalSec int
	Payload     string
}

// AddTimer persists a follow-up or periodic timer. The scheduler, not the
// model, owns the actual wake-up.
func (s *Store) AddTimer(ctx context.Context, t Timer) error {
	if t.ID == "" {
		t.ID = uuid.NewString()
	}
	if t.Kind == "" {
		t.Kind = aoa.TimerFollowUp
	}
	if t.Kind == aoa.TimerPeriodic && t.IntervalSec <= 0 {
		return errors.New("store: periodic timer requires a positive interval")
	}
	if t.DueAt.IsZero() {
		return errors.New("store: timer due time is required")
	}
	if t.IntervalSec > 30*24*3600 {
		return errors.New("store: timer interval exceeds 30 days")
	}
	if len(t.Payload) > 4096 {
		return errors.New("store: timer payload exceeds 4096 bytes")
	}
	// INSERT OR IGNORE makes the deterministic timer id idempotent when a run
	// replays after a crash between the side effect and the inbox Ack.
	_, err := s.db.ExecContext(ctx, `
INSERT OR IGNORE INTO timers(id, session_id, kind, due_at, interval_sec, payload, status)
VALUES (?, ?, ?, ?, ?, ?, 'pending')`,
		t.ID, t.SessionID, string(t.Kind), t.DueAt.UnixMilli(),
		t.IntervalSec, t.Payload)
	if err != nil {
		return fmt.Errorf("store: add timer: %w", err)
	}
	return nil
}

// DueTimers returns timers whose due time has passed.
func (s *Store) DueTimers(ctx context.Context, now time.Time, limit int) ([]Timer, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, session_id, kind, due_at, interval_sec, payload
FROM timers WHERE status='pending' AND due_at <= ? ORDER BY due_at LIMIT ?`,
		now.UnixMilli(), limit)
	if err != nil {
		return nil, fmt.Errorf("store: select due timers: %w", err)
	}
	defer rows.Close()
	var out []Timer
	for rows.Next() {
		var t Timer
		var due int64
		var kind string
		if err := rows.Scan(&t.ID, &t.SessionID, &kind, &due,
			&t.IntervalSec, &t.Payload); err != nil {
			return nil, fmt.Errorf("store: scan due timer: %w", err)
		}
		t.Kind = aoa.TimerKind(kind)
		t.DueAt = time.UnixMilli(due)
		out = append(out, t)
	}
	return out, rows.Err()
}

// MarkTimerFired closes a one-shot timer or advances a periodic one. The
// status predicate makes the transition idempotent under a retry.
func (s *Store) MarkTimerFired(ctx context.Context, t Timer) error {
	var res sql.Result
	var err error
	if t.Kind == aoa.TimerPeriodic {
		res, err = s.db.ExecContext(ctx, `
UPDATE timers SET due_at=due_at + ? WHERE id=? AND status='pending'`,
			t.IntervalSec*1000, t.ID)
	} else {
		res, err = s.db.ExecContext(ctx,
			`UPDATE timers SET status='fired' WHERE id=? AND status='pending'`, t.ID)
	}
	if err != nil {
		return fmt.Errorf("store: mark timer fired %q: %w", t.ID, err)
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		return fmt.Errorf("store: timer %q already fired", t.ID)
	}
	return nil
}

// OutboxMessage is one pending outbound delivery.
type OutboxMessage struct {
	ID        string
	SessionID string
	Kind      aoa.OutboxKind
	Payload   string
	Attempts  int
}

// EnqueueOutbox records a message for the dispatcher. The run ends here;
// the run itself never talks to a user channel.
func (s *Store) EnqueueOutbox(ctx context.Context, sessionID string, kind aoa.OutboxKind, payload string) error {
	if len(payload) > 16<<10 {
		return errors.New("store: outbox payload exceeds 16 KiB")
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO outbox(id, session_id, kind, payload, status, created_at)
VALUES (?, ?, ?, ?, 'pending', ?)`,
		uuid.NewString(), sessionID, string(kind), payload, time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("store: enqueue outbox: %w", err)
	}
	return nil
}

// PendingOutbox returns messages the dispatcher has not delivered yet.
func (s *Store) PendingOutbox(ctx context.Context, limit int) ([]OutboxMessage, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, session_id, kind, payload, attempts FROM outbox
WHERE status='pending' ORDER BY created_at LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: select pending outbox: %w", err)
	}
	defer rows.Close()
	var out []OutboxMessage
	for rows.Next() {
		var m OutboxMessage
		var kind string
		if err := rows.Scan(&m.ID, &m.SessionID, &kind, &m.Payload, &m.Attempts); err != nil {
			return nil, fmt.Errorf("store: scan outbox: %w", err)
		}
		m.Kind = aoa.OutboxKind(kind)
		out = append(out, m)
	}
	return out, rows.Err()
}

// MarkOutboxSent marks a delivery complete.
func (s *Store) MarkOutboxSent(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE outbox SET status='sent' WHERE id=?`, id)
	if err != nil {
		return fmt.Errorf("store: mark outbox sent %q: %w", id, err)
	}
	return nil
}

// MarkOutboxFailed leaves the row pending and counts the attempt.
func (s *Store) MarkOutboxFailed(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE outbox SET attempts=attempts+1 WHERE id=?`, id)
	if err != nil {
		return fmt.Errorf("store: mark outbox failed %q: %w", id, err)
	}
	return nil
}

// Approval is a suspended tool call waiting for a human decision.
type Approval struct {
	ID          string
	SessionID   string
	InterruptID string
	Request     string
	Status      string
}

// UpsertApproval stores the approval card for a session. A new interrupt on
// the same session replaces the stale card.
func (s *Store) UpsertApproval(ctx context.Context, a Approval) error {
	if a.ID == "" {
		a.ID = uuid.NewString()
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO approvals(id, session_id, interrupt_id, request, status)
VALUES (?, ?, ?, ?, 'pending')
ON CONFLICT(session_id) DO UPDATE SET
    id=excluded.id,
    interrupt_id=excluded.interrupt_id,
    request=excluded.request,
    status='pending'`,
		a.ID, a.SessionID, a.InterruptID, a.Request)
	if err != nil {
		return fmt.Errorf("store: upsert approval: %w", err)
	}
	return nil
}

// ResolveApproval consumes a pending approval atomically. A second reply for
// the same card returns ErrApprovalAlreadyResolved.
var ErrApprovalAlreadyResolved = errors.New("store: approval already resolved")

// ListPendingApprovals returns every card still waiting for a decision. An
// Agent Inbox style UI renders these cards.
func (s *Store) ListPendingApprovals(ctx context.Context) ([]Approval, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, session_id, interrupt_id, request, status FROM approvals
WHERE status='pending' ORDER BY rowid`)
	if err != nil {
		return nil, fmt.Errorf("store: list pending approvals: %w", err)
	}
	defer rows.Close()
	var out []Approval
	for rows.Next() {
		var a Approval
		if err := rows.Scan(&a.ID, &a.SessionID, &a.InterruptID, &a.Request, &a.Status); err != nil {
			return nil, fmt.Errorf("store: scan approval: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// GetApproval reads an approval card by id.
func (s *Store) GetApproval(ctx context.Context, id string) (Approval, error) {
	var a Approval
	err := s.db.QueryRowContext(ctx, `
SELECT id, session_id, interrupt_id, request, status FROM approvals WHERE id=?`, id).
		Scan(&a.ID, &a.SessionID, &a.InterruptID, &a.Request, &a.Status)
	if err != nil {
		return Approval{}, fmt.Errorf("store: get approval %q: %w", id, err)
	}
	return a, nil
}

// ResolveApproval marks the card resolved and returns it. Only a pending
// card can be resolved.
func (s *Store) ResolveApproval(ctx context.Context, id string, approved bool) (Approval, error) {
	status := "rejected"
	if approved {
		status = "approved"
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE approvals SET status=?, resolved_at=? WHERE id=? AND status='pending'`,
		status, time.Now().UnixMilli(), id)
	if err != nil {
		return Approval{}, fmt.Errorf("store: resolve approval %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return Approval{}, fmt.Errorf("store: resolve approval rows affected: %w", err)
	}
	if n == 0 {
		return Approval{}, ErrApprovalAlreadyResolved
	}
	return s.GetApproval(ctx, id)
}

// ErrApprovalNotFound is returned when an approval id does not exist.
var ErrApprovalNotFound = errors.New("store: approval not found")

// ResolveApprovalAndEnqueue resolves a card and writes the reply event in one
// transaction. Without this order a crash between the two writes either loses
// the human decision or replays a stale approval card.
func (s *Store) ResolveApprovalAndEnqueue(ctx context.Context, approvalID, decision, resumeJSON string) (bool, error) {
	status := "rejected"
	if decision == "approve" {
		status = "approved"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("store: begin approval tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var sessionID, current string
	err = tx.QueryRowContext(ctx,
		`SELECT session_id, status FROM approvals WHERE id=?`, approvalID).
		Scan(&sessionID, &current)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrApprovalNotFound
	}
	if err != nil {
		return false, fmt.Errorf("store: read approval %q: %w", approvalID, err)
	}
	if current != "pending" {
		// Idempotent replay: never enqueue a second, conflicting decision.
		return false, nil
	}

	now := time.Now().UnixMilli()
	sourceID := fmt.Sprintf("approval:%s:%s", approvalID, decision)
	if _, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO dedup(source_id, seen_at) VALUES(?, ?)`, sourceID, now); err != nil {
		return false, fmt.Errorf("store: approval dedup: %w", err)
	}
	res, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO inbox
    (id, source_id, kind, session_id, payload, resume, status, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)`,
		uuid.NewString(), sourceID,
		string(aoa.KindApproval), sessionID, "Human decision: "+decision,
		resumeJSON, now, now)
	if err != nil {
		return false, fmt.Errorf("store: enqueue approval reply: %w", err)
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE approvals SET status=?, resolved_at=? WHERE id=? AND status='pending'`,
		status, now, approvalID); err != nil {
		return false, fmt.Errorf("store: resolve approval %q: %w", approvalID, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: commit approval: %w", err)
	}
	return inserted == 1, nil
}

// Prune removes terminal rows older than cutoff. All deletes run in one
// transaction so a crash cannot leave a half-pruned control plane. Dedup
// tombstones use the same cutoff: after that window a replayed event is
// treated as new by design.
func (s *Store) Prune(ctx context.Context, cutoff time.Time) (int64, error) {
	cutoffMS := cutoff.UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("store: begin prune tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	statements := []string{
		`DELETE FROM inbox WHERE status IN ('done','failed') AND updated_at < ?`,
		`DELETE FROM outbox WHERE status IN ('sent','dead') AND created_at < ?`,
		`DELETE FROM timers WHERE status='fired' AND due_at < ?`,
		`DELETE FROM approvals WHERE status IN ('approved','rejected') AND resolved_at > 0 AND resolved_at < ?`,
		`DELETE FROM dedup WHERE seen_at < ?`,
	}
	var total int64
	for _, query := range statements {
		res, err := tx.ExecContext(ctx, query, cutoffMS)
		if err != nil {
			return total, fmt.Errorf("store: prune: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, err
		}
		total += n
	}
	if err := tx.Commit(); err != nil {
		return total, fmt.Errorf("store: commit prune: %w", err)
	}
	return total, nil
}
