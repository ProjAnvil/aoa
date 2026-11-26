// Package aoa holds the shared event vocabulary of the always-on agent.
package aoa

import "time"

// Kind classifies an inbound event. Every external source maps to one of
// these kinds before it reaches the inbox.
type Kind string

const (
	KindUserMessage Kind = "user.message"
	KindWebhook     Kind = "webhook"
	KindTimer       Kind = "timer.fired"
	KindApproval    Kind = "approval.decision"
	KindBackground  Kind = "background.job"
)

// CloudEvent type values. A Knative Trigger filters on these values and
// forwards matching events to the aoa-ingress Service.
const (
	CESource          = "aoa/ingress"
	CETypeUserMessage = "com.projanvil.aoa.user.message"
	CETypeWebhook     = "com.projanvil.aoa.webhook"
	CETypeTimer       = "com.projanvil.aoa.timer"
	CETypeApproval    = "com.projanvil.aoa.approval"
	CETypeBackground  = "com.projanvil.aoa.background"
)

// Event is one durable inbox row. The inbox is the single source of truth:
// a worker can die after claiming an event and a replacement worker picks
// the same row up again.
type Event struct {
	ID         string
	SourceID   string
	Kind       Kind
	SessionID  string
	Payload    string
	ResumeJSON string
	Attempts   int
	CreatedAt  time.Time
}

// Inbound is the write shape of an event. SourceID is the dedup key,
// normally the CloudEvent id.
type Inbound struct {
	SourceID   string
	Kind       Kind
	SessionID  string
	Payload    string
	ResumeJSON string
}

// KindForCloudEventType maps a CloudEvent type to an inbox kind.
func KindForCloudEventType(ceType string) Kind {
	switch ceType {
	case CETypeUserMessage:
		return KindUserMessage
	case CETypeApproval:
		return KindApproval
	case CETypeTimer:
		return KindTimer
	case CETypeBackground:
		return KindBackground
	default:
		return KindWebhook
	}
}

// TimerKind separates a one-shot follow-up from a recurring schedule.
type TimerKind string

const (
	TimerFollowUp TimerKind = "follow-up"
	TimerPeriodic TimerKind = "periodic"
)

// OutboxKind separates a user-facing message from an approval card.
type OutboxKind string

const (
	OutboxMessage  OutboxKind = "message"
	OutboxApproval OutboxKind = "approval"
)
