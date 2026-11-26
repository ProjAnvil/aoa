package aoa

import "testing"

func TestKindForCloudEventType(t *testing.T) {
	cases := map[string]Kind{
		CETypeUserMessage:    KindUserMessage,
		CETypeApproval:       KindApproval,
		CETypeTimer:          KindTimer,
		CETypeBackground:     KindBackground,
		CETypeWebhook:        KindWebhook,
		"com.example.custom": KindWebhook,
	}
	for in, want := range cases {
		if got := KindForCloudEventType(in); got != want {
			t.Fatalf("KindForCloudEventType(%q) = %q, want %q", in, got, want)
		}
	}
}
