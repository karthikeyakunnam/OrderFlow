package order

import "testing"

func TestIsAllowedStatus(t *testing.T) {
	validStatuses := []string{
		"CREATED",
		"CONFIRMED",
		"PROCESSING",
		"SHIPPED",
		"DELIVERED",
		"CANCELLED",
	}

	for _, status := range validStatuses {
		if !isAllowedStatus(status) {
			t.Errorf("expected %s to be allowed", status)
		}
	}
}

func TestIsAllowedStatusRejectsUnknownValue(t *testing.T) {
	if isAllowedStatus("UNKNOWN") {
		t.Fatal("expected UNKNOWN to be rejected")
	}
}

func TestCanTransitionRejectsBackwardTransitions(t *testing.T) {
	if canTransition("DELIVERED", "CREATED") {
		t.Fatal("expected backward transition to be rejected")
	}
	if !canTransition("CREATED", "CANCELLED") {
		t.Fatal("expected cancellation transition to be allowed")
	}
}
