package events

import "testing"

func TestNewEventCreatesUniqueID(t *testing.T) {
	first, err := New("TestEvent", map[string]string{"value": "one"})
	if err != nil {
		t.Fatal(err)
	}

	second, err := New("TestEvent", map[string]string{"value": "two"})
	if err != nil {
		t.Fatal(err)
	}

	if first.ID == "" || second.ID == "" {
		t.Fatal("expected event IDs")
	}
	if first.ID == second.ID {
		t.Fatal("expected unique event IDs")
	}
}

func TestDerivedEventIDIsDeterministic(t *testing.T) {
	first, err := NewDerived("InventoryReserved", "source-1", map[string]int{"order_id": 7})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewDerived("InventoryReserved", "source-1", map[string]int{"order_id": 7})
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("expected deterministic IDs, got %s and %s", first.ID, second.ID)
	}
}
