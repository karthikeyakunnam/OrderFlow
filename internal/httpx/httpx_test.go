package httpx

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeJSONRejectsTrailingValues(t *testing.T) {
	request := httptest.NewRequest("POST", "/", strings.NewReader(`{"name":"cartnova"} {"extra":true}`))
	var target struct {
		Name string `json:"name"`
	}
	if err := DecodeJSON(request, &target); err == nil {
		t.Fatal("expected trailing JSON to be rejected")
	}
}

func TestDecodeJSONRejectsUnknownFields(t *testing.T) {
	request := httptest.NewRequest("POST", "/", strings.NewReader(`{"unknown":true}`))
	var target struct {
		Name string `json:"name"`
	}
	if err := DecodeJSON(request, &target); err == nil {
		t.Fatal("expected unknown field to be rejected")
	}
}
