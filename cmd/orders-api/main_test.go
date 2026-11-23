package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type recordingPublisher struct {
	events []orderCreatedEvent
}

func (p *recordingPublisher) Publish(_ context.Context, event orderCreatedEvent) error {
	p.events = append(p.events, event)
	return nil
}

func TestCreateOrderPublishesEventAndReturnsAccepted(t *testing.T) {
	publisher := &recordingPublisher{}
	application := api{publisher: publisher}
	body := []byte(`{"items":[{"productId":"book-123","quantity":2}]}`)
	request := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(body))
	response := httptest.NewRecorder()

	application.createOrder(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusAccepted)
	}
	if len(publisher.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(publisher.events))
	}
	if publisher.events[0].Items[0].ProductID != "book-123" || publisher.events[0].Items[0].Quantity != 2 {
		t.Fatalf("published event has unexpected item: %+v", publisher.events[0].Items[0])
	}

	var result map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result["status"] != "accepted" || result["orderId"] == "" {
		t.Fatalf("unexpected response: %v", result)
	}
}

func TestCreateOrderRejectsNonPositiveQuantity(t *testing.T) {
	publisher := &recordingPublisher{}
	application := api{publisher: publisher}
	body := []byte(`{"items":[{"productId":"book-123","quantity":0}]}`)
	request := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(body))
	response := httptest.NewRecorder()

	application.createOrder(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if len(publisher.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(publisher.events))
	}
}
