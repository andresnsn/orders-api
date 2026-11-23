package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const maxRequestBytes = 1 << 20 // 1 MiB

type orderItem struct {
	ProductID string `json:"productId"`
	Quantity  int    `json:"quantity"`
}

type createOrderRequest struct {
	Items []orderItem `json:"items"`
}

type orderCreatedEvent struct {
	OrderID   string      `json:"orderId"`
	Items     []orderItem `json:"items"`
	CreatedAt time.Time   `json:"createdAt"`
}

type eventPublisher interface {
	Publish(ctx context.Context, event orderCreatedEvent) error
}

type api struct {
	publisher eventPublisher
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	brokers := kafkaBrokersFromEnv()
	topic := envOrDefault("ORDERS_TOPIC", "orders.created.v1")
	publisher, err := newKafkaPublisher(brokers, topic)
	if err != nil {
		log.Fatalf("configure Kafka publisher: %v", err)
	}
	defer publisher.Close()

	application := api{publisher: publisher}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", application.health)
	mux.HandleFunc("POST /orders", application.createOrder)

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("orders-api listening on :%s; Kafka brokers=%s topic=%s", port, strings.Join(brokers, ","), topic)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP server: %v", err)
	}
}

func kafkaBrokersFromEnv() []string {
	value := envOrDefault("KAFKA_BROKERS", "localhost:9092")
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func (a api) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a api) createOrder(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()

	var request createOrderRequest
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	if err := ensureSingleJSONValue(decoder); err != nil {
		writeError(w, http.StatusBadRequest, "request must contain one JSON object")
		return
	}
	if err := validateOrder(request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	orderID, err := newOrderID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create order")
		return
	}

	event := orderCreatedEvent{
		OrderID:   orderID,
		Items:     request.Items,
		CreatedAt: time.Now().UTC(),
	}
	if err := a.publisher.Publish(r.Context(), event); err != nil {
		writeError(w, http.StatusServiceUnavailable, "could not publish order event")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"orderId": orderID,
		"status":  "accepted",
	})
}

func validateOrder(request createOrderRequest) error {
	if len(request.Items) == 0 {
		return errors.New("items must contain at least one product")
	}
	for _, item := range request.Items {
		if strings.TrimSpace(item.ProductID) == "" {
			return errors.New("each item must have a productId")
		}
		if item.Quantity <= 0 {
			return errors.New("each item quantity must be greater than zero")
		}
	}
	return nil
}

func ensureSingleJSONValue(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after JSON object")
	}
	return nil
}

func newOrderID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("could not encode HTTP response: %v", err)
	}
}
