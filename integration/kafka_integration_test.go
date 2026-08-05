//go:build integration

package integration_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spice-framework/spice/messaging"
	kafkastarter "github.com/spice-framework/starter-kafka"
)

const acceptanceTimeout = 30 * time.Second

func TestAuthenticatedBrokerRoundTripAndCommit(t *testing.T) {
	broker := requireEnvironment(t, "SPICE_KAFKA_BROKER")
	username := requireEnvironment(t, "SPICE_KAFKA_USERNAME")
	password := requireEnvironment(t, "SPICE_KAFKA_PASSWORD")
	topic := environmentOr("SPICE_KAFKA_TOPIC", "spice.acceptance")
	group := environmentOr("SPICE_KAFKA_GROUP", "spice-acceptance")
	tlsConfig, insecure := integrationTLS(t)
	transport := kafkastarter.Config{
		Brokers:        []string{broker},
		Username:       username,
		Password:       password,
		SASLMechanism:  kafkastarter.SASLSCRAMSHA256,
		TLSConfig:      tlsConfig,
		AllowInsecure:  insecure,
		DialTimeout:    5 * time.Second,
		RequestTimeout: 10 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), acceptanceTimeout)
	defer cancel()
	assertAuthenticationFailureIsSafe(t, ctx, transport, password)

	consumer, closeConsumer, err := kafkastarter.OpenConsumer(
		kafkastarter.ConsumerConfig{
			Transport:      transport,
			GroupID:        group,
			Topics:         []string{topic},
			MaxPollRecords: 1,
		},
	)
	if err != nil {
		t.Fatalf("open consumer: %v", err)
	}
	defer cleanup(t, closeConsumer)
	if err := consumer.Ping(ctx); err != nil {
		t.Fatalf("ping consumer: %v", err)
	}

	received := make(chan messaging.Message, 2)
	consumerContext, stopConsumer := context.WithCancel(ctx)
	consumerResult := make(chan error, 1)
	go func() {
		consumerResult <- consumer.Run(
			consumerContext,
			func(_ context.Context, message messaging.Message) error {
				received <- message
				if len(received) == cap(received) {
					stopConsumer()
				}
				return nil
			},
		)
	}()

	publisher, closePublisher, err := kafkastarter.Open(transport)
	if err != nil {
		t.Fatalf("open publisher: %v", err)
	}
	defer cleanup(t, closePublisher)
	if err := publisher.Ping(ctx); err != nil {
		t.Fatalf("ping publisher: %v", err)
	}
	for index := range 2 {
		if err := publisher.Publish(ctx, integrationMessage(t, topic, index)); err != nil {
			t.Fatalf("publish message %d: %v", index, err)
		}
	}
	if err := <-consumerResult; err != nil {
		t.Fatalf("run consumer: %v", err)
	}

	close(received)
	var identifiers []string
	for message := range received {
		identifiers = append(identifiers, message.ID())
		if string(message.Payload()) != "{\"safe\":\"broker-round-trip\"}" {
			t.Fatalf("payload = %q", message.Payload())
		}
	}
	if strings.Join(identifiers, ",") != "acceptance-0,acceptance-1" {
		t.Fatalf("message order = %v", identifiers)
	}

	assertCommittedGroupDoesNotRedeliver(t, transport, topic, group)
}

func assertAuthenticationFailureIsSafe(
	t *testing.T,
	ctx context.Context,
	config kafkastarter.Config,
	secret string,
) {
	t.Helper()
	config.Password = "deliberately-wrong-integration-secret"
	publisher, closePublisher, err := kafkastarter.Open(config)
	if err != nil {
		t.Fatalf("open wrong-credential producer: %v", err)
	}
	defer cleanup(t, closePublisher)
	wrongContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = publisher.Ping(wrongContext)
	if err == nil {
		t.Fatal("wrong credentials unexpectedly authenticated")
	}
	for _, forbidden := range []string{
		secret,
		config.Password,
		"{\"safe\":\"broker-round-trip\"}",
	} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("authentication failure exposed sensitive value %q: %v", forbidden, err)
		}
	}
}

func assertCommittedGroupDoesNotRedeliver(
	t *testing.T,
	transport kafkastarter.Config,
	topic string,
	group string,
) {
	t.Helper()
	consumer, closeConsumer, err := kafkastarter.OpenConsumer(kafkastarter.ConsumerConfig{
		Transport:      transport,
		GroupID:        group,
		Topics:         []string{topic},
		MaxPollRecords: 1,
	})
	if err != nil {
		t.Fatalf("reopen committed consumer: %v", err)
	}
	defer cleanup(t, closeConsumer)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var deliveries int
	err = consumer.Run(ctx, func(context.Context, messaging.Message) error {
		deliveries++
		return nil
	})
	if err != nil {
		t.Fatalf("run committed consumer: %v", err)
	}
	if deliveries != 0 {
		t.Fatalf("committed group redelivered %d records", deliveries)
	}
}

func integrationMessage(t *testing.T, topic string, index int) messaging.Message {
	t.Helper()
	message, err := messaging.NewMessage(messaging.MessageSpec{
		ID:          "acceptance-" + fmt.Sprint(index),
		Topic:       topic,
		Key:         "ordered-key",
		ContentType: "application/json",
		Payload:     []byte("{\"safe\":\"broker-round-trip\"}"),
		OccurredAt:  time.Unix(int64(index+1), 0),
	})
	if err != nil {
		t.Fatalf("construct integration message: %v", err)
	}
	return message
}

func integrationTLS(t *testing.T) (*tls.Config, bool) {
	t.Helper()
	caPath := os.Getenv("SPICE_KAFKA_CA")
	if caPath == "" {
		return nil, true
	}
	pem, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatalf("read broker CA: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatal("broker CA contains no certificates")
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: requireEnvironment(t, "SPICE_KAFKA_TLS_SERVER_NAME"),
	}, false
}

func cleanup(t *testing.T, cleanup func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cleanup(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("cleanup: %v", err)
	}
}

func requireEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}

func environmentOr(name string, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
