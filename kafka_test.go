package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/StevenBuglione/spice/messaging"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNormalizeConfigSecureDefaults(t *testing.T) {
	t.Parallel()
	config, err := normalizeConfig(Config{
		Brokers:              []string{"broker-b:9092", "broker-a:9092"},
		Username:             "service",
		Password:             "secret",
		AllowUnauthenticated: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(
		config.Brokers,
		[]string{"broker-a:9092", "broker-b:9092"},
	) ||
		config.ClientID != defaultClientID ||
		config.DialTimeout != defaultDialTimeout ||
		config.RequestTimeout != defaultRequestTimeout ||
		config.BatchMaxBytes != defaultBatchMaxBytes ||
		config.TLSConfig == nil ||
		config.TLSConfig.MinVersion != tls.VersionTLS12 ||
		config.TLSConfig.InsecureSkipVerify {
		t.Fatalf("normalized config = %#v", config)
	}
}

func TestNormalizeConfigRejectsUnsafeValues(t *testing.T) {
	t.Parallel()
	valid := Config{
		Brokers:              []string{"broker:9092"},
		AllowInsecure:        true,
		AllowUnauthenticated: true,
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "missing broker", mutate: func(config *Config) { config.Brokers = nil }},
		{name: "duplicate broker", mutate: func(config *Config) {
			config.Brokers = []string{"broker:9092", "broker:9092"}
		}},
		{name: "URL broker", mutate: func(config *Config) {
			config.Brokers = []string{"kafka://broker:9092"}
		}},
		{name: "invalid port", mutate: func(config *Config) {
			config.Brokers = []string{"broker:0"}
		}},
		{name: "invalid client ID", mutate: func(config *Config) {
			config.ClientID = "client\nsecret"
		}},
		{name: "partial credentials", mutate: func(config *Config) {
			config.Username = "service"
		}},
		{name: "unauthenticated", mutate: func(config *Config) {
			config.AllowUnauthenticated = false
		}},
		{name: "unverified TLS", mutate: func(config *Config) {
			config.TLSConfig = &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: true, //nolint:gosec // Rejection fixture.
			}
		}},
		{name: "old TLS", mutate: func(config *Config) {
			config.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS11}
		}},
		{name: "negative timeout", mutate: func(config *Config) {
			config.DialTimeout = -time.Second
		}},
		{name: "oversized timeout", mutate: func(config *Config) {
			config.RequestTimeout = maxTimeout + time.Second
		}},
		{name: "small batch", mutate: func(config *Config) {
			config.BatchMaxBytes = 100
		}},
		{name: "large batch", mutate: func(config *Config) {
			config.BatchMaxBytes = defaultBatchMaxBytes + 1
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := valid
			test.mutate(&config)
			if _, err := normalizeConfig(config); err == nil {
				t.Fatal("normalizeConfig() error = nil")
			}
		})
	}
}

func TestPublisherPublishesRecordAndObserves(t *testing.T) {
	t.Parallel()
	client := &fakeProducer{}
	var observed []Result
	observer := observerFunc(func(
		ctx context.Context,
		interaction Interaction,
	) (context.Context, func(Result)) {
		if interaction.Topic != "orders.placed" {
			t.Fatalf("interaction = %#v", interaction)
		}
		return context.WithValue(ctx, contextKey{}, "observed"), func(result Result) {
			observed = append(observed, result)
		}
	})
	publisher := newPublisher(client, []Observer{observer})
	message := testMessage(t)
	if err := publisher.Publish(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if len(client.records) != 1 ||
		client.contextValue != "observed" {
		t.Fatalf(
			"records=%d context=%q",
			len(client.records),
			client.contextValue,
		)
	}
	record := client.records[0]
	if record.Topic != message.Topic() ||
		string(record.Key) != message.Key() ||
		string(record.Value) != string(message.Payload()) ||
		len(record.Headers) != 4 {
		t.Fatalf("record = %#v", record)
	}
	if len(observed) != 1 ||
		observed[0].Err != nil ||
		observed[0].Duration < 0 {
		t.Fatalf("observed = %#v", observed)
	}
}

func TestPublisherReportsFailuresAndReservedHeaders(t *testing.T) {
	t.Parallel()
	publishFailure := errors.New("broker rejected record")
	client := &fakeProducer{produceErr: publishFailure}
	publisher := newPublisher(client, nil)
	if err := publisher.Publish(
		context.Background(),
		testMessage(t),
	); !errors.Is(err, publishFailure) {
		t.Fatalf("Publish() error = %v", err)
	}
	reserved, err := messaging.NewMessage(messaging.MessageSpec{
		ID:          "message-1",
		Topic:       "orders.placed",
		ContentType: "application/json",
		Payload:     []byte("{}"),
		Headers: []messaging.Header{{
			Name:  "content-type",
			Value: "text/plain",
		}},
		OccurredAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.Publish(
		context.Background(),
		reserved,
	); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("Publish(reserved header) error = %v", err)
	}
}

func TestPublisherPingAndCloseAreBoundedAndIdempotent(t *testing.T) {
	t.Parallel()
	flushFailure := errors.New("flush failed")
	client := &fakeProducer{flushErr: flushFailure}
	publisher := newPublisher(client, nil)
	if err := publisher.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := publisher.Close(
		context.Background(),
	); !errors.Is(err, flushFailure) {
		t.Fatalf("Close() error = %v", err)
	}
	if err := publisher.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if client.closeCalls.Load() != 1 ||
		client.flushCalls.Load() != 1 {
		t.Fatalf(
			"close calls=%d flush calls=%d",
			client.closeCalls.Load(),
			client.flushCalls.Load(),
		)
	}
	if err := publisher.Publish(
		context.Background(),
		testMessage(t),
	); err == nil {
		t.Fatal("Publish(after close) error = nil")
	}
	if err := publisher.Ping(context.Background()); err == nil {
		t.Fatal("Ping(after close) error = nil")
	}
}

func TestPublisherRejectsInvalidUse(t *testing.T) {
	t.Parallel()
	message := testMessage(t)
	publisher := newPublisher(&fakeProducer{}, nil)
	if err := publisher.Publish(nilContext(), message); err == nil {
		t.Fatal("Publish(nil context) error = nil")
	}
	if err := (*Publisher)(nil).Publish(context.Background(), message); err == nil {
		t.Fatal("Publish(nil producer) error = nil")
	}
	if err := publisher.Ping(nilContext()); err == nil {
		t.Fatal("Ping(nil context) error = nil")
	}
	if err := (*Publisher)(nil).Ping(context.Background()); err == nil {
		t.Fatal("Ping(nil producer) error = nil")
	}
	if err := publisher.Close(nilContext()); err == nil {
		t.Fatal("Close(nil context) error = nil")
	}
	if err := (*Publisher)(nil).Close(context.Background()); err == nil {
		t.Fatal("Close(nil producer) error = nil")
	}
	var typedNil *observerFunc
	if _, _, err := Open(Config{
		Brokers:              []string{"broker:9092"},
		AllowInsecure:        true,
		AllowUnauthenticated: true,
	}, typedNil); err == nil {
		t.Fatal("Open(typed nil observer) error = nil")
	}
}

func TestManifest(t *testing.T) {
	t.Parallel()
	spec := Manifest().Spec()
	if spec.ID != "github.com/StevenBuglione/spice/starter/kafka" ||
		!slices.Equal(
			spec.Capabilities,
			[]string{
				"messaging.kafka.consumer-group",
				"messaging.kafka.producer",
			},
		) ||
		len(spec.Dependencies) != 1 ||
		spec.Dependencies[0].Version != "v1.21.0" {
		t.Fatalf("Manifest() = %#v", spec)
	}
}

func testMessage(t *testing.T) messaging.Message {
	t.Helper()
	message, err := messaging.NewMessage(messaging.MessageSpec{
		ID:          "message-1",
		Topic:       "orders.placed",
		Key:         "order-1",
		ContentType: "application/json",
		Payload:     []byte(`{"order":"1"}`),
		Headers: []messaging.Header{{
			Name:  "trace-id",
			Value: "trace-1",
		}},
		OccurredAt: time.Unix(1, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	return message
}

type contextKey struct{}

type fakeProducer struct {
	records      []*kgo.Record
	contextValue string
	produceErr   error
	flushErr     error
	pingErr      error
	closeCalls   atomic.Int32
	flushCalls   atomic.Int32
	mu           sync.Mutex
}

func (client *fakeProducer) ProduceSync(
	ctx context.Context,
	records ...*kgo.Record,
) kgo.ProduceResults {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.records = append(client.records, records...)
	if value, found := ctx.Value(contextKey{}).(string); found {
		client.contextValue = value
	}
	results := make(kgo.ProduceResults, len(records))
	for index, record := range records {
		results[index] = kgo.ProduceResult{
			Record: record,
			Err:    client.produceErr,
		}
	}
	return results
}

func (client *fakeProducer) Flush(context.Context) error {
	client.flushCalls.Add(1)
	return client.flushErr
}

func (client *fakeProducer) Ping(context.Context) error {
	return client.pingErr
}

func (client *fakeProducer) Close() {
	client.closeCalls.Add(1)
}

type observerFunc func(
	context.Context,
	Interaction,
) (context.Context, func(Result))

func (observer observerFunc) BeginPublish(
	ctx context.Context,
	interaction Interaction,
) (context.Context, func(Result)) {
	return observer(ctx, interaction)
}

func nilContext() context.Context {
	return nil
}
