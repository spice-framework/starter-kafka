package kafka

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spice-framework/spice/messaging"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConsumerOptionsNormalizeGroupTopicsAndBounds(t *testing.T) {
	t.Parallel()
	_, normalized, err := consumerOptions(ConsumerConfig{
		Transport: Config{
			Brokers:              []string{"broker:9092"},
			AllowInsecure:        true,
			AllowUnauthenticated: true,
		},
		GroupID: "orders",
		Topics:  []string{"orders.updated", "orders.created"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(
		normalized.Topics,
		[]string{"orders.created", "orders.updated"},
	) || normalized.MaxPollRecords != defaultMaxPollRecords {
		t.Fatalf("normalized = %#v", normalized)
	}
	tests := []ConsumerConfig{
		{
			Transport: normalized.Transport,
			GroupID:   "",
			Topics:    []string{"orders"},
		},
		{
			Transport: normalized.Transport,
			GroupID:   "orders",
			Topics:    nil,
		},
		{
			Transport: normalized.Transport,
			GroupID:   "orders",
			Topics:    []string{"orders", "orders"},
		},
		{
			Transport:      normalized.Transport,
			GroupID:        "orders",
			Topics:         []string{"orders"},
			MaxPollRecords: maxPollRecords + 1,
		},
	}
	for index, config := range tests {
		if _, _, err := consumerOptions(config); err == nil {
			t.Fatalf("consumerOptions(test %d) error = nil", index)
		}
	}
}

func TestConsumerRunAcknowledgesAndObserves(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeConsumer{
		fetches:  []kgo.Fetches{fetchWithRecord(testKafkaRecord(t))},
		onCommit: cancel,
	}
	var observed []DeliveryResult
	observer := consumerObserverFunc(func(
		ctx context.Context,
		interaction DeliveryInteraction,
	) (context.Context, func(DeliveryResult)) {
		if interaction.Group != "orders" ||
			interaction.Topic != "orders.placed" ||
			interaction.Partition != 2 {
			t.Fatalf("interaction = %#v", interaction)
		}
		return context.WithValue(ctx, contextKey{}, "delivery"), func(result DeliveryResult) {
			observed = append(observed, result)
		}
	})
	consumer := newConsumer(client, validConsumerConfig(), []ConsumerObserver{observer})
	var handled messaging.Message
	if err := consumer.Run(ctx, func(
		handlerContext context.Context,
		message messaging.Message,
	) error {
		if handlerContext.Value(contextKey{}) != "delivery" {
			t.Fatal("observer context was not propagated")
		}
		handled = message
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if handled.ID() != "message-1" ||
		handled.Topic() != "orders.placed" ||
		handled.Key() != "order-1" ||
		len(client.commits) != 1 ||
		len(observed) != 1 ||
		observed[0].Err != nil {
		t.Fatalf(
			"handled=%#v commits=%d observed=%#v",
			handled,
			len(client.commits),
			observed,
		)
	}
}

func TestConsumerRunLeavesRetryUncommitted(t *testing.T) {
	t.Parallel()
	handlerFailure := errors.New("database unavailable")
	client := &fakeConsumer{
		fetches: []kgo.Fetches{fetchWithRecord(testKafkaRecord(t))},
	}
	consumer := newConsumer(client, validConsumerConfig(), nil)
	err := consumer.Run(context.Background(), func(
		context.Context,
		messaging.Message,
	) error {
		return handlerFailure
	})
	if err == nil {
		t.Fatal("Run() error = nil")
	}
	if !errors.Is(err, handlerFailure) ||
		!strings.Contains(err.Error(), "offset 9") ||
		len(client.commits) != 0 {
		t.Fatalf("Run() error=%v commits=%d", err, len(client.commits))
	}
}

func TestConsumerRunReportsCommitFailure(t *testing.T) {
	t.Parallel()
	commitFailure := errors.New("commit unavailable")
	client := &fakeConsumer{
		fetches:   []kgo.Fetches{fetchWithRecord(testKafkaRecord(t))},
		commitErr: commitFailure,
	}
	consumer := newConsumer(client, validConsumerConfig(), nil)
	err := consumer.Run(
		context.Background(),
		func(context.Context, messaging.Message) error { return nil },
	)
	if !errors.Is(err, commitFailure) {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestConsumerRunReportsMalformedRecordsAndFetchFailures(t *testing.T) {
	t.Parallel()
	malformed := testKafkaRecord(t)
	malformed.Headers = nil
	consumer := newConsumer(
		&fakeConsumer{fetches: []kgo.Fetches{fetchWithRecord(malformed)}},
		validConsumerConfig(),
		nil,
	)
	if err := consumer.Run(
		context.Background(),
		func(context.Context, messaging.Message) error { return nil },
	); err == nil || !strings.Contains(err.Error(), "spice-occurred-at") {
		t.Fatalf("Run(malformed) error = %v", err)
	}
	fetchFailure := errors.New("broker fetch failed")
	consumer = newConsumer(
		&fakeConsumer{fetches: []kgo.Fetches{kgo.NewErrFetch(fetchFailure)}},
		validConsumerConfig(),
		nil,
	)
	if err := consumer.Run(
		context.Background(),
		func(context.Context, messaging.Message) error { return nil },
	); !errors.Is(err, fetchFailure) {
		t.Fatalf("Run(fetch failure) error = %v", err)
	}
}

func TestConsumerRunRejectsConcurrentRun(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	client := &fakeConsumer{pollStarted: started}
	consumer := newConsumer(client, validConsumerConfig(), nil)
	result := make(chan error, 1)
	go func() {
		result <- consumer.Run(
			ctx,
			func(context.Context, messaging.Message) error { return nil },
		)
	}()
	<-started
	if err := consumer.Run(
		context.Background(),
		func(context.Context, messaging.Message) error { return nil },
	); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("concurrent Run() error = %v", err)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("canceled Run() error = %v", err)
	}
}

func TestConsumerLifecycleAndInvalidUse(t *testing.T) {
	t.Parallel()
	client := &fakeConsumer{}
	consumer := newConsumer(client, validConsumerConfig(), nil)
	if err := consumer.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if client.closeCalls.Load() != 1 {
		t.Fatalf("close calls = %d", client.closeCalls.Load())
	}
	if err := consumer.Ping(context.Background()); err == nil {
		t.Fatal("Ping(after close) error = nil")
	}
	if err := consumer.Run(
		context.Background(),
		func(context.Context, messaging.Message) error { return nil },
	); err == nil {
		t.Fatal("Run(after close) error = nil")
	}
	if err := consumer.Run(nilContext(), func(
		context.Context,
		messaging.Message,
	) error {
		return nil
	}); err == nil {
		t.Fatal("Run(nil context) error = nil")
	}
	if err := (*Consumer)(nil).Run(
		context.Background(),
		func(context.Context, messaging.Message) error { return nil },
	); err == nil {
		t.Fatal("nil consumer Run() error = nil")
	}
	if err := (*Consumer)(nil).Close(context.Background()); err == nil {
		t.Fatal("nil consumer Close() error = nil")
	}
	if err := (*Consumer)(nil).Ping(context.Background()); err == nil {
		t.Fatal("nil consumer Ping() error = nil")
	}
	if err := consumer.Close(nilContext()); err == nil {
		t.Fatal("Close(nil context) error = nil")
	}
	var typedNil *consumerObserverFunc
	if _, _, err := OpenConsumer(ConsumerConfig{
		Transport: Config{
			Brokers:              []string{"broker:9092"},
			AllowInsecure:        true,
			AllowUnauthenticated: true,
		},
		GroupID: "orders",
		Topics:  []string{"orders.created"},
	}, typedNil); err == nil {
		t.Fatal("OpenConsumer(typed nil observer) error = nil")
	}
}

func TestMessageFromRecordRejectsDuplicateHeaders(t *testing.T) {
	t.Parallel()
	record := testKafkaRecord(t)
	record.Headers = append(record.Headers, kgo.RecordHeader{
		Key:   "Content-Type",
		Value: []byte("text/plain"),
	})
	if _, err := messageFromRecord(record); err == nil ||
		!strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("messageFromRecord() error = %v", err)
	}
	if _, err := messageFromRecord(nil); err == nil {
		t.Fatal("messageFromRecord(nil) error = nil")
	}
}

func validConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		Transport: Config{
			RequestTimeout: time.Second,
		},
		GroupID:        "orders",
		Topics:         []string{"orders.created"},
		MaxPollRecords: defaultMaxPollRecords,
	}
}

func testKafkaRecord(t *testing.T) *kgo.Record {
	t.Helper()
	record, err := kafkaRecord(testMessage(t))
	if err != nil {
		t.Fatal(err)
	}
	record.Partition = 2
	record.Offset = 9
	return record
}

func fetchWithRecord(record *kgo.Record) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: record.Topic,
			Partitions: []kgo.FetchPartition{{
				Partition: record.Partition,
				Records:   []*kgo.Record{record},
			}},
		}},
	}}
}

type fakeConsumer struct {
	mu          sync.Mutex
	fetches     []kgo.Fetches
	commits     []*kgo.Record
	onCommit    func()
	commitErr   error
	pollStarted chan struct{}
	pingErr     error
	closeCalls  atomic.Int32
}

func (client *fakeConsumer) PollRecords(
	ctx context.Context,
	_ int,
) kgo.Fetches {
	if client.pollStarted != nil {
		select {
		case <-client.pollStarted:
		default:
			close(client.pollStarted)
		}
	}
	client.mu.Lock()
	if len(client.fetches) > 0 {
		fetches := client.fetches[0]
		client.fetches = client.fetches[1:]
		client.mu.Unlock()
		return fetches
	}
	client.mu.Unlock()
	<-ctx.Done()
	return kgo.NewErrFetch(context.Cause(ctx))
}

func (client *fakeConsumer) CommitRecords(
	_ context.Context,
	records ...*kgo.Record,
) error {
	client.mu.Lock()
	client.commits = append(client.commits, records...)
	client.mu.Unlock()
	if client.onCommit != nil {
		client.onCommit()
	}
	return client.commitErr
}

func (client *fakeConsumer) Ping(context.Context) error {
	return client.pingErr
}

func (client *fakeConsumer) Close() {
	client.closeCalls.Add(1)
}

type consumerObserverFunc func(
	context.Context,
	DeliveryInteraction,
) (context.Context, func(DeliveryResult))

func (observer consumerObserverFunc) BeginDelivery(
	ctx context.Context,
	interaction DeliveryInteraction,
) (context.Context, func(DeliveryResult)) {
	return observer(ctx, interaction)
}
