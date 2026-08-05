package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/spice-framework/spice/lifecycle"
	"github.com/spice-framework/spice/messaging"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultMaxPollRecords = 100
	maxPollRecords        = 1000
)

// ConsumerConfig defines one explicit, sequential Kafka consumer group.
// Transport applies the same secure connection policy as Publisher.
type ConsumerConfig struct {
	Transport      Config
	GroupID        string
	Topics         []string
	MaxPollRecords int
}

// DeliveryInteraction is the payload-free consumer fact supplied to observers.
type DeliveryInteraction struct {
	Group     string
	Topic     string
	Partition int32
}

// DeliveryResult describes one completed handler delivery.
type DeliveryResult struct {
	Interaction DeliveryInteraction
	Duration    time.Duration
	Err         error
}

// ConsumerObserver receives delivery begin/end information on the Run
// goroutine. It must not retain or expose message payloads.
type ConsumerObserver interface {
	BeginDelivery(
		context.Context,
		DeliveryInteraction,
	) (context.Context, func(DeliveryResult))
}

type consumerClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	Ping(context.Context) error
	Close()
}

// Consumer owns one Kafka group client. Run polls and handles records
// sequentially so acknowledgement order and shutdown behavior remain
// deterministic.
type Consumer struct {
	mu             sync.Mutex
	client         consumerClient
	group          string
	maxPollRecords int
	requestTimeout time.Duration
	observers      []ConsumerObserver
	running        bool
	closed         bool
}

// OpenConsumer validates deterministic transport and group policy and
// constructs a caller-owned consumer. It performs no network I/O.
func OpenConsumer(
	config ConsumerConfig,
	observers ...ConsumerObserver,
) (*Consumer, lifecycle.Cleanup, error) {
	options, normalized, err := consumerOptions(config)
	if err != nil {
		return nil, nil, err
	}
	if observerErr := validateConsumerObservers(observers); observerErr != nil {
		return nil, nil, observerErr
	}
	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, nil, errors.New(
			"construct Kafka consumer: client configuration is invalid",
		)
	}
	consumer := newConsumer(client, normalized, observers)
	return consumer, consumer.Close, nil
}

func newConsumer(
	client consumerClient,
	config ConsumerConfig,
	observers []ConsumerObserver,
) *Consumer {
	return &Consumer{
		client:         client,
		group:          config.GroupID,
		maxPollRecords: config.MaxPollRecords,
		requestTimeout: config.Transport.RequestTimeout,
		observers:      append([]ConsumerObserver(nil), observers...),
	}
}

// Ping verifies broker connectivity using the caller-owned context.
func (consumer *Consumer) Ping(ctx context.Context) error {
	if ctx == nil {
		return errors.New("ping Kafka consumer: context is nil")
	}
	client, err := consumer.availableClient("ping")
	if err != nil {
		return err
	}
	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("ping Kafka consumer: %w", err)
	}
	return nil
}

// Run polls and handles records until cancellation, closure, or a delivery
// failure. Successful and explicitly rejected deliveries commit their record;
// retryable failures remain uncommitted and stop the run for caller-managed
// restart/backoff.
func (consumer *Consumer) Run(
	ctx context.Context,
	handler messaging.Handler,
) error {
	if ctx == nil {
		return errors.New("run Kafka consumer: context is nil")
	}
	if handler == nil {
		return errors.New("run Kafka consumer: handler is nil")
	}
	client, err := consumer.beginRun()
	if err != nil {
		return err
	}
	defer consumer.endRun()
	for {
		fetches := client.PollRecords(ctx, consumer.maxPollRecords)
		if cause := context.Cause(ctx); cause != nil {
			return nil
		}
		if fetchErr := firstFetchError(fetches); fetchErr != nil {
			if errors.Is(fetchErr, kgo.ErrClientClosed) {
				return nil
			}
			return fmt.Errorf("poll Kafka consumer group %q: %w", consumer.group, fetchErr)
		}
		for _, record := range fetches.Records() {
			if err := consumer.handleRecord(ctx, client, record, handler); err != nil {
				return err
			}
		}
	}
}

// Close stops polling and releases the client once.
func (consumer *Consumer) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("close Kafka consumer: context is nil")
	}
	if consumer == nil {
		return errors.New("close Kafka consumer: consumer is nil")
	}
	consumer.mu.Lock()
	if consumer.closed {
		consumer.mu.Unlock()
		return nil
	}
	if consumer.client == nil {
		consumer.mu.Unlock()
		return errors.New("close Kafka consumer: consumer is invalid")
	}
	consumer.closed = true
	client := consumer.client
	consumer.mu.Unlock()
	client.Close()
	return nil
}

func (consumer *Consumer) handleRecord(
	ctx context.Context,
	client consumerClient,
	record *kgo.Record,
	handler messaging.Handler,
) error {
	message, err := messageFromRecord(record)
	if err != nil {
		return err
	}
	interaction := DeliveryInteraction{
		Group:     consumer.group,
		Topic:     record.Topic,
		Partition: record.Partition,
	}
	observedContext, finish := beginConsumerObservers(
		ctx,
		interaction,
		consumer.observers,
	)
	started := time.Now()
	settlement := recordSettlement{
		client:  client,
		record:  record,
		timeout: consumer.requestTimeout,
	}
	delivery, err := messaging.NewDelivery(message, messaging.DeliveryMetadata{
		Consumer:   consumer.group,
		Attempt:    1,
		ReceivedAt: time.Now(),
	}, settlement)
	if err == nil {
		err = delivery.Handle(observedContext, handler)
	}
	if err != nil {
		err = fmt.Errorf(
			"consume Kafka record from %s[%d] at offset %d: %w",
			record.Topic,
			record.Partition,
			record.Offset,
			err,
		)
	}
	finish(DeliveryResult{
		Interaction: interaction,
		Duration:    time.Since(started),
		Err:         err,
	})
	return err
}

func consumerOptions(
	config ConsumerConfig,
) ([]kgo.Opt, ConsumerConfig, error) {
	transport, err := normalizeConfig(config.Transport)
	if err != nil {
		return nil, ConsumerConfig{}, fmt.Errorf(
			"construct Kafka consumer: %w",
			err,
		)
	}
	config.Transport = transport
	if identityErr := validateIdentity(
		"group ID",
		config.GroupID,
	); identityErr != nil {
		return nil, ConsumerConfig{}, fmt.Errorf(
			"construct Kafka consumer: %w",
			identityErr,
		)
	}
	topics, err := normalizeTopics(config.Topics)
	if err != nil {
		return nil, ConsumerConfig{}, err
	}
	config.Topics = topics
	if config.MaxPollRecords == 0 {
		config.MaxPollRecords = defaultMaxPollRecords
	}
	if config.MaxPollRecords < 1 || config.MaxPollRecords > maxPollRecords {
		return nil, ConsumerConfig{}, fmt.Errorf(
			"construct Kafka consumer: max poll records must be between 1 and %d",
			maxPollRecords,
		)
	}
	options, err := clientOptions(transport)
	if err != nil {
		return nil, ConsumerConfig{}, err
	}
	options = append(
		options,
		kgo.ConsumerGroup(config.GroupID),
		kgo.ConsumeTopics(config.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxBytes(defaultBatchMaxBytes),
	)
	return options, config, nil
}

func normalizeTopics(topics []string) ([]string, error) {
	if len(topics) == 0 || len(topics) > maxBrokerCount {
		return nil, fmt.Errorf(
			"construct Kafka consumer: topics must contain 1 to %d names",
			maxBrokerCount,
		)
	}
	normalized := append([]string(nil), topics...)
	slices.Sort(normalized)
	for index, topic := range normalized {
		if err := validateIdentity("topic", topic); err != nil {
			return nil, fmt.Errorf("construct Kafka consumer: %w", err)
		}
		if index > 0 && topic == normalized[index-1] {
			return nil, fmt.Errorf(
				"construct Kafka consumer: duplicate topic %q",
				topic,
			)
		}
	}
	return normalized, nil
}

func messageFromRecord(record *kgo.Record) (messaging.Message, error) {
	if record == nil {
		return messaging.Message{}, errors.New(
			"consume Kafka record: record is nil",
		)
	}
	values := make(map[string]string, len(record.Headers))
	headers := make([]messaging.Header, 0, len(record.Headers))
	for _, header := range record.Headers {
		name := strings.ToLower(header.Key)
		if _, exists := values[name]; exists {
			return messaging.Message{}, fmt.Errorf(
				"consume Kafka record: duplicate header %q",
				name,
			)
		}
		value := string(header.Value)
		values[name] = value
		if !slices.Contains(reservedHeaders, name) {
			headers = append(headers, messaging.Header{
				Name:  name,
				Value: value,
			})
		}
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, values["spice-occurred-at"])
	if err != nil {
		return messaging.Message{}, errors.New(
			"consume Kafka record: spice-occurred-at header is invalid",
		)
	}
	message, err := messaging.NewMessage(messaging.MessageSpec{
		ID:          values["spice-message-id"],
		Topic:       record.Topic,
		Key:         string(record.Key),
		ContentType: values["content-type"],
		Payload:     record.Value,
		Headers:     headers,
		OccurredAt:  occurredAt,
	})
	if err != nil {
		return messaging.Message{}, fmt.Errorf(
			"consume Kafka record: %w",
			err,
		)
	}
	return message, nil
}

func firstFetchError(fetches kgo.Fetches) error {
	errors := fetches.Errors()
	if len(errors) == 0 {
		return nil
	}
	return errors[0].Err
}

func (consumer *Consumer) availableClient(action string) (consumerClient, error) {
	if consumer == nil {
		return nil, fmt.Errorf("%s Kafka consumer: consumer is nil", action)
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.closed || consumer.client == nil {
		return nil, fmt.Errorf("%s Kafka consumer: consumer is closed", action)
	}
	return consumer.client, nil
}

func (consumer *Consumer) beginRun() (consumerClient, error) {
	if consumer == nil {
		return nil, errors.New("run Kafka consumer: consumer is nil")
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.closed || consumer.client == nil {
		return nil, errors.New("run Kafka consumer: consumer is closed")
	}
	if consumer.running {
		return nil, errors.New("run Kafka consumer: consumer is already running")
	}
	consumer.running = true
	return consumer.client, nil
}

func (consumer *Consumer) endRun() {
	consumer.mu.Lock()
	consumer.running = false
	consumer.mu.Unlock()
}

type recordSettlement struct {
	client  consumerClient
	record  *kgo.Record
	timeout time.Duration
}

func (settlement recordSettlement) Settle(
	ctx context.Context,
	disposition messaging.Disposition,
	_ error,
) error {
	if disposition == messaging.DispositionRetry {
		return nil
	}
	commitContext, cancel := context.WithTimeout(ctx, settlement.timeout)
	defer cancel()
	if err := settlement.client.CommitRecords(commitContext, settlement.record); err != nil {
		return fmt.Errorf("commit Kafka record: %w", err)
	}
	return nil
}

func beginConsumerObservers(
	ctx context.Context,
	interaction DeliveryInteraction,
	observers []ConsumerObserver,
) (context.Context, func(DeliveryResult)) {
	finishers := make([]func(DeliveryResult), 0, len(observers))
	observedContext := beginConsumerObserverChain(
		ctx,
		interaction,
		observers,
		&finishers,
	)
	return observedContext, func(result DeliveryResult) {
		for _, finish := range slices.Backward(finishers) {
			finish(result)
		}
	}
}

func beginConsumerObserverChain(
	ctx context.Context,
	interaction DeliveryInteraction,
	observers []ConsumerObserver,
	finishers *[]func(DeliveryResult),
) context.Context {
	if len(observers) == 0 {
		return ctx
	}
	next, finish := observers[0].BeginDelivery(ctx, interaction)
	if next == nil {
		next = ctx
	}
	if finish != nil {
		*finishers = append(*finishers, finish)
	}
	return beginConsumerObserverChain(
		next,
		interaction,
		observers[1:],
		finishers,
	)
}

func validateConsumerObservers(observers []ConsumerObserver) error {
	for index, observer := range observers {
		if nilInterface(observer) {
			return fmt.Errorf(
				"construct Kafka consumer: observer %d is nil",
				index,
			)
		}
	}
	return nil
}
