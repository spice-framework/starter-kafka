// Package kafka provides a reviewed franz-go-backed Apache Kafka producer
// starter for Spice applications.
package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spice-framework/spice/lifecycle"
	"github.com/spice-framework/spice/messaging"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const (
	defaultClientID       = "spice"
	defaultDialTimeout    = 10 * time.Second
	defaultRequestTimeout = 30 * time.Second
	defaultBatchMaxBytes  = int32(2 << 20)
	maxBrokerCount        = 32
	maxIdentityBytes      = 255
	maxCredentialBytes    = 4 << 10
	maxTimeout            = 5 * time.Minute
)

var reservedHeaders = []string{
	"content-type",
	"spice-message-id",
	"spice-occurred-at",
}

// SASLMechanism selects the explicit username/password authentication
// exchange used with Kafka brokers.
type SASLMechanism uint8

const (
	// SASLPlain uses the PLAIN exchange and therefore requires the default TLS
	// policy outside explicitly insecure local development.
	SASLPlain SASLMechanism = iota
	// SASLSCRAMSHA256 uses SCRAM-SHA-256 challenge-response authentication.
	SASLSCRAMSHA256
	// SASLSCRAMSHA512 uses SCRAM-SHA-512 challenge-response authentication.
	SASLSCRAMSHA512
)

// Config defines one explicit Kafka producer. TLS and authentication are
// required by default; local development must opt out of each independently.
type Config struct {
	Brokers              []string
	ClientID             string
	Username             string
	Password             string
	SASLMechanism        SASLMechanism
	TLSConfig            *tls.Config
	DialTimeout          time.Duration
	RequestTimeout       time.Duration
	BatchMaxBytes        int32
	AllowInsecure        bool
	AllowUnauthenticated bool
}

// Interaction is the payload-free producer fact supplied to observers.
type Interaction struct {
	Topic string
}

// Result describes one completed synchronous publication.
type Result struct {
	Interaction Interaction
	Duration    time.Duration
	Err         error
}

// Observer receives producer begin/end information on the caller goroutine.
type Observer interface {
	BeginPublish(context.Context, Interaction) (context.Context, func(Result))
}

type producerClient interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Flush(context.Context) error
	Ping(context.Context) error
	Close()
}

// Publisher owns one Kafka client and publishes messaging.Message values
// synchronously with Kafka's idempotent producer enabled.
type Publisher struct {
	mu        sync.RWMutex
	client    producerClient
	observers []Observer
	closed    bool
}

// Open validates deterministic transport policy and constructs a caller-owned
// producer. It performs no network I/O.
func Open(
	config Config,
	observers ...Observer,
) (*Publisher, lifecycle.Cleanup, error) {
	options, err := clientOptions(config)
	if err != nil {
		return nil, nil, err
	}
	if observerErr := validateObservers(observers); observerErr != nil {
		return nil, nil, observerErr
	}
	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, nil, errors.New(
			"construct Kafka producer: client configuration is invalid",
		)
	}
	publisher := newPublisher(client, observers)
	return publisher, publisher.Close, nil
}

func newPublisher(
	client producerClient,
	observers []Observer,
) *Publisher {
	return &Publisher{
		client:    client,
		observers: append([]Observer(nil), observers...),
	}
}

// Ping verifies broker connectivity using the caller-owned context.
func (publisher *Publisher) Ping(ctx context.Context) error {
	if ctx == nil {
		return errors.New("ping Kafka producer: context is nil")
	}
	if publisher == nil {
		return errors.New("ping Kafka producer: producer is nil")
	}
	publisher.mu.RLock()
	defer publisher.mu.RUnlock()
	if publisher.closed || publisher.client == nil {
		return errors.New("ping Kafka producer: producer is closed")
	}
	if err := publisher.client.Ping(ctx); err != nil {
		return fmt.Errorf("ping Kafka producer: %w", err)
	}
	return nil
}

// Publish writes one message synchronously. Topic comes from the immutable
// message, the optional key controls partitioning, and reserved metadata is
// added as Kafka headers without exposing payloads to observers.
func (publisher *Publisher) Publish(
	ctx context.Context,
	message messaging.Message,
) error {
	if ctx == nil {
		return errors.New("publish Kafka message: context is nil")
	}
	if publisher == nil {
		return errors.New("publish Kafka message: producer is nil")
	}
	record, err := kafkaRecord(message)
	if err != nil {
		return err
	}
	publisher.mu.RLock()
	defer publisher.mu.RUnlock()
	if publisher.closed || publisher.client == nil {
		return errors.New("publish Kafka message: producer is closed")
	}
	interaction := Interaction{Topic: message.Topic()}
	observedContext, finish := beginObservers(
		ctx,
		interaction,
		publisher.observers,
	)
	started := time.Now()
	result := publisher.client.ProduceSync(observedContext, record)
	publishErr := result.FirstErr()
	if publishErr != nil {
		publishErr = fmt.Errorf(
			"publish Kafka message to topic %q: %w",
			message.Topic(),
			publishErr,
		)
	}
	finish(Result{
		Interaction: interaction,
		Duration:    time.Since(started),
		Err:         publishErr,
	})
	return publishErr
}

// Close flushes pending records with the lifecycle context and releases the
// client once. New publications cannot race past shutdown.
func (publisher *Publisher) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("close Kafka producer: context is nil")
	}
	if publisher == nil {
		return errors.New("close Kafka producer: producer is nil")
	}
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if publisher.closed {
		return nil
	}
	if publisher.client == nil {
		return errors.New("close Kafka producer: producer is invalid")
	}
	publisher.closed = true
	flushErr := publisher.client.Flush(ctx)
	publisher.client.Close()
	if flushErr != nil {
		return fmt.Errorf("close Kafka producer: flush: %w", flushErr)
	}
	return nil
}

func clientOptions(config Config) ([]kgo.Opt, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	options := []kgo.Opt{
		kgo.SeedBrokers(normalized.Brokers...),
		kgo.ClientID(normalized.ClientID),
		kgo.DialTimeout(normalized.DialTimeout),
		kgo.RequestTimeoutOverhead(normalized.RequestTimeout),
		kgo.ProducerBatchMaxBytes(normalized.BatchMaxBytes),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
	}
	if normalized.TLSConfig != nil {
		options = append(
			options,
			kgo.DialTLSConfig(normalized.TLSConfig),
		)
	}
	if normalized.Username != "" {
		auth := scram.Auth{User: normalized.Username, Pass: normalized.Password}
		switch normalized.SASLMechanism {
		case SASLPlain:
			options = append(options, kgo.SASL(plain.Auth{
				User: normalized.Username,
				Pass: normalized.Password,
			}.AsMechanism()))
		case SASLSCRAMSHA256:
			options = append(options, kgo.SASL(auth.AsSha256Mechanism()))
		case SASLSCRAMSHA512:
			options = append(options, kgo.SASL(auth.AsSha512Mechanism()))
		}
	}
	return options, nil
}

func normalizeConfig(config Config) (Config, error) {
	brokers, err := normalizeBrokers(config.Brokers)
	if err != nil {
		return Config{}, err
	}
	config.Brokers = brokers
	if config.ClientID == "" {
		config.ClientID = defaultClientID
	}
	if identityErr := validateIdentity(
		"client ID",
		config.ClientID,
	); identityErr != nil {
		return Config{}, identityErr
	}
	if authErr := validateAuthentication(config); authErr != nil {
		return Config{}, authErr
	}
	tlsSelection, err := normalizeTLS(config)
	if err != nil {
		return Config{}, err
	}
	config.TLSConfig = tlsSelection.config
	if config.DialTimeout == 0 {
		config.DialTimeout = defaultDialTimeout
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = defaultRequestTimeout
	}
	if config.DialTimeout < 0 ||
		config.DialTimeout > maxTimeout ||
		config.RequestTimeout < 0 ||
		config.RequestTimeout > maxTimeout {
		return Config{}, fmt.Errorf(
			"construct Kafka producer: timeouts must be between 1ns and %s",
			maxTimeout,
		)
	}
	if config.BatchMaxBytes == 0 {
		config.BatchMaxBytes = defaultBatchMaxBytes
	}
	if config.BatchMaxBytes < 1024 ||
		config.BatchMaxBytes > defaultBatchMaxBytes {
		return Config{}, errors.New(
			"construct Kafka producer: batch max bytes must be between 1024 and 2097152",
		)
	}
	return config, nil
}

func normalizeBrokers(brokers []string) ([]string, error) {
	if len(brokers) == 0 || len(brokers) > maxBrokerCount {
		return nil, fmt.Errorf(
			"construct Kafka producer: brokers must contain 1 to %d addresses",
			maxBrokerCount,
		)
	}
	normalized := append([]string(nil), brokers...)
	slices.Sort(normalized)
	for index, broker := range normalized {
		if index > 0 && broker == normalized[index-1] {
			return nil, fmt.Errorf(
				"construct Kafka producer: duplicate broker %q",
				broker,
			)
		}
		if err := validateBroker(broker); err != nil {
			return nil, err
		}
	}
	return normalized, nil
}

func validateBroker(broker string) error {
	if len(broker) > maxIdentityBytes ||
		strings.TrimSpace(broker) != broker ||
		strings.ContainsAny(broker, "\x00\r\n\t /") {
		return errors.New(
			"construct Kafka producer: broker must be an exact host:port",
		)
	}
	host, portText, err := net.SplitHostPort(broker)
	if err != nil || host == "" {
		return errors.New(
			"construct Kafka producer: broker must be an exact host:port",
		)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return errors.New(
			"construct Kafka producer: broker port must be between 1 and 65535",
		)
	}
	return nil
}

func validateAuthentication(config Config) error {
	if config.SASLMechanism > SASLSCRAMSHA512 {
		return errors.New(
			"construct Kafka producer: authentication mechanism is unsupported",
		)
	}
	if (config.Username == "") != (config.Password == "") {
		return errors.New(
			"construct Kafka producer: username and password must be supplied together",
		)
	}
	if len(config.Username) > maxCredentialBytes ||
		len(config.Password) > maxCredentialBytes {
		return fmt.Errorf(
			"construct Kafka producer: credentials must not exceed %d bytes",
			maxCredentialBytes,
		)
	}
	if config.Username == "" &&
		!config.AllowUnauthenticated &&
		(config.TLSConfig == nil || len(config.TLSConfig.Certificates) == 0) {
		return errors.New(
			"construct Kafka producer: authenticated access is required",
		)
	}
	return nil
}

type normalizedTLS struct {
	config *tls.Config
}

func normalizeTLS(config Config) (normalizedTLS, error) {
	if config.TLSConfig == nil {
		if config.AllowInsecure {
			return normalizedTLS{}, nil
		}
		return normalizedTLS{
			config: &tls.Config{MinVersion: tls.VersionTLS12},
		}, nil
	}
	cloned := config.TLSConfig.Clone()
	if cloned.InsecureSkipVerify {
		return normalizedTLS{}, errors.New(
			"construct Kafka producer: TLS certificate verification is required",
		)
	}
	if cloned.MinVersion == 0 {
		cloned.MinVersion = tls.VersionTLS12
	}
	if cloned.MinVersion < tls.VersionTLS12 {
		return normalizedTLS{}, errors.New(
			"construct Kafka producer: TLS 1.2 or newer is required",
		)
	}
	return normalizedTLS{config: cloned}, nil
}

func kafkaRecord(message messaging.Message) (*kgo.Record, error) {
	if message.ID() == "" ||
		message.Topic() == "" ||
		message.ContentType() == "" ||
		message.OccurredAt().IsZero() {
		return nil, errors.New("publish Kafka message: message is invalid")
	}
	headers := message.Headers()
	for _, header := range headers {
		if slices.Contains(reservedHeaders, header.Name) {
			return nil, fmt.Errorf(
				"publish Kafka message: header %q is reserved",
				header.Name,
			)
		}
	}
	nativeHeaders := make(
		[]kgo.RecordHeader,
		0,
		len(headers)+len(reservedHeaders),
	)
	for _, header := range headers {
		nativeHeaders = append(nativeHeaders, kgo.RecordHeader{
			Key:   header.Name,
			Value: []byte(header.Value),
		})
	}
	nativeHeaders = append(
		nativeHeaders,
		kgo.RecordHeader{
			Key:   "content-type",
			Value: []byte(message.ContentType()),
		},
		kgo.RecordHeader{
			Key:   "spice-message-id",
			Value: []byte(message.ID()),
		},
		kgo.RecordHeader{
			Key: "spice-occurred-at",
			Value: []byte(
				message.OccurredAt().Format(time.RFC3339Nano),
			),
		},
	)
	return &kgo.Record{
		Topic:   message.Topic(),
		Key:     []byte(message.Key()),
		Value:   message.Payload(),
		Headers: nativeHeaders,
	}, nil
}

func beginObservers(
	ctx context.Context,
	interaction Interaction,
	observers []Observer,
) (context.Context, func(Result)) {
	finishers := make([]func(Result), 0, len(observers))
	observedContext := beginObserverChain(
		ctx,
		interaction,
		observers,
		&finishers,
	)
	return observedContext, func(result Result) {
		for _, finish := range slices.Backward(finishers) {
			finish(result)
		}
	}
}

func beginObserverChain(
	ctx context.Context,
	interaction Interaction,
	observers []Observer,
	finishers *[]func(Result),
) context.Context {
	if len(observers) == 0 {
		return ctx
	}
	next, finish := observers[0].BeginPublish(ctx, interaction)
	if next == nil {
		next = ctx
	}
	if finish != nil {
		*finishers = append(*finishers, finish)
	}
	return beginObserverChain(
		next,
		interaction,
		observers[1:],
		finishers,
	)
}

func validateObservers(observers []Observer) error {
	for index, observer := range observers {
		if nilInterface(observer) {
			return fmt.Errorf(
				"construct Kafka producer: observer %d is nil",
				index,
			)
		}
	}
	return nil
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() { //nolint:exhaustive // Only nil-capable reflection kinds require explicit handling.
	case reflect.Chan, reflect.Func, reflect.Interface,
		reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func validateIdentity(label, value string) error {
	if value == "" ||
		len(value) > maxIdentityBytes ||
		strings.TrimSpace(value) != value ||
		strings.ContainsAny(value, "\x00\r\n\t /") {
		return fmt.Errorf(
			"construct Kafka producer: %s must contain 1 to %d safe bytes",
			label,
			maxIdentityBytes,
		)
	}
	return nil
}

var _ messaging.Publisher = (*Publisher)(nil)
