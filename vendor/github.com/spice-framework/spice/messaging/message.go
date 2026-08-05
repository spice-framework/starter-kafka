// Package messaging provides immutable, transport-neutral messages and
// explicit publish/consume contracts for opt-in broker starters.
package messaging

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxIdentityBytes = 255
	maxHeaderCount   = 64
	maxHeaderBytes   = 8 << 10
	maxPayloadBytes  = 1 << 20
)

var identityPattern = regexp.MustCompile(
	`^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,253}[A-Za-z0-9])?$`,
)

// Header is one immutable message metadata field. Names use a portable
// lowercase ASCII spelling so Kafka, AMQP, and test transports agree.
type Header struct {
	Name  string
	Value string
}

// MessageSpec is the inspectable input to NewMessage.
type MessageSpec struct {
	ID          string
	Topic       string
	Key         string
	ContentType string
	Payload     []byte
	Headers     []Header
	OccurredAt  time.Time
}

// Message is one immutable serialized application message.
type Message struct {
	id          string
	topic       string
	key         string
	contentType string
	payload     []byte
	headers     []Header
	occurredAt  time.Time
}

// NewMessage validates and defensively freezes one message. ID is the
// caller-owned idempotency key. Topic and key are logical values; a starter
// remains responsible for mapping them to its broker.
func NewMessage(spec MessageSpec) (Message, error) {
	if err := validateIdentity("message ID", spec.ID, true); err != nil {
		return Message{}, err
	}
	if err := validateIdentity("topic", spec.Topic, true); err != nil {
		return Message{}, err
	}
	if err := validateIdentity("key", spec.Key, false); err != nil {
		return Message{}, err
	}
	contentType, err := normalizeContentType(spec.ContentType)
	if err != nil {
		return Message{}, err
	}
	if len(spec.Payload) == 0 || len(spec.Payload) > maxPayloadBytes {
		return Message{}, fmt.Errorf(
			"construct message: payload must be between 1 and %d bytes",
			maxPayloadBytes,
		)
	}
	if spec.OccurredAt.IsZero() {
		return Message{}, errors.New(
			"construct message: occurrence time is required",
		)
	}
	headers, err := normalizeHeaders(spec.Headers)
	if err != nil {
		return Message{}, err
	}
	return Message{
		id:          spec.ID,
		topic:       spec.Topic,
		key:         spec.Key,
		contentType: contentType,
		payload:     append([]byte(nil), spec.Payload...),
		headers:     headers,
		occurredAt:  spec.OccurredAt.UTC(),
	}, nil
}

// ID returns the caller-owned idempotency key.
func (message Message) ID() string {
	return message.id
}

// Topic returns the logical message contract or destination.
func (message Message) Topic() string {
	return message.topic
}

// Key returns the optional ordering or partitioning key.
func (message Message) Key() string {
	return message.key
}

// ContentType returns the normalized payload media type.
func (message Message) ContentType() string {
	return message.contentType
}

// Payload returns a defensive copy.
func (message Message) Payload() []byte {
	return append([]byte(nil), message.payload...)
}

// Headers returns a defensive copy sorted by name.
func (message Message) Headers() []Header {
	return append([]Header(nil), message.headers...)
}

// OccurredAt returns the UTC application occurrence time.
func (message Message) OccurredAt() time.Time {
	return message.occurredAt
}

// Publisher is the narrow dependency injected into application producers.
// Implementations must honor cancellation and use Message.ID as the
// downstream idempotency key when the transport supports one.
type Publisher interface {
	Publish(context.Context, Message) error
}

// Handler consumes one immutable message. It owns no acknowledgement policy;
// a broker starter defines retry, dead-letter, and commit behavior explicitly.
type Handler func(context.Context, Message) error

func normalizeContentType(value string) (string, error) {
	mediaType, parameters, err := mime.ParseMediaType(value)
	typeName, subtype, separated := strings.Cut(mediaType, "/")
	if err != nil ||
		!separated ||
		typeName == "" ||
		subtype == "" {
		return "", errors.New("construct message: content type is invalid")
	}
	normalized := mime.FormatMediaType(mediaType, parameters)
	if len(normalized) > maxIdentityBytes {
		return "", errors.New("construct message: content type is too long")
	}
	return normalized, nil
}

func normalizeHeaders(headers []Header) ([]Header, error) {
	if len(headers) > maxHeaderCount {
		return nil, fmt.Errorf(
			"construct message: at most %d headers are allowed",
			maxHeaderCount,
		)
	}
	frozen := make([]Header, len(headers))
	seen := make(map[string]struct{}, len(headers))
	totalBytes := 0
	for index, header := range headers {
		name := strings.ToLower(strings.TrimSpace(header.Name))
		if err := validateIdentity("header name", name, true); err != nil {
			return nil, fmt.Errorf(
				"construct message: header %d: %w",
				index,
				err,
			)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf(
				"construct message: duplicate header %q",
				name,
			)
		}
		if !utf8.ValidString(header.Value) ||
			strings.ContainsAny(header.Value, "\x00\r\n") {
			return nil, fmt.Errorf(
				"construct message: header %q has an invalid value",
				name,
			)
		}
		totalBytes += len(name) + len(header.Value)
		if totalBytes > maxHeaderBytes {
			return nil, fmt.Errorf(
				"construct message: headers exceed %d bytes",
				maxHeaderBytes,
			)
		}
		seen[name] = struct{}{}
		frozen[index] = Header{Name: name, Value: header.Value}
	}
	slices.SortFunc(frozen, func(left, right Header) int {
		return strings.Compare(left.Name, right.Name)
	})
	return frozen, nil
}

func validateIdentity(label, value string, required bool) error {
	if value == "" && !required {
		return nil
	}
	if len(value) > maxIdentityBytes || !identityPattern.MatchString(value) {
		return fmt.Errorf(
			"construct message: %s must be a portable identifier of at most %d bytes",
			label,
			maxIdentityBytes,
		)
	}
	return nil
}
