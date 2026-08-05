package messaging

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// ErrHandlerPanicked identifies a recovered message handler panic without
// exposing the panic value, which may contain payload or credential data.
var ErrHandlerPanicked = errors.New("message handler panicked")

// ErrSettlementPanicked identifies a recovered transport settlement panic
// without exposing the panic value.
var ErrSettlementPanicked = errors.New("message settlement panicked")

// Disposition identifies the caller's explicit delivery outcome.
type Disposition string

const (
	// DispositionAcknowledge commits a successful delivery.
	DispositionAcknowledge Disposition = "acknowledge"
	// DispositionRetry releases a failed delivery for retry.
	DispositionRetry Disposition = "retry"
	// DispositionReject rejects a failed delivery without requesting retry.
	DispositionReject Disposition = "reject"
)

// DeliveryMetadata contains transport-owned, payload-free delivery facts.
type DeliveryMetadata struct {
	Consumer   string
	Attempt    int
	ReceivedAt time.Time
}

// Settlement is the narrow transport-owned acknowledgement seam.
type Settlement interface {
	Settle(context.Context, Disposition, error) error
}

// Delivery combines an immutable message with one explicit settlement.
type Delivery struct {
	message  Message
	metadata DeliveryMetadata
	state    *deliveryState
}

type deliveryState struct {
	mu         sync.Mutex
	settled    bool
	settlement Settlement
}

// NewDelivery validates one received message and its settlement owner.
func NewDelivery(
	message Message,
	metadata DeliveryMetadata,
	settlement Settlement,
) (Delivery, error) {
	if err := validateMessage(message); err != nil {
		return Delivery{}, fmt.Errorf("construct delivery: %w", err)
	}
	if err := validateIdentity(
		"consumer",
		metadata.Consumer,
		true,
	); err != nil {
		return Delivery{}, fmt.Errorf("construct delivery: %w", err)
	}
	if metadata.Attempt < 1 {
		return Delivery{}, errors.New(
			"construct delivery: attempt must be positive",
		)
	}
	if metadata.ReceivedAt.IsZero() {
		return Delivery{}, errors.New(
			"construct delivery: receipt time is required",
		)
	}
	if nilSettlement(settlement) {
		return Delivery{}, errors.New(
			"construct delivery: settlement is required",
		)
	}
	return Delivery{
		message: message,
		metadata: DeliveryMetadata{
			Consumer:   metadata.Consumer,
			Attempt:    metadata.Attempt,
			ReceivedAt: metadata.ReceivedAt.UTC(),
		},
		state: &deliveryState{settlement: settlement},
	}, nil
}

// Message returns the immutable application message.
func (delivery Delivery) Message() Message {
	return delivery.message
}

// Metadata returns the payload-free delivery facts.
func (delivery Delivery) Metadata() DeliveryMetadata {
	return delivery.metadata
}

// Handle invokes the handler and settles exactly once. Success acknowledges;
// a handler failure requests retry; caller cancellation rejects without retry.
// Settlement failures are joined with the handler or cancellation cause.
func (delivery Delivery) Handle(
	ctx context.Context,
	handler Handler,
) error {
	if ctx == nil {
		return errors.New("handle message delivery: context is nil")
	}
	if handler == nil {
		return errors.New("handle message delivery: handler is nil")
	}
	settlement, err := delivery.claimSettlement()
	if err != nil {
		return err
	}
	if cause := context.Cause(ctx); cause != nil {
		settleErr := invokeSettlement(
			settlement,
			context.WithoutCancel(ctx),
			DispositionReject,
			cause,
		)
		return errors.Join(cause, settleErr)
	}
	handlerErr := invokeHandler(ctx, handler, delivery.message)
	disposition := DispositionAcknowledge
	if handlerErr != nil {
		disposition = DispositionRetry
	}
	settleErr := invokeSettlement(
		settlement,
		context.WithoutCancel(ctx),
		disposition,
		handlerErr,
	)
	if settleErr != nil {
		settleErr = fmt.Errorf("settle message delivery: %w", settleErr)
	}
	return errors.Join(handlerErr, settleErr)
}

func (delivery Delivery) claimSettlement() (Settlement, error) {
	if delivery.state == nil ||
		nilSettlement(delivery.state.settlement) {
		return nil, errors.New(
			"handle message delivery: delivery is invalid",
		)
	}
	delivery.state.mu.Lock()
	defer delivery.state.mu.Unlock()
	if delivery.state.settled {
		return nil, errors.New(
			"handle message delivery: delivery is already settled",
		)
	}
	delivery.state.settled = true
	return delivery.state.settlement, nil
}

func invokeSettlement(
	settlement Settlement,
	ctx context.Context,
	disposition Disposition,
	cause error,
) (resultErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			resultErr = ErrSettlementPanicked
		}
	}()
	return settlement.Settle(ctx, disposition, cause)
}

func invokeHandler(
	ctx context.Context,
	handler Handler,
	message Message,
) (resultErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			resultErr = ErrHandlerPanicked
		}
	}()
	if err := handler(ctx, message); err != nil {
		return fmt.Errorf("handle message delivery: %w", err)
	}
	return nil
}

func nilSettlement(settlement Settlement) bool {
	if settlement == nil {
		return true
	}
	value := reflect.ValueOf(settlement)
	switch value.Kind() { //nolint:exhaustive // Only nil-capable reflection kinds require explicit handling.
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func validateMessage(message Message) error {
	if message.id == "" ||
		message.topic == "" ||
		message.contentType == "" ||
		len(message.payload) == 0 ||
		message.occurredAt.IsZero() {
		return errors.New("message is invalid")
	}
	return nil
}
