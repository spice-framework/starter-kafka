# Spice Kafka Starter

`starter-kafka` provides a synchronous idempotent Kafka producer and a bounded,
sequential consumer group for Spice applications. Both are ordinary Go values
with explicit construction and cleanup; there is no global client, reflection,
environment discovery, or network I/O during construction.

## Install

```text
go get github.com/spice-framework/starter-kafka@latest
```

Go 1.26.5 is required. Supported Spice core boundaries are declared in
[`spice-compatibility.json`](spice-compatibility.json).

## Producer

```go
publisher, cleanup, err := kafka.Open(kafka.Config{
    Brokers:       []string{"broker.example:9093"},
    ClientID:      "orders",
    Username:      config.Username,
    Password:      config.Password,
    SASLMechanism: kafka.SASLSCRAMSHA256,
    TLSConfig: &tls.Config{
        MinVersion: tls.VersionTLS12,
        RootCAs:    roots,
        ServerName: "broker.example",
    },
})
```

The producer is idempotent, requires all in-sync replica acknowledgements,
bounds batch size and timeouts, and publishes synchronously. `Close` flushes
with the caller's lifecycle context and is idempotent.

## Consumer group

```go
consumer, cleanup, err := kafka.OpenConsumer(kafka.ConsumerConfig{
    Transport: producerConfig,
    GroupID:   "inventory",
    Topics:    []string{"orders.placed"},
})
```

`Run` handles records sequentially. Successful deliveries and explicit
rejections commit; retryable handler failures remain uncommitted and return to
the caller for an explicit restart/backoff policy. Auto-commit is disabled and
poll size is bounded.

## Security

- TLS 1.2 or newer with certificate verification is the default.
- Authentication is independently required by default.
- PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512 are explicit choices. PLAIN should
  only be used with verified TLS.
- Plaintext and unauthenticated operation require separate, explicit local
  development flags.
- Broker addresses, identities, credentials, timeouts, topics, headers, and
  payload boundaries are validated.
- Producer and consumer observations contain only topic/group/partition,
  duration, and outcome information—never message keys, headers, or payloads.

Applications own secret loading, CA roots, retry/backoff policy, and franz-go
hooks used for tracing or metrics.

## Broker acceptance

The repository acceptance suite runs producer authentication failure,
connectivity, ordered publication, consumer-group delivery, manual commits,
restart/no-redelivery, cancellation, and cleanup against Redpanda v25.1.9 at the
immutable multi-platform image digest documented in
[`docs/broker-acceptance.md`](docs/broker-acceptance.md).

That evidence validates the Kafka protocol path against Redpanda. Production
teams must additionally run the same suite against their broker distribution,
version, TLS certificates, authentication mechanism, replication policy, and
failure topology before approving deployment.

## Verification

```text
make check
make compatibility
make lint
make security
make verify
```

See [`docs/dependency-review.md`](docs/dependency-review.md) and
[`docs/support.md`](docs/support.md).

## License

Apache License 2.0.
