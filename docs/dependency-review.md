# Dependency review: franz-go

- Decision: approved for the isolated `starter/kafka` package.
- Version: `github.com/twmb/franz-go` v1.21.0.
- Upstream: <https://github.com/twmb/franz-go>.
- License: BSD-3-Clause; retained in the vendored module license.
- Maintenance: the active v1 line is pure Go and documents Kafka-compatible
  broker support through Kafka 4.2+, consumer groups, idempotent and
  transactional producers, administration, and current protocol KIPs.
- Security: verified TLS 1.2 or newer and authentication are independent
  defaults. Plaintext and unauthenticated local development each require an
  explicit opt-out. Broker addresses are exact bounded `host:port` values;
  credentials and payloads never enter errors or observations.
- Cancellation: ping, synchronous publish, and shutdown flush use caller-owned
  contexts. Spice sets finite dial and request-overhead defaults and bounds
  every configured timeout.
- Observability: franz-go has neutral hooks. The first Spice slice exposes
  payload-free synchronous publication observations and leaves broader
  OpenTelemetry hook installation caller-owned.
- Configuration: `Open` performs no network I/O, fixes all-ISR acknowledgement,
  retains franz-go's idempotent producer, disables linger for synchronous
  behavior, bounds batches, and returns exact lifecycle cleanup.
- Integration: unit tests use the narrow client seam to prove records,
  cancellation, observation, flush, and idempotent close. A pinned real-broker
  producer/consumer workflow is required before adding consumer-group
  generation or classifying Kafka as available.

Primary references:

- <https://github.com/twmb/franz-go/releases/tag/v1.21.0>
- <https://github.com/twmb/franz-go#features>
- <https://github.com/twmb/franz-go/blob/master/docs/producing-and-consuming.md>
- <https://github.com/twmb/franz-go/blob/master/LICENSE>
