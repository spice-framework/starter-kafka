# Support policy

## Supported environment

- Go 1.26.5 on Windows, Linux, and macOS.
- The Spice core minimum and current versions in
  `spice-compatibility.json`.
- Standard module and vendored, offline builds.
- Kafka-compatible brokers supported by franz-go v1.21.0, subject to
  application-owned acceptance against the exact broker configuration.

## Compatibility

Before 1.0, public APIs may change between minor versions. Patch releases do not
intentionally break source compatibility. Raising the minimum Spice core or Go
version requires a reviewed manifest change and release note.

## Security reports

Use GitHub private vulnerability reporting. Do not open public issues containing
credentials, broker certificates, message payloads, or exploit details.

## Ownership boundary

Applications own broker provisioning, certificates, credentials, ACLs,
replication/min-ISR policy, schema evolution, dead-letter policy, retries,
backoff, tracing hooks, and operational monitoring. This starter owns secure
client defaults, bounded synchronous production, sequential consumption,
manual settlement, lifecycle cleanup, and payload-free interaction metadata.
