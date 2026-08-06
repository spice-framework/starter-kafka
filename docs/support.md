# Support policy

## Supported environment

- Go 1.26.5 on Windows, Linux, and macOS.
- The Spice core minimum and current versions in
  `spice-compatibility.json`.
- Standard module and vendored, offline builds.
- Release parity through
  `github.com/spice-framework/development/cmd/spice-dev` at
  `v0.0.0-20260806052122-9025218a91c0`.
- Independent verifier:
  `github.com/spice-framework/toolchain/cmd/spice-library-release-verify` at
  `v0.0.0-20260806054457-a83d9b58034c`.
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

The pinned central tool renders unsigned rehearsal candidates only. Windows
and Linux CI compare them with the retained builder under vendor-only offline
resolution; the retained command remains the signed production authority.
