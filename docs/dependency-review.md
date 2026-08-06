# Dependency review: franz-go

- Decision: approved for the standalone
  `github.com/spice-framework/starter-kafka` module.
- Version: `github.com/twmb/franz-go` v1.21.0.
- Upstream: <https://github.com/twmb/franz-go>.
- License: BSD-3-Clause; retained in the vendored module license.
- Maintenance: the active v1 line is pure Go and documents Kafka-compatible
  broker support through Kafka 4.2+, consumer groups, idempotent and
  transactional producers, administration, and current protocol KIPs.
- Security: verified TLS 1.2 or newer and authentication are independent
  defaults. Plaintext and unauthenticated local development each require an
  explicit opt-out. Broker addresses are exact bounded `host:port` values;
  credentials and payloads never enter starter-produced errors or interaction
  metadata. PLAIN and SCRAM mechanisms are explicit.
- Cancellation: ping, synchronous publish, and shutdown flush use caller-owned
  contexts. Spice sets finite dial and request-overhead defaults and bounds
  every configured timeout.
- Observability: franz-go has neutral hooks. Spice exposes payload-free
  synchronous publication and consumer-delivery observations and leaves
  broader OpenTelemetry hook installation caller-owned.
- Configuration: `Open` performs no network I/O, fixes all-ISR acknowledgement,
  retains franz-go's idempotent producer, disables linger for synchronous
  behavior, bounds batches, and returns exact lifecycle cleanup.
- Integration: unit tests use narrow client seams to prove records,
  cancellation, observation, flush, idempotent close, bounded group polling,
  and acknowledge/retry/reject commit behavior. A pinned Redpanda real-broker
  workflow proves SCRAM authentication, publish/consume ordering, commits,
  restart behavior, and cleanup. Target production broker/TLS configurations
  still require their own acceptance.

Primary references:

- <https://github.com/twmb/franz-go/releases/tag/v1.21.0>
- <https://github.com/twmb/franz-go#features>
- <https://github.com/twmb/franz-go/blob/master/docs/producing-and-consuming.md>
- <https://github.com/twmb/franz-go/blob/master/LICENSE>

## Build-only dependencies: Spice release tools

- Decision: approved only as the repository-authorized release-parity tool.
- Signer version: `github.com/spice-framework/development`
  `v0.0.0-20260806121906-963bb6676069`.
- Signer tool: `github.com/spice-framework/development/cmd/spice-dev` through the
  standard Go `tool` directive; invocations always use the full package path.
- Verifier version: `github.com/spice-framework/toolchain`
  `v0.0.0-20260806054457-a83d9b58034c`.
- Verifier tool:
  `github.com/spice-framework/toolchain/cmd/spice-library-release-verify`.
- License: Apache-2.0, with its notice retained in `vendor`.
- Runtime scope: none. Product packages do not import the development module,
  and released applications acquire no runtime dependency on it.
- Dependency graph: the tool participates in normal Go minimal-version
  selection. That build-time coupling is accepted and visible in `go.mod`,
  `go.sum`, and `vendor/modules.txt`; no parallel tool registry is introduced.
- Integrity and network behavior: the exact pseudo-version is pinned and
  checksummed. Release parity runs with `GOWORK=off`, `GOPROXY=off`,
  `GOTOOLCHAIN=local`, and `GOFLAGS=-mod=vendor`, so it cannot select an ambient
  checkout, upgrade itself, or download dependencies.
- Security: the trusted native tool reads the exact committed Git graph and
  writes only to caller-supplied temporary output directories. The rehearsal
  emits no signatures or signing material.
- Maintenance: the retained local builder and production signing workflow stay
  in place. A dual-builder gate detects central renderer regressions before any
  future authority migration.
