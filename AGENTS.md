# Starter Kafka Implementation Contract

This repository owns Spice's franz-go-backed Kafka producer and consumer-group
starter.

## Invariants

- Work directly on `main` and preserve the filtered product history.
- Require exactly Go 1.26.5.
- Keep construction network-free and dependency injection explicit.
- Require verified TLS and authentication by default; each local-development
  opt-out must remain independent and explicit.
- Preserve franz-go's idempotent producer and require all in-sync replica
  acknowledgements.
- Keep consumer delivery sequential, bounded, manually committed, and
  deterministic.
- Preserve caller cancellation, cleanup ownership, and payload-free
  observability metadata.
- Never expose credentials or message payloads in starter-produced errors.
- Keep the standalone SDK manifest and strict compatibility boundaries current.
- Use focused checks during development, run one real broker acceptance, and
  run `make verify` exactly once on the final tree before commit.

## Compatibility

The product module directly requires the oldest supported Spice core. The
quality gate verifies that boundary and the explicit current boundary without
rewriting the repository.

Release-parity work must preserve the exact `spice-dev` tool version authorized
by the root `go.mod`, invoke its full package path, and run both central and
retained rehearsals with workspace and network resolution disabled in vendor
mode. The protected central workflow is the production authority; the retained
repository builder remains an unsigned parity oracle and must never receive
signatures or key material.
