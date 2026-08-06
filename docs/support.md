# Support policy

## Supported environment

- Go 1.26.5 on Windows, Linux, and macOS.
- The Spice core minimum and current versions in
  `spice-compatibility.json`.
- Standard module and vendored, offline builds.
- Release parity through
  `github.com/spice-framework/development/cmd/spice-dev` at
  `v0.0.0-20260806132124-4c308d1b9fda`.
- Independent verifier:
  `github.com/spice-framework/toolchain/cmd/spice-library-release-verify` at
  `v0.0.0-20260806133530-71211498297c`.
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

The pinned central signer and independent verifier power the protected reusable
production workflow. The reviewed repository-specific trust anchor is
`security/release/ed25519-public.pem` (SHA-256 fingerprint
`54e8eabf2130a73b889dad1681cc097bdf8fc2be8d0af8645810a3b4e3159196`).
Its private key exists only as the repository Actions secret
`SPICE_LIBRARY_RELEASE_SIGNING_KEY`, passed through the caller's exact one-name
mapping. The protected `release-signing` and `release-publish` environments
remain approval gates and contain no signing secret. Windows and Linux CI still
compare unsigned central and retained outputs under vendor-only offline
resolution; the retained command is only a parity oracle until the first signed
cutover passes.
