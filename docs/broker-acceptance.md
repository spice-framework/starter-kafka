# Real broker acceptance

The integration test targets Redpanda v25.1.9 using this immutable
multi-platform manifest digest:

```text
docker.redpanda.com/redpandadata/redpanda@sha256:8f7e9e4c1422baaa1a5e2a6c6c668cfe05442cb3cb476542c7dff61725e6fe31
```

The manifest contains Linux amd64 and arm64 images. CI starts a fresh single
node broker with a SCRAM-SHA-256 bootstrap superuser and runs:

- a rejected wrong-password probe whose error is checked for secret safety;
- authenticated producer and consumer pings;
- explicit one-partition topic creation followed by two synchronous, ordered,
  same-key publications;
- sequential consumer-group delivery;
- explicit manual offset commits;
- restart of the same group proving no committed redelivery;
- cancellation and idempotent cleanup.

The test accepts an optional `SPICE_KAFKA_CA` and
`SPICE_KAFKA_TLS_SERVER_NAME` to exercise a verified TLS listener. The
repository's default CI broker uses authenticated local plaintext and therefore
does not claim that a particular production PKI or broker TLS deployment has
been validated. TLS configuration, minimum-version, certificate-verification,
and unsafe-policy rejection are covered by deterministic tests.

Run the test against an already-started broker:

```text
SPICE_KAFKA_BROKER=127.0.0.1:19092 \
SPICE_KAFKA_USERNAME=spice \
SPICE_KAFKA_PASSWORD=integration-secret \
go test -tags=integration -count=1 ./integration/...
```

Production approval requires running this suite against the intended broker,
TLS, SASL, ACL, replication, and failure configuration.
