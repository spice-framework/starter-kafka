# Releasing starter-kafka

This library owns its release contract. It does not use GoReleaser or a second
dependency/build graph: the organization-owned reusable workflow and pinned Go
tools read the Git-tracked source tree and the committed `go.mod`, `go.sum`, and
`vendor/modules.txt` accepted by repository verification.

## Artifact contract

For `v0.1.0`, a production build creates exactly:

- `starter-kafka_0.1.0_source.tar.gz`, containing every file in the exact
  committed `HEAD` tree under the single `starter-kafka-0.1.0/` prefix;
- `starter-kafka_0.1.0_sbom.spdx.json`, an SPDX 2.3 document for the root
  module and every exact module in the committed vendor graph;
- `checksums.txt`, with SHA-256 hashes for the source archive and SBOM;
- `checksums.txt.sig`, a raw Ed25519 signature over the exact checksum file;
- `checksums.txt.pem`, the matching public key.

Archive ordering, paths, executable modes, safe relative symlinks, tar/PAX
headers, gzip headers, and SPDX creation time are derived only from sorted
`HEAD` tree objects and the source commit epoch. Gitlinks, unsafe paths, and
symlinks that escape the archive root fail closed.
Generated metadata contains no current time or absolute workspace path. The
builder performs no dependency resolution or network access. It refuses stale
vendor metadata, unsafe tracked paths, an existing output directory, or partial
output: artifacts are staged and renamed only after the complete build succeeds.

Production mode fails closed unless the checkout is completely clean, `HEAD`
has the exact requested canonical `vX.Y.Z` tag, the supplied source epoch equals
the tagged commit epoch, and an Ed25519 private key is supplied. Even untracked
files make a production checkout dirty.

`-rehearsal` is an explicit local exception. A rehearsal is always unsigned,
may be untagged or dirty, and rejects `-signing-key` rather than producing an
artifact that could be confused with a production release:

```text
go run ./cmd/starter-kafka-release -rehearsal -version v0.1.0-rc.1 -output dist-rehearsal
```

Even in a dirty rehearsal, the source archive contains committed `HEAD` bytes,
not uncommitted worktree or index content.

## Unsigned dual-builder rehearsal

The library module authorizes an exact central renderer through its
`go.mod` tool directive. `make release-parity` runs that fully qualified tool
and the retained repository builder twice each with `GOWORK=off`,
`GOPROXY=off`, `GOTOOLCHAIN=local`, and `GOFLAGS=-mod=vendor`. It first asks the
central tool for a read-only plan and then renders the plan without resolving
an ambient workspace or downloading a module.

The central renderer and signer are the production implementation. The retained
repository builder remains only an unsigned parity oracle during the migration:

```text
make release-parity
```

Both rehearsals are unsigned, deterministic across two independent outputs,
and archive the exact committed `HEAD` tree. The older retained builder and the
central renderer intentionally spell the single archive root differently:
`starter-kafka-VERSION/` and `starter-kafka_VERSION/`, respectively. Parity
therefore decodes and fully drains both PAX/gzip streams, normalizes only those
exact prefixes, and requires identical entry order, paths, modes, types, links,
sizes, timestamps, extended records, gzip metadata, and content hashes. Hidden
decompressed data, an additional gzip member, or compressed trailing bytes fail
closed. The compressed archives are not claimed to be byte-identical.

The SPDX documents must contain the same package facts and dependency
relationships after semantic ordering. These R1 differences are intentional
and validated explicitly:

- document name (`Spice Kafka Starter VERSION` retained and
  `starter-kafka VERSION` centrally);
- namespace identity (the central namespace includes `spdx/v1/`);
- tool creator identifying the actual builder;
- package and relationship ordering; and
- the central document's one `DESCRIBES` relationship, which the retained R1
  builder predates and omits.

Both builders use `Organization: Spice Framework`; changing that value is not
an allowed provenance difference. Every other decoded SPDX field must match.
Each checksum file must be canonical and verify its own archive and SBOM.
Because both payloads have documented differences, checksum files are not
expected to be byte-identical. Extra artifacts, signatures, malformed
checksums, archive entry drift, or undocumented SBOM drift fail closed.

`make verify-release` runs this dual-builder proof. The retained builder is not
removed by this cutover and never receives production signing authority;
removal requires a separate reviewed change after the central signed path has
durable release evidence.

## Signing and verification

Generate a user-owned Ed25519 PKCS#8 key dedicated to this repository and keep
the private key outside the repository:

```text
openssl genpkey -algorithm ED25519 -out starter-kafka-release-key.pem
```

Review and commit the matching public key as
`security/release/ed25519-public.pem`. Store the private key only as
`SPICE_LIBRARY_RELEASE_SIGNING_KEY` in the protected `release-signing`
environment. Configure both `release-signing` and `release-publish` with the
required human reviewers. The private key is never copied into source, SBOM,
logs, or release output.

Verify downloaded assets before use:

```text
openssl pkeyutl -verify -pubin -inkey checksums.txt.pem -rawin -in checksums.txt -sigfile checksums.txt.sig
sha256sum -c checksums.txt
```

Consumers must authenticate the signature against the reviewed public key from
the exact tagged source, not against a public key supplied only beside release
assets. Until that trust anchor and both protected environments are configured,
this repository must not publish a tag.

PowerShell users can compare the first checksum column with
`Get-FileHash -Algorithm SHA256` for each named artifact.

## Release ceremony

1. Confirm the reviewed public anchor, protected `release-signing` secret, and
   both protected environments exist. Do not proceed if any control is absent.
2. Run `make verify` once on the final clean commit, then `make verify-release`.
3. Create and push an annotated canonical `vX.Y.Z` tag.
4. The caller invokes the organization workflow at its immutable commit and
   passes only the exact module path; it maps no secrets.
5. The workflow verifies, signs, independently authenticates, and publishes the
   artifacts through its separated protected environments.
6. Download the published assets and independently verify the signature,
   checksums, source prefix, SPDX document, and reviewed public key.

GitHub is the distribution mirror; the same repository command constructs
identical artifacts offline on Windows, Linux, and macOS.
