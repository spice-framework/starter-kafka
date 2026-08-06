module github.com/spice-framework/starter-kafka

go 1.26.0

toolchain go1.26.5

require (
	github.com/spice-framework/spice v0.0.0-20260805222830-a2ecd56df246
	github.com/twmb/franz-go v1.21.5
)

require (
	github.com/klauspost/compress v1.18.7 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/spice-framework/development v0.0.0-20260806132124-4c308d1b9fda // indirect
	github.com/spice-framework/toolchain v0.0.0-20260806133530-71211498297c // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/crypto v0.52.0 // indirect
	golang.org/x/mod v0.38.0 // indirect
)

tool (
	github.com/spice-framework/development/cmd/spice-dev
	github.com/spice-framework/toolchain/cmd/spice-library-release-verify
)
