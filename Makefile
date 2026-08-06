.PHONY: check compatibility fmt integration lint release-rehearsal security verify verify-release

check:
	go run ./internal/qualitygate -mode=check

compatibility:
	go run ./internal/qualitygate -mode=compatibility

fmt:
	go run ./internal/qualitygate -mode=fmt

integration:
	go test -tags=integration -count=1 ./integration/...

lint:
	go run ./internal/qualitygate -mode=lint

release-rehearsal: export GOWORK := off
release-rehearsal: export GOPROXY := off
release-rehearsal: export GOTOOLCHAIN := local
release-rehearsal: export GOFLAGS := -mod=vendor
release-rehearsal:
	go run ./internal/qualitygate -mode=release-rehearsal

security:
	go run ./internal/qualitygate -mode=security

verify:
	go run ./internal/qualitygate -mode=verify

verify-release:
	go run ./internal/qualitygate -mode=verify-release
