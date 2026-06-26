module github.com/scitrera/agent-harness-go

go 1.25.11

require (
	github.com/scitrera/ecosystem-messaging-spec/go v1.2.0
	go.opentelemetry.io/otel v1.43.0
	go.opentelemetry.io/otel/trace v1.43.0
)

// Local dev bridge until the spec's go/v1.2.0 tag is published; drop after.
replace github.com/scitrera/ecosystem-messaging-spec/go => ../../scitrera-ecosystem-messaging-spec/go

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
)
