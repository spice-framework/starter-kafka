package kafka

import spicestarter "github.com/spice-framework/spice/annotation/sdk/starter"

// Manifest returns Kafka starter compatibility and review metadata.
func Manifest() spicestarter.Manifest {
	return spicestarter.Must(spicestarter.Spec{
		Schema:    spicestarter.Schema,
		ID:        "github.com/spice-framework/starter-kafka",
		Version:   "0.1.0-dev",
		Module:    "github.com/spice-framework/starter-kafka",
		SpiceAPI:  spicestarter.APIVersion,
		MinimumGo: "1.26",
		License:   "Apache-2.0",
		Review:    "docs/dependency-review.md",
		Activation: spicestarter.Activation{
			Mode: spicestarter.ActivationExplicitConstructor,
			EntryPoints: []spicestarter.EntryPoint{
				{
					Package: "github.com/spice-framework/starter-kafka",
					Symbol:  "Open",
				},
				{
					Package: "github.com/spice-framework/starter-kafka",
					Symbol:  "OpenConsumer",
				},
			},
		},
		Capabilities: []string{
			"messaging.kafka.consumer-group",
			"messaging.kafka.producer",
		},
		Dependencies: []spicestarter.Dependency{{
			Module:  "github.com/twmb/franz-go",
			Version: "v1.21.0",
			License: "BSD-3-Clause",
		}},
	})
}
