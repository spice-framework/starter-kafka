package kafka

import spicestarter "github.com/StevenBuglione/spice/starter"

// Manifest returns Kafka starter compatibility and review metadata.
func Manifest() spicestarter.Manifest {
	return spicestarter.Must(spicestarter.Spec{
		Schema:    spicestarter.Schema,
		ID:        "github.com/StevenBuglione/spice/starter/kafka",
		Version:   "0.1.0-dev",
		Module:    "github.com/StevenBuglione/spice",
		SpiceAPI:  spicestarter.APIVersion,
		MinimumGo: "1.26",
		License:   "Apache-2.0",
		Review:    "docs/dependency-reviews/franz-go.md",
		Activation: spicestarter.Activation{
			Mode: spicestarter.ActivationExplicitConstructor,
			EntryPoints: []spicestarter.EntryPoint{{
				Package: "github.com/StevenBuglione/spice/starter/kafka",
				Symbol:  "Open",
			}},
		},
		Capabilities: []string{"messaging.kafka.producer"},
		Dependencies: []spicestarter.Dependency{{
			Module:  "github.com/twmb/franz-go",
			Version: "v1.21.0",
			License: "BSD-3-Clause",
		}},
	})
}
