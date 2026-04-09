package version

import "fmt"

// These variables are set at build time via -ldflags.
var (
	Version   = "unknown"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

// BuildTag returns a compact identifier suitable for embedding in Kafka
// client.id values, e.g. "v1.0.0-abc1234" or "dev-unknown".
func BuildTag() string {
	return fmt.Sprintf("%s-%s", Version, GitCommit)
}

// Full returns a human-readable version string for logging.
func Full() string {
	return fmt.Sprintf("%s (commit=%s built=%s)", Version, GitCommit, BuildTime)
}
