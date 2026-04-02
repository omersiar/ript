package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// AuthConfig holds all authentication-related settings for Kafka connections.
type AuthConfig struct {
	// SecurityProtocol is one of: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
	SecurityProtocol string

	// SASL settings
	SASLMechanism string // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER
	SASLUsername  string
	SASLPassword  string

	// OAUTHBEARER settings (client-credentials grant)
	OAuthTokenEndpoint string
	OAuthClientID      string
	OAuthClientSecret  string
	OAuthScope         string

	// TLS settings
	TLSCACertFile     string
	TLSClientCertFile string
	TLSClientKeyFile  string
	TLSInsecureSkip   bool
}

// BuildAuthOpts converts an AuthConfig into a slice of kgo.Opt that can be
// appended to any franz-go client constructor. Returns an error if the
// configuration is invalid or certificate files cannot be loaded.
func BuildAuthOpts(cfg AuthConfig) ([]kgo.Opt, error) {
	protocol := strings.ToUpper(strings.TrimSpace(cfg.SecurityProtocol))
	if protocol == "" {
		protocol = "PLAINTEXT"
	}

	var opts []kgo.Opt

	switch protocol {
	case "PLAINTEXT":
		return nil, nil

	case "SSL":
		tlsCfg, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("SSL config: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))

	case "SASL_PLAINTEXT":
		saslOpt, err := buildSASLOpt(cfg)
		if err != nil {
			return nil, fmt.Errorf("SASL_PLAINTEXT config: %w", err)
		}
		opts = append(opts, saslOpt)

	case "SASL_SSL":
		tlsCfg, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("SASL_SSL TLS config: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))

		saslOpt, err := buildSASLOpt(cfg)
		if err != nil {
			return nil, fmt.Errorf("SASL_SSL SASL config: %w", err)
		}
		opts = append(opts, saslOpt)

	default:
		return nil, fmt.Errorf("unsupported KAFKA_SECURITY_PROTOCOL: %q (valid: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)", protocol)
	}

	return opts, nil
}

func buildTLSConfig(cfg AuthConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.TLSInsecureSkip,
	}

	// Load CA certificate if provided (server verification).
	if cfg.TLSCACertFile != "" {
		caPEM, err := os.ReadFile(cfg.TLSCACertFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA cert file %q: %w", cfg.TLSCACertFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("CA cert file %q contains no valid certificates", cfg.TLSCACertFile)
		}
		tlsCfg.RootCAs = pool
	}

	// Load client certificate + key if provided (mTLS).
	if cfg.TLSClientCertFile != "" && cfg.TLSClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSClientCertFile, cfg.TLSClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client cert/key (%q, %q): %w", cfg.TLSClientCertFile, cfg.TLSClientKeyFile, err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	} else if cfg.TLSClientCertFile != "" || cfg.TLSClientKeyFile != "" {
		return nil, fmt.Errorf("both KAFKA_TLS_CLIENT_CERT_FILE and KAFKA_TLS_CLIENT_KEY_FILE must be set for mTLS")
	}

	return tlsCfg, nil
}

func buildSASLOpt(cfg AuthConfig) (kgo.Opt, error) {
	mechanism := strings.ToUpper(strings.TrimSpace(cfg.SASLMechanism))
	if mechanism == "" {
		return nil, fmt.Errorf("KAFKA_SASL_MECHANISM is required when using SASL security protocol")
	}

	switch mechanism {
	case "PLAIN":
		if cfg.SASLUsername == "" || cfg.SASLPassword == "" {
			return nil, fmt.Errorf("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required for SASL/PLAIN")
		}
		return kgo.SASL(plain.Auth{
			User: cfg.SASLUsername,
			Pass: cfg.SASLPassword,
		}.AsMechanism()), nil

	case "SCRAM-SHA-256":
		if cfg.SASLUsername == "" || cfg.SASLPassword == "" {
			return nil, fmt.Errorf("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required for SASL/SCRAM-SHA-256")
		}
		return kgo.SASL(scram.Auth{
			User: cfg.SASLUsername,
			Pass: cfg.SASLPassword,
		}.AsSha256Mechanism()), nil

	case "SCRAM-SHA-512":
		if cfg.SASLUsername == "" || cfg.SASLPassword == "" {
			return nil, fmt.Errorf("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required for SASL/SCRAM-SHA-512")
		}
		return kgo.SASL(scram.Auth{
			User: cfg.SASLUsername,
			Pass: cfg.SASLPassword,
		}.AsSha512Mechanism()), nil

	case "OAUTHBEARER":
		if cfg.OAuthTokenEndpoint == "" {
			return nil, fmt.Errorf("KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL is required for SASL/OAUTHBEARER")
		}
		if cfg.OAuthClientID == "" || cfg.OAuthClientSecret == "" {
			return nil, fmt.Errorf("KAFKA_SASL_OAUTHBEARER_CLIENT_ID and KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET are required for SASL/OAUTHBEARER")
		}
		return kgo.SASL(oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
			token, err := fetchOAuthToken(ctx, cfg.OAuthTokenEndpoint, cfg.OAuthClientID, cfg.OAuthClientSecret, cfg.OAuthScope)
			if err != nil {
				return oauth.Auth{}, err
			}
			return oauth.Auth{Token: token}, nil
		})), nil

	default:
		return nil, fmt.Errorf("unsupported KAFKA_SASL_MECHANISM: %q (valid: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER)", mechanism)
	}
}

func splitScopes(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	scopes := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			scopes = append(scopes, s)
		}
	}
	return scopes
}

// fetchOAuthToken performs a client-credentials grant against the token
// endpoint and returns the access_token string.
func fetchOAuthToken(ctx context.Context, endpoint, clientID, clientSecret, scope string) (string, error) {
	data := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
	}
	if scopes := splitScopes(scope); len(scopes) > 0 {
		data.Set("scope", strings.Join(scopes, " "))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("building token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token endpoint returned status %d", resp.StatusCode)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("decoding token response: %w", err)
	}
	if tokenResp.AccessToken == "" {
		return "", fmt.Errorf("token endpoint returned empty access_token")
	}
	return tokenResp.AccessToken, nil
}
