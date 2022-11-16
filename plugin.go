package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type KafkaConfig struct {
	BootstrapUrl string `json:"bootstrapUrl,omitempty"`
}

// Config the plugin configuration.
type Config struct {
	Kafka KafkaConfig `json:"kafka,omitempty"`
}

// CreateConfig creates the default plugin configuration.
func CreateConfig() *Config {
	return &Config{
		Kafka: KafkaConfig{
			BootstrapUrl: "",
		},
	}
}

// Demo a RequestLogger plugin.
type RequestLogger struct {
	next         http.Handler
	bootstrapUrl string
	name         string
}

// New created a new Demo plugin.
func New(ctx context.Context, next http.Handler, config *Config, name string) (http.Handler, error) {
	if len(config.Kafka.BootstrapUrl) == 0 {
		return nil, fmt.Errorf("must provide a boostrapUrl")
	}

	return &RequestLogger{
		bootstrapUrl: config.Kafka.BootstrapUrl,
		next:         next,
		name:         name,
	}, nil
}

func (a *RequestLogger) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	var bodyBytes []byte
	var err error

	if req.Body != nil {
		bodyBytes, err = ioutil.ReadAll(req.Body)
		if err != nil {
			fmt.Printf("Body reading error: %v", err)
			return
		}
		defer req.Body.Close()
	}

	fmt.Printf("Headers: %+v\n", req.Header)

	if len(bodyBytes) > 0 {
		var prettyJSON bytes.Buffer
		if err = json.Indent(&prettyJSON, bodyBytes, "", "\t"); err != nil {
			fmt.Printf("JSON parse error: %v", err)
			return
		}
		fmt.Println(prettyJSON.String())
	} else {
		fmt.Printf("Body: No Body Supplied\n")
	}

	rw.Header().Add("X-Request-Id", "1")

	a.next.ServeHTTP(rw, req)
}
