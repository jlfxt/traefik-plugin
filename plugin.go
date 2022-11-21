package traefik_plugin

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

type TlsConfig struct {
	CaFile    string `json:"caFile,omitempty"`
	CertFile  string `json:"certFile,omitempty"`
	KeyFile   string `json:"keyFile,omitempty"`
	VerifySsl bool   `json:"verifySsl,omitempty"`
}

type KafkaConfig struct {
	BootstrapUrl string    `json:"bootstrapUrl,omitempty"`
	Tls          TlsConfig `json:"tls,omitempty"`
}

// Config the plugin configuration.
type Config struct {
	Kafka KafkaConfig `json:"kafka,omitempty"`
}

// CreateConfig creates the default plugin configuration.
func CreateConfig() *Config {
	return &Config{
		Kafka: KafkaConfig{
			Tls: TlsConfig{},
		},
	}
}

// Demo a RequestLogger plugin.
type RequestLogger struct {
	next         http.Handler
	bootstrapUrl string
	name         string
	producer     sarama.SyncProducer
}

type RequestLogEntry struct {
	Method       string              `json:"method"`
	Host         string              `json:"host"`
	Path         string              `json:"path"`
	IP           string              `json:"ip"`
	ResponseTime float64             `json:"response_time"`
	Headers      map[string][]string `json:"headers"`

	encoded []byte
	err     error
}

func (rle *RequestLogEntry) ensureEncoded() {
	if rle.encoded == nil && rle.err == nil {
		rle.encoded, rle.err = json.Marshal(rle)
	}
}

func (rle *RequestLogEntry) Length() int {
	rle.ensureEncoded()
	return len(rle.encoded)
}

func (rle *RequestLogEntry) Encode() ([]byte, error) {
	rle.ensureEncoded()
	return rle.encoded, rle.err
}

func New(ctx context.Context, next http.Handler, config *Config, name string) (http.Handler, error) {
	if len(config.Kafka.BootstrapUrl) == 0 {
		return nil, fmt.Errorf("must provide a bootstrapUrl")
	}

	return &RequestLogger{
		bootstrapUrl: config.Kafka.BootstrapUrl,
		next:         next,
		name:         name,
		producer:     newRequestLogProducer(config.Kafka),
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

	rw.Header().Add("x-request-id", "1")

	started := time.Now()

	a.next.ServeHTTP(rw, req)

	entry := &RequestLogEntry{
		Method:       req.Method,
		Host:         req.Host,
		Path:         req.RequestURI,
		Headers:      req.Header,
		IP:           req.RemoteAddr,
		ResponseTime: float64(time.Since(started)) / float64(time.Second),
	}

	// We will use the client's IP address as key. This will cause
	// all the access log entries of the same IP address to end up
	// on the same partition.
	/* 	a.producer.Input() <- &sarama.ProducerMessage{
		Topic: "msglog",
		Key:   sarama.StringEncoder("localhost"),
		Value: entry,
	} */

	partition, offset, err := a.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "msglog",
		Key:   sarama.StringEncoder("localhost"),
		Value: entry,
	})

	if err != nil {
		log.Fatalf("Failed to store your data: %s", err)
	} else {
		// The tuple (topic, partition, offset) can be used as a unique identifier
		// for a message in a Kafka cluster.
		log.Printf("Your data is stored with unique identifier important/%d/%d", partition, offset)
	}
}

func createTlsConfiguration(config TlsConfig) (t *tls.Config) {
	if config.CertFile != "" && config.KeyFile != "" && config.CaFile != "" {
		cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := os.ReadFile(config.CaFile)
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: config.VerifySsl,
		}
	}
	// will be nil by default if nothing is provided
	return t
}

func newRequestLogProducer(kc KafkaConfig) sarama.SyncProducer {
	// For the request log, we are looking for AP semantics, with high throughput.
	// By creating batches of compressed messages, we reduce network I/O at a cost of more latency.
	config := sarama.NewConfig()
	tlsConfig := createTlsConfiguration(kc.Tls)
	if tlsConfig != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}
	config.Producer.RequiredAcks = sarama.WaitForLocal // Only wait for the leader to ack
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	producer, err := sarama.NewSyncProducer([]string{kc.BootstrapUrl}, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	// We will just log to STDOUT if we're not able to produce messages.
	// Note: messages will only be returned here after all retry attempts are exhausted.
	/* 	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write access log entry:", err)
		}
	}() */

	return producer
}
