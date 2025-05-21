package kafka

import (
	"context"
	"crypto/tls"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const (
	TraceIDHeaderKey = "Trace-Id"
	TraceIDCtxKey    = "trace_id"
	RequestIDCtxKey  = "request_id"
)

type Config struct {
	// SecurityProtocol defines the security protocol to use. It can be one of the following: NONE, SSL, SASL_PLAINTEXT, SASL_SSL.
	// Default is NONE.
	SecurityProtocol string

	// SASLMechanism defines the SASL mechanism to use. It can be one of the following: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.
	SASLMechanism string

	// Brokers is a list of Kafka brokers to connect to.
	Brokers []string

	// Username is the username for SASL authentication.
	Username string

	// Password is the password for SASL authentication.
	Password string

	// TlsConfig is the TLS configuration to use for secure connections.
	TlsConfig *tls.Config
}

type Dealer struct {
	Config    *Config
	Mechanism sasl.Mechanism
	TlsConfig *tls.Config
	Context   context.Context
}

func NewDealer(config *Config) *Dealer {
	if config == nil {
		panic("config cannot be nil")
	}
	if config.Brokers == nil || len(config.Brokers) == 0 {
		panic("brokers cannot be nil or empty")
	}
	if config.SecurityProtocol == "" {
		config.SecurityProtocol = "NONE"
	}
	if config.TlsConfig == nil {
		config.TlsConfig = &tls.Config{}
	}

	var tlsConfig = config.TlsConfig
	var mechanism sasl.Mechanism
	var err error

	if config.SecurityProtocol == "NONE" {
		mechanism = nil
		tlsConfig = nil
	} else if config.SecurityProtocol == "SSL" {
		mechanism = nil
		tlsConfig = &tls.Config{}
	} else if config.SecurityProtocol == "SASL_PLAINTEXT" && config.SASLMechanism == "PLAIN" {
		mechanism = plain.Mechanism{
			Username: config.Username,
			Password: config.Password,
		}
		tlsConfig = nil
	} else if config.SecurityProtocol == "SASL_PLAINTEXT" && config.SASLMechanism == "SCRAM-SHA-256" {
		mechanism, err = scram.Mechanism(scram.SHA256, config.Username, config.Password)
		if err != nil {
			panic(err)
		}
		tlsConfig = nil
	} else if config.SecurityProtocol == "SASL_PLAINTEXT" && config.SASLMechanism == "SCRAM-SHA-512" {
		mechanism, err = scram.Mechanism(scram.SHA512, config.Username, config.Password)
		if err != nil {
			panic(err)
		}
		tlsConfig = nil
	} else if config.SecurityProtocol == "SASL_SSL" && config.SASLMechanism == "PLAIN" {
		mechanism = plain.Mechanism{
			Username: config.Username,
			Password: config.Password,
		}
		tlsConfig = &tls.Config{}
	} else if config.SecurityProtocol == "SASL_SSL" && config.SASLMechanism == "SCRAM-SHA-256" {
		mechanism, err = scram.Mechanism(scram.SHA256, config.Username, config.Password)
		if err != nil {
			panic(err)
		}
		tlsConfig = &tls.Config{}
	} else if config.SecurityProtocol == "SASL_SSL" && config.SASLMechanism == "SCRAM-SHA-512" {
		mechanism, err = scram.Mechanism(scram.SHA512, config.Username, config.Password)
		if err != nil {
			panic(err)
		}
		tlsConfig = &tls.Config{}
	} else {
		slog.Error("unsupported security protocol or SASL mechanism")
		os.Exit(1)
	}

	return &Dealer{
		Config:    config,
		Mechanism: mechanism,
		TlsConfig: tlsConfig,
		Context:   context.Background(),
	}
}

func NewDealerWithContext(ctx context.Context, config *Config) *Dealer {
	dealer := NewDealer(config)
	dealer.Context = ctx
	return dealer
}

func (d *Dealer) DefaultReader(topic string, groupID ...string) *kafka.Reader {
	config := kafka.ReaderConfig{
		Brokers: d.Config.Brokers,
		Topic:   topic,
		Dialer: &kafka.Dialer{
			SASLMechanism: d.Mechanism,
			TLS:           d.TlsConfig,
		},
		ErrorLogger: kafka.LoggerFunc(func(s string, i ...any) {
			slog.Error(s, i...)
		}),
		Logger: kafka.LoggerFunc(func(s string, i ...any) {
			slog.Info(s, i...)
		}),
	}
	if len(groupID) > 0 {
		config.GroupID = groupID[0]
	}
	return kafka.NewReader(config)
}

func (d *Dealer) DefaultWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(d.Config.Brokers...),
		Topic:        topic,
		Async:        true,
		RequiredAcks: kafka.RequireOne,
		BatchSize:    100,
		BatchTimeout: time.Second,
		Compression:  kafka.Lz4,
		ErrorLogger: kafka.LoggerFunc(func(s string, i ...any) {
			slog.Error(s, i...)
		}),
		Logger: kafka.LoggerFunc(func(s string, i ...any) {
			slog.Info(s, i...)
		}),
		Transport: &kafka.Transport{
			TLS:  d.TlsConfig,
			SASL: d.Mechanism,
		},
		AllowAutoTopicCreation: true,
	}
}

type ConsumerFunc func(ctx context.Context, message kafka.Message) error

func (d *Dealer) DefaultConsumer(consumerFunc ConsumerFunc, topic string, groupID ...string) {
	reader := d.DefaultReader(topic, groupID...)
	defer func() {
		if err := reader.Close(); err != nil {
			slog.Error("Kafka: Failed to close reader", "error", err.Error())
		}
	}()

	for {
		select {
		case <-d.Context.Done():
			return
		default:
			requestID := uuid.New().String()
			ctx := context.WithValue(d.Context, RequestIDCtxKey, requestID)

			message, err := reader.FetchMessage(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "Kafka: Failed to fetch message", "topic", topic, "error", err.Error())
				continue
			}

			traceID := requestID
			for _, h := range message.Headers {
				if strings.ToLower(string(h.Key)) == strings.ToLower(TraceIDHeaderKey) {
					traceID = string(h.Value)
					break
				}
			}
			ctx = context.WithValue(ctx, TraceIDCtxKey, traceID)

			slog.InfoContext(ctx, "Kafka: Received message", "topic", message.Topic, "key", string(message.Key), "value", string(message.Value))
			if err := consumerFunc(ctx, message); err != nil {
				slog.ErrorContext(ctx, "Kafka: Failed to process message", "topic", message.Topic, "key", string(message.Key), "value", string(message.Value), "error", err.Error())
			} else {
				if err := reader.CommitMessages(ctx, message); err != nil {
					slog.ErrorContext(ctx, "Kafka: Failed to commit message", "topic", message.Topic, "key", string(message.Key), "value", string(message.Value), "error", err.Error())
				} else {
					slog.InfoContext(ctx, "Kafka: Committed message", "topic", message.Topic, "key", string(message.Key), "value", string(message.Value))
				}
			}
		}
	}
}

func (d *Dealer) DefaultPublisher(ctx context.Context, topic string, data ...kafka.Message) error {
	writer := d.DefaultWriter(topic)
	defer writer.Close()
	return writer.WriteMessages(ctx, data...)
}
