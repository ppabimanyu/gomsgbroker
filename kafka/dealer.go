package kafka

import (
	"context"
	"crypto/tls"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"os"
	"time"
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

	// Logger is the logger to use for logging.
	Logger *zerolog.Logger
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
	if config.Logger == nil {
		logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
		config.Logger = &logger
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
		panic("unsupported security protocol or SASL mechanism")
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
		ErrorLogger: d.Config.Logger,
		Logger:      d.Config.Logger,
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
		Logger:       d.Config.Logger,
		ErrorLogger:  d.Config.Logger,
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
			d.Config.Logger.Error().Err(err).Msg("Kafka: Failed to close reader")
		}
	}()

	for {
		select {
		case <-d.Context.Done():
			return
		default:
			message, err := reader.FetchMessage(d.Context)
			if err != nil {
				d.Config.Logger.Error().Err(err).Msg("Kafka: Failed to fetch message")
				continue
			}
			d.Config.Logger.Info().Str("topic", message.Topic).Str("key", string(message.Key)).Str("value", string(message.Value)).Msg("Kafka: Received message")
			if err := consumerFunc(d.Context, message); err != nil {
				d.Config.Logger.Error().Err(err).Str("topic", message.Topic).Msg("Kafka: Failed to process message")
			} else {
				if err := reader.CommitMessages(d.Context, message); err != nil {
					d.Config.Logger.Error().Err(err).Str("topic", message.Topic).Msg("Kafka: Failed to commit message")
				}
				d.Config.Logger.Info().Str("topic", message.Topic).Str("key", string(message.Key)).Msg("Kafka: Committed message")
			}
		}
	}
}

func (d *Dealer) DefaultPublisher(ctx context.Context, topic string, data ...kafka.Message) error {
	writer := d.DefaultWriter(topic)
	defer writer.Close()
	return writer.WriteMessages(ctx, data...)
}
