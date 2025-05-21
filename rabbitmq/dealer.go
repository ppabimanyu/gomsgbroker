package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
)

const (
	TraceIDHeaderKey = "Trace-Id"
	TraceIDCtxKey    = "trace_id"
	RequestIDCtxKey  = "request_id"
)

type Config struct {
	// Host is the RabbitMQ server host.
	// Default is "localhost".
	Host string

	// Port is the RabbitMQ server port.
	// Default is 5672.
	Port int

	// Username is the RabbitMQ username.
	// Default is "guest".
	Username string

	// Password is the RabbitMQ password.
	// Default is "guest".
	Password string

	// VHost is the RabbitMQ virtual host.
	// Default is "/".
	VHost string
}

type Dealer struct {
	config *Config
	ctx    context.Context
}

func NewDealer(config *Config) *Dealer {
	if config == nil {
		panic("config cannot be nil")
	}
	if config.Host == "" {
		config.Host = "localhost"
	}
	if config.Port == 0 {
		config.Port = 5672
	}
	if config.Username == "" {
		config.Username = "guest"
	}
	if config.Password == "" {
		config.Password = "guest"
	}

	return &Dealer{
		config: config,
		ctx:    context.Background(),
	}
}

func NewDealerWithContext(ctx context.Context, config *Config) *Dealer {
	dealer := NewDealer(config)
	dealer.ctx = ctx
	return dealer
}

func (d *Dealer) CreateConnection() *amqp091.Channel {
	conn, err := amqp091.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/%s", d.config.Username, d.config.Password, d.config.Host, d.config.Port, d.config.VHost))
	if err != nil {
		slog.Error("RabbitMQ: Failed to connect to RabbitMQ", "error", err.Error())
		return nil
	}

	channel, err := conn.Channel()
	if err != nil {
		slog.Error("RabbitMQ: Failed to open a channel", "error", err.Error())
		return nil
	}

	return channel
}

func (d *Dealer) _createConnectionAndRetry() *amqp091.Channel {
	var conn *amqp091.Channel
	for conn == nil {
		conn = d.CreateConnection()
		if conn == nil {
			time.Sleep(15 * time.Second)
		}
	}
	return conn
}

type ConsumerFunc func(ctx context.Context, message amqp091.Delivery) error

func (d *Dealer) DefaultConsumer(consumerFunc ConsumerFunc, queue string, consumerName ...string) {
	conn := d._createConnectionAndRetry()
	defer conn.Close()

	if len(consumerName) == 0 {
		consumerName = append(consumerName, "default-consumer")
	}

	consumer, err := conn.ConsumeWithContext(d.ctx, queue, consumerName[0], false, false, false, false, nil)
	if err != nil {
		slog.Error("RabbitMQ: Failed to register consumer", "error", err.Error())
		return
	}

	for message := range consumer {
		requestID := uuid.New().String()
		traceID := requestID
		if val, ok := message.Headers[TraceIDHeaderKey]; ok {
			traceID = val.(string)
		}
		ctx := context.WithValue(d.ctx, RequestIDCtxKey, requestID)
		ctx = context.WithValue(ctx, TraceIDCtxKey, traceID)

		slog.InfoContext(ctx, "RabbitMQ: Received message", "queue", queue, "body", string(message.Body))
		err := consumerFunc(d.ctx, message)
		if err != nil {
			slog.ErrorContext(ctx, "RabbitMQ: Failed to process message", "queue", queue, "body", string(message.Body), "error", err.Error())
			if err := message.Nack(false, false); err != nil {
				slog.ErrorContext(ctx, "RabbitMQ: Failed to Nack message", "queue", queue, "body", string(message.Body), "error", err.Error())
			}
		} else {
			if err := message.Ack(false); err != nil {
				slog.ErrorContext(ctx, "RabbitMQ: Failed to Ack message", "queue", queue, "body", string(message.Body), "error", err.Error())
			} else {
				slog.InfoContext(ctx, "RabbitMQ: Acknowledged message", "queue", queue, "body", string(message.Body))
			}
		}
	}
}

func (d *Dealer) DefaultPublisher(ctx context.Context, exchange, routingKey string, data ...amqp091.Publishing) error {
	conn := d.CreateConnection()
	if conn == nil {
		return fmt.Errorf("RabbitMQ: Failed to create connection")
	}
	defer conn.Close()

	for _, message := range data {
		if err := conn.PublishWithContext(ctx, exchange, routingKey, false, false, message); err != nil {
			return err
		}
	}
	return nil
}
