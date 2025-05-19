package rabbitmq

import (
	"context"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
	"os"
	"time"
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

	// Logger is the logger to use for logging.
	Logger *zerolog.Logger
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
	if config.Logger == nil {
		logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
		config.Logger = &logger
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
		d.config.Logger.Error().Ctx(d.ctx).Err(err).Msg("RabbitMQ: Failed to connect to RabbitMQ")
		return nil
	}

	channel, err := conn.Channel()
	if err != nil {
		d.config.Logger.Error().Err(err).Msg("RabbitMQ: Failed to open a channel")
		return nil
	}
	if channel == nil {
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
		d.config.Logger.Error().Err(err).Msg("RabbitMQ: Failed to register consumer")
		return
	}

	for message := range consumer {
		d.config.Logger.Info().Str("queue", queue).Str("body", string(message.Body)).Msg("RabbitMQ: Received message")
		err := consumerFunc(d.ctx, message)
		if err != nil {
			d.config.Logger.Error().Err(err).Msg("RabbitMQ: Failed to process message")
			if err := message.Nack(false, false); err != nil {
				d.config.Logger.Error().Err(err).Msg("RabbitMQ: Failed to Nack message")
			}
		} else {
			if err := message.Ack(false); err != nil {
				d.config.Logger.Error().Err(err).Msg("RabbitMQ: Failed to Ack message")
			} else {
				d.config.Logger.Info().Str("queue", queue).Msg("RabbitMQ: Acknowledged message")
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
