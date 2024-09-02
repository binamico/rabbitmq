package rabbitmq

import (
	"fmt"

	"github.com/streadway/amqp"
)

// Queue represents a RabbitMQ queue, which holds the connection and channel to RabbitMQ.
type Queue struct {
	connection *Connection // RabbitMQ connection
	channel    *Channel    // RabbitMQ channel
}

// NewRabbitMQ creates and returns a new Queue instance by establishing a connection to RabbitMQ and opening a channel.
//
// Parameters:
//   - dsn string: The Data Source Name (DSN) or URI for connecting to the RabbitMQ server.
//
// Returns:
//   - *Queue: A new Queue instance with an open connection and channel to RabbitMQ.
//   - error: Error if there was an issue parsing the DSN, establishing the connection, or opening the channel.
//
// Example usage:
//
//	queue, err := NewRabbitMQ("amqp://user:pass@host:port/vhost")
//	if err != nil {
//		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
//	}
func NewRabbitMQ(dsn string) (*Queue, error) {
	q := &Queue{}

	// Parse the URI to ensure it's valid and extract connection information.
	uri, err := amqp.ParseURI(dsn)
	if err != nil {
		return nil, err
	}

	// Establish a connection to RabbitMQ.
	con, err := Dial(uri.String())
	if err != nil {
		return nil, fmt.Errorf("rabbitmq connection error: %w", err)
	}

	// Open a channel on the established connection.
	ch, err := con.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq channel error: %w", err)
	}

	// Assign the connection and channel to the Queue struct.
	q.connection = con
	q.channel = ch

	return q, nil
}
