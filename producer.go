package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Producer is responsible for producing and publishing messages to a RabbitMQ queue.
type Producer struct {
	qName string      // Name of the queue
	ch    *Channel    // RabbitMQ channel for communication
	queue *amqp.Queue // Reference to the RabbitMQ queue
	mu    sync.Mutex  // Mutex to ensure thread safety
}

// NewProducer initializes and returns a new instance of Producer.
// It creates a queue with a Dead Letter Exchange (DLX) and binds it.
//
// Parameters:
//   - queueName string: The name of the queue where messages will be published.
//
// Returns:
//   - *Producer: A pointer to a new Producer instance.
//   - error: An error if the queue creation fails.
func (q *Queue) NewProducer(
	queueName string,
	opts ...MakeQueueOption,
) (*Producer, error) {
	// Create a queue with Dead Letter Exchange
	queue, err := MakeQueue(q.channel, queueName, opts...)
	if err != nil {
		return nil, fmt.Errorf("make queue with dead letter for %s failed: %w", queueName, err)
	}

	// Initialize a new Producer
	producer := Producer{
		qName: queueName,
		ch:    q.channel,
		queue: queue,
	}

	return &producer, err
}

// Publish sends a message to the RabbitMQ queue.
//
// Parameters:
//   - ctx context.Context: Context for managing the lifecycle of the publish operation.
//   - message interface{}: The message to be published. It can be a UUID or any other serializable type.
//
// Returns:
//   - error: An error if the message could not be published.
func (p *Producer) Publish(ctx context.Context, message interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var payload []byte
	var err error

	// Convert the message to the appropriate byte payload
	switch msg := message.(type) {
	case uuid.UUID:
		id := msg.String()
		payload = []byte(id)
	default:
		payload, err = json.Marshal(message)
		if err != nil {
			return err
		}
	}

	// Generate a new unique message ID
	messageID := uuid.New().String()
	logrus.Infof("publishing message: %s", messageID)

	// Publish the message to the queue
	return p.ch.Publish(
		"",
		p.queue.Name, // routing_key
		false,        // no mandatory
		false,        // no immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // Ensure message persistence
			MessageId:    messageID,
			Timestamp:    time.Now(),
			Body:         payload,
		},
	)
}

// HealthCheck verifies the connection status with the RabbitMQ broker.
//
// Returns:
//   - error: An error if the connection to the RabbitMQ channel is closed.
func (p *Producer) HealthCheck(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if the channel is closed
	if p.ch.IsClosed() {
		return amqp.ErrClosed
	}
	return nil
}
