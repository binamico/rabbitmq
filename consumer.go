package rabbitmq

import (
	"context"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// messageHandler defines the function signature for processing incoming messages.
type messageHandler func(context.Context, []byte) error

// Consumer represents a RabbitMQ consumer, responsible for consuming messages from a queue.
type Consumer struct {
	qName    string               // Name of the queue the consumer is connected to.
	ch       *Channel             // Channel to the RabbitMQ server.
	messages <-chan amqp.Delivery // Channel for receiving messages from the queue.
	handler  messageHandler       // Function to handle incoming messages.
	mu       sync.Mutex           // Mutex for synchronizing access to the consumer.
}

// NewConsumer creates and returns a new Consumer that consumes messages from the specified queue.
//
// Parameters:
//   - queueName string: The name of the queue to consume messages from.
//   - handler messageHandler: The function that processes each incoming message.
//
// Returns:
//   - *Consumer: A pointer to the newly created Consumer instance.
//   - error: Error if there was an issue creating the queue or starting the consumer.
//
// Example usage:
//
//	consumer, err := queue.NewConsumer("my-queue", myMessageHandler)
//	if err != nil {
//		log.Fatalf("Failed to create consumer: %v", err)
//	}
func (q *Queue) NewConsumer(
	queueName string,
	handler messageHandler,
	opts ...MakeQueueOption,
) (*Consumer, error) {
	queue, err := MakeQueue(q.channel, queueName, opts...)
	if err != nil {
		return nil, fmt.Errorf("make queue with dead letter for %s failed: %w", queueName, err)
	}
	messages, err := q.channel.Consume(
		queue.Name, // queue name
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no local
		false,      // no wait
		nil,        // arguments
	)
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		ch:       q.channel,
		messages: messages,
		handler:  handler,
	}

	return consumer, nil
}

// Run starts the consumer to listen for messages and process them using the handler function.
//
// Parameters:
//   - ctx context.Context: Context for managing the consumer's lifecycle.
//
// Returns:
//   - error: Error if the context is canceled or if an error occurs during message processing.
func (c *Consumer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		msg, ok := c.nextMessage()
		if !ok {
			logrus.Warnf("channel is closed before message was delivered")
			continue
		}
		logrus.WithContext(ctx).Infof("message received: %s", msg.MessageId)
		if err := c.handler(ctx, msg.Body); err != nil {
			logrus.WithContext(ctx).Errorf("error when handle message: ID=%s; err=%s", msg.MessageId, err)
			if nackErr := c.nack(msg.DeliveryTag); nackErr != nil {
				logrus.WithContext(ctx).Errorf("queue nack message error: %s", nackErr)
			}
			continue
		}

		if err := c.ack(msg.DeliveryTag); err != nil {
			logrus.WithContext(ctx).Errorf("error when ack message: ID=%s; err=%s", msg.MessageId, err)
			return err
		}
	}

	return ctx.Err()
}

// nextMessage fetches the next message from the queue.
//
// Returns:
//   - amqp.Delivery: The next message from the queue.
//   - bool: False if the channel is closed, otherwise true.
func (c *Consumer) nextMessage() (amqp.Delivery, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	msg, ok := <-c.messages
	return msg, ok
}

// nack negatively acknowledges a message, indicating that it could not be processed successfully.
//
// Parameters:
//   - dTag uint64: The delivery tag of the message to nack.
//
// Returns:
//   - error: Error if the nack operation fails.
func (c *Consumer) nack(dTag uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ch.Nack(dTag, false, false)
}

// ack acknowledges a message, indicating that it was processed successfully.
//
// Parameters:
//   - dTag uint64: The delivery tag of the message to acknowledge.
//
// Returns:
//   - error: Error if the ack operation fails.
func (c *Consumer) ack(dTag uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ch.Ack(dTag, false)
}

// Shutdown gracefully shuts down the consumer by closing the channel.
//
// Parameters:
//   - ctx context.Context: Context for managing the shutdown process.
//
// Returns:
//   - error: Error if the channel cannot be closed.
func (c *Consumer) Shutdown(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.ch.Close()
}

// HealthCheck checks the health status of the consumer.
//
// Parameters:
//   - ctx context.Context: Context for the health check (currently unused).
//
// Returns:
//   - error: Error if the channel is closed, indicating that the consumer is not healthy.
func (c *Consumer) HealthCheck(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ch.IsClosed() {
		return amqp.ErrClosed
	}
	return nil
}
