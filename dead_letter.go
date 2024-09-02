package rabbitmq

import (
	"fmt"
)

const (
	queueDeadLetterExchangePostfix = "_dlx" // Suffix for the Dead Letter Exchange
	queueDeadLetterPostfix         = "_dl"  // Suffix for the Dead Letter Queue
)

// makeDeadLetter creates a Dead Letter Exchange (DLX) and a queue that is associated with it.
// The Dead Letter Exchange is used to handle messages that cannot be processed successfully from the primary queue.
//
// Parameters:
//   - ch *Channel: The RabbitMQ channel used to communicate with the RabbitMQ server.
//   - queueName string: The name of the primary queue for which the DLX is created.
//
// Returns:
//   - string: The name of the created Dead Letter Exchange.
//   - error: An error if the Dead Letter Exchange or queue cannot be created or bound.
func makeDeadLetter(
	ch *Channel,
	queueName string,
) (string, error) {
	// Create the name for the Dead Letter Exchange
	exchangeName := queueName + queueDeadLetterExchangePostfix

	// Declare the Dead Letter Exchange with type "fanout"
	if err := ch.ExchangeDeclare(
		exchangeName, // Name of the exchange
		"fanout",     // Type of exchange
		true,         // Durable (persistent exchange)
		false,        // Auto-deleted
		false,        // Internal
		false,        // No-wait
		nil,          // Arguments
	); err != nil {
		return "", fmt.Errorf("create dead letter exchange failed: %w", err)
	}

	// Create the name for the Dead Letter Queue
	queue := queueName + queueDeadLetterPostfix

	// Declare the Dead Letter Queue
	if _, err := ch.QueueDeclare(
		queue, // Name of the queue
		true,  // Durable (persistent queue)
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	); err != nil {
		return "", fmt.Errorf("create dead letter queue failed: %w", err)
	}

	// Bind the Dead Letter Queue to the Dead Letter Exchange
	if err := ch.QueueBind(
		queue,        // Name of the queue
		"",           // Routing key
		exchangeName, // Name of the exchange
		false,        // No-wait
		nil,          // Arguments
	); err != nil {
		return "", fmt.Errorf("bind dead letter queue with exchange failed: %w", err)
	}
	return exchangeName, nil
}
