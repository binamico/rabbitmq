package rabbitmq

import (
	"fmt"

	"github.com/streadway/amqp"
)

type MakeQueueOption func(option *makeQueueOptions)

type makeQueueOptions struct {
	withDeadLetter bool
}

func WithDeadLetter() MakeQueueOption {
	return func(options *makeQueueOptions) {
		options.withDeadLetter = true
	}
}

const deadLetterQueueHeader = "x-dead-letter-exchange"

func MakeQueue(ch *Channel, name string, opts ...MakeQueueOption) (*amqp.Queue, error) {
	options := makeQueueOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	var t amqp.Table
	if options.withDeadLetter {
		dl, err := makeDeadLetter(ch, name)
		if err != nil {
			return nil, fmt.Errorf("failed to make dead letter: %w", err)
		}
		t = amqp.Table{deadLetterQueueHeader: dl}
	}
	// Declare the queue with the Dead Letter Exchange binding
	queue, err := ch.QueueDeclare(
		name,  // Name of the queue
		true,  // Durable (persistent queue)
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		t,     // Arguments with Dead Letter Exchange binding
	)
	if err != nil {
		return nil, fmt.Errorf("declare queue with dead letter exchange failed: %w", err)
	}

	return &queue, nil
}
