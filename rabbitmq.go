package rabbitmq

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

const delay = 3 // Delay in seconds for reconnect attempts

// Connection is a wrapper around amqp.Connection that provides automatic reconnection support.
type Connection struct {
	*amqp.Connection            // Embedding the amqp.Connection struct
	guard            sync.Mutex // Mutex to guard against concurrent access to the connection
}

// Channel wraps amqp.Connection.Channel and provides auto-reconnection support for channels.
//
// It returns a wrapped Channel that will attempt to reconnect automatically if the underlying RabbitMQ channel is closed.
func (c *Connection) Channel() (*Channel, error) {
	// Create a new channel from the connection
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}

	// Wrap the channel with our custom Channel type
	channel := &Channel{
		Channel: ch,
	}

	// Start a goroutine to handle reconnection logic for the channel
	go func() {
		for {
			// Listen for the channel to be closed
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// If the channel was closed manually, exit the goroutine
			if !ok || channel.IsClosed() {
				logrus.Info("channel closed")
				channel.Close() // Ensure the closed flag is set
				break
			}
			logrus.Warnf("channel closed, reason: %v", reason)

			// Attempt to reconnect if the channel was not closed manually
			for {
				time.Sleep(delay * time.Second) // Wait before attempting reconnection

				// Try to create a new channel
				ch, err := c.Connection.Channel()
				if err == nil {
					logrus.Info("channel recreate success")
					channel.Channel = ch // Update the channel reference
					break
				}

				logrus.Errorf("channel recreate failed, err: %v", err)
			}
		}
	}()

	return channel, nil
}

// Dial wraps amqp.Dial and provides automatic reconnection support for the connection.
//
// It returns a wrapped Connection that will attempt to reconnect automatically if the underlying RabbitMQ connection is closed.
func Dial(url string) (*Connection, error) {
	// Dial the RabbitMQ server
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	// Wrap the connection with our custom Connection type
	connection := &Connection{
		Connection: conn,
	}

	// Start a goroutine to handle reconnection logic for the connection
	go func() {
		for {
			connection.guard.Lock()
			reason, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))
			connection.guard.Unlock()
			// If the connection was closed manually, exit the goroutine
			if !ok {
				logrus.Info("connection closed")
				break
			}
			logrus.Errorf("connection closed, reason: %v", reason)

			// Attempt to reconnect if the connection was not closed manually
			for {
				time.Sleep(delay * time.Second) // Wait before attempting reconnection

				// Try to dial the RabbitMQ server again
				conn, err := amqp.Dial(url)
				if err == nil {
					connection.guard.Lock()
					connection.Connection = conn // Update the connection reference
					connection.guard.Unlock()
					logrus.Info("reconnect success")
					break
				}

				logrus.Errorf("reconnect failed, err: %v", err)
			}
		}
	}()

	return connection, nil
}

// Channel is a wrapper around amqp.Channel that provides automatic reconnection support.
//
// The closed flag is used to track whether the channel was closed by the developer.
type Channel struct {
	*amqp.Channel            // Embedding the amqp.Channel struct
	closed        int32      // Atomic flag to indicate if the channel was closed by the developer
	guard         sync.Mutex // Mutex to guard against concurrent access to the channel
}

// IsClosed returns whether the channel was closed by the developer.
func (ch *Channel) IsClosed() bool {
	return (atomic.LoadInt32(&ch.closed) == 1)
}

// Close ensures that the closed flag is set and closes the channel.
//
// If the channel is already closed, it returns amqp.ErrClosed.
func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	// Set the closed flag atomically
	atomic.StoreInt32(&ch.closed, 1)

	// Lock the channel to ensure thread safety while closing
	ch.guard.Lock()
	defer ch.guard.Unlock()

	// Close the underlying RabbitMQ channel
	return ch.Channel.Close()
}

// Consume wraps amqp.Channel.Consume and returns a delivery channel that will continue consuming
// messages even after a reconnection.
//
// The returned channel will only stop delivering messages when the channel is closed by the developer.
func (ch *Channel) Consume(
	queue, consumer string,
	autoAck, exclusive, noLocal, noWait bool,
	args amqp.Table,
) (<-chan amqp.Delivery, error) {
	// Create a channel to deliver messages
	deliveries := make(chan amqp.Delivery)

	// Start a goroutine to consume messages from the queue
	go func() {
		for {
			// Lock the channel for thread-safe access
			ch.guard.Lock()
			// Start consuming messages from the queue
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			ch.guard.Unlock()
			if err != nil {
				logrus.Errorf("consume failed, err: %v", err)
				time.Sleep(delay * time.Second) // Wait before retrying
				continue
			}

			// Forward messages to the deliveries channel
			for msg := range d {
				deliveries <- msg
			}

			// Sleep before checking if the channel is closed
			time.Sleep(delay * time.Second)

			// Exit the loop if the channel is closed
			if ch.IsClosed() {
				break
			}
		}
	}()

	return deliveries, nil
}
