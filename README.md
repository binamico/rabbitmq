# RabbitMQ Wrapper for Auto-Reconnection in Go

This Go package provides a wrapper around the streadway/amqp library to handle automatic reconnection for RabbitMQ
connections and channels. It simplifies working with RabbitMQ by ensuring that your application can recover from
connection issues without manual intervention.

## Features

Automatic Reconnection: Handles automatic reconnection for both RabbitMQ connections and channels.
Channel Management: Provides channel wrappers that will automatically recreate channels if they are closed.
Graceful Handling of Failures: Reconnects on failures, ensuring your message production and consumption can continue
without manual handling.
Thread-Safe Operations: Uses synchronization mechanisms to ensure thread safety when working with connections and
channels.

### Installation

To use this package, install it via `go get`:

```shell
go get github.com/binamico/rabbitmq
```

Then, import it in your Go project:

```go
import "github.com/yourusername/rabbitmq"
```

## Usage

Creating a Connection
The Dial function wraps the standard amqp.Dial function, providing automatic reconnection:

```go
conn, err := rabbitmq.Dial("amqp://guest:guest@localhost:5672/")
if err != nil {
log.Fatalf("failed to connect to RabbitMQ: %v", err)
}
```

## Creating a Channel

Once you have a connection, you can create a channel. This channel will automatically reconnect if the connection drops:

```go
ch, err := conn.Channel()
if err != nil {
log.Fatalf("failed to create channel: %v", err)
}
```

## Producer Example

Here is an example of creating a producer that publishes messages to a queue with Dead Letter support:

```go
producer, err := queue.NewProducer("my-queue")
if err != nil {
log.Fatalf("failed to create producer: %v", err)
}

message := "Hello, RabbitMQ!"
ctx := context.Background()
err = producer.Publish(ctx, message)
if err != nil {
log.Fatalf("failed to publish message: %v", err)
}
```

## Consumer Example

The Consume method wraps the amqp.Channel.Consume method and automatically handles reconnection. Here is an example of
consuming messages from a queue:

```go
deliveries, err := ch.Consume("my-queue", "my-consumer", true, false, false, false, nil)
if err != nil {
log.Fatalf("failed to start consuming: %v", err)
}

go func () {
for msg := range deliveries {
log.Printf("Received a message: %s", msg.Body)
}
}()
```

## Handling Connection Health

You can implement a health check to verify that your producer or consumer is still connected to RabbitMQ:

```go
err := producer.HealthCheck(context.Background())
if err != nil {
log.Fatalf("Producer is not connected: %v", err)
}

```

## Dead Letter Exchange Support
This package also includes support for creating queues with Dead Letter Exchanges (DLX). When a message cannot be
processed, it is sent to a Dead Letter Queue (DLQ) for further inspection or retries.

The makeQueueWithDeadLetter function automatically creates the main queue and associates it with a DLX and DLQ.

## Logging
The package uses logrus for logging. Make sure to configure logrus in your application to capture and store the logs
appropriately.

## Contributing
If you would like to contribute to this project, feel free to submit a pull request. Issues and suggestions are also
welcome.

## License
This package is distributed under the MIT License. See the LICENSE file for more information.

## Acknowledgments
This package is built on top of the streadway/amqp library and provides additional reconnection logic to make RabbitMQ
usage more robust in production environments.

This README provides a basic overview of how to use the RabbitMQ wrapper with automatic reconnection. For more details
and advanced configurations, please refer to the source code and comments within the package.