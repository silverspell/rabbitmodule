package rabbitmodule

import (
	"errors"
	"fmt"
	"os"

	"github.com/streadway/amqp"
)

var E chan error = make(chan error)

func getEnv(key string) string {
	var env string
	var ok bool
	if env, ok = os.LookupEnv(key); !ok {
		return ""
	}
	return env

}

func connect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(getEnv("AMQP_HOST"))
	go func() {
		<-conn.NotifyClose(make(chan *amqp.Error))
		E <- errors.New("connection closed")
	}()
	return conn, err
}

func exchangeDeclare(ch *amqp.Channel, excType, exchange string) error {
	return ch.ExchangeDeclare(
		exchange,
		excType,
		true,
		false,
		false,
		false,
		nil,
	)
}

func declareQueue(ch *amqp.Channel, queueName string) (amqp.Queue, error) {
	return ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
}

func bindQueue(ch *amqp.Channel, queueName, exchange, routeKey string) error {
	return ch.QueueBind(
		queueName,
		routeKey,
		exchange,
		false,
		nil,
	)
}

func consume(ch *amqp.Channel, queueName string) (<-chan amqp.Delivery, error) {
	return ch.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
}

func publish(ch *amqp.Channel, exchange, msg, routeKey string) error {
	return ch.Publish(
		exchange, // exchange
		routeKey, // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
		})
}

func ConnectSubscriber(reply chan string, exchange string) {

	conn, err := connect()
	failOnError(err, "[SUBSCRIBER] Failed to connect")
	defer conn.Close()
	ch, _ := conn.Channel()
	defer ch.Close()
	err = exchangeDeclare(ch, "fanout", exchange)
	failOnError(err, "[SUBSCRIBER] failed to declare exchange")
	q, err := declareQueue(ch, "")
	failOnError(err, "[SUBSCRIBER] failed to declare queue")
	err = bindQueue(ch, q.Name, exchange, "")
	failOnError(err, "[SUBSCRIBER] Fail to bind")
	msgs, _ := consume(ch, q.Name)
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			fmt.Printf("[SUBSCRIBE] %s\n", string(d.Body))
			reply <- string(d.Body)
			d.Ack(false)
		}
	}()

	fmt.Println("[RABBITMAN] Waiting for messages")
	<-forever
	fmt.Println("[RABBITMAN] Exiting?")
}

func ConnectPublisher(listen chan string, exchange string) {
	conn, err := connect()
	failOnError(err, "[PUBLISHER] Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "[PUBLISHER] Failed to open a channel")
	defer ch.Close()

	err = exchangeDeclare(ch, "fanout", exchange)
	failOnError(err, "[PUBLISHER] Failed to declare an exchange")

	for {
		msg := <-listen
		err = publish(ch, exchange, msg, "")
		failOnError(err, "[PUBLISHER] Failed to publish a message")

		if getEnv("RABBIT_ENV") != "" {
			fmt.Printf("[x] Sent %s\n", msg)
		}
	}

}

func ConnectSubscriberDirect(reply chan string, exchange, routeKey string) {
	conn, err := connect()
	failOnError(err, "[SUBSCRIBER] Failed to connect")
	defer conn.Close()
	ch, _ := conn.Channel()
	defer ch.Close()
	err = exchangeDeclare(ch, "direct", exchange)
	failOnError(err, "[SUBSCRIBER] failed to declare exchange")
	q, err := declareQueue(ch, "")
	failOnError(err, "[SUBSCRIBER] failed to declare queue")
	err = bindQueue(ch, q.Name, exchange, routeKey)
	failOnError(err, "[SUBSCRIBER] Fail to bind")
	msgs, _ := consume(ch, q.Name)
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			fmt.Printf("[SUBSCRIBE] %s\n", string(d.Body))
			reply <- string(d.Body)
			d.Ack(false)
		}
	}()

	fmt.Println("[RABBITMAN] Waiting for messages")
	<-forever
	fmt.Println("[RABBITMAN] Exiting?")
}

func ConnectPublisherDirect(listen chan string, exchange, routeKey string) {
	conn, err := connect()
	failOnError(err, "[PUBLISHER] Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "[PUBLISHER] Failed to open a channel")
	defer ch.Close()

	err = exchangeDeclare(ch, "direct", exchange)
	failOnError(err, "[PUBLISHER] Failed to declare an exchange")

	for {
		msg := <-listen
		err = publish(ch, exchange, msg, routeKey)
		failOnError(err, "[PUBLISHER] Failed to publish a message")

		if getEnv("RABBIT_ENV") != "" {
			fmt.Printf("[x] Sent %s\n", msg)
		}
	}
}

func ConnectSubscriberTaskQueue(reply chan string, queueName string) {
	conn, err := connect()
	failOnError(err, "[SUBSCRIBER] Failed to connect")
	defer conn.Close()
	ch, _ := conn.Channel()
	defer ch.Close()
	q, err := declareQueue(ch, queueName)
	failOnError(err, "[SUBSCRIBER] failed to declare queue")
	err = ch.Qos(1, 0, false) //prefetch count, prefetch size
	failOnError(err, "[SUSBCRIBER] failed to set qos")
	msgs, _ := consume(ch, q.Name)

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			fmt.Printf("[SUBSCRIBE] %s\n", string(d.Body))
			reply <- string(d.Body)
			d.Ack(false)
		}
	}()

	fmt.Println("[RABBITMAN] Waiting for messages")
	<-forever
	fmt.Println("[RABBITMAN] Exiting?")
}

func ConnectPublisherTaskQueue(listen chan string, queueName string) {
	conn, err := connect()
	failOnError(err, "[PUBLISHER] Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "[PUBLISHER] Failed to open a channel")
	defer ch.Close()

	q, err := declareQueue(ch, queueName)
	failOnError(err, "[PUBLISHER] failed to declare Queue")
	for {
		msg := <-listen
		err = publish(ch, "", msg, q.Name)
		failOnError(err, "[PUBLISHER] Failed to publish a message")

		if getEnv("RABBIT_ENV") != "" {
			fmt.Printf("[x] Sent %s\n", msg)
		}
	}
}
