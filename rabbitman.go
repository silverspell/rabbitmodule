package rabbitmodule

import (
	"fmt"
	"os"

	"github.com/streadway/amqp"
)

func getEnv(key string) string {
	var env string
	var ok bool
	if env, ok = os.LookupEnv(key); !ok {
		return ""
	}
	return env

}

func ConnectSubscriber(reply chan string, exchange string) {

	conn, err := amqp.Dial(getEnv("AMQP_HOST"))

	failOnError(err, "[SUBSCRIBER] Failed to connect")

	defer conn.Close()

	ch, _ := conn.Channel()

	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchange,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)

	failOnError(err, "[SUBSCRIBER] failed to declare exchange")

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)

	failOnError(err, "[SUBSCRIBER] failed to declare queue")

	err = ch.QueueBind(
		q.Name,
		"",
		exchange,
		false,
		nil,
	)

	failOnError(err, "[SUBSCRIBER] Fail to bind")

	msgs, _ := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			fmt.Printf("[SUBSCRIBE] %s\n", string(d.Body))
			reply <- string(d.Body)
		}
	}()

	fmt.Println("[RABBITMAN] Waiting for messages")
	<-forever
	fmt.Println("[RABBITMAN] Exiting?")
}

func ConnectPublisher(listen chan string, exchange string) {
	conn, err := amqp.Dial(getEnv("AMQP_HOST"))
	failOnError(err, "[PUBLISHER] Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "[PUBLISHER] Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchange, // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "[PUBLISHER] Failed to declare an exchange")

	for {

		msg := <-listen

		err = ch.Publish(
			exchange, // exchange
			"",       // routing key
			false,    // mandatory
			false,    // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg),
			})
		failOnError(err, "[PUBLISHER] Failed to publish a message")

		if getEnv("RABBIT_ENV") != "" {
			fmt.Printf("[x] Sent %s\n", msg)
		}
	}

}
