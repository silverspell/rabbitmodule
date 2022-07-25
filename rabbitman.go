package rabbitmodule

import (
	"errors"
	"fmt"
	"os"

	"github.com/streadway/amqp"
)

var E chan error = make(chan error)
var SubscriberError chan error = make(chan error)
var Conn *amqp.Connection = nil

func getEnv(key string) string {
	var env string
	var ok bool
	if env, ok = os.LookupEnv(key); !ok {
		return ""
	}
	return env

}

func connect() error {
	var err error
	if Conn == nil {
		Conn, err = amqp.Dial(getEnv("AMQP_HOST"))
		go func() {
			<-Conn.NotifyClose(make(chan *amqp.Error))
			E <- errors.New("connection closed")
		}()
	}
	return err
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
	ctag := getEnv("AMQP_CTAG")
	if ctag == "" {
		ctag = "anon"
	}
	return ch.Consume(
		queueName,
		ctag,
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
	errChan := make(chan error)
	if Conn == nil {
		err := connect()
		failOnError(err, "[PUBLISHER] Failed to connect to RabbitMQ")
	}
	ch, _ := Conn.Channel()
	defer ch.Close()
	err := exchangeDeclare(ch, "fanout", exchange)
	failOnError(err, "[SUBSCRIBER] failed to declare exchange")
	q, err := declareQueue(ch, "")
	failOnError(err, "[SUBSCRIBER] failed to declare queue")
	err = bindQueue(ch, q.Name, exchange, "")
	failOnError(err, "[SUBSCRIBER] Fail to bind")
	msgs, _ := consume(ch, q.Name)

	go func() {
		for d := range msgs {
			fmt.Printf("[SUBSCRIBE] %s\n", string(d.Body))
			reply <- string(d.Body)
			d.Ack(false)
		}
		errChan <- fmt.Errorf("rabbit error %s", exchange)
	}()

	fmt.Println("[RABBITMAN] Waiting for messages")
	err = <-errChan
	fmt.Printf("Fanout error %+v", err)
	go reconnect(reply, exchange, "", nil, ConnectSubscriber)
	fmt.Println("[RABBITMAN] Exiting?")
}

func ConnectPublisher(listen chan string, exchange string) {
	if Conn == nil {
		err := connect()
		failOnError(err, "[PUBLISHER] Failed to connect to RabbitMQ")
	}

	ch, err := Conn.Channel()
	failOnError(err, "[PUBLISHER] Failed to open a channel")
	defer ch.Close()

	err = exchangeDeclare(ch, "fanout", exchange)
	failOnError(err, "[PUBLISHER] Failed to declare an exchange")

	for {
		select {
		case msg := <-listen:
			err = publish(ch, exchange, msg, "")
			failOnError(err, "[PUBLISHER] Failed to publish a message")

			if getEnv("RABBIT_ENV") != "" {
				fmt.Printf("[x] Sent %s\n", msg)
			}
		case e := <-E:
			fmt.Printf("Trying to reconnect..... Error %+v\n", e)
			go reconnect(listen, exchange, "", nil, ConnectPublisher)
		}
	}

}

func ConnectSubscriberDirect(reply chan string, exchange, routeKey string) {
	if Conn == nil {
		err := connect()
		failOnError(err, "[PUBLISHER] Failed to connect to RabbitMQ")
	}
	ch, _ := Conn.Channel()
	defer ch.Close()
	err := exchangeDeclare(ch, "direct", exchange)
	failOnError(err, "[SUBSCRIBER] failed to declare exchange")
	q, err := declareQueue(ch, "")
	failOnError(err, "[SUBSCRIBER] failed to declare queue")
	err = bindQueue(ch, q.Name, exchange, routeKey)
	failOnError(err, "[SUBSCRIBER] Fail to bind")
	msgs, _ := consume(ch, q.Name)
	errChan := make(chan error)
	go func() {
		for d := range msgs {
			fmt.Printf("[SUBSCRIBE] %s\n", string(d.Body))
			reply <- string(d.Body)
			d.Ack(false)
		}
		errChan <- fmt.Errorf("rabbit error %s", exchange)
	}()

	fmt.Println("[RABBITMAN] Waiting for messages")
	err = <-errChan
	fmt.Printf("Direct error %+v", err)
	go reconnect(reply, exchange, routeKey, ConnectSubscriberDirect, nil)
	fmt.Println("[RABBITMAN] Exiting?")
}

func ConnectPublisherDirect(listen chan string, exchange, routeKey string) {
	if Conn == nil {
		err := connect()
		failOnError(err, "[PUBLISHER] Failed to connect to RabbitMQ")
	}

	ch, err := Conn.Channel()
	failOnError(err, "[PUBLISHER] Failed to open a channel")
	defer ch.Close()

	err = exchangeDeclare(ch, "direct", exchange)
	failOnError(err, "[PUBLISHER] Failed to declare an exchange")

	for {
		select {
		case msg := <-listen:
			err = publish(ch, exchange, msg, routeKey)
			failOnError(err, "[PUBLISHER] Failed to publish a message")

			if getEnv("RABBIT_ENV") != "" {
				fmt.Printf("[x] Sent %s\n", msg)
			}
		case e := <-E:
			fmt.Printf("Error %+v\n", e)
			go reconnect(listen, exchange, routeKey, ConnectPublisherDirect, nil)
		}
	}
}

func ConnectSubscriberTaskQueue(reply chan string, queueName string) {
	if Conn == nil {
		err := connect()
		failOnError(err, "[PUBLISHER] Failed to connect to RabbitMQ")
	}
	ch, _ := Conn.Channel()
	defer ch.Close()
	q, err := declareQueue(ch, queueName)
	failOnError(err, "[SUBSCRIBER] failed to declare queue")
	err = ch.Qos(5, 0, false) //prefetch count, prefetch size
	failOnError(err, "[SUSBCRIBER] failed to set qos")
	msgs, _ := consume(ch, q.Name)
	errChan := make(chan error)
	go func() {
		for d := range msgs {
			fmt.Printf("[SUBSCRIBE] %s => %s\n", queueName, string(d.Body))
			reply <- string(d.Body)
			d.Ack(false)
		}
		errChan <- fmt.Errorf("rabbit error %s", queueName)
	}()

	fmt.Println("[RABBITMAN] Waiting for messages")
	err = <-errChan
	fmt.Printf("TaskQueue error %+v", err)
	go reconnect(reply, queueName, "", nil, ConnectSubscriberTaskQueue)
	fmt.Println("[RABBITMAN] Reconnect")
}

func ConnectPublisherTaskQueue(listen chan string, queueName string) {
	if Conn == nil {
		err := connect()
		failOnError(err, "[PUBLISHER] Failed to connect to RabbitMQ")
	}

	ch, err := Conn.Channel()
	failOnError(err, "[PUBLISHER] Failed to open a channel")
	defer ch.Close()

	q, err := declareQueue(ch, queueName)
	failOnError(err, "[PUBLISHER] failed to declare Queue")
	for {

		select {
		case msg := <-listen:
			err = publish(ch, "", msg, q.Name)
			failOnError(err, fmt.Sprintf("[PUBLISHER] Failed to publish a message, %s\n", q.Name))

			if getEnv("RABBIT_ENV") != "" {
				fmt.Printf("[x] Sent %s\n", msg)
			}
		case e := <-E:
			fmt.Printf("Error %+v\n", e)
			go reconnect(listen, queueName, "", nil, ConnectPublisherTaskQueue)
		}

	}
}

func reconnect(c chan string, p1, p2 string, f func(chan string, string, string), f2 func(chan string, string)) {

	fmt.Printf("Reconnecting %s %s\n", p1, p2)
	if Conn == nil || Conn.IsClosed() {
		Conn = nil
	}

	if f != nil { // direct queue
		f(c, p1, p2)
	} else {
		f2(c, p1)
	}
}
