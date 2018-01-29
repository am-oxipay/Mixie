package main

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func main() {
	const host string = "172.28.128.3"
	const port int = 5672
	connectionString := fmt.Sprintf("amqp://gx_provider1:foobar123@%s:%d/", host, port)
	conn, err := amqp.Dial(connectionString)
	defer conn.Close()

	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	exchangeName := "foo"
	exchangeNameDead := fmt.Sprint(exchangeName, ".dead")

	args := make(map[string]string)
	args["x-dead-letter-exchange"] = "some.exchange.name"

	declareExchange(ch, exchangeName)
	declareExchange(ch, exchangeNameDead, args)

	failOnError(err, "Failed to declare a queue")

	body := "hello"

	for i := 1; i <= 10; i++ {
		go publishMessage(ch, exchangeName, body)
	}

}

func declareExchange(ch *amqp.Channel, myname string, args ...map[string]string) {

	err := ch.ExchangeDeclare(
		myname,   // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		args,     // arguments
	)
	failOnError(err, "Failed to declare an exchange")
}

func publishMessage(ch *amqp.Channel, exchangeName string, body string) {
	err := ch.Publish(
		exchangeName, // exchange
		"",           // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
