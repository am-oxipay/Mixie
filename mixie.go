package main

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func main() {
	const host string = "172.28.128.7"
	const port int = 5672
	connectionString := fmt.Sprintf("amqp://gx_admin:foobar123@%s:%d/", host, port)
	conn, err := amqp.Dial(connectionString)
	defer conn.Close()

	failOnError(err, "Failed to connect to RabbitMQ", "")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel", "")
	defer ch.Close()

	exchangeName := "galaxy.incoming"
	exchangeNameDead := fmt.Sprint(exchangeName, ".dead")
	queueName := "incoming queue"

	args := make(amqp.Table)
	args["x-dead-letter-exchange"] = exchangeNameDead

	declareExchange(ch, exchangeName, args)
	declareExchange(ch, exchangeNameDead, nil)
	declareQueue(ch, queueName, nil)

	body := "hello "

	for i := 1; i <= 1000000; i++ {
		declareExchange(ch, exchangeName, args)
		go publishMessage(ch, exchangeName, queueName, fmt.Sprintf("%s %d", body, i))
	}

	// consumeMessages(ch, queueName)
}

func declareExchange(ch *amqp.Channel, myname string, args amqp.Table) {

	err := ch.ExchangeDeclare(
		myname,   // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		args,     // arguments
	)
	successMsg := "queue " + myname + " has been created"
	failOnError(err, "Failed to declare a queue", successMsg)
}

func publishMessage(ch *amqp.Channel, exchangeName string, key string, body string) {
	err := ch.Publish(
		exchangeName, // exchange
		key,          // routing key
		true,         // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message to "+exchangeName, "Message Published Successfully")
}

func consumeMessages(ch *amqp.Channel, queueName string) {
	msgs, err := ch.Consume(
		queueName, // queue
		"myname",  // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	failOnError(err, "Failed to consume from "+queueName, "")
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)

		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func declareQueue(ch *amqp.Channel, queueName string, args amqp.Table) amqp.Queue {
	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		args,      // arguments
	)
	failOnError(err, "Failed to declare a queue", "Declared Queue: "+queueName)
	return q
}

func failOnError(err error, failure string, success string) {
	if err != nil {
		log.Fatalf("%s: %s", failure, err)
		panic(fmt.Sprintf("%s: %s", failure, err))
	} else if success != "" {
		log.Println(success)
	}
}
