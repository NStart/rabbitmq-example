package main

import (
	"bytes"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"task_queue", //name
		true,         //durable,持久化
		false,        //delete when unused
		false,        //exclusive
		false,        //no-wait
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	//设置每次只能接收一条消息，以确保在处理一条消息时不会同时接收多条
	err = ch.Qos(
		1,     //prefetch count
		0,     //prefetch size
		false, //global
	)
	failOnError(err, "Failed to set Qos")

	msgs, err := ch.Consume(
		q.Name, //name
		"",     //consumer
		false,  // auto-ack  关闭自动确认，需要手动确认，对应：d.Ack(false)
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forver chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			log.Printf("Done")
			d.Ack(false) //确认消息已经处理
		}
	}()

	log.Printf(" [*] Waiting for message. To exit press CTRL+C")
	<-forver
}
