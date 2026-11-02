package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"project/shared"
)

type Worker struct {
	id int
	mu   sync.RWMutex
	data []shared.HeadlinePayload
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: worker <worker_id>")
		return
	}

	id, err := strconv.Atoi(os.Args[1])
	if err != nil || id < 0 {
		fmt.Println("invalid worker_id")
		return
	}

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		fmt.Println("connect:", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("channel:", err)
		return
	}
	defer ch.Close()

	worker := &Worker{id: id}
	if err := worker.sendReady(ch); err != nil {
		fmt.Println("Worker ready:", err)
		return
	}
	messages, err := ch.Consume(
		shared.WorkerQueue(id),
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Println("Worker consume:", err)
		return
	}

	fmt.Printf("Worker %d started\n", id)

	for delivery := range messages {
		worker.handleMessage(delivery, ch)
	}
}

func (w *Worker) sendReady(ch *amqp.Channel) error {
	body, err := shared.NewMessage(
		"worker_ready",
		shared.WorkerReady{
			WorkerID: w.id,
		},
	)
	if err != nil {
		return err
	}

	return ch.Publish(
		shared.ExchangeResponses,
		"manager",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body: body,
		},
	)
}

func (w *Worker) handleMessage(delivery amqp.Delivery, ch *amqp.Channel) {
	var message shared.Message

	if err := json.Unmarshal(delivery.Body, &message); err != nil {
		_ = delivery.Nack(false, false)
		return
	}

	switch message.Type {
	case "headline_store":
		w.handleStore(delivery,	message, ch)
	case "purge":
		w.handlePurge(delivery, ch)
	case "find":
		w.handleFind(delivery,  message, ch)
	default:
		_ = delivery.Nack(false, false)
	}
}

func (w *Worker) handleStore(delivery amqp.Delivery, message shared.Message, ch *amqp.Channel) {
	var headline shared.HeadlinePayload

	if err := json.Unmarshal(message.Payload, &headline); err != nil {
		_ = delivery.Nack(false, false)
		return
	}

	w.mu.Lock()
	w.data = append(w.data, headline)
	w.mu.Unlock()
	_ = delivery.Ack(false)
}

func (w *Worker) handlePurge(delivery amqp.Delivery, ch *amqp.Channel) {
	w.mu.Lock()
	w.data = nil
	w.mu.Unlock()

	body, err := shared.NewMessage(
		"purge_done",
		shared.PurgeDone{WorkerID: w.id},
	)
	if err != nil {
		_ = delivery.Nack(false, false)
		return
	}

	if err := ch.Publish(
		shared.ExchangeResponses,
		"manager",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body: body,
		},
	); err != nil {
		_ = delivery.Nack(false, false)
		return
	}

	_ = delivery.Ack(false)
}

func (w *Worker) handleFind(delivery amqp.Delivery, message shared.Message,	ch *amqp.Channel) {
	var request shared.FindRequest
	var result []shared.HeadlinePayload
	if err := json.Unmarshal(message.Payload, &request); err != nil {
		_ = delivery.Nack(false, false)
		return
	}

	word := strings.ToLower(request.Word)
	w.mu.RLock()

	for _, headline := range w.data {
		if strings.Contains(strings.ToLower(headline.Title), word) {
			result = append(result,	headline)
		}
	}
	w.mu.RUnlock()

	body, err := shared.NewMessage(
		"find_result",
		shared.FindResult{
			WorkerID: w.id,
			Items:    result,
		},
	)
	if err != nil {
		_ = delivery.Nack(false, false)
		return
	}

	if err := ch.Publish(
		shared.ExchangeResponses,
		"manager",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body: body,
		},
	); err != nil {
		_ = delivery.Nack(false, false)//fix зависание менеджера
		return
	}

	_ = delivery.Ack(false)
}