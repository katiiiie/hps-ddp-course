package shared

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeCommands = "commands"
	ExchangeResponses = "responses"
	ExchangeWorkers = "workers"
	ExchangeWorkersFanout = "workers_fanout"
	QueueManager = "manager"
	QueueClient = "client"
	QueueManagerResponses = "manager_responses"
)

type Message struct {
	Type string `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type LoadRequest struct {
	Folder string `json:"folder"`
	Mode string `json:"mode"`
}

type FindRequest struct {
	Word string `json:"word"`
}

type HeadlinePayload struct {
	Title string `json:"title"`
	Filename string `json:"filename"`
}

type WorkerReady struct {
	WorkerID int `json:"worker_id"`
}

type ETLDone struct {
	ETLID int `json:"etl_id"`
	HeadlineCount int `json:"headline_count"`
}

type HeadlineStored struct {
	WorkerID int `json:"worker_id"`
	Title string `json:"title"`
	Filename string `json:"filename"`
}

type PurgeDone struct {
	WorkerID int `json:"worker_id"`
}

type FindResult struct {
	WorkerID int `json:"worker_id"`
	Items []HeadlinePayload `json:"items"`
}

type FindResponse struct {
	Items []HeadlineResult `json:"items"`
}

type HeadlineResult struct {
	Title string `json:"title"`
	Filename string `json:"filename"`
	WorkerID int `json:"worker_id"`
}

func WorkerQueue(id int) string {
	return fmt.Sprintf("worker_%d", id)
}

func NewMessage(typ string, payload any) ([]byte, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	return json.Marshal(Message{Type: typ, Payload: data})
}

func DeclareTopology(ch *amqp.Channel, k int) error {
	for _, exchange := range []string{ExchangeCommands,	ExchangeResponses,
		ExchangeWorkers} {
		if err := ch.ExchangeDeclare(
			exchange,
			"direct",
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("declare exchange %s: %w", exchange, err)
		}
	}

	if err := ch.ExchangeDeclare(
		ExchangeWorkersFanout,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare exchange %s: %w",
			ExchangeWorkersFanout, err)
	}

	if err := declareAndBind(
		ch,
		QueueManager,
		ExchangeCommands,
		"manager",
	); err != nil {
		return err
	}

	if err := declareAndBind(
		ch,
		QueueClient,
		ExchangeResponses,
		"client",
	); err != nil {
		return err
	}

	if err := declareAndBind(
		ch,
		QueueManagerResponses,
		ExchangeResponses,
		"manager",
	); err != nil {
		return err
	}

	for i := 0; i < k; i++ {
		if err := declareAndBind(
			ch,
			WorkerQueue(i),
			ExchangeWorkers,
			WorkerQueue(i),
		); err != nil {
			return err
		}

		if err := declareAndBind(
			ch,
			WorkerQueue(i),
			ExchangeWorkersFanout,
			WorkerQueue(i),
		); err != nil {
			return err
		}
	}

	return nil
}

func declareAndBind(ch *amqp.Channel, queue string,	
	exchange string, routingKey string) error {
	_, err := ch.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("declare queue %s: %w", queue, err)
	}

	if err := ch.QueueBind(
		queue,
		routingKey,
		exchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind queue %s to %s: %w", queue, exchange, err)
	}

	return nil
}