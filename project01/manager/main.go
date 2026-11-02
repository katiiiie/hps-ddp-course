package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"project/shared"
)

type Manager struct {
	k  int
	ch *amqp.Channel
	workerEvents chan shared.Message
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: manager <k>")
		return
	}

	k, err := strconv.Atoi(os.Args[1])
	if err != nil || k <= 0 {
		fmt.Println("invalid k")
		return
	}

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		fmt.Println("Manager connect:", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Manager channel:", err)
		return
	}
	defer ch.Close()

	manager := &Manager{k: k, ch: ch,
		workerEvents: make(chan shared.Message, 1000),
	}

	if err := manager.waitForWorkers(); err != nil {
		fmt.Println("Manager workers:", err)
		return
	}

	if err := manager.sendClient("system_ready", struct{}{}); err != nil {
		fmt.Println("system ready:", err)
		return
	}

	commands, err := ch.Consume(
		shared.QueueManager,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Println("Manager consume commands:", err)
		return
	}

	for delivery := range commands {
		var message shared.Message

		if err := json.Unmarshal(delivery.Body, &message); err != nil {
			fmt.Println("Manger invalid command:", err)
			continue
		}

		switch message.Type {
		case "load":
			if err := manager.handleLoad(message); err != nil {
				fmt.Println("Manger load:", err)
				_ = manager.sendClient("load_failed", map[string]string{
						"error": err.Error(),
					},
				)
			} else {
				_ = manager.sendClient("load_done",	struct{}{})
			}
		case "purge":
			if err := manager.handlePurge(); err != nil {
				fmt.Println("Manger purge:", err)
				_ = manager.sendClient("purge_failed", map[string]string{
						"error": err.Error(),
					},
				)
			} else {
				_ = manager.sendClient("purge_done", struct{}{})
			}
		case "find":
			if err := manager.handleFind(message); err != nil {
				fmt.Println("Manger find:", err)
				_ = manager.sendClient("find_failed", map[string]string{
						"error": err.Error(),
					},
				)
			}
		}
	}
}

func (m *Manager) waitForWorkers() error {
	responses, err := m.ch.Consume(
		shared.QueueManagerResponses,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	ready := make(map[int]bool)

	for len(ready) < m.k {
		resp := <-responses
		var message shared.Message
		var payload shared.WorkerReady

		if err := json.Unmarshal(resp.Body, &message); err != nil ||
				message.Type != "worker_ready"{
			continue
		}
		if err := json.Unmarshal(message.Payload, &payload); err != nil {
			continue
		}

		ready[payload.WorkerID] = true
	}

	go m.readWorkerEvents(responses)

	return nil
}

func (m *Manager) readWorkerEvents(events <-chan amqp.Delivery) {
	for delivery := range events {
		var message shared.Message

		if err := json.Unmarshal(delivery.Body, &message); err != nil {
			continue
		}
		m.workerEvents <- message
	}
}

func (m *Manager) handleLoad(message shared.Message) error {
	var request shared.LoadRequest

	if err := json.Unmarshal(message.Payload, &request); err != nil {
		return fmt.Errorf("invalid load request: %w", err)
	}
	cmds := make([]*exec.Cmd, 0, m.k)

	for i := 0; i < m.k; i++ {
		cmd := exec.Command("./etl/etl", strconv.Itoa(i), request.Folder,
			request.Mode, strconv.Itoa(m.k))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			for _, running := range cmds {
				_ = running.Process.Kill()
			}

			return fmt.Errorf("start ETL %d: %w", i, err)
		}
		cmds = append(cmds, cmd)
	}

	results := make(chan error, m.k)
	var wg sync.WaitGroup

	for i, cmd := range cmds {
		wg.Add(1)

		go func(id int, cmd *exec.Cmd) {
			defer wg.Done()
			results <- cmd.Wait()
		}(i, cmd)
	}

	wg.Wait()
	close(results)

	for result := range results {
		if result != nil {
			return fmt.Errorf("ETL failed: %w",	result)
		}
	}
	return nil
}

func (m *Manager) handlePurge() error {
	body, err := shared.NewMessage("purge", struct{}{})
    if err != nil {
        return err
    }

    if err := m.ch.Publish(
        shared.ExchangeWorkersFanout,
        "",
        false,
        false,
        amqp.Publishing{
            ContentType: "application/json",
            Body: body,
        },
    ); err != nil {
        return fmt.Errorf("send purge: %w", err)
    }

	purged := make(map[int]bool)

	for len(purged) < m.k {
		message := <-m.workerEvents
		var payload shared.PurgeDone

		if message.Type != "purge_done" {
			continue
		}
		if err := json.Unmarshal(message.Payload, &payload); err != nil {
			continue
		}
		purged[payload.WorkerID] = true
	}

	return nil
}

func (m *Manager) handleFind(message shared.Message) error {
	var request shared.FindRequest

	if err := json.Unmarshal(message.Payload, &request); err != nil {
		return fmt.Errorf("invalid find request: %w", err)
	}
	body, err := shared.NewMessage("find", request)
	if err != nil {
		return err
	}
	if err := m.ch.Publish(
        shared.ExchangeWorkersFanout,
        "",
        false,
        false,
        amqp.Publishing{
            ContentType: "application/json",
            Body: body,
        },
    ); err != nil {
        return fmt.Errorf("send find: %w", err)
    }

	var results []shared.HeadlineResult
	received := make(map[int]bool)

	for len(received) < m.k {
		message := <-m.workerEvents
		var result shared.FindResult

		if message.Type != "find_result" {
			continue
		}
		if err := json.Unmarshal(message.Payload, &result); err != nil {
			continue
		}
		if received[result.WorkerID] {
			continue
		}

		received[result.WorkerID] = true

		for _, item := range result.Items {
			results = append(
				results,
				shared.HeadlineResult{
					Title: item.Title,
					Filename: item.Filename,
					WorkerID: result.WorkerID,
				},
			)
		}
	}

	return m.sendClient("find_done", shared.FindResponse{Items: results})
}

func (m *Manager) sendClient(typ string, payload any) error {
	body, err := shared.NewMessage(typ, payload)
	if err != nil {
		return err
	}

	return m.ch.Publish(
		shared.ExchangeResponses,
		"client",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body: body,
		},
	)
}