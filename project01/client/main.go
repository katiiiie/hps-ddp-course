package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"project/shared"
)

type Client struct {
	k int
	started bool
	loaded  bool
	manager *exec.Cmd
	workers []*exec.Cmd
	conn *amqp.Connection
	ch   *amqp.Channel

	responses <-chan amqp.Delivery
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: client <k>")
		return
	}

	k, err := strconv.Atoi(os.Args[1])
	if err != nil || k <= 0 {
		fmt.Println("error: k must be a positive integer")
		return
	}

	client := &Client{k: k}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		<-signals
		client.shutdown()
		os.Exit(0)
	}()

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		if err := client.handleInput(scanner.Text()); err != nil {
			fmt.Println("Client error:", err)
		}
	}

	client.shutdown()
}

func (cl *Client) handleInput(input string) error {
	parts := strings.Fields(strings.TrimSpace(input))

	if len(parts) == 0 {
		return nil
	}

	switch parts[0] {
	case "start":
		return cl.handleStart(parts)
	case "load":
		return cl.handleLoad(parts)
	case "purge":
		return cl.handlePurge(parts)
	case "find":
		return cl.handleFind(parts)
	default:
		return fmt.Errorf("unknown command %q", parts[0])
	}
}

func (cl *Client) handleStart(parts []string) error {
	if len(parts) != 1 {
		return fmt.Errorf("usage: start")
	}
	if cl.started {
		return fmt.Errorf("system already started")
	}
	if err := cl.startSystem(); err != nil {
		return err
	}
	if _, err := cl.waitForMessage("system_ready"); err != nil {
		cl.shutdown()
		return err
	}

	cl.started = true
	fmt.Println("System ready")

	return nil
}

func (cl *Client) handleLoad(parts []string) error {
	if !cl.started {
		return fmt.Errorf("system is not started")
	}
	
	if len(parts) != 3 {
		return fmt.Errorf("usage: load folder mode")
	}

	mode := parts[2]

	if mode != "d" && mode != "h" {
		return fmt.Errorf("mode must be d or h")
	}

	body, err := shared.NewMessage("load",
		shared.LoadRequest{
			Folder: parts[1],
			Mode:   mode,
		},
	)
	if err != nil {
		return err
	}

	if err := cl.sendToManager(body); err != nil {
		return err
	}
	if _, err := cl.waitForMessage("load_done", "load_failed"); err != nil {
		return err
	}

	cl.loaded = true
	fmt.Println("Load completed")

	return nil
}

func (cl *Client) handlePurge(parts []string) error {
	if !cl.started {
		return fmt.Errorf("system is not started")
	}
	if !cl.loaded {
		return fmt.Errorf("text corpus is not loaded")
	}
	if len(parts) != 1 {
		return fmt.Errorf("usage: purge")
	}
	body, err := shared.NewMessage("purge", struct{}{})
	if err != nil {
		return err
	}
	if err := cl.sendToManager(body); err != nil {
		return err
	}
	if _, err := cl.waitForMessage("purge_done", "purge_failed"); err != nil {
		return err
	}

	cl.loaded = false
	fmt.Println("Purge completed")

	return nil
}

func (cl *Client) handleFind(parts []string) error {
	if !cl.started {
		return fmt.Errorf("system is not started")
	}
	if !cl.loaded {
		return fmt.Errorf("text corpus is not loaded")
	}
	if len(parts) != 2 {
		return fmt.Errorf("usage: find word")
	}

	body, err := shared.NewMessage("find",
		shared.FindRequest{
			Word: parts[1],
		},
	)
	if err != nil {
		return err
	}
	if err := cl.sendToManager(body); err != nil {
		return err
	}
	message, err := cl.waitForMessage("find_done","find_failed")
	if err != nil {
		return err
	}

	var response shared.FindResponse
	if err := json.Unmarshal(message.Payload, &response); err != nil {
		return fmt.Errorf("invalid find response: %w", err)
	}
	for _, item := range response.Items {
		fmt.Printf("%s | %s | worker %d\n", item.Title, item.Filename, item.WorkerID)
	}

	return nil
}

func (cl *Client) startSystem() error {
	if cl.started {
        return fmt.Errorf("system already started")
    }
	var err error
	cl.conn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		cl.shutdown()
		return fmt.Errorf("connect to RabbitMQ: %w", err)
	}

	cl.ch, err = cl.conn.Channel()
	if err != nil {
		cl.shutdown()
		return fmt.Errorf("open RabbitMQ channel: %w", err)
	}

	if err = shared.DeclareTopology(cl.ch, cl.k); err != nil {
		cl.shutdown()
		return fmt.Errorf("declare topology: %w", err)
	}

	cl.responses, err = cl.ch.Consume(
		shared.QueueClient,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	
	if err != nil {
		cl.shutdown()
		return fmt.Errorf("consume client queue: %w", err)
	}

	manager := exec.Command("./manager/manager", strconv.Itoa(cl.k))
	manager.Stdout = os.Stdout
	manager.Stderr = os.Stderr

	if err = manager.Start(); err != nil {
		cl.shutdown()
		return fmt.Errorf("start manager: %w", err)
	}

	cl.workers = make([]*exec.Cmd, 0, cl.k)

	for i := 0; i < cl.k; i++ {
		worker := exec.Command("./worker/worker", strconv.Itoa(i))
		worker.Stdout = os.Stdout
		worker.Stderr = os.Stderr

		if err := worker.Start(); err != nil {
			cl.shutdown()
			return fmt.Errorf("start worker %d: %w", i, err)
		}
		cl.workers = append(cl.workers, worker)
	}

	cl.manager = manager
	return nil
}

func (cl *Client) sendToManager(body []byte) error {
	return cl.ch.Publish(shared.ExchangeCommands,
		"manager",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body: body,
		},
	)
}

func (c *Client) waitForMessage(types ...string) (shared.Message, error) {
	for message := range c.responses {
		var response shared.Message

		if err := json.Unmarshal(message.Body, &response); err != nil {
			continue
		}

		for _, typ := range types {
			if response.Type != typ {
				continue
			}

			if typ == "load_failed" || typ == "purge_failed" || typ == "find_failed" {
				return response, fmt.Errorf("%s", response.Payload)
			}

			return response, nil
		}
	}

	return shared.Message{}, fmt.Errorf(
		"manager connection closed",
	)
}

func (cl *Client) shutdown() {
	for _, worker := range cl.workers {
		if worker != nil && worker.Process != nil {
			_ = worker.Process.Kill()
		}
	}

	if cl.manager != nil && cl.manager.Process != nil {
		_ = cl.manager.Process.Kill()
	}
	if cl.ch != nil {
		_ = cl.ch.Close()
	}

	if cl.conn != nil {
		_ = cl.conn.Close()
	}

	cl.manager = nil
	cl.workers = nil
	cl.ch = nil
	cl.conn = nil
	cl.responses = nil
	cl.started = false
	cl.loaded = false

	fmt.Println("System stopped\n")
}