package main

import (
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
    "bufio"
	"html"

	amqp "github.com/rabbitmq/amqp091-go"
	"project/shared"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Println("usage: etl <etl_id> <folder> <mode> <k>")
		os.Exit(1)
	}

	etlID, err := strconv.Atoi(os.Args[1])
	if err != nil || etlID < 0 {
		fmt.Println("invalid etl_id")
		os.Exit(1)
	}

	folder := os.Args[2]
	mode := os.Args[3]
	k, err := strconv.Atoi(os.Args[4])

	if err != nil || k <= 0 {
		fmt.Println("Etl invalid k")
		os.Exit(1)
	}
	if etlID >= k {
		fmt.Println("Etl etl_id must be less than k")
		os.Exit(1)
	}
	if mode != "d" && mode != "h" {
		fmt.Println("Etl mode must be d or h")
		os.Exit(1)
	}
	if err := run(etlID, folder, mode, k); err != nil {
		fmt.Println("ETL error:", err)
		os.Exit(1)
	}
}

func run(etlID int, folder string, mode string, k int) error {
	files, err := getFiles(folder)
	if err != nil {
		return fmt.Errorf("get files: %w", err)
	}

	myFiles := getETLFiles(files, etlID, k)
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")

	if err != nil {
		return fmt.Errorf("dial RabbitMQ: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %w",
			err,
		)
	}
	defer ch.Close()

	for _, filename := range myFiles {
		headlines, err := extractHeadlines(filename)
		if err != nil {
			return fmt.Errorf("extract %s: %w", filename, err)
		}

		for _, title := range headlines {
			workerID := etlID
			if mode == "h" {
				h := fnv.New32a()
				h.Write([]byte(title))
				workerID = int(h.Sum32() % uint32(k))
			}

			body, err := shared.NewMessage(
				"headline_store",
				shared.HeadlinePayload{
					Title: title,
					Filename: filename,
				},
			)
			if err != nil {
				return err
			}

			if err := ch.Publish(
				shared.ExchangeWorkers,
				shared.WorkerQueue(workerID),
				false,
				false,
				amqp.Publishing{
					ContentType: "application/json",
					Body: body,
				},
			); err != nil {
				return fmt.Errorf("publish headline: %w", err)
			}
		}
	}

	fmt.Printf("ETL %d: processed %d files\n",	etlID, len(myFiles))
	return nil
}

func extractHeadlines(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var headlines []string
	var title strings.Builder
	inTitle := false

	for {
		line, err := reader.ReadString('\n')
		
		for len(line) > 0 {
			if !inTitle {
				start := strings.Index(line, "<TITLE>")
				if start == -1 {
					break
				}
				inTitle = true
				line = line[start+len("<TITLE>"):]
			}

			end := strings.Index(line, "</TITLE>")
			if end == -1 {
				title.WriteString(line)
				break
			}

			title.WriteString(line[:end])
			headline := strings.TrimSpace(title.String())
			headline = html.UnescapeString(headline)
			
			if headline != "" {
				headlines = append(headlines, headline)
			}

			title.Reset()
			inTitle = false
			line = line[end+len("</TITLE>"):]
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	return headlines, nil
}

func getFiles(folder string) ([]string, error) {
	entries, err := os.ReadDir(folder)
	if err != nil {
		return nil, err
	}

	var files []string

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(strings.ToLower(entry.Name()), ".sgm") {
			continue
		}

		files = append(files, filepath.Join(folder, entry.Name()))
	}

	sort.Strings(files)
	return files, nil
}

func getETLFiles(files []string, etlID int, k int) []string {
	n := len(files)
	start := etlID * (n / k) + min(etlID, n % k)
	size := n / k

	if etlID < n % k {
		size++
	}

	end := start + size
	return files[start:end]
}