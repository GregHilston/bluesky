package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/websocket"
)

var wsURL = "wss://jetstream2.us-east.bsky.network/subscribe"

// Event represents the main message structure from the firehose
type Event struct {
	Did    string  `json:"did"`
	TimeUS int64   `json:"time_us"`
	Kind   string  `json:"kind,omitempty"`
	Commit *Commit `json:"commit,omitempty"`
}

// Commit represents the commit information in an event
type Commit struct {
	Rev        string          `json:"rev,omitempty"`
	Operation  string          `json:"operation,omitempty"`
	Collection string          `json:"collection,omitempty"`
	RKey       string          `json:"rkey,omitempty"`
	Record     json.RawMessage `json:"record,omitempty"`
	CID        string          `json:"cid,omitempty"`
}

// Post represents the structure of a post record
type Post struct {
	Text      string    `json:"text"`
	CreatedAt time.Time `json:"createdAt"`
}

func main() {
	// Connect to websocket
	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	// Set up channel for graceful shutdown
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// Start reading messages
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}

			var event Event
			if err := json.Unmarshal(message, &event); err != nil {
				log.Printf("Error unmarshaling event: %v", err)
				continue
			}

			fmt.Printf("event.Kind: %s\n", event.Kind)

			// Only process commit events
			if event.Kind == "commit" && event.Commit != nil {
				// Only process create operations
				if event.Commit.Operation == "create" {
					// Print the basic event information
					fmt.Printf("\n--- New Post Event ---\n")
					fmt.Printf("DID: %s\n", event.Did)
					fmt.Printf("Time: %s\n", time.UnixMicro(event.TimeUS).Format(time.RFC3339))
					fmt.Printf("Operation: %s\n", event.Commit.Operation)
					fmt.Printf("Collection: %s\n", event.Commit.Collection)
					fmt.Printf("RKey: %s\n", event.Commit.RKey)
					fmt.Printf("CID: %s\n", event.Commit.CID)
					fmt.Printf("Rev: %s\n", event.Commit.Rev)

					// If it's a post, try to decode the post content
					if event.Commit.Collection == "app.bsky.feed.post" {
						var post Post
						if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
							log.Printf("Error unmarshaling post: %v", err)
							continue
						}
						fmt.Printf("Post Text: %s\n", post.Text)
						fmt.Printf("Post Created At: %s\n", post.CreatedAt)
					}
					fmt.Println("-------------------")
				}
			}
		}
	}()

	// Wait for interrupt signal
	for {
		select {
		case <-done:
			return
		case <-interrupt:
			log.Println("Received interrupt signal, closing connection...")
			err := c.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("write close:", err)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		}
	}
}
