package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var wsURL = "wss://jetstream2.us-east.bsky.network/subscribe"

// Event represents the main message structure from the firehose
type Event struct {
	Did      string    `json:"did"`
	TimeUS   int64     `json:"time_us"`
	Kind     string    `json:"kind,omitempty"`
	Commit   *Commit   `json:"commit,omitempty"`
	Identity *Identity `json:"identity,omitempty"`
	Account  *Account  `json:"account,omitempty"`
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

// Identity represents identity changes like handle updates
type Identity struct {
	Handle      string `json:"handle,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
	Description string `json:"description,omitempty"`
	Seq         int64  `json:"seq"`
	Time        string `json:"time"`
}

// Account represents account status changes
type Account struct {
	Active bool   `json:"active"`
	Seq    int64  `json:"seq"`
	Time   string `json:"time"`
}

// Post represents the structure of a post record
type Post struct {
	Type      string    `json:"$type,omitempty"`
	Text      string    `json:"text"`
	CreatedAt time.Time `json:"createdAt"`
}

func processEvent(event Event) {
	switch event.Kind {
	case "commit":
		if event.Commit != nil {
			processCommit(event)
		}
	case "identity":
		if event.Identity != nil {
			processIdentity(event)
		}
	case "account":
		if event.Account != nil {
			processAccount(event)
		}
	}
}

func processCommit(event Event) {
	// Only process create operations
	if event.Commit.Operation == "create" || event.Commit.Operation == "update" {
		// If it's a post, try to decode the post content
		if event.Commit.Collection == "app.bsky.feed.post" {
			var post Post
			if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
				log.Printf("Error unmarshaling post: %v", err)
				return
			}
			// Process post data here
			fmt.Printf("Post Text: %s\n", post.Text)
			fmt.Printf("Post %sd At: %s\n", event.Commit.Operation, post.CreatedAt)
		}
	}
}

func processIdentity(event Event) {
	// Process identity updates
	fmt.Printf("\n--- Identity Update ---\n")
	fmt.Printf("DID: %s\n", event.Did)
	fmt.Printf("Handle: %s\n", event.Identity.Handle)
	fmt.Printf("Display Name: %s\n", event.Identity.DisplayName)
	fmt.Printf("Description: %s\n", event.Identity.Description)
	fmt.Printf("Sequence: %d\n", event.Identity.Seq)
	fmt.Printf("Time: %s\n", event.Identity.Time)
}

func processAccount(event Event) {
	// Process account status changes
	fmt.Printf("\n--- Account Update ---\n")
	fmt.Printf("DID: %s\n", event.Did)
	fmt.Printf("Active: %v\n", event.Account.Active)
	fmt.Printf("Sequence: %d\n", event.Account.Seq)
	fmt.Printf("Time: %s\n", event.Account.Time)
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

	// Add counter for messages
	var messageCount uint64

	// Start a goroutine to print the rate every second
	ticker := time.NewTicker(time.Second)
	go func() {
		var lastCount uint64
		for range ticker.C {
			currentCount := atomic.LoadUint64(&messageCount)
			rate := currentCount - lastCount
			fmt.Printf("Messages per second: %d\n", rate)
			lastCount = currentCount
		}
	}()
	defer ticker.Stop()

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

			// Increment the message counter
			atomic.AddUint64(&messageCount, 1)

			var event Event
			if err := json.Unmarshal(message, &event); err != nil {
				log.Printf("Error unmarshaling event: %v", err)
				continue
			}

			processEvent(event)
		}
	}()

	// Wait for interrupt signal
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

