// Exercises the non-blocking request/reply pair against a live broker:
//
//	go run ./example/replyprobe
//
// Start a broker (tcp://127.0.0.1:25999, or $WISP_BROKER) and something
// answering svc/echo first - a broker never routes a message back to its own sender, so the
// responder has to be a separate process.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/erladion/wisp/bindings/go/wisp"
)

// Overridable so bindings/smoke.sh can point it at its own broker.
func brokerAddress() string {
	if addr := os.Getenv("WISP_BROKER"); addr != "" {
		return addr
	}
	return "tcp://127.0.0.1:25999"
}

func main() {
	wisp.SetLogLevel(wisp.LogError)
	if err := wisp.InitConnection(wisp.Config{Address: brokerAddress(), ClientID: "go-asker"}); err != nil {
		fmt.Println("init:", err)
		os.Exit(1)
	}
	defer wisp.ShutdownConnection()

	if err := wisp.WaitForConnection(3000); err != nil {
		fmt.Println("no broker:", err)
		os.Exit(1)
	}

	replyTopic, err := wisp.MakeReplyTopic("svc/echo")
	if err != nil {
		fmt.Println("MakeReplyTopic:", err)
		os.Exit(1)
	}
	fmt.Println("MakeReplyTopic:", replyTopic)

	again, _ := wisp.MakeReplyTopic("svc/echo")
	if again == replyTopic {
		fmt.Println("FAIL: reply topics must be unique")
	}

	answers := make(chan string, 1)
	sub, err := wisp.RegisterCallback(replyTopic, func(topic string, data []byte) {
		select {
		case answers <- string(data):
		default:
		}
	})
	if err != nil {
		fmt.Println("RegisterCallback:", err)
		os.Exit(1)
	}
	defer wisp.UnregisterCallback(sub)

	// A reserved reply topic must be refused rather than silently lost.
	if err := wisp.SendDataWithReply("svc/echo", []byte("x"), "__nope__"); err == nil {
		fmt.Println("FAIL: reserved reply topic was accepted")
	} else {
		fmt.Println("reserved reply topic refused:", err)
	}

	if err := wisp.SendDataWithReply("svc/echo", []byte("hello from go"), replyTopic); err != nil {
		fmt.Println("SendDataWithReply:", err)
		os.Exit(1)
	}

	select {
	case answer := <-answers:
		fmt.Println("answer:", answer)
	case <-time.After(4 * time.Second):
		fmt.Println("answer: (none)")
	}
}
