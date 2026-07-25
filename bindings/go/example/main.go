// Demo for the Go binding, in two roles:
//
//	go run . listen     subscribe to demo.chat, answer demo.echo
//	go run . send        publish on demo.chat, then request demo.echo
//
// Start a broker (./build/server/wisp-broker) and a listener first, then send.
// Two processes are required: the broker never routes a message back to its own
// sender. The cgo directives in the wisp package find libwisp.so in the repo
// build tree and set an rpath, so no LD_LIBRARY_PATH is needed for a repo build.
package main

import (
	"fmt"
	"os"

	"github.com/erladion/wisp/bindings/go/wisp"
)

const broker = "tcp://127.0.0.1:5555"

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	wisp.SetLogLevel(wisp.LogWarning)

	switch os.Args[1] {
	case "listen":
		cfg := wisp.DefaultConfig()
		cfg.Address = broker
		cfg.ClientID = "go-listener"
		must(wisp.InitConnection(cfg))
		must(wisp.WaitForConnection(5000))
		if _, err := wisp.RegisterCallback("demo.chat", func(topic string, data []byte) {
			fmt.Printf("[%s] %s\n", topic, string(data))
		}); err != nil {
			must(err)
		}
		if _, err := wisp.RegisterCallback("demo.echo", func(_ string, data []byte) {
			_ = wisp.ReplyToSender(append([]byte("echo: "), data...))
		}); err != nil {
			must(err)
		}
		fmt.Println("listening on demo.chat / demo.echo (Ctrl-C to stop)")
		select {} // block forever

	case "send":
		cfg := wisp.DefaultConfig()
		cfg.Address = broker
		cfg.ClientID = "go-sender"
		must(wisp.InitConnection(cfg))
		must(wisp.WaitForConnection(5000))
		must(wisp.SendMessage("demo.chat", "hello from Go"))
		reply, err := wisp.SendRequest("demo.echo", []byte("ping"), 2000, 256)
		must(err)
		fmt.Printf("request answered: %s\n", string(reply))
		wisp.ShutdownConnection()

	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: demo <listen|send>")
	os.Exit(2)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
