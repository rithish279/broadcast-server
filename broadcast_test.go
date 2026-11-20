package main

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

func startTestServer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	hub := newHub()
	go hub.run(ctx)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatalf("failed to listen: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			client := newClient(conn, hub)
			hub.register <- client
			go client.readPump()
			go client.writePump()
		}
	}()

	cleanup = func() {
		listener.Close()
		cancel()
	}
	return listener.Addr().String(), cleanup
}

func TestBroadcast(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	connA, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("client A dial: %v", err)
	}
	defer connA.Close()

	connB, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("client B dial: %v", err)
	}
	defer connB.Close()

	// Give the hub a moment to register both clients
	time.Sleep(100 * time.Millisecond)

	// Client A sends a message
	if _, err := connA.Write([]byte("hello from A\n")); err != nil {
		t.Fatalf("client A write: %v", err)
	}

	// Client B should receive it
	connB.SetReadDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(connB)
	got, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("client B read: %v", err)
	}

	want := "hello from A\n"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}