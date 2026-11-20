package main

import (
	"context"
	"log"
)

type Hub struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	broadcast  chan []byte
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan []byte),
	}
}

func (h *Hub) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("hub shutting down (%d clients)", len(h.clients))
			for client := range h.clients {
				close(client.send)
				client.conn.Close()
			}
			return

		case client := <-h.register:
			h.clients[client] = true
			log.Printf("client connected: %s (total: %d)",
				client.conn.RemoteAddr(), len(h.clients))

		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				log.Printf("client disconnected: %s (total: %d)",
					client.conn.RemoteAddr(), len(h.clients))
			}

		case message := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
		}
	}
}