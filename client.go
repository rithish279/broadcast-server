package main

import (
	"bufio"
	"net"
)

type Client struct {
	conn net.Conn
	hub  *Hub
	send chan []byte
}

func newClient(conn net.Conn, hub *Hub) *Client {
	return &Client{
		conn: conn,
		hub:  hub,
		send: make(chan []byte, 256),
	}
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	reader := bufio.NewReader(c.conn)
	for {
		message, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		c.hub.broadcast <- message
	}
}

func (c *Client) writePump() {
	defer c.conn.Close()

	for message := range c.send {
		if _, err := c.conn.Write(message); err != nil {
			return
		}
	}
}