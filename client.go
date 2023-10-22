package main

import (
	log "github.com/sirupsen/logrus"
	"net"
)

type aetherClient struct {
	id        string
	conn      net.Conn
	sink      *sink
	parser    *parser
	log       *log.Entry
	server    *AetherServer
	responses chan response
	replica   bool
}

func newClient(conn net.Conn, s *AetherServer) *aetherClient {
	c := new(aetherClient)
	c.id = s.nextClientId()
	c.conn = conn
	c.log = log.WithFields(log.Fields{"client": c})

	c.server = s

	src := newBufferedSource(conn, 128)
	c.parser = newParser(src)
	c.sink = newSink(conn, 1024)
	c.responses = make(chan response)
	return c
}

func (c *aetherClient) close() {
	err := c.conn.Close()
	if err != nil {
		c.log.WithFields(log.Fields{"error": err}).Error("Error closing socket")
	}
}

func (c *aetherClient) getOriginAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *aetherClient) run() {
	go c.read()
	go c.write()
}

func (c *aetherClient) logWritingError(err error) {
	c.log.WithFields(log.Fields{"error": err}).Error("Error writing to client")
}

func (c *aetherClient) logReadingError(err *parsingError) {

	if err.isEOF() {
		c.log.Warn("Client closed connection (EOF before EXIT)")
		return
	}

	c.log.WithField("error", err).Error("Error reading from client")
}

func (c *aetherClient) getBytesWritten() int {
	return c.sink.getBytesWritten()
}

func (c *aetherClient) getBytesRead() int {
	return c.parser.getBytesRead()
}

func (c *aetherClient) getNumberOfReads() int {
	return c.parser.getNumberOfReads()
}

func (c *aetherClient) getNumberOfWrites() int {
	return c.sink.getNumberOfWrites()
}

func (c *aetherClient) enqueueReply(r response) {
	c.responses <- r
}

func (c *aetherClient) getId() string {
	return c.id
}

func (c *aetherClient) logNewCommand(command *command) {
	c.log.WithField("command", command.getCode()).Trace("New command")
}

func (c *aetherClient) logExit() {
	if c.isAReplica() {
		c.log.Warn("Replica disconnected")
	} else {
		c.log.Info("Client disconnected")
	}
}

func (c *aetherClient) setReplica(replica bool) {
	c.replica = replica
}

func (c *aetherClient) isAReplica() bool {
	return c.replica
}

func (c *aetherClient) logNewClient() {
	c.log.WithField("address", c.getOriginAddr()).Info("New client")
}

func (c *aetherClient) String() string {
	return c.getId()
}

func (c *aetherClient) read() {
	for {
		command, data, err := c.parser.next()
		if err != nil {
			e := newReadingErrorEvent(c, err)
			c.server.newEvent(e)

			if err.isFatal() {
				return // Stop the reading loop
			} else {
				continue
			}
		}

		c.logNewCommand(command)

		e := newCmdEvent(c, command)
		c.server.newEvent(e)

		io := newNetworkReadEvent(data)
		c.server.newEvent(io)

		if command.isFinal() {
			return
		}
	}
}

func (c *aetherClient) write() {
	for {
		response := <-c.responses
		data, err := response.write(c.sink)
		if err != nil {
			e := newWritingErrorEvent(c, err)
			go c.server.newEvent(e)
			return
		}

		io := newNetworkWriteEvent(data)
		go c.server.newEvent(io)

		if response.isFinal() {
			e := newCloseClientEvent(c)
			go c.server.newEvent(e)
			return
		}
	}
}

func (c *aetherClient) getStats() ioStats {
	return ioStats{
		In:     c.getBytesRead(),
		Out:    c.getBytesWritten(),
		Reads:  c.getNumberOfReads(),
		Writes: c.getNumberOfWrites(),
	}
}
