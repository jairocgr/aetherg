package main

import (
	log "github.com/sirupsen/logrus"
	"net"
	"os"
)

type master struct {
	address string
	conn    net.Conn
	parser  *parser
	sink    *sink
}

func (m *master) follow(server *AetherServer) {
	for {
		command, _, err := m.parser.next()
		if err != nil {
			e := newErrorReadingFromMasterEvent(err)
			server.newEvent(e)
			return
		}

		e := newSourceCommand(command)
		server.newEvent(e)
	}
}

func (m *master) sync(server *AetherServer) {

	info("SYNC with master node", log.Fields{"address": m.address})

	_, err := m.sink.flushAsRawBytes("SYNC\r\n")
	if err != nil {
		fatalError("Error writing SYNC to master", err)
	}

	token, _, parsingErr := m.parser.nextToken()
	if parsingErr != nil {
		fatalError("Error reading from master", err)
	}

	if token.getType() != tokenArray {
		fatal("Invalid first token from master (expecting array)", log.Fields{"type": token.getType()})
	}

	numOfKeys := token.getSize()

	info("Downloading keys from main node", log.Fields{"keys": numOfKeys})

	for i := 0; i < numOfKeys; i++ {
		command, _, parsingErr := m.parser.next()
		if parsingErr != nil {
			log.WithFields(log.Fields{
				"error": parsingErr,
				"cause": parsingErr.getCause(),
			}).Error("Error reading from master")
			os.Exit(EXIT_FAILURE)
		}

		switch command.getCode() {
		case commandSet:
			server.hm.set(command.getKey(), command.getValue(), command.getExpiration())
		default:
			fatal("Invalid command during initial SYNC", log.Fields{"code": command.getCode()})
		}
	}

	info("Keys downloaded from main server", log.Fields{"keys": numOfKeys})
}

func (m *master) close() {
	m.sendExit()
	m.closeConnection()
}

func (m *master) open() {
	conn := m.dial()
	src := newBufferedSource(conn, 128)
	m.parser = newParser(src)
	m.sink = newSink(conn, 1024)
	m.conn = conn
}

func (m *master) dial() net.Conn {
	conn, err := net.Dial("tcp", m.address)
	if err != nil {
		fatal("Can not open connection to master server", log.Fields{
			"error":  err,
			"master": m.address,
		})
	}
	return conn
}

func (m *master) sendExit() {
	_, err := m.sink.flushAsRawBytes("EXIT\r\n")
	if err != nil {
		logError("Error sending EXIT to master", err)
	}
}

func (m *master) closeConnection() {
	err := m.conn.Close()
	if err != nil {
		logError("Error closing connection with master", err)
	}
}

func newMasterNode(address string) *master {
	return &master{address: address}
}
