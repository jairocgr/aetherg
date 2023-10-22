package main

import (
	log "github.com/sirupsen/logrus"
	"net"
	"os"
)

type event interface {
	exec(server *AetherServer) bool
}

type newCommandEvent struct {
	client  *aetherClient
	command *command
}

func (e *newCommandEvent) exec(server *AetherServer) bool {
	client := e.client
	command := e.command
	code := command.getCode()

	if server.isAReplica() && !command.canRunOnAReplica() {
		response := newErrorResponse("this instance is a read replica (read-only)", true)
		client.enqueueReply(response)
		return false
	}

	runner := commandRunners[code]
	response := runner(command, client, server)
	client.enqueueReply(response)
	if command.isWriteCommand() {
		server.broadcast(command)
	}
	return false
}

func newCmdEvent(c *aetherClient, command *command) event {
	return &newCommandEvent{client: c, command: command}
}

type signalEvent struct {
	sig os.Signal
}

func (e *signalEvent) exec(_ *AetherServer) bool {
	log.WithFields(log.Fields{"signal": e.sig}).Warn("Signal recv")
	return true
}

func newSignalEvent(sig os.Signal) event {
	return &signalEvent{sig: sig}
}

type newConnectionEvent struct {
	conn net.Conn
}

func (e *newConnectionEvent) exec(server *AetherServer) bool {
	client := newClient(e.conn, server)
	client.logNewClient()
	server.add(client)
	go client.run()
	return false
}

func newConnEvent(conn net.Conn) event {
	return &newConnectionEvent{conn: conn}
}

type closeClientEvent struct {
	client *aetherClient
}

func (e *closeClientEvent) exec(server *AetherServer) bool {
	server.disconnect(e.client)
	return false
}

func newCloseClientEvent(client *aetherClient) event {
	return &closeClientEvent{client: client}
}

type heartBeat struct {
	no int
}

func (e *heartBeat) exec(server *AetherServer) bool {
	server.evictExpiredKeys()
	server.updateStatistics()
	if e.everyOneHundred() && server.mustSave() {
		items := server.getItems()
		server.washClean()
		server.setSnapshotting(true)
		go server.persist(items)
	}
	return false
}

func (e *heartBeat) everyOneHundred() bool {
	return e.no%100 == 0
}

func newHeartBeat(no int) event {
	return &heartBeat{no: no}
}

type writingErrorEvent struct {
	client *aetherClient
	err    error
}

func (e *writingErrorEvent) exec(server *AetherServer) bool {
	e.client.logWritingError(e.err)
	server.disconnect(e.client)
	return false
}

func newWritingErrorEvent(client *aetherClient, err error) event {
	return &writingErrorEvent{client: client, err: err}
}

type readingErrorEvent struct {
	client *aetherClient
	err    *parsingError
}

func (e *readingErrorEvent) exec(server *AetherServer) bool {

	e.client.logReadingError(e.err)

	if e.err.isTechnical() {
		server.disconnect(e.client)
	} else {
		response := e.err.toErrorResponse()
		e.client.enqueueReply(response)
	}
	return false
}

func newReadingErrorEvent(client *aetherClient, err *parsingError) event {
	return &readingErrorEvent{client: client, err: err}
}

type errorAcceptingConnectionEvent struct {
	err error
}

func (e *errorAcceptingConnectionEvent) exec(_ *AetherServer) bool {
	log.WithField("error", e.err).Error("Error accepting new connection")
	return true
}

func newErrorAcceptingConnectionEvent(err error) event {
	return &errorAcceptingConnectionEvent{err: err}
}

type sourceCommand struct {
	command *command
}

func (e *sourceCommand) exec(server *AetherServer) bool {
	command := e.command

	log.WithFields(log.Fields{"command": command.getCode()}).Trace("New command recv")

	code := command.getCode()
	runner := commandRunners[code]
	_ = runner(command, nil, server)
	return false
}

func newSourceCommand(command *command) event {
	return &sourceCommand{command: command}
}

type errorReadingFromMasterEvent struct {
	err *parsingError
}

func (e *errorReadingFromMasterEvent) exec(_ *AetherServer) bool {
	if e.err.isEOF() {
		log.Error("Master closed the connection (EOF)")
	} else {
		logError("Error reading from master", e.err)
	}
	return true
}

func newErrorReadingFromMasterEvent(err *parsingError) event {
	return &errorReadingFromMasterEvent{err: err}
}

type ioEvent struct {
	device ioDevice
	kind   ioType
	data   ioData
}

func (e *ioEvent) exec(server *AetherServer) bool {
	server.accountFor(e)
	return false
}

func newIoDataEvent(device ioDevice, kind ioType, data ioData) event {
	return &ioEvent{device: device, kind: kind, data: data}
}

func newNetworkWriteEvent(data *ioData) event {
	return newIoDataEvent(network, output, *data)
}

func newNetworkReadEvent(data *ioData) event {
	return newIoDataEvent(network, input, *data)
}

func newDiskWriteEvent(data *ioData) event {
	return newIoDataEvent(disk, output, *data)
}

type connectionLimitReachedEvent struct {
	conn net.Conn
}

func (e *connectionLimitReachedEvent) exec(_ *AetherServer) bool {
	msg := bprintf("-ERR too many connections (limit %v)\r\n", maxClientsAllowed)
	_, _ = e.conn.Write(msg) // No error handling (best-effort basis)
	_ = e.conn.Close()
	return false
}

func newConnectionLimitReached(conn net.Conn) event {
	return &connectionLimitReachedEvent{conn: conn}
}
