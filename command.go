package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
)

type commandCode string
type commandRunner func(*command, *aetherClient, *AetherServer) response

const (
	commandSet   commandCode = "SET"
	commandGet   commandCode = "GET"
	commandRm    commandCode = "RM"
	commandRmall commandCode = "RMALL"
	commandStats commandCode = "STATS"
	commandList  commandCode = "LIST"
	commandPing  commandCode = "PING"
	commandSync  commandCode = "SYNC"
	commandExit  commandCode = "EXIT"
)

var commandCodes = []commandCode{
	commandSet,
	commandGet,
	commandRm,
	commandRmall,
	commandStats,
	commandList,
	commandPing,
	commandSync,
	commandExit,
}

var writeCommands = []commandCode{
	commandSet,
	commandRm,
	commandRmall,
}

var readCommands = []commandCode{
	commandGet,
	commandList,
}

var controlCommands = []commandCode{
	commandPing,
	commandStats,
	commandExit,
}

type command struct {
	code       commandCode
	key        string
	value      []byte
	expiration int
}

func newCommand(code commandCode, key string, value []byte, expiration int) *command {
	return &command{
		code:       code,
		key:        key,
		value:      value,
		expiration: expiration,
	}
}

func (command *command) getCode() commandCode {
	return command.code
}

func (command *command) getKey() string {
	return command.key
}

func (command *command) getValue() []byte {
	return command.value
}

func (command *command) getExpiration() int {
	return command.expiration
}

func (command *command) isWriteCommand() bool {
	for _, item := range writeCommands {
		if item == command.getCode() {
			return true
		}
	}
	return false
}

func (command *command) toPieces() [][]byte {
	pieces := make([][]byte, 0)
	pieces = append(pieces, []byte(command.code))
	switch command.code {
	case commandRm:
		pieces = append(pieces, []byte(command.key))
	case commandSet:
		pieces = append(pieces, []byte(command.key))
		pieces = append(pieces, command.value)
		if command.hasExpirationTime() {
			pieces = append(pieces, bprintf("EXP"))
			pieces = append(pieces, bprintf("%v", command.expiration))
		}

	case commandRmall:
		break
	default:
		log.WithField("code", command.code).Fatal("Conversion to pieces not supported")
	}
	return pieces
}

func (command *command) hasExpirationTime() bool {
	return command.expiration != 0
}

func (command *command) isFinal() bool {
	return command.code == commandExit
}

func (command *command) isReadCommand() bool {
	for _, item := range readCommands {
		if item == command.getCode() {
			return true
		}
	}
	return false
}

func (command *command) isControlCommand() bool {
	for _, item := range controlCommands {
		if item == command.getCode() {
			return true
		}
	}
	return false
}

func (command *command) canRunOnAReplica() bool {
	return command.isControlCommand() || command.isReadCommand()
}

var commandRunners = map[commandCode]commandRunner{
	commandGet: func(command *command, _ *aetherClient, server *AetherServer) response {
		i, found := server.hm.get(command.key)
		if found {
			value := i.getValue()
			return newStringResponse(value)
		} else {
			msg := fmt.Sprintf("Key \"%v\" not found", command.key)
			return newErrorResponse(msg, false)
		}
	},

	commandSet: func(command *command, _ *aetherClient, server *AetherServer) response {
		server.hm.set(command.key, command.value, command.expiration)
		return okResponse
	},

	commandRm: func(command *command, _ *aetherClient, server *AetherServer) response {
		server.hm.rm(command.key)
		return okResponse
	},

	commandRmall: func(_ *command, _ *aetherClient, server *AetherServer) response {
		server.hm.rmall()
		return okResponse
	},

	commandStats: func(_ *command, _ *aetherClient, server *AetherServer) response {
		stats := server.getStats()
		return newJsonResponse(stats)
	},

	commandList: func(_ *command, _ *aetherClient, server *AetherServer) response {
		keys := server.getKeys()
		return newJsonResponse(keys)
	},

	commandPing: func(_ *command, _ *aetherClient, _ *AetherServer) response {
		return pongResponse
	},

	commandSync: func(_ *command, c *aetherClient, s *AetherServer) response {
		items := s.getItems()
		s.addReplica(c)
		c.setReplica(true)

		info("New read replica", log.Fields{
			"client":  c.getId(),
			"address": c.getOriginAddr(),
		})

		return newSyncResponse(items)
	},

	commandExit: func(_ *command, _ *aetherClient, _ *AetherServer) response {
		return byeResponse
	},
}
