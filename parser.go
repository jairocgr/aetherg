package main

import (
	"fmt"
	"strconv"
	"strings"
)

type parserState string

const (
	parserInit           parserState = "INIT"
	parserReadingCommand parserState = "READING_COMMAND"
	parserReadingArray   parserState = "READING_ARRAY"
)

type parser struct {
	state        parserState
	tokenizer    *tokenizer
	args         []*token
	expectedArgs int
	in           *ioData
}

func (parser *parser) getNumberOfReads() int {
	return parser.tokenizer.getNumberOfReads()
}

func (parser *parser) getBytesRead() int {
	return parser.tokenizer.getBytesRead()
}

const maxTokenSize = 1024 * 4 // 4kb

func newParser(src *source) *parser {
	parser := new(parser)
	parser.tokenizer = newTokenizer(src, maxTokenSize)
	parser.state = parserInit
	return parser
}

func (parser *parser) nextToken() (*token, *ioData, *parsingError) {
	return parser.tokenizer.next()
}

func (parser *parser) next() (*command, *ioData, *parsingError) {
	parser.in = newIoData()
	for {
		token, in, err := parser.tokenizer.next()
		parser.in.merge(in)
		if err != nil {
			return nil, parser.in, err
		}

		switch parser.state {
		case parserInit:
			switch token.getType() {
			case tokenEol, tokenComment:
				// Just eat the token
				parser.state = parserInit
			case tokenBinString:
				return nil, parser.in, parser.binStringBeforeArray()
			case tokenString, tokenIdentifier:
				parser.addArgs(token)
				parser.state = parserReadingCommand
			case tokenArray:
				parser.state = parserReadingArray
				// TODO: if array size is zero this will break the parser
				parser.expectedArgs = token.getSize()
			}
		case parserReadingCommand:
			switch token.getType() {
			case tokenEol:
				parser.state = parserInit
				return parser.parseArgs()
			case tokenString, tokenIdentifier:
				parser.addArgs(token)
				parser.state = parserReadingCommand
			case tokenComment:
				// Just discard the token and continue to read
				parser.state = parserReadingCommand
			case tokenBinString:
				return nil, parser.in, parser.binStringBeforeArray()
			case tokenArray:
				return nil, parser.in, parser.arrayMiddleOfLine()
			}
		case parserReadingArray:
			switch token.getType() {
			case tokenEol:
				// Just discard the EOL and continue reading the array pieces
				parser.state = parserReadingArray
			case tokenString, tokenIdentifier, tokenComment:
				return nil, parser.in, parser.invalidTokenMiddleOfArray(token)
			case tokenBinString:
				parser.addArgs(token)
				parser.state = parserReadingArray
				if len(parser.args) == parser.expectedArgs {
					parser.state = parserInit
					return parser.parseArgs()
				}
			case tokenArray:
				return nil, parser.in, parser.arrayInsideArray()
			}
		default:
			panic(fmt.Errorf("invalid parser state %v", parser.state))
		}
	}
}

func (parser *parser) addArgs(token *token) {
	parser.args = append(parser.args, token)
}

func (parser *parser) cleanArgs() {
	parser.args = make([]*token, 0)
}

func (parser *parser) getArg(index int) string {
	return parser.args[index].value()
}

func (parser *parser) getArgData(index int) []byte {
	return parser.args[index].getData()
}

func (parser *parser) readCommand() (commandCode, *parsingError) {

	arg0 := parser.getArg(0)

	for _, code := range commandCodes {
		if strings.ToUpper(arg0) == string(code) {
			return code, nil
		}
	}

	return "", parser.invalidCommand(arg0)
}

func (parser *parser) parseArgs() (*command, *ioData, *parsingError) {

	defer parser.cleanArgs()

	code, err := parser.readCommand()

	if err != nil {
		return nil, parser.in, err
	}

	nparams := len(parser.args) - 1

	switch code {
	case commandSet:
		if nparams < 2 {
			return nil, parser.in, newParsingError("to few args, expected as least 2")
		}
		if nparams > 4 {
			return nil, parser.in, newParsingError("unknow args, expected max %v given %v", 4, nparams)
		}

		key := parser.getArg(1)
		value := parser.getArgData(2)
		var expiration int

		if nparams > 2 {
			arg3 := parser.getArg(3)
			if strings.ToUpper(arg3) == "EXP" {
				arg4 := parser.getArg(4)
				var err2 error
				expiration, err2 = strconv.Atoi(arg4)
				if err2 != nil {
					return nil, parser.in, newParsingError("invalid expiration lastMeasurement \"%s\"", arg4)
				}
			} else {
				return nil, parser.in, newParsingError("unknown argument \"%s\"", arg3)
			}
		}

		return newCommand(code, key, value, expiration), parser.in, nil

	case commandGet, commandRm:
		if nparams < 1 {
			return nil, parser.in, newParsingError("to few args, expected as least 1")
		}
		if nparams > 1 {
			return nil, parser.in, newParsingError("unknow args, expected max %v given %v", 1, nparams)
		}

		key := parser.getArg(1)

		return newCommand(code, key, []byte{}, 0), parser.in, nil

	case commandRmall, commandStats, commandList, commandPing, commandExit, commandSync:
		if nparams > 0 {
			return nil, parser.in, newParsingError("unknow args, expcted 0 but %v was given", nparams)
		}

		return newCommand(code, "", []byte{}, 0), parser.in, nil
	default:
		// TODO: maybe convert this to a event
		panic(fmt.Errorf("invalid state, command code = %v", code))
	}
}

func (parser *parser) binStringBeforeArray() *parsingError {
	return newParsingError("binary string before and array")
}

func (parser *parser) arrayInsideArray() *parsingError {
	return newParsingError("array inside an array")
}

func (parser *parser) invalidTokenMiddleOfArray(token *token) *parsingError {
	return newParsingError("unexpected token %v while reading an array", token.getType())
}

func (parser *parser) arrayMiddleOfLine() *parsingError {
	return newParsingError("unexpected array in the middle of a command line")
}

func (parser *parser) invalidCommand(code string) *parsingError {
	return newParsingError("invalid command \"%v\"", code)
}
