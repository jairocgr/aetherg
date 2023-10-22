package main

import (
	"io"
	"testing"
	"testing/fstest"
)

type commandCodeKeyValueExpirationTuple struct {
	code       commandCode
	key        string
	data       []byte
	expiration int
}

func TestParseACommand(t *testing.T) {
	fileContent := "GET F398BC5672A51D8D \n"
	expectedCommands := []commandCodeKeyValueExpirationTuple{
		{commandGet, "F398BC5672A51D8D", []byte{}, 0},
	}
	runParsingTest(fileContent, expectedCommands, t)
}

func TestParseOfMultiplesCommands(t *testing.T) {
	fileContent := "GET F398BC5672A51D8D\n" +
		"set key0 F398BC5672A51D8D exp 360\r\n" +
		"	rm key_0\n" +
		"PING\r\n"
	expectedCommands := []commandCodeKeyValueExpirationTuple{
		{commandGet, "F398BC5672A51D8D", []byte{}, 0},
		{commandSet, "key0", []byte("F398BC5672A51D8D"), 360},
		{commandRm, "key_0", []byte{}, 0},
		{commandPing, "", []byte{}, 0},
	}
	runParsingTest(fileContent, expectedCommands, t)
}

func TestParseOfWireProtocol(t *testing.T) {
	fileContent := " *2\r\n$3\nGET\n$13\nA4B5F8E68751D\n" +
		"*5\n$3\r\nset\r\n$4\r\nkey0\r\n$16\nF398BC5672A51D8D\r\n$3\r\nexp\r\n$3\r\n360" +
		"\n*1\r\n$4\r\nPING\r\n"
	expectedCommands := []commandCodeKeyValueExpirationTuple{
		{commandGet, "A4B5F8E68751D", []byte{}, 0},
		{commandSet, "key0", []byte("F398BC5672A51D8D"), 360},
		{commandPing, "", []byte{}, 0},
	}
	runParsingTest(fileContent, expectedCommands, t)
}

func TestParseMessyCommandLineInteraction(t *testing.T) {
	fileContent := "\t\t\r\n 	get	 '.;c`-hB/!A`Vm\"7'\n\n\r\n\n" +
		"GET \"F398BC567'2'A51D8D\"\n" +
		"set Y(h7=UjIM9F8Q> F398BC5672A51D8D \"exp\" 		'360'\n" +
		"	rm 	key_0     \t\n" +
		"'ping'\r\n" +
		"RMALL \n"
	expectedCommands := []commandCodeKeyValueExpirationTuple{
		{commandGet, ".;c`-hB/!A`Vm\\\"7", []byte{}, 0},
		{commandGet, "F398BC567'2'A51D8D", []byte{}, 0},
		{commandSet, "Y(h7=UjIM9F8Q>", []byte("F398BC5672A51D8D"), 360},
		{commandRm, "key_0", []byte{}, 0},
		{commandPing, "", []byte{}, 0},
		{commandRmall, "", []byte{}, 0},
	}
	runParsingTest(fileContent, expectedCommands, t)
}

func runParsingTest(fileContent string, expectedCommands []commandCodeKeyValueExpirationTuple, t *testing.T) {
	fs := fstest.MapFS{
		"test/tokenizer/test.txt": {
			Data: []byte(fileContent),
		},
	}

	inputFile, err := fs.Open("test/tokenizer/test.txt")
	if err != nil {
		panic(err)
	}

	defer inputFile.Close()

	const buffSize = 8
	src := newBufferedSource(inputFile, buffSize)
	parser := newParser(src)

	compareBytes := func(a []byte, b []byte) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	for i, expected := range expectedCommands {
		command, _, err := parser.next()
		if err != nil {
			panic(err)
		}
		if command.getCode() != expected.code {
			t.Errorf("Command #%v code %v not typed as %v", i+1, command.getCode(), expected.code)
		}
		if command.getKey() != expected.key {
			t.Errorf("Command #%v key %v not as expected %v", i+1, command.getKey(), expected.key)
		}
		if !compareBytes(command.getValue(), expected.data) {
			t.Errorf("Command #%v key %v not as expected %v", i+1, command.getValue(), expected.data)
		}
		if command.getExpiration() != expected.expiration {
			t.Errorf("Command #%v expiration %v not as expected %v", i+1, command.getExpiration(), expected.expiration)
		}
	}

	_, _, parsingErr := parser.next()
	if parsingErr.getCause() != io.EOF {
		t.Errorf("Expected EOF")
	}
}
