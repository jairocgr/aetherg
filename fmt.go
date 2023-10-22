package main

import "fmt"

func bprintf(str string, a ...any) []byte {
	formatted := fmt.Sprintf(str, a...)
	return []byte(formatted)
}
