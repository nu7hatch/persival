package persival

import (
	"testing"
	"bytes"
)

func TestNewLog(t *testing.T) {
	if log := NewLog(bytes.NewBuffer([]byte{})); log == nil {
		t.Errorf("Expected to create a log")
	}
}

func TestAppendAndReadLog(t *testing.T) {
	source := bytes.NewBuffer([]byte{})
	log := NewLog(source)
	log.Append(&Operation{OpWrite, 1, []byte("hello")})
	var ops []*Operation
	for op := range ReadLog(source) {
		ops = append(ops[:], op)
	}
	if len(ops) != 1 {
		t.Errorf("Expected to read one operation from log")
	}
	if ops[0].Kind != OpWrite || ops[0].Key != 1 || string(ops[0].Data) != "hello" {
		t.Errorf("Expected to have proper log entry")
	}
}