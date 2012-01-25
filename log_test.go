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
	log.Append(&Change{CW, 1, "hello"})
	log.Append(&Change{CW, 2, "world"})
	log.Append(&Change{CD, 1, nil})
	log.Append(&Change{CW, 2, "hello"})
	data, err := ReadLog(source)
	if err != nil {
		t.Errorf("Expected to read log without problems")
	}
	if len(data) != 1 {
		t.Errorf("Expected to read one record from log")
	}
	if data[2].(string) != "hello" {
		t.Errorf("Expected to have proper log entry, got %v", data[2].(string))
	}
}