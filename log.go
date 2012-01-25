package persival

import (
	"encoding/gob"
	"os"
	"io"
)

// OperationType represents a kind of the operation.
type OperationType int

// Operation types.
const (
	OpWrite  OperationType = 1
	OpDelete OperationType = 2
)

// Operation ia a representation of a single operation performed on
// the storage.
type Operation struct {
	// The operation type.
	Kind OperationType
	// The affected key.
	Key int
	// The data commited while this operation.
	Data []byte
}

// Log implements interface for storage operations logging. It uses
// gob to encode stored operations.
type Log struct {
	// The logger's input/output source.
	source io.Writer
	// Internal encoder
	enc    *gob.Encoder
}

// NewLog allocates new log instance and returns it.
//
// source - A source stream.
//
func NewLog(source io.Writer) (log *Log) {
	log = &Log{}
	log.switchSource(source)
	return
}

// ReadLog reads operations from the specified source and passes them
// to the specified channel.
//
// source - A source stream.
//
// Returns a channel from which results can be read.
func ReadLog(source io.Reader) <-chan *Operation {
	out := make(chan *Operation)
	go func() {
		dec := gob.NewDecoder(source)
		for {
			var op *Operation
			if err := dec.Decode(&op); err != nil {
				goto exit
			}
			out <- op
		}
	exit:
		close(out)
	}()
	return out
}

// switchSource changes logger's source stream into specified one.
//
// source - A source stream.
//
func (log *Log) switchSource(source io.Writer) {
	log.source, log.enc = source, gob.NewEncoder(source)	
}
	
// Append writes given operation to the log file.
//
// op - The operation to be written.
//
// Returns an error if something went wrong.
func (log *Log) Append(op *Operation) error {
	if err := log.enc.Encode(op); err != nil {
		return err
	}
	if c, ok := log.source.(*os.File); ok {
		c.Sync()
	}
	return nil
}