package persival

import (
	"encoding/gob"
	"io"
)

// ChangeType represents a kind of the operation.
type ChangeType int

// Change types.
const (
	CW ChangeType = 1
	CD ChangeType = 2
)

// Change ia a representation of a single operation performed on
// the storage.
type Change struct {
	// The operation type.
	Kind ChangeType
	// The affected key.
	Key int
	// The data commited while this operation.
	Data interface{}
}

// Log implements interface for storage operations logging. It uses
// gob to encode stored operations.
type Log struct {
	// The logger's input/output source.
	source io.Writer
	// Internal encoder
	enc *gob.Encoder
}

// NewLog allocates new log instance and returns it.
//
// source - A source stream.
//
func NewLog(source io.Writer) (log *Log) {
	log = &Log{}
	log.setSource(source)
	return
}

// ReadLog reads operations from the specified source and passes them
// to the specified channel.
//
// source - A source stream.
//
// Returns a channel from which results can be read.
func ReadLog(source io.Reader) (map[int]interface{}, error) {
	ops := make(map[int]interface{})
	dec := gob.NewDecoder(source)
	for {
		var op *Change
		if err := dec.Decode(&op); err != nil {
			if err == io.EOF {
				goto exit
			}
			return nil, err
		}
		switch op.Kind {
		case CW:
			ops[op.Key] = op.Data
		case CD:
			delete(ops, op.Key)
		}
	}
exit:
	return ops, nil
}

// setSource changes logger's source stream into specified one.
//
// source - A source stream.
//
func (log *Log) setSource(source io.Writer) {
	log.source, log.enc = source, gob.NewEncoder(source)	
}
	
// Append writes given operation to the log file.
//
// op - The operation to be written.
//
// Returns an error if something went wrong.
func (log *Log) Append(op *Change) error {
	if err := log.enc.Encode(op); err != nil {
		return err
	}
	return nil
}

// Close closes the log.
func (log *Log) Close() {
	if c, ok := log.source.(io.Closer); ok {
		c.Close()
	}
}

// Initializer
func init() {
	gob.Register(&Change{})
}