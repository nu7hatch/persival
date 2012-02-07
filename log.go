// Copyright (C) 2011 by Krzysztof Kowalik <chris@nu7hat.ch>
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
}

// NewLog allocates new log instance and returns it.
//
// source - A source stream.
//
func NewLog(source io.Writer) *Log {
	return &Log{source}
}

// ReadLog reads operations from the specified source and passes them
// to the specified channel.
//
// source - A source stream.
//
// Returns a channel from which results can be read.
func ReadLog(source io.Reader) (map[int]interface{}, error) {
	ops := make(map[int]interface{})
	for {
		dec := gob.NewDecoder(source)
		var op Change
		if err := dec.Decode(&op); err == io.EOF {
			goto exit
		} else if err != nil {
			continue
			//return ops, err
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

// Append writes given operation to the log file.
//
// op - The operation to be written.
//
// Returns an error if something went wrong.
func (log *Log) Append(op *Change) error {
	enc := gob.NewEncoder(log.source)
	if err := enc.Encode(op); err != nil {
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
