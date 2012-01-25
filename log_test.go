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