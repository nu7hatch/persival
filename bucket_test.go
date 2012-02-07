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

import "testing"

type dummy struct {
	A string
}

func init() {
	Register(&dummy{})
}

func TestNewBucket(t *testing.T) {
	bkt, err := NewBucket("/tmp/foo.bkt", 0)
	if err != nil {
		t.Errorf("Expected to create new bucket, error: %v", err)
	}
	if err = bkt.Destroy(); err != nil {
		t.Errorf("Expected to destroy the bucket afterword, error: %v", err)
	}
}

func TestBucketSet(t *testing.T) {
	bkt, _ := NewBucket("/tmp/foo.bkt", 0)
	defer bkt.Destroy()
	if key, err := bkt.Set(&dummy{"hello"}); err != nil || key != 1 {
		t.Errorf("Expected to get key of the set value, error: %v", err)
	}
}

func TestBucketGet(t *testing.T) {
	bkt, _ := NewBucket("/tmp/foo.bkt", 0)
	defer bkt.Destroy()
	key, _ := bkt.Set(&dummy{"hello"})
	if val, err := bkt.Get(key); err != nil || val.(*dummy).A != "hello" {
		t.Errorf("Expected to get proper value from specfied key, error; %v", err)
	}
	if _, err := bkt.Get(123); err == nil {
		t.Errorf("Expected to get nothing from non-existent key")
	}
}

func TestBucketUpdate(t *testing.T) {
	bkt, _ := NewBucket("/tmp/foo.bkt", 0)
	defer bkt.Destroy()
	key, _ := bkt.Set(&dummy{"hello"})
	if err := bkt.Update(key, &dummy{"world"}); err != nil {
		t.Errorf("Expected to update value of the specified key, error: %v", err)
	}
	if val, err := bkt.Get(key); err != nil || val.(*dummy).A != "world" {
		t.Errorf("Expected to have proper value after update, error: %v", err)
	}
	if err := bkt.Update(123, &dummy{"hello"}); err == nil {
		t.Errorf("Expected to get an error when updating non-existant record")
	}
}

func TestBucketDelete(t *testing.T) {
	bkt, _ := NewBucket("/tmp/foo.bkt", 0)
	defer bkt.Destroy()
	key, _ := bkt.Set(&dummy{"hello"})
	if err := bkt.Delete(key); err != nil {
		t.Errorf("Expected to delete specified record, error: %v", err)
	}
	if _, err := bkt.Get(key); err == nil {
		t.Errorf("Expected to get an error when retrieving deleted record")
	}
	if err := bkt.Delete(123); err == nil {
		t.Errorf("Expected to get an error when deleting non-existent record")
	}
}

func TestBucketExists(t *testing.T) {
	bkt, _ := NewBucket("/tmp/foo.bkt", 0)
	defer bkt.Destroy()
	key, _ := bkt.Set(&dummy{"hello"})
	if ok := bkt.Exists(key); !ok {
		t.Errorf("Expected to get true when record exists")
	}
	if ok := bkt.Exists(123); ok {
		t.Errorf("Expected to get false when record does not exist")
	}
}

func TestBucketLen(t *testing.T) {
	bkt, _ := NewBucket("/tmp/foo.bkt", 0)
	defer bkt.Destroy()
	bkt.Set(&dummy{"hello"})
	bkt.Set(&dummy{"world"})
	bkt.Set(&dummy{"hurra"})
	bkt.Delete(2)
	if bkt.Len() != 2 {
		t.Errorf("Expected to get proper bucket size")
	}
}

func TestBucketSyncAndReopen(t *testing.T) {
	var bkt *Bucket
	var err error
	bkt, _ = NewBucket("/tmp/foo.bkt", 0)
	bkt.Set(&dummy{"hello"})
	bkt.Set(&dummy{"world"})
	bkt.Set(&dummy{"hurra"})
	bkt.Delete(2)
	bkt.Close()
	bkt, err = NewBucket("/tmp/foo.bkt", 0)
	if err != nil {
		t.Errorf("Expected to reopen the bucket, error: %v", err)
	}
	if bkt.Len() != 2 {
		t.Errorf("Expected to have 2 records after load, got %d", bkt.Len())
	}
	hello, _ := bkt.Get(1)
	hurra, _ := bkt.Get(3)
	if hello.(*dummy).A != "hello" || hurra.(*dummy).A != "hurra" {
		t.Errorf("Expected to have proper values after load")
	}
	bkt.Delete(1)
	if _, err = bkt.Set(&dummy{"hello"}); err != nil {
		t.Errorf("Expected to set something new, error: %v", err)
	}
	bkt.Close()
	bkt, err = NewBucket("/tmp/foo.bkt", 0)
	if err != nil {
		t.Errorf("Expected to reopen the bucket, error: %v", err)
	}
	if bkt.Len() != 2 {
		t.Errorf("Expected to have 2 records after load, got %d", bkt.Len())
	}
	hurra, _ = bkt.Get(3)
	hello, _ = bkt.Get(4)
	if hello.(*dummy).A != "hello" || hurra.(*dummy).A != "hurra" {
		t.Errorf("Expected to have proper values after load")
	}
	bkt.Destroy()
}

const numberOfRecords = 10000

func BenchmarkBucketWrite(b *testing.B) {
	b.StopTimer()
	bkt, _ := NewBucket("/tmp/bench.bkt", 0)
	defer bkt.Destroy()
	b.StartTimer()
	for i := 0; i < numberOfRecords; i += 1 {
		bkt.Set("hello")
	}
}

func BenchmarkBucketRead(b *testing.B) {
	b.StopTimer()
	bkt, _ := NewBucket("/tmp/bench.bkt", 0)
	defer bkt.Destroy()
	for i := 0; i < numberOfRecords; i += 1 {
		bkt.Set("hello")
	}
	b.StartTimer()
	for i := 0; i < numberOfRecords; i += 1 {
		bkt.Get(i)
	}
}

func BenchmarkBucketDelete(b *testing.B) {
	b.StopTimer()
	bkt, _ := NewBucket("/tmp/bench.bkt", 0)
	defer bkt.Destroy()
	for i := 0; i < numberOfRecords; i += 1 {
		bkt.Set("hello")
	}
	b.StartTimer()
	for i := 0; i < numberOfRecords; i += 1 {
		bkt.Delete(i)
	}
}

func BenchmarkBucketOpen(b *testing.B) {
	b.StopTimer()
	bkt, _ := NewBucket("/tmp/bench.bkt", 0)
	for i := 0; i < numberOfRecords; i += 1 {
		bkt.Set("hello")
	}
	bkt.Close()
	b.StartTimer()
	bkt, _ = NewBucket("/tmp/bench.bkt", 0)
	defer bkt.Destroy()
}
