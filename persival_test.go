package persival

import (
	"os"
	"sync"
	"testing"
)

func TestNew(t *testing.T) {
	if bkt := NewBucket(); bkt == nil {
		t.Errorf("Expected to create new bucket")
	}
}

func TestSet(t *testing.T) {
	bkt := NewBucket()
	bkt.Open("/tmp/foo.gdb", 0)
	if key := bkt.Set([]byte("hello")); key == -1 {
		t.Errorf("Expected to get key of the set value")
	}
}

func TestGet(t *testing.T) {
	bkt := NewBucket()
	bkt.Open("/tmp/foo.gdb", 0)
	key := bkt.Set([]byte("hello"))
	if val := bkt.Get(key); string(val) != "hello" {
		t.Errorf("Expected to get proper value from specfied key")
	}
	if val := bkt.Get(123); len(val) > 0 {
		t.Errorf("Expected to get nothing from non-existent key")
	}
}

func TestUpdate(t *testing.T) {
	bkt := NewBucket()
	bkt.Open("/tmp/foo.gdb", 0)
	key := bkt.Set([]byte("hello"))
	if ok := bkt.Update(key, []byte("world")); !ok {
		t.Errorf("Expected to have true confirmation from update operation")
	}
	if val := bkt.Get(key); string(val) != "world" {
		t.Errorf("Expected to have proper value after update")
	}
	if ok := bkt.Update(123, []byte("hello")); ok {
		t.Errorf("Expected to have false confirmation after updating non-existent record")
	}
}

func TestDelete(t *testing.T) {
	bkt := NewBucket()
	bkt.Open("/tmp/foo.gdb", 0)
	key := bkt.Set([]byte("hello"))
	if ok := bkt.Delete(key); !ok {
		t.Errorf("Expected to have true confirmation from delete operation")
	}
	if val := bkt.Get(key); len(val) != 0 {
		t.Errorf("Expected to delete value")
	}
	if ok := bkt.Delete(123); ok {
		t.Errorf("Expected to have false confirmation after deleting non-existent record")
	}
}

func TestExists(t *testing.T) {
	bkt := NewBucket()
	bkt.Open("/tmp/foo.gdb", 0)
	key := bkt.Set([]byte("hello"))
	if ok := bkt.Exists(key); !ok {
		t.Errorf("Expected to have proper result of existance check when record exists")
	}
	if ok := bkt.Exists(123); ok {
		t.Errorf("Expected to have proper result of existance check when record not exists")
	}
}

func TestClear(t *testing.T) {
	bkt := NewBucket()
	bkt.Open("/tmp/foo.gdb", 0)
	bkt.Set([]byte("hello"))
	bkt.Set([]byte("world"))
	if bkt.Len() != 2 {
		t.Errorf("Expected to have 2 elements stored")
	}
	bkt.Clear()
	if bkt.Len() != 0 {
		t.Errorf("Expected to clear the bucket")
	}
}

func TestSyncAndReopen(t *testing.T) {
	bkt := NewBucket()
	if err := bkt.Open("/tmp/foo.gdb", 0); err != nil {
		t.Errorf("%v", err)
	}
	bkt.Set([]byte("hello"))
	bkt.Set([]byte("world"))
	bkt.Set([]byte("super"))
	bkt.Delete(1)
	if err := bkt.Sync(); err != nil {
		t.Errorf("%v", err)
	}
	bkt.Close()
	bkt = NewBucket()
	if err := bkt.Open("/tmp/foo.gdb", 0); err != nil {
		t.Errorf("%v", err)
	}
	if bkt.Len() != 2 {
		t.Errorf("Expected to have 2 records after load")
	}
	if len(bkt.freeKeys) != 1 || bkt.freeKeys[0] != 1 {
		t.Errorf("Expected to have proper free key after load")
	}
	if string(bkt.Get(0)) != "hello" || string(bkt.Get(2)) != "super" {
		t.Errorf("Expected to have proper values after load")
	}
	os.Remove("/tmp/foo.gdb")
}

func TestSetDeleteAndSetAgain(t *testing.T) {
	bkt := NewBucket()
	bkt.Open("/tmp/foo.gdb", 0)
	key1 := bkt.Set([]byte("hello"))
	bkt.Set([]byte("world"))
	bkt.Delete(key1)
	if bkt.Len() != 1 {
		t.Errorf("Expected bucket size to be 1, given %d", bkt.Len())
	}
	key3 := bkt.Set([]byte("hello again!"))
	if key3 != key1 {
		t.Errorf("Expected to write again on the same key")
	}
}

const numberOfOps = 1e5

func BenchmarkBucketWritesAndReads(b *testing.B) {
	b.StopTimer()
	bkt := NewBucket()
	bkt.Open("/tmp/bench.gdb", 0)
	var wg sync.WaitGroup
	b.StartTimer()
	wg.Add(2)
	go func() {
		for i := 0; i < numberOfOps; i += 1 {
			bkt.Set([]byte("hello"))
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < numberOfOps; i += 1 {
			bkt.Get(i)
		}
		wg.Done()
	}()
	wg.Wait()
}

func BenchmarkMapWritesAndReads(b *testing.B) {
	b.StopTimer()
	m := map[int][]byte{}
	var mtx sync.Mutex
	var wg sync.WaitGroup
	b.StartTimer()
	wg.Add(2)
	go func() {
		for i := 0; i < numberOfOps; i += 1 {
			mtx.Lock()
			m[i] = []byte("hello")
			mtx.Unlock()
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < numberOfOps; i += 1 {
			mtx.Lock()
			_ = m[i]
			mtx.Unlock()
		}
		wg.Done()
	}()
	wg.Wait()
}
