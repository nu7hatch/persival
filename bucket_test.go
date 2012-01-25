package persival

import "testing"

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
	if key, err := bkt.Set([]byte("hello")); err != nil || key != 1 {
		t.Errorf("Expected to get key of the set value, error: %v", err)
	}
}

type dummy struct {
	A string
}

func TestBucketGet(t *testing.T) {
	bkt, _ := NewBucket("/tmp/foo.bkt", 0)
	defer bkt.Destroy()
	Register(&dummy{})
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
	key, _ := bkt.Set("hello")
	if err := bkt.Update(key, "world"); err != nil {
		t.Errorf("Expected to update value of the specified key, error: %v", err)
	}
	if val, err := bkt.Get(key); err != nil || val.(string) != "world" {
		t.Errorf("Expected to have proper value after update, error: %v", err)
	}
	if err := bkt.Update(123, "hello"); err == nil {
		t.Errorf("Expected to get an error when updating non-existant record")
	}
}

func TestBucketDelete(t *testing.T) {
	bkt, _ := NewBucket("/tmp/foo.bkt", 0)
	defer bkt.Destroy()
	key, _ := bkt.Set("hello")
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
	key, _ := bkt.Set("hello")
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
	bkt.Set("hello")
	bkt.Set("world")
	bkt.Set("hurra")
	bkt.Delete(2)
	if bkt.Len() != 2 {
		t.Errorf("Expected to get proper bucket size")
	}
}

func TestBucketSyncAndReopen(t *testing.T) {
	bkt, _ := NewBucket("/tmp/foo.bkt", 0)
	bkt.Set("hello")
	bkt.Set("world")
	bkt.Set("hurra")
	bkt.Delete(2)
	bkt.Close()
	bkt, err := NewBucket("/tmp/foo.bkt", 0)
	if err != nil {
		t.Errorf("Expected to reopen the bucket, error: %v", err)
	}
	if bkt.Len() != 2 {
		t.Errorf("Expected to have 2 records after load, got %d", bkt.Len())
	}
	hello, _ := bkt.Get(1)
	hurra, _ := bkt.Get(3)
	if hello.(string) != "hello" || hurra.(string) != "hurra" {
		t.Errorf("Expected to have proper values after load")
	}
	if _, err = bkt.Set("hello"); err != nil {
		t.Errorf("Expected to set something new, error: %v", err)
	}
	bkt.Destroy()
}

const numberOfOps = 100000

func BenchmarkBucketWrite(b *testing.B) {
	b.StopTimer()
	bkt, _ := NewBucket("/tmp/bench.bkt", 0)
	defer bkt.Destroy()
	b.StartTimer()
	for i := 0; i < numberOfOps; i += 1 {
		bkt.Set("hello")
	}
}

func BenchmarkBucketRead(b *testing.B) {
	b.StopTimer()
	bkt, _ := NewBucket("/tmp/bench.bkt", 0)
	defer bkt.Destroy()
	for i := 0; i < numberOfOps; i += 1 {
		bkt.Set("hello")
	}
	b.StartTimer()
	for i := 0; i < numberOfOps; i += 1 {
		bkt.Get(i)
	}
}

func BenchmarkBucketDelete(b *testing.B) {
	b.StopTimer()
	bkt, _ := NewBucket("/tmp/bench.bkt", 0)
	defer bkt.Destroy()
	for i := 0; i < numberOfOps; i += 1 {
		bkt.Set("hello")
	}
	b.StartTimer()
	for i := 0; i < numberOfOps; i += 1 {
		bkt.Delete(i)
	}
}

func BenchmarkBucketOpen(b *testing.B) {
	b.StopTimer()
	bkt, _ := NewBucket("/tmp/bench.bkt", 0)
	for i := 0; i < numberOfOps; i += 1 {
		bkt.Set("hello")
	}
	bkt.Close()
	b.StartTimer()
	bkt, _ = NewBucket("/tmp/bench.bkt", 0)
	defer bkt.Destroy()
}