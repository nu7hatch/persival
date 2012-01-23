package persival

import (
	"encoding/gob"
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
	"time"
)

type Bucket struct {
	store    *os.File
	data     [][]byte
	freeKeys []int
	dataLen  int
	tick     <-chan time.Time
	mtx      sync.Mutex
	smtx     sync.Mutex
}

func init() {
	gob.Register([][]byte{})
}

func NewBucket() (bkt *Bucket) {
	return &Bucket{dataLen: 0}
}

func (bkt *Bucket) Set(val []byte) (key int) {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	if len(bkt.freeKeys) > 0 {
		key = bkt.freeKeys[0]
		bkt.freeKeys[0] = 0
		bkt.freeKeys = bkt.freeKeys[1:]
		bkt.data[key] = val
	} else {
		bkt.data = append(bkt.data[:], val)
		key = bkt.dataLen
		bkt.dataLen += 1
	}
	return
}

func (bkt *Bucket) Get(key int) (val []byte) {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	if len(bkt.data) > key {
		val = bkt.data[key]
	}
	return
}

func (bkt *Bucket) Delete(key int) (ok bool) {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	if len(bkt.data) > key {
		bkt.data[key] = nil
		bkt.freeKeys = append(bkt.freeKeys, key)
		ok = true
	}
	return
}

func (bkt *Bucket) Update(key int, val []byte) (ok bool) {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	if len(bkt.data) > key {
		bkt.data[key] = val
		ok = true
	}
	return
}

func (bkt *Bucket) Exists(key int) (ok bool) {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	if len(bkt.data) > key {
		if val := bkt.data[key]; len(val) > 0 {
			ok = true
		}
	}
	return
}

func (bkt *Bucket) Clear() bool {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	_, bkt.data = bkt.data[:], make([][]byte, 0)
	return true
}

func (bkt *Bucket) Len() int {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	return len(bkt.data) - len(bkt.freeKeys)
}

func (bkt *Bucket) Open(file string, flags int) (err error) {
	bkt.smtx.Lock()
	defer bkt.smtx.Unlock()
	var fd int
	if fd, err = syscall.Open(file, os.O_RDWR, 0666); err != nil {
		if err == os.ENOENT {
			if fd, err = syscall.Open(file, os.O_CREATE, 0666); err != nil {
				return
			}
		}
		return
	}
	bkt.store = os.NewFile(fd, file)
	de := gob.NewDecoder(bkt.store)
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	if err = de.Decode(&bkt.data); err != nil {
		if err == io.EOF {
			return nil
		}
		return
	}
	for i, val := range bkt.data {
		if len(val) == 0 {
			bkt.freeKeys = append(bkt.freeKeys, i)
		}
	}
	return
}

func (bkt *Bucket) SyncWatch(d time.Duration) {
	if bkt.tick != nil {
		return
	}
	go func() {
		bkt.tick = time.Tick(d)
		for {
			if bkt.tick == nil {
				break
			}
			<-bkt.tick
			bkt.Sync()
		}
	}()
}

func (bkt *Bucket) StopWatch() {
	bkt.tick = nil
}

func (bkt *Bucket) Sync() (err error) {
	bkt.smtx.Lock()
	defer bkt.smtx.Unlock()
	if bkt.store == nil {
		err = errors.New("file not available")
		return
	}
	bkt.store.Seek(0, os.SEEK_SET)
	bkt.store.Truncate(0)
	en := gob.NewEncoder(bkt.store)
	bkt.mtx.Lock()
	err = en.Encode(bkt.data)
	bkt.mtx.Unlock()
	if err != nil {
		return
	}
	bkt.store.Sync()
	return
}

func (bkt *Bucket) Close() (err error) {
	bkt.StopWatch()
	err = bkt.Sync()
	if err != nil {
		return
	}
	err = bkt.store.Close()
	return
}
