package persival

import (
	"os"
	"sync"
	"fmt"
	"encoding/gob"
)

// Error thrown when record not eixsts.
type RecordNotFound struct {
	// A key of the missing record.
	Key int
}

// Error returns an error message.
func (e *RecordNotFound) Error() string {
	return fmt.Sprintf("record not found (key: %d)", e.Key) 
}

// Bucket implements API for managing and accessing stored data. Bucket is
// a threadsafe wrapper for underlaying map of bytes, also providing persistance
// layer via log files.
type Bucket struct {
	// The data stored in the bucket.
	data map[int]interface{}
	// The value of the next key.
	autoincr int
	// The storage file name.
	fileName string
	// Configuration flags.
	flags int
	// The storage log.
	log *Log
	// Internal semaphore.
	mtx sync.Mutex
}

// NewBucket allocates memory for a new bucket instance.
//
// file  - The name of the destination storage file.
// flags - The configuration flags.
//
// Returns new bucket or an error if something went wrong.
func NewBucket(file string, flags int) (bkt *Bucket, err error) {
	bkt = &Bucket{fileName: file, flags: flags, data: make(map[int]interface{})}
	if err = bkt.setup(); err != nil {
		return nil, err
	}
	return
}

// Registers is just an alias for gob.Register.
func Register(x interface{}) {
	gob.Register(x)
}

// setup opens or creates the storage file and initializes the bucket.
// Returns an error if something went wrong.
func (bkt *Bucket) setup() (err error) {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	var f *os.File
	// Read the log file (if exists) or try to create new one...
	if f, err = os.Open(bkt.fileName); err == nil {
		bkt.data, err = ReadLog(f)
		for k, _ := range bkt.data {
			if k > bkt.autoincr {
				bkt.autoincr = k
			}
		}
	} else if f, err = os.Create(bkt.fileName); err != nil {
		return
	}
	bkt.log = NewLog(f)
	return
}

// All returns a map with all the records stored in the bucket.
func (bkt *Bucket) All() map[int]interface{} {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	return bkt.data
}

// Set writes given value to the storage. After the new record is be stored
// the corresponding key will be returned.
//
// val - The value to be stored.
//
// Returns an identifier of the created record or an error if something
// went wrong.
func (bkt *Bucket) Set(val interface{}) (key int, err error) {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	bkt.autoincr += 1
	key = bkt.autoincr
	bkt.data[key] = val
	err = bkt.log.Append(&Change{CW, key, val})
	return
}

// Get retrieves and returns the value of the record from the specified key.
//
// key - The key of value to be found.
//
// Returns the value or an error if something went wrong or record doesn't exist.
func (bkt *Bucket) Get(key int) (val interface{}, err error) {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	var ok bool
	if val, ok = bkt.data[key]; !ok {
		err = &RecordNotFound{key}
	}
	return
}

// Delete removes a record with the specified key from the storage.
//
// key - A key of the record to be deleted.
//
// Returns an error if something went wrong or record doesn't exist.
func (bkt *Bucket) Delete(key int) (err error) {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	if _, ok := bkt.data[key]; ok {
		delete(bkt.data, key)
		err = bkt.log.Append(&Change{CD, key, nil})
	} else {
		err = &RecordNotFound{key}
	}
	return
}

// Update sets new value for the existing record. If record doesn't exist
// then error will be returned.
//
// key - A key of the record to be deleted.
// val - The value to be stored.
//
// Returns an error if something went wrong.
func (bkt *Bucket) Update(key int, val interface{}) (err error) {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	if _, ok := bkt.data[key]; ok {
		bkt.data[key] = val
		err = bkt.log.Append(&Change{CD, key, val})
	} else {
		err = &RecordNotFound{key}
	}
	return
}

// Exists returns whether the bucket contains a record with the specified key.
//
// key - The key to be checked.
//
func (bkt *Bucket) Exists(key int) (ok bool) {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	_, ok = bkt.data[key]
	return
}

// Len returns number of the records currently stored by the bucket.
func (bkt *Bucket) Len() int {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	return len(bkt.data)
}

// Destroy closes the bucket and permanently removes its storage file.
// Returns an error if something went wrong.
func (bkt *Bucket) Destroy() (err error) {
	bkt.Close()
	err = os.Remove(bkt.fileName)
	return
}

// Close closes the bucket.
func (bkt *Bucket) Close() {
	bkt.mtx.Lock()
	defer bkt.mtx.Unlock()
	bkt.log.Close()
}
