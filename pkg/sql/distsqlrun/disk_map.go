// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Alfonso Subiotto Marqués (alfonso@cockroachlabs.com)

package distsqlrun

import "github.com/cockroachdb/cockroach/pkg/storage/engine"

// SortedDiskMapIterator is a simple iterator used to iterate over keys and/or
// values.
// Example use of iterating over all keys:
// 	for SortedDiskMapIterator.Rewind(); SortedDiskMapIterator.Valid(); SortedDiskMapIterator.Next() {
// 		key := SortedDiskMapIterator.Key()
//		// Do something.
// 	}
type SortedDiskMapIterator interface {
	Seek(key []byte)
	// Rewind seeks to the start key.
	Rewind()
	Valid() bool
	Next()
	Key() []byte
	Value() []byte

	// TODO(asubiotto): Should Close return an error? It seems it's common
	// interface practice for Close() to return an error but we don't seem to
	// do that (see pkg/storage/engine/engine.go)
	Close()
}

// SortedDiskMapWriteBatch batches writes to a SortedDiskMap.
type SortedDiskMapWriteBatch interface {
	Put(k []byte, v []byte) error
	// Flush flushes all writes to the underlying store. The batch can be
	// reused after a call to Flush().
	Flush() error

	Close()
}

// SortedDiskMap is an on-disk map. Keys are iterated over in sorted order.
type SortedDiskMap interface {
	Put(k []byte, v []byte) error
	Get(k []byte) ([]byte, error)

	NewIterator() SortedDiskMapIterator
	NewWriteBatch() SortedDiskMapWriteBatch
	NewWriteBatchSize(size int) SortedDiskMapWriteBatch

	Close()
}

const defaultBatchSize = 4096

type RocksDBMapWriteBatch struct {
	// capacity is the number of bytes to write before a Flush() is triggered.
	capacity         int
	numBytesBuffered int

	batch engine.Batch
	store *engine.RocksDB
}

type RocksDBMapIterator struct {
	iter engine.Iterator
}

type RocksDBMap struct {
	// TODO(asubiotto): Add memory accounting.
	store *engine.RocksDB
}

var _ SortedDiskMapWriteBatch = &RocksDBMapWriteBatch{}
var _ SortedDiskMapIterator = RocksDBMapIterator{}
var _ SortedDiskMap = RocksDBMap{}

func NewRocksDBMap(r *engine.RocksDB) RocksDBMap {
	return RocksDBMap{store: r}
}

// TODO(asubiotto): We can bypass MVCCKey creation for Put and Get. More
// generally, there is a lot of stuff in the engine package that we can not use.
// Maybe work on making that more general.
// TODO(asubiotto): Duplicates will be overwritten. Is this ok from this
// perspective? It is for a map, but not for external sort. Processors that want
// different behaviors might have to implement different key encodings.
func (r RocksDBMap) Put(k []byte, v []byte) error {
	return r.store.Put(engine.MVCCKey{Key: k}, v)
}

// TODO(asubiotto): Getting a non-existent key is not distinguishable from a key
// with an empty value. This makes checking for existence impossible unless
// there is another way.
func (r RocksDBMap) Get(k []byte) ([]byte, error) {
	return r.store.Get(engine.MVCCKey{Key: k})
}

func (r RocksDBMap) NewIterator() SortedDiskMapIterator {
	return RocksDBMapIterator{iter: r.store.NewIterator(false /* prefix */)}
}

func (r RocksDBMap) NewWriteBatch() SortedDiskMapWriteBatch {
	return r.NewWriteBatchSize(defaultBatchSize)
}

func (r RocksDBMap) NewWriteBatchSize(size int) SortedDiskMapWriteBatch {
	return &RocksDBMapWriteBatch{capacity: size, batch: r.store.NewWriteOnlyBatch(), store: r.store}
}

func (r RocksDBMap) Close() {
	// TODO(asubiotto): This is where we will delete all the keys with the
	// passed in prefix.
}

func (i RocksDBMapIterator) Seek(k []byte) {
	i.iter.Seek(engine.MVCCKey{Key: k})
}

func (i RocksDBMapIterator) Rewind() {
	i.iter.Seek(engine.NilKey)
}

func (i RocksDBMapIterator) Valid() bool {
	ok, err := i.iter.Valid()
	// TODO(asubiotto): Understand what errors can be returned, it's a bit
	// annoying that this returns an error.
	if err != nil {
		panic(err)
	}
	return ok
}

func (i RocksDBMapIterator) Next() {
	i.iter.Next()
}

func (i RocksDBMapIterator) Key() []byte {
	return i.iter.Key().Key
}

func (i RocksDBMapIterator) Value() []byte {
	return i.iter.Value()
}

func (i RocksDBMapIterator) Close() {
	i.iter.Close()
}

func (b *RocksDBMapWriteBatch) Put(k []byte, v []byte) error {
	if err := b.batch.Put(engine.MVCCKey{Key: k}, v); err != nil {
		return err
	}
	if b.numBytesBuffered += len(k) + len(v); b.numBytesBuffered >= b.capacity {
		return b.Flush()
	}
	return nil
}

func (b *RocksDBMapWriteBatch) Flush() error {
	if b.numBytesBuffered < 1 {
		return nil
	}
	if err := b.batch.Commit(false /* syncCommit */); err != nil {
		return err
	}
	b.batch = b.store.NewWriteOnlyBatch()
	b.numBytesBuffered = 0
	return nil
}

func (b *RocksDBMapWriteBatch) Close() {
	// TODO(asubiotto): Close should return errors for this reason.
	_ = b.Flush()
	b.batch.Close()
}
