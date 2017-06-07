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

// SortedDiskMap is an on-disk map. Keys are iterated over in sorted order.
type SortedDiskMap interface {
	Put(k []byte, v []byte) error
	Get(k []byte) ([]byte, error)
	NewIterator() SortedDiskMapIterator

	Close()
}

type RocksDBMap struct {
	// TODO(asubiotto): Add memory accounting.
	store *engine.RocksDB
}

var _ SortedDiskMap = RocksDBMap{}

type RocksDBMapIterator struct {
	iter engine.Iterator
}

var _ SortedDiskMapIterator = RocksDBMapIterator{}

func NewRocksDBMap(r *engine.RocksDB) RocksDBMap {
	return RocksDBMap{store: r}
}

// TODO(asubiotto): We can bypass MVCCKey creation for Put and Get.
// TODO(asubiotto): Duplicates will be overwritten. Is this ok from this
// perspective? It is for a map, but not for external sort. Processors that want
// different behaviors might have to implement different key encodings.
// TODO(asubiotto): Add an interface for batching as well.
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
