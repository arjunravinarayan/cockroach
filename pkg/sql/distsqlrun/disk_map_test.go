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

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDiskMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r, err := engine.NewRocksDB(
		roachpb.Attributes{},
		rocksDBMapPath,
		engine.NewRocksDBCache(512<<20),
		0,
		engine.DefaultMaxOpenFiles,
	)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		r.Close()
		os.RemoveAll(rocksDBMapPath)
		os.Mkdir(rocksDBMapPath, 0700)
	}()

	diskMap := NewRocksDBMap(r)
	defer diskMap.Close()

	writeBatch := diskMap.NewWriteBatchSize(64)
	defer writeBatch.Close()

	rng := rand.New(rand.NewSource(int64(time.Now().UnixNano())))

	numKeysToWrite := 100
	for i := 0; i < numKeysToWrite; i++ {
		k := []byte(fmt.Sprintf("%d", rng.Int()))
		v := []byte(fmt.Sprintf("%d", rng.Int()))

		// Use batch on every other write.
		if i%2 == 0 {
			if err := diskMap.Put(k, v); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := writeBatch.Put(k, v); err != nil {
				t.Fatal(err)
			}
		}
	}

	if err := writeBatch.Flush(); err != nil {
		t.Fatal(err)
	}

	i := diskMap.NewIterator()
	defer i.Close()

	i.Rewind()
	if !i.Valid() {
		t.Fatal("unexpectedly invalid")
	}
	lastKey := i.Key()
	i.Next()

	numKeysRead := 1
	for ; i.Valid(); i.Next() {
		curKey := i.Key()
		if bytes.Compare(curKey, lastKey) < 0 {
			t.Fatalf("expected keys in sorted order but %s is larger than %s", curKey, lastKey)
		}
		lastKey = curKey
		numKeysRead++
	}
	if numKeysRead != numKeysToWrite {
		t.Fatalf("expected to read %d keys but only read %d", numKeysToWrite, numKeysRead)
	}
}
