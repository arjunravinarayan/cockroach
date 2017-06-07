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

package engine

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDiskMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	diskMap, err := NewRocksDBMap()
	if err != nil {
		t.Fatal(err)
	}
	defer diskMap.Close()

	rng := rand.New(rand.NewSource(int64(time.Now().UnixNano())))

	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("%d", rng.Int())
		v := fmt.Sprintf("%d", rng.Int())
		if err := diskMap.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	i := diskMap.NewIterator()
	defer i.Close()

	i.Rewind()
	if !i.Valid() {
		t.Fatal("unexpectedly invalid")
	}
	lastKey := i.Key()
	i.Next()

	for ; i.Valid(); i.Next() {
		curKey := i.Key()
		if bytes.Compare(curKey, lastKey) < 0 {
			t.Fatalf("expected keys in sorted order but %s is larger than %s", curKey, lastKey)
		}
		lastKey = curKey
	}
}
