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
// Author: Alfonso Subiotto Marques (alfonso@cockroachlabs.com)

package distsqlrun

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TODO(asubiotto): Note that this fails because I don't handle duplicate rows
// so sometimes read less than we write.
func TestDiskRowContainer(t *testing.T) {
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

	// TODO(asubiotto): Add more complicated orderings.
	orderings := []sqlbase.ColumnOrdering{
		sqlbase.ColumnOrdering{
			sqlbase.ColumnOrderInfo{
				ColIdx:    0,
				Direction: encoding.Ascending,
			},
		},
		sqlbase.ColumnOrdering{
			sqlbase.ColumnOrderInfo{
				ColIdx:    0,
				Direction: encoding.Descending,
			},
		},
	}

	rng := rand.New(rand.NewSource(int64(time.Now().UnixNano())))

	numRows := 1024
	numCols := 4
	ctx := context.Background()
	evalCtx := parser.MakeTestingEvalContext()
	for _, ordering := range orderings {
		// numRows rows with numCols columns of the same random type.
		rows := sqlbase.RandEncDatumRows(rng, numRows, numCols)
		types := make([]sqlbase.ColumnType, len(rows[0]))
		for i := range types {
			types[i] = rows[0][i].Type
		}
		// Make the diskRowContainer with half of these rows and insert the
		// other half normally.
		memoryContainer := makeRowContainer(ordering, types, &evalCtx)
		midIdx := len(rows) / 2
		for i := 0; i < midIdx; i++ {
			if err := memoryContainer.AddRow(ctx, rows[i]); err != nil {
				t.Fatal(err)
			}
		}

		d := makeDiskRowContainer(
			ctx,
			r,
			types,
			ordering,
			memoryContainer,
		)
		for i := midIdx; i < len(rows); i++ {
			if err := d.AddRow(ctx, rows[i]); err != nil {
				t.Fatal(err)
			}
		}

		i := d.NewIterator(ctx)
		i.Rewind()
		if !i.Valid() {
			t.Fatal("unexpectedly invalid")
		}
		// Copy because row is only valid until next call to Row().
		lastRow := make(sqlbase.EncDatumRow, numCols)
		copy(lastRow, i.Row(ctx))
		i.Next()

		numKeysRead := 1
		for ; i.Valid(); i.Next() {
			row := i.Row(ctx)

			// Check sorted order.
			for _, orderInfo := range ordering {
				cmp, err := lastRow[orderInfo.ColIdx].Compare(&d.datumAlloc, &evalCtx, &row[orderInfo.ColIdx])
				if err != nil {
					t.Fatal(err)
				}
				if cmp != 0 {
					if orderInfo.Direction == encoding.Descending {
						cmp = -cmp
					}
					if cmp > 0 {
						t.Fatalf("expected %s to be greater than %s", row, lastRow)
					}
				}
			}

			copy(lastRow, row)
			numKeysRead++
		}
		if numKeysRead != numRows {
			t.Fatalf("expected to read %d keys but only read %d", numRows, numKeysRead)
		}
	}
}
