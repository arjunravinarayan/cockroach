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
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TODO(asubiotto): Comment.
type diskRowContainer struct {
	diskMap       SortedDiskMap
	bufferedRows  SortedDiskMapWriteBatch
	iterator      SortedDiskMapIterator
	scratchKey    []byte
	scratchEncRow sqlbase.EncDatumRow

	// TODO(asubiotto): Maybe make this a slice of a composite type.
	types    []sqlbase.ColumnType
	ordering sqlbase.ColumnOrdering
	// encodings keeps around the DatumEncoding equivalents of the encoding
	// directions in ordering to avoid conversions in hot paths.
	encodings []sqlbase.DatumEncoding

	datumAlloc sqlbase.DatumAlloc
}

// makeDiskRowContainer creates a diskRowContainer with the rows from the passed
// in rowContainer. Note that makeDiskRowContainer does not free up any memory,
// the caller must Clear() or Close() the passed in rowContainer to do so.
func makeDiskRowContainer(
	ctx context.Context,
	r *engine.RocksDB,
	types []sqlbase.ColumnType,
	ordering sqlbase.ColumnOrdering,
	rowContainer rowContainer,
) diskRowContainer {
	d := diskRowContainer{
		diskMap:       NewRocksDBMap(r),
		types:         types,
		ordering:      ordering,
		scratchEncRow: make(sqlbase.EncDatumRow, len(types)),
	}
	d.bufferedRows = d.diskMap.NewWriteBatch()

	// The ordering is specified for a subset of the columns. To improve the
	// AddRow hot path we append the remaining columns to this ordering to
	// generalize the row addition logic.
	orderingIndexes := make(map[int]struct{})
	for _, orderInfo := range d.ordering {
		orderingIndexes[orderInfo.ColIdx] = struct{}{}
	}
	nextIdx := len(d.ordering)
	d.ordering = append(d.ordering, make(sqlbase.ColumnOrdering, len(d.types)-len(d.ordering))...)
	for i := range d.types {
		if _, ok := orderingIndexes[i]; ok {
			continue
		}
		d.ordering[nextIdx].ColIdx = i
		d.ordering[nextIdx].Direction = encoding.Ascending
		nextIdx++
	}

	d.encodings = make([]sqlbase.DatumEncoding, len(d.ordering))
	for i, orderInfo := range ordering {
		datumEncoding, err := sqlbase.EncodingDirToDatumEncoding(orderInfo.Direction)
		if err != nil {
			log.Fatal(ctx, errors.Wrap(err, "unable to make diskRowContainer"))
		}
		d.encodings[i] = datumEncoding
	}

	if len(d.ordering) != len(d.types) {
		panic("ordering and types should have the same length now")
	}

	for i := 0; i < rowContainer.Len(); i++ {
		// TODO(asubiotto): We have to be careful here because we don't want to
		// crash the server with an OOM due to buffering these writes.
		if err := d.AddRow(ctx, rowContainer.EncRow(i)); err != nil {
			panic(err)
		}
	}
	return d
}

func (d *diskRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	if len(row) != len(d.types) {
		log.Fatalf(ctx, "invalid row length %d, expected %d", len(row), len(d.types))
	}

	for i, orderInfo := range d.ordering {
		var err error
		d.scratchKey, err = row[orderInfo.ColIdx].Encode(&d.datumAlloc, d.encodings[i], d.scratchKey)
		if err != nil {
			return errors.Wrap(err, "could not add row")
		}
	}

	// TODO(asubiotto): Make unique.
	d.bufferedRows.Put(d.scratchKey, nil)
	d.scratchKey = d.scratchKey[:0]
	return nil
}

// keyToRow decodes a byte slice into an sqlbase.EncDatumRow according to
// d.types. The returned EncDatumRow is only valid until the next call to
// keyToRow().
func (d *diskRowContainer) keyToRow(ctx context.Context, k []byte) sqlbase.EncDatumRow {
	// TODO(asubiotto): Collated string keys cannot be decoded? See
	// pkg/sql/sqlbase/table.go:1176
	for i, orderInfo := range d.ordering {
		var err error
		d.scratchEncRow[orderInfo.ColIdx], k, err = sqlbase.EncDatumFromBuffer(d.types[i], d.encodings[i], k)
		if err != nil {
			log.Fatal(ctx, errors.Wrap(err, "unable to decode row"))
		}
	}
	return d.scratchEncRow
}

// TODO(asubiotto): Could also be a common interface.
type RowIterator struct {
	rowContainer *diskRowContainer
	SortedDiskMapIterator
}

func (d *diskRowContainer) NewIterator(ctx context.Context) RowIterator {
	if err := d.bufferedRows.Flush(); err != nil {
		log.Fatal(ctx, err)
	}
	return RowIterator{rowContainer: d, SortedDiskMapIterator: d.diskMap.NewIterator()}
}

// Row returns the current row. The returned sqlbase.EncDatumRow is only valid
// until the next call to Row().
func (r RowIterator) Row(ctx context.Context) sqlbase.EncDatumRow {
	if !r.Valid() {
		log.Fatalf(ctx, "invalid row")
	}

	return r.rowContainer.keyToRow(ctx, r.Key())
}
