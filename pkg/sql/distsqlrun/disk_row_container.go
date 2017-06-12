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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// TODO(asubiotto): Comment.
type diskRowContainer struct {
	diskMap       SortedDiskMap
	bufferedRows  SortedDiskMapWriteBatch
	scratchKey    []byte
	scratchVal    []byte
	scratchEncRow sqlbase.EncDatumRow

	// TODO(asubiotto): The code can be DRYed if we construct a type
	// intelligently instead of having all these slices.
	types    []sqlbase.ColumnType
	ordering sqlbase.ColumnOrdering
	// encodings keeps around the DatumEncoding equivalents of the encoding
	// directions in ordering to avoid conversions in hot paths.
	encodings []sqlbase.DatumEncoding
	// valueIdxs holds the indexes of the columns that we encode as values.
	valueIdxs []int

	datumAlloc sqlbase.DatumAlloc
}

// makeDiskRowContainer creates a diskRowContainer with the rows from the passed
// in rowContainer. Note that makeDiskRowContainer does not free up any memory,
// the caller must Clear() or Close() the passed in rowContainer to do so.
func makeDiskRowContainer(
	ctx context.Context,
	processorId uint64,
	types []sqlbase.ColumnType,
	ordering sqlbase.ColumnOrdering,
	rowContainer rowContainer,
	r *engine.RocksDB,
) diskRowContainer {
	d := diskRowContainer{
		// Use the processorId as the prefix.
		diskMap:       NewRocksDBMap(processorId, r),
		types:         types,
		ordering:      ordering,
		scratchEncRow: make(sqlbase.EncDatumRow, len(types)),
	}
	d.bufferedRows = d.diskMap.NewWriteBatch()

	// The ordering is specified for a subset of the columns. For the other
	// columns and for types that cannot be decoded from a key encoding, we get
	// the indexes to have them ready for hot paths.
	// A row is added to this container by storing the columns according to
	// d.ordering as an encoded key into a SortedDiskMap. The remaining columns
	// are stored in the value associated with the key. For the case in which
	// types whose key encodings cannot be decoded (at the time of writing only
	// CollatedString) are specified in d.ordering, the Datum is encoded both in
	// the key for comparison and in the value for decoding.
	orderingIndexes := make(map[int]struct{})
	for _, orderInfo := range d.ordering {
		orderingIndexes[orderInfo.ColIdx] = struct{}{}
	}
	d.valueIdxs = make([]int, 0, len(d.types))
	for i := range d.types {
		if _, ok := orderingIndexes[i]; ok && d.types[i].Kind != sqlbase.ColumnType_COLLATEDSTRING {
			continue
		}
		d.valueIdxs = append(d.valueIdxs, i)
	}

	d.encodings = make([]sqlbase.DatumEncoding, len(d.ordering))
	for i, orderInfo := range ordering {
		datumEncoding, err := sqlbase.EncodingDirToDatumEncoding(orderInfo.Direction)
		if err != nil {
			log.Fatal(ctx, errors.Wrap(err, "unable to make diskRowContainer"))
		}
		d.encodings[i] = datumEncoding
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

	// TODO(asubiotto): Just make one slice with all the necessary information.
	for i, orderInfo := range d.ordering {
		var err error
		d.scratchKey, err = row[orderInfo.ColIdx].Encode(&d.datumAlloc, d.encodings[i], d.scratchKey)
		if err != nil {
			return errors.Wrap(err, "could not add row")
		}
	}
	for i := range d.valueIdxs {
		var err error
		d.scratchVal, err = row[i].Encode(&d.datumAlloc, sqlbase.DatumEncoding_VALUE, d.scratchVal)
		if err != nil {
			return errors.Wrap(err, "could not add row")
		}
	}

	// Put a unique row to keep track of duplicates. Note that this will not
	// mess with key decoding.
	d.bufferedRows.Put(append(d.scratchKey, uuid.MakeV4().GetBytes()...), d.scratchVal)
	d.scratchKey = d.scratchKey[:0]
	d.scratchVal = d.scratchVal[:0]
	return nil
}

func (d *diskRowContainer) Close() {
	d.bufferedRows.Close()
	d.diskMap.Close()
}

// keyValToRow decodes a key and a value byte slice stored with AddRow() into
// a sqlbase.EncDatumRow. The returned EncDatumRow is only valid until the next
// call to keyValToRow().
func (d *diskRowContainer) keyValToRow(ctx context.Context, k []byte, v []byte) sqlbase.EncDatumRow {
	for i, orderInfo := range d.ordering {
		// CollatedStrings are decoded from the value.
		if d.types[orderInfo.ColIdx].Kind == sqlbase.ColumnType_COLLATEDSTRING {
			continue
		}
		var err error
		d.scratchEncRow[orderInfo.ColIdx], k, err = sqlbase.EncDatumFromBuffer(d.types[orderInfo.ColIdx], d.encodings[i], k)
		if err != nil {
			log.Fatal(ctx, errors.Wrap(err, "unable to decode row"))
		}
	}
	for _, i := range d.valueIdxs {
		var err error
		d.scratchEncRow[i], v, err = sqlbase.EncDatumFromBuffer(d.types[i], sqlbase.DatumEncoding_VALUE, v)
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

	return r.rowContainer.keyValToRow(ctx, r.Key(), r.Value())
}
