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
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlrun

import (
	"math"
	"sync"

	"golang.org/x/net/context"

	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// processor is a common interface implemented by all processors, used by the
// higher-level flow orchestration code.
type processor interface {
	// Run is the main loop of the processor.
	// If wg is non-nil, wg.Done is called before exiting.
	Run(ctx context.Context, wg *sync.WaitGroup)
}

// procOutputHelper is a helper type that performs filtering and projection on
// the output of a processor.
type procOutputHelper struct {
	numInternalCols int
	output          RowReceiver
	rowAlloc        sqlbase.EncDatumRowAlloc

	filter      *exprHelper
	renderExprs []exprHelper
	renderTypes []sqlbase.ColumnType
	outputCols  []uint32

	// offset is the number of rows that are suppressed.
	offset uint64
	// maxRowIdx is the number of rows after which we can stop (offset + limit),
	// or MaxUint64 if there is no limit.
	maxRowIdx uint64

	rowIdx uint64
}

// init sets up a procOutputHelper. The types describe the internal schema of
// the processor (as described for each processor core spec); they can be
// omitted if there is no filtering expression.
func (h *procOutputHelper) init(
	post *PostProcessSpec,
	types []sqlbase.ColumnType,
	evalCtx *parser.EvalContext,
	output RowReceiver,
) error {
	if !post.Projection && len(post.OutputColumns) > 0 {
		return errors.Errorf("post-processing has projection unset but output columns set: %s", post)
	}
	if post.Projection && len(post.RenderExprs) > 0 {
		return errors.Errorf("post-processing has both projection and rendering: %s", post)
	}
	h.output = output
	h.numInternalCols = len(types)
	if post.Filter.Expr != "" {
		h.filter = &exprHelper{}
		if err := h.filter.init(post.Filter, types, evalCtx); err != nil {
			return err
		}
	}
	if post.Projection {
		for _, col := range post.OutputColumns {
			if int(col) >= h.numInternalCols {
				return errors.Errorf("invalid output column %d (only %d available)", col, h.numInternalCols)
			}
		}
		h.outputCols = post.OutputColumns
		if h.outputCols == nil {
			// nil indicates no projection; use an empty slice.
			h.outputCols = make([]uint32, 0)
		}
	} else if len(post.RenderExprs) > 0 {
		h.renderExprs = make([]exprHelper, len(post.RenderExprs))
		h.renderTypes = make([]sqlbase.ColumnType, len(post.RenderExprs))
		for i, expr := range post.RenderExprs {
			if err := h.renderExprs[i].init(expr, types, evalCtx); err != nil {
				return err
			}
			h.renderTypes[i] = sqlbase.DatumTypeToColumnType(h.renderExprs[i].expr.ResolvedType())
		}
	}

	h.offset = post.Offset
	if post.Limit == 0 || post.Limit >= math.MaxUint64-h.offset {
		h.maxRowIdx = math.MaxUint64
	} else {
		h.maxRowIdx = h.offset + post.Limit
	}

	return nil
}

// neededColumns calculates the set of internal processor columns that are
// actually used by the post-processing stage.
func (h *procOutputHelper) neededColumns() []bool {
	needed := make([]bool, h.numInternalCols)
	if h.outputCols == nil && h.renderExprs == nil {
		// No projection or rendering; all columns are needed.
		for i := range needed {
			needed[i] = true
		}
		return needed
	}
	for _, c := range h.outputCols {
		needed[c] = true
	}
	if h.filter != nil {
		for i := range needed {
			if !needed[i] {
				needed[i] = h.filter.vars.IndexedVarUsed(i)
			}
		}
	}
	if h.renderExprs != nil {
		for i := range needed {
			if !needed[i] {
				for j := range h.renderExprs {
					if h.renderExprs[j].vars.IndexedVarUsed(i) {
						needed[i] = true
						break
					}
				}
			}
		}
	}
	return needed
}

// emitHelper is a utility wrapper on top of procOutputHelper.emitRow().
// It takes a row to emit and, if anything happens other than the normal
// situation where the emitting succeeds and the consumer still needs rows, both
// the (potentially many) inputs and the output are properly closed after
// potentially draining the inputs. It's allowed to not pass any inputs, in
// which case nothing will be drained (this can happen when the caller has
// already fully consumed the inputs).
//
// As opposed to emitRow(), this also supports metadata rows which bypass the
// procOutputHelper and are routed directly to its output.
//
// If the consumer signals the producer to drain, the message is relayed and all
// the draining metadata is consumed and forwarded.
//
// inputs are optional.
//
// Returns true if more rows are needed, false otherwise. If false is returned
// both the inputs and the output have been properly closed.
func emitHelper(
	ctx context.Context,
	output *procOutputHelper,
	row sqlbase.EncDatumRow,
	meta ProducerMetadata,
	inputs ...RowSource,
) bool {
	var consumerStatus ConsumerStatus
	if !meta.Empty() {
		if row != nil {
			log.Fatalf(ctx, "both row data and metadata in the same emitHelper call. "+
				"row: %s. meta: %+v", row, meta)
		}
		// Bypass emitRow() and send directly to output.output.
		consumerStatus = output.output.Push(nil /* row */, meta)
	} else {
		var err error
		consumerStatus, err = output.emitRow(ctx, row)
		if err != nil {
			output.output.Push(nil /* row */, ProducerMetadata{Err: err})
			for _, input := range inputs {
				input.ConsumerClosed()
			}
			output.close()
			return false
		}
	}
	switch consumerStatus {
	case NeedMoreRows:
		return true
	case DrainRequested:
		log.VEventf(ctx, 1, "no more rows required. drain requested.")
		DrainAndClose(ctx, output.output, nil /* cause */, inputs...)
		return false
	case ConsumerClosed:
		log.VEventf(ctx, 1, "no more rows required. Consumer shut down.")
		for _, input := range inputs {
			input.ConsumerClosed()
		}
		output.close()
		return false
	default:
		log.Fatalf(ctx, "unexpected consumerStatus: %d", consumerStatus)
		return false
	}
}

// emitRow sends a row through the post-processing stage. The same row can be
// reused.
//
// It returns the consumer's status that was observed when pushing this row. If
// an error is returned, it's coming from the procOutputHelper's filtering or
// rendering processing; the output has not been closed.
//
// Note: check out emitHelper() for a useful wrapper.
func (h *procOutputHelper) emitRow(
	ctx context.Context, row sqlbase.EncDatumRow,
) (ConsumerStatus, error) {
	if h.rowIdx >= h.maxRowIdx {
		return DrainRequested, nil
	}
	if h.filter != nil {
		// Filtering.
		passes, err := h.filter.evalFilter(row)
		if err != nil {
			return ConsumerClosed, err
		}
		if !passes {
			if log.V(3) {
				log.Infof(ctx, "filtered out row %s", row)
			}
			return NeedMoreRows, nil
		}
	}
	h.rowIdx++
	if h.rowIdx <= h.offset {
		// Suppress row.
		return NeedMoreRows, nil
	}
	var outRow sqlbase.EncDatumRow
	if h.renderExprs != nil {
		// Rendering.
		outRow = h.rowAlloc.AllocRow(len(h.renderExprs))
		for i := range h.renderExprs {
			datum, err := h.renderExprs[i].eval(row)
			if err != nil {
				return ConsumerClosed, err
			}
			outRow[i] = sqlbase.DatumToEncDatum(h.renderTypes[i], datum)
		}
	} else if h.outputCols != nil {
		// Projection.
		outRow = h.rowAlloc.AllocRow(len(h.outputCols))
		for i, col := range h.outputCols {
			outRow[i] = row[col]
		}
	} else {
		// No rendering or projection.
		outRow = h.rowAlloc.AllocRow(len(row))
		copy(outRow, row)
	}
	if log.V(3) {
		log.InfofDepth(ctx, 1, "pushing row %s", outRow)
	}
	if r := h.output.Push(outRow, ProducerMetadata{}); r != NeedMoreRows {
		log.VEventf(ctx, 1, "no more rows required. drain requested: %t",
			r == DrainRequested)
		return r, nil
	}
	if h.rowIdx == h.maxRowIdx {
		log.VEventf(ctx, 1, "hit row limit; asking producer to drain")
		return DrainRequested, nil
	}
	return NeedMoreRows, nil
}

func (h *procOutputHelper) close() {
	h.output.ProducerDone()
}

// noopProcessor is a processor that simply passes rows through from the
// synchronizer to the post-processing stage. It can be useful for its
// post-processing or in the last stage of a computation, where we may only
// need the synchronizer to join streams.
type noopProcessor struct {
	flowCtx *FlowCtx
	input   RowSource
	out     procOutputHelper
}

var _ processor = &noopProcessor{}

func newNoopProcessor(
	flowCtx *FlowCtx, input RowSource, post *PostProcessSpec, output RowReceiver,
) (*noopProcessor, error) {
	n := &noopProcessor{flowCtx: flowCtx, input: input}
	if err := n.out.init(post, input.Types(), &flowCtx.evalCtx, output); err != nil {
		return nil, err
	}
	return n, nil
}

// processorSpan creates a child span for a processor (if we are doing any
// tracing). The returned span needs to be finished using tracing.FinishSpan.
func processorSpan(ctx context.Context, name string) (context.Context, opentracing.Span) {
	parentSp := opentracing.SpanFromContext(ctx)
	if parentSp == nil || tracing.IsBlackHoleSpan(parentSp) {
		return ctx, nil
	}
	newSpan := tracing.StartChildSpan(name, parentSp, true /* separateRecording */)
	return opentracing.ContextWithSpan(ctx, newSpan), newSpan
}

// Run is part of the processor interface.
func (n *noopProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	ctx, span := processorSpan(ctx, "noop")
	defer tracing.FinishSpan(span)

	for {
		row, meta := n.input.Next()
		if row == nil && meta.Empty() {
			sendTraceData(ctx, n.out.output)
			n.out.close()
			return
		}
		if !emitHelper(ctx, &n.out, row, meta, n.input) {
			return
		}
	}
}

func (f *Flow) newProcessor(
	core *ProcessorCoreUnion,
	post *PostProcessSpec,
	inputs []RowSource,
	outputs []RowReceiver,
) (processor, error) {
	if core.Noop != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newNoopProcessor(&f.FlowCtx, inputs[0], post, outputs[0])
	}
	if core.Values != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		return newValuesProcessor(&f.FlowCtx, core.Values, post, outputs[0])
	}
	if core.TableReader != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		return newTableReader(&f.FlowCtx, core.TableReader, post, outputs[0])
	}
	if core.JoinReader != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newJoinReader(&f.FlowCtx, core.JoinReader, inputs[0], post, outputs[0])
	}
	if core.Sorter != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		newProcessorID := atomic.AddUint64(f.processorRegistry, 1)
		return newSorter(&f.FlowCtx, core.Sorter, inputs[0], post, outputs[0], f.localStorage, newProcessorID)
	}
	if core.Distinct != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newDistinct(&f.FlowCtx, core.Distinct, inputs[0], post, outputs[0])
	}
	if core.Aggregator != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newAggregator(&f.FlowCtx, core.Aggregator, inputs[0], post, outputs[0])
	}
	if core.MergeJoiner != nil {
		if err := checkNumInOut(inputs, outputs, 2, 1); err != nil {
			return nil, err
		}
		return newMergeJoiner(
			&f.FlowCtx, core.MergeJoiner, inputs[0], inputs[1], post, outputs[0],
		)
	}
	if core.HashJoiner != nil {
		if err := checkNumInOut(inputs, outputs, 2, 1); err != nil {
			return nil, err
		}
		return newHashJoiner(&f.FlowCtx, core.HashJoiner, inputs[0], inputs[1], post, outputs[0])
	}
	if core.Backfiller != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		switch core.Backfiller.Type {
		case BackfillerSpec_Index:
			return newIndexBackfiller(&f.FlowCtx, *core.Backfiller, post, outputs[0])
		case BackfillerSpec_Column:
			return newColumnBackfiller(&f.FlowCtx, *core.Backfiller, post, outputs[0])
		}
	}
	if core.SetOp != nil {
		if err := checkNumInOut(inputs, outputs, 2, 1); err != nil {
			return nil, err
		}
		return newAlgebraicSetOp(&f.FlowCtx, core.SetOp, inputs[0], inputs[1], post, outputs[0])
	}
	return nil, errors.Errorf("unsupported processor core %s", core)
}

// Equals returns true if two aggregation specifiers are identical (and thus
// will always yield the same result).
func (a AggregatorSpec_Aggregation) Equals(b AggregatorSpec_Aggregation) bool {
	if a.Func != b.Func || a.Distinct != b.Distinct {
		return false
	}
	if a.FilterColIdx == nil {
		if b.FilterColIdx != nil {
			return false
		}
	} else {
		if a.FilterColIdx == nil || *a.FilterColIdx != *b.FilterColIdx {
			return false
		}
	}
	if len(a.ColIdx) != len(b.ColIdx) {
		return false
	}
	for i, c := range a.ColIdx {
		if c != b.ColIdx[i] {
			return false
		}
	}
	return true
}
