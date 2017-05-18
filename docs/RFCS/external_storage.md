- Feature Name: External Storage for DistSQL processors
- Status: draft
- Start Date: 2017-05-11
- Authors: Arjun Narayan and Alfonso Subiotto Marques
- RFC PR: TDB
- Cockroach Issue: [#15206](https://github.com/cockroachdb/cockroach/issues/15206)

# Summary

Add support for DistSQL processors to use external storage in addition
to memory to store intermediate data when processing large DistSQL
queries.

# Motivation and Background

Currently, DistSQL processors have a hard limit on the amount of
memory they can use before they get OOM-killed. This limits the size
of certain queries, particularly when efficient secondary indices do
not exist, so some amount of computation must be done holding a large
amount of intermediate state at a processor. For instance, a JOIN on
two very large tables, where there is no ordering on either table's
JOIN predicate columns, must either

* Use a HashJoiner, which stores one table in a single large
  hashtable, and stream the other table, looking up for matches on the
  hashtable, emitting rows when appropriate.

* Sort both tables by the JOIN predicate columns, and then stream them
  to a MergeJoiner.

Either of these two solutions runs out of memory at some query size,
and requires external storage to process larger queries.

# Scope

This problem was first encountered in running TPC-H queries on
moderately sized scale factors (5 and above). It was exacerbated by
the fact that DistSQL currently does not plan MergeJoins and resorts
to HashJoins in all cases. TPC-H itself should not be bottlenecked on
the lack of external storage, as relevant secondary indices exist for
all queries that we are aware of, such that optimal planning keeps
tuples flowing smoothly through the dataflow graph without requiring
any processor to accumulate a disproportional number of
tuples. However, users might still want to run the occasional
analytics query without indexes, and we should support that use case.

A stretch goal is to adopt a solution that would also be efficient for
an eventual Naiad-on-DistSQL implementation for materialized view
support. Differential Dataflow processors keep as much intermediate
state as possible, only discarding tuples when forced to, much like
CockroachDB keeps MVCC KVs from older timestamps that are only
compacted if necessary, supporting time travel queries. Unlike regular
DistSQL processors, Differential Dataflow processors take advantage of
computed tuples from older timestamps to avoid recomputation, for
example, if an update is just a revert.

At a minimum, external storage should provide enough swap space that
it stops all DistSQL processors from ever being OOM-killed. Whatever
storage format is chosen, this should be compatible with all DistSQL
processors.


# Detailed requirements

Examining all DistSQL processors, we have the following data
structures used:

| Processor name  | External storage required?  | On Disk Data structure
|-----------------|-----------------------------|----------:|
| NoopCore        | No, used for joining streams| -         |
| TableReader     | No                          | -         |
| JoinReader      | No                          | -         |
| Sorter          | Yes                         |Sorted tree|
| Aggregator      | Yes                         | TODO: read the code to figure it out |
| Distinct        | Yes                         | Hashtable |
| MergeJoiner     | No                          | --        |
| HashJoiner      | Yes                         | Hashtable |
| Values          | No                          | --        |
| AlgebraicSetOp  | Yes                         | Hashtable |



## Sorter
The idea would be to consume as many rows as we can into memory before
overflowing to disk. Our use of RocksDB will ensure that these on-disk rows
are sorted as expected (by providing a comparator).

Once all rows have been consumed we will perform a sort of the rows in-memory
followed by merging these in-memory rows with the on-disk rows, emitting a row
at each step.

### Optimizations
#### Sorting with a limit
Our current implementation for sorting with a limit, k, is to use a max heap of
k elements. A way to combine on-disk storage with this optimization is to add
as many elements up to k as memory can hold before neglecting the max heap and
storing the remaining rows in RocksDB. We can once again perform a merge step
up to k times.

#### Merging in memory rows with on disk rows
In both cases we need to merge memory rows with on disk rows to emit a certain
number of rows in sorted order. It can be the case that at some point enough
memory is available that a batch read from RocksDB can be performed to improve
the total query latency.

## HashJoiner
The HashJoiner will have a hash table in memory that will overflow to disk.
RocksDB will be used as an on-disk map. Once the probe phase is finished, the
HashJoiner will emit all rows from memory and read the on-disk data in batches.

### Possible Optimizations
#### Planning subsequent joins as merge joins
Using RocksDB to store on-disk data will result in this on-disk data sorted by
the join key. The in-memory data could also be sorted so that if any joins are
performed after this HashJoin with the emitted data subsequent joins could be
planned as merge joins.

### Alternatives
#### GRACE Hash Join
A hash table is built in memory with all other data overflowed to disk. The
input stream is gone through completely and any matches emitted. Once the input
stream is completely processed, the next batch of data is loaded into memory and
the process is repeated until there is no data left. This would require a new
processor that allows multiple reads of the same input stream. The upside is
that the storage solution can be extremely simple since the on-disk data does
not need to be sorted and the only reads that happen are sequential.

# Options

1. Use RocksDB
1a. Use the same RocksDB instance as the rest of CockroachDB.
1b. Use a 2nd rocksdb instance, since we need different tunings.

2. Use our own temporary flat files.
2b. What format to use? sorted, unsorted, up to the processor?

# Concerns

Write amplification/read amplification.

Cleaning up data from processors that are done.
