package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.chunk.BlackHoleBlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTE_ON_MYSQL;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTOR;

public class RangeScanSortExec extends TableScanSortExec {

    public RangeScanSortExec(LogicalView logicalView, ExecutionContext context, TableScanClient scanClient,
                             long maxRowCount, long skipped, long fetched, SpillerFactory spillerFactory,
                             List<DataType> dataTypeList) {
        super(logicalView, context, scanClient, maxRowCount, skipped, fetched, spillerFactory, dataTypeList);
    }

    @Override
    void doOpen() {
        checkStatus();
        reorderSplits();
        if (fetched > 0) {
            realOpen();
        }
    }

    /**
     * Reorders splits. This method sorts splits based on the logical table name and schema,
     * and then updates the sorted list in the scanClient.
     */
    private void reorderSplits() {
        List<Split> splitList = scanClient.splitList;
        String logicalTableName = logicalView.getTableNames().get(0);
        String logicalSchema = logicalView.getSchemaName();
        if (!splitList.isEmpty()) {
            List<Integer> partitions = getPhysicalPartitions(splitList, logicalSchema, logicalTableName);
            int[] index = sortPartitions(partitions);
            List<Split> orderedSplits = IntStream.of(index)
                .mapToObj(splitList::get).collect(Collectors.toList());
            // Clears the current split list and adds the sorted splits back to the scanClient
            scanClient.splitList.clear();
            scanClient.splitList.addAll(orderedSplits);
        }
    }

    /**
     * Checks the scanning status to ensure specific conditions are met.
     * This method takes no parameters and returns no value.
     * It performs the following checks:
     * 1. Ensures there are no more splits to process.
     * 2. Confirms that all splits are of type JdbcSplit.
     * 3. Verifies that the logical view contains only one table.
     * If any check fails, a TddlRuntimeException is thrown.
     */
    private void checkStatus() {
        // Check if there are no more splits to process
        if (!scanClient.noMoreSplit()) {
            throw new TddlRuntimeException(ERR_EXECUTE_ON_MYSQL, "RangeScanSortExec input split not ready");
        }

        List<Split> splitList = scanClient.splitList;
        // Check if all splits are JdbcSplits
        boolean allJdbcSplit = splitList.stream().allMatch(split -> split.getConnectorSplit() instanceof JdbcSplit);
        if (!allJdbcSplit) {
            throw new TddlRuntimeException(ERR_EXECUTOR, "all splits should be jdbc split under range scan mode");
        }
        // Check if the logical view contains exactly one table
        if (logicalView.getTableNames().size() != 1) {
            throw new TddlRuntimeException(ERR_EXECUTOR,
                "logical view should contains only one table under range scan mode");
        }

        boolean allSplitHasOnePhyTable = splitList.stream().allMatch(split -> {
            JdbcSplit jdbcSplit = (JdbcSplit) split.getConnectorSplit();
            return jdbcSplit.getTableNames().size() == 1 && jdbcSplit.getTableNames().get(0).size() == 1;
        });
        if (!allSplitHasOnePhyTable) {
            throw new TddlRuntimeException(ERR_EXECUTOR,
                "all splits should has one physical table under range scan mode");
        }
    }

    /**
     * Retrieves physical partition numbers from a list of splits based on the logical schema and table name.
     *
     * @param splitList A list of split objects.
     * @param logicalSchema The logical schema name.
     * @param logicalTableName The logical table name.
     * @return A list of integer representing the physical partition IDs.
     * @throws TddlRuntimeException If no partition information is found in range scan mode.
     */
    private static List<Integer> getPhysicalPartitions(List<Split> splitList, String logicalSchema,
                                                       String logicalTableName) {
        // Convert the splitList to JdbcSplits and extract the physical schema and table names as pairs
        List<Pair<String, String>> phySchemaAndPhyTables =
            splitList.stream().map(split -> (JdbcSplit) split.getConnectorSplit())
                .map(split -> Pair.of(split.getDbIndex(), split.getTableNames().get(0).get(0))).collect(
                    Collectors.toList());

        // Calculate the physical partition numbers using the logical schema, logical table, and extracted pairs, then collect them
        List<Integer> partitions = phySchemaAndPhyTables.stream()
            .map(pair -> PartitionUtils.calcPartition(logicalSchema, logicalTableName, pair.getKey(), pair.getValue()))
            .collect(Collectors.toList());

        // Check if any partition was not found, and if so, throw an exception
        boolean notFoundPart = partitions.stream().anyMatch(part -> part < 0);
        if (notFoundPart) {
            throw new TddlRuntimeException(ERR_EXECUTOR, "not found partition info under range scan mode");
        }
        return partitions;
    }

    /**
     * Sorts the given list of partitions.
     *
     * @param partitions A list of integers representing the partitions to be sorted.
     * @return index An array of indices representing the sorted order of the partitions.
     */
    private int[] sortPartitions(List<Integer> partitions) {
        int[] index = new int[partitions.size()];
        for (int i = 0; i < index.length; i++) {
            index[i] = i;
        }
        // Obtain sorting information from the logical view
        Sort sort = (Sort) logicalView.getOptimizedPushedRelNodeForMetaQuery();
        RelCollation collation = sort.getCollation();
        RelFieldCollation.Direction direction = collation.getFieldCollations().get(0).direction;
        // Determine whether the sort order is descending
        boolean isDesc = direction.isDescending();
        IntArrays.quickSort(index, new AbstractIntComparator() {
            @Override
            public int compare(int position1, int position2) {
                int part1 = partitions.get(position1);
                int part2 = partitions.get(position2);
                // Compare according to sort direction
                return !isDesc ? part1 - part2 : part2 - part1;
            }
        });
        return index;
    }

    /**
     * Fetches a sorted Chunk.
     * This method is responsible for retrieving data from the result set and constructing a Chunk to return. If the result set is empty or has been fully iterated, it attempts to fetch a new result set from the scan client.
     * If no more data is available, it notifies completion and returns null.
     *
     * @return Chunk A Chunk containing the fetched data, or null if no data is available.
     */
    @Override
    protected Chunk fetchSortedChunk() {
        // Try to get a data chunk from the current result set. If it's null, attempt to pop a new one from the scan client.
        if (consumeResultSet == null) {
            consumeResultSet = scanClient.popResultSet();
            // If no result set is available, notify completion and try again.
            if (consumeResultSet == null) {
                notifyFinish();
                if (consumeResultSet == null) {
                    return null;
                }
            }
        }
        int count = 0;
        // Loop until the chunk limit is reached or there is no more data
        while (count < chunkLimit && fetched > 0 && !isFinish) {
            // If the current result set is exhausted, close it and fetch a new one
            if (!consumeResultSet.next()) {
                consumeResultSet.close();
                consumeResultSet = scanClient.popResultSet();
                // If no more result sets, notify completion and exit the loop
                if (consumeResultSet == null) {
                    notifyFinish();
                    if (consumeResultSet == null) {
                        break;
                    }
                }
                // If the new result set is also empty, continue the loop
                if (!consumeResultSet.next()) {
                    continue;
                }
            }
            try {
                // Fill the chunk based on the result set's async mode
                if (consumeResultSet.isPureAsyncMode()) {
                    // Handle skipping a specified number of rows
                    if (skipped > 0) {
                        BlockBuilder[] blackHoleBlockBuilders = new BlockBuilder[blockBuilders.length];
                        for (int i = 0; i < blockBuilders.length; ++i) {
                            blackHoleBlockBuilders[i] = new BlackHoleBlockBuilder();
                        }
                        int skip = consumeResultSet.fillChunk(dataTypes, blackHoleBlockBuilders, (int) skipped);
                        assert skip == blackHoleBlockBuilders[0].getPositionCount();
                        skipped -= skip;
                        // If more rows need to be skipped, continue the loop
                        if (skipped > 0) {
                            continue;
                        }
                    }
                    // Fill the chunk up to the limit or until no more data is available
                    int maxFill = (int) Math.min(chunkLimit - count, fetched);
                    final int filled = consumeResultSet.fillChunk(dataTypes, blockBuilders, maxFill);
                    count += filled;
                    fetched -= filled;
                } else {
                    // Row filling logic for non-pure async mode
                    if (skipped > 0) {
                        BlockBuilder[] blackHoleBlockBuilders = new BlockBuilder[blockBuilders.length];
                        for (int i = 0; i < blockBuilders.length; ++i) {
                            blackHoleBlockBuilders[i] = new BlackHoleBlockBuilder();
                        }
                        ResultSetCursorExec.buildOneRow(consumeResultSet.current(), dataTypes, blackHoleBlockBuilders,
                            context);
                        skipped -= 1;
                        // If more rows need to be skipped, continue the loop
                        if (skipped > 0) {
                            continue;
                        }
                    }
                    appendRow(consumeResultSet);
                    count++;
                    fetched--;
                }
                // Exit the loop if the fetched data limit is reached
                if (fetched <= 0) {
                    break;
                }
            } catch (Exception ex) {
                TddlRuntimeException exception =
                    new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ex, ex.getMessage());
                scanClient.setException(exception);
                if (isFinish) {
                    log.debug(context.getTraceId() + " here occur error, but current scan is closed!", ex);
                    return null;
                } else {
                    scanClient.throwIfFailed();
                }
            }
        }
        // Return null if no data was fetched; otherwise, build and return the chunk
        if (count == 0) {
            return null;
        } else {
            Chunk ret = buildChunkAndReset();
            return ret;
        }
    }
}
