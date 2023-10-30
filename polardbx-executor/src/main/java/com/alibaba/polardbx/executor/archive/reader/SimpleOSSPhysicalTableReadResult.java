/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.archive.reader;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.archive.columns.ColumnProviders;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.mpp.operator.factory.HashAggExecutorFactory;
import com.alibaba.polardbx.executor.operator.HashAggExec;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimpleOSSPhysicalTableReadResult {

    protected List<ColumnProvider<?>> columnProviders;

    protected SessionProperties sessionProperties;

    protected List<AggregateCall> aggCalls;
    protected ImmutableBitSet groupSet;
    protected RelDataType dataType;

    public SimpleOSSPhysicalTableReadResult() {
    }

    public SimpleOSSPhysicalTableReadResult(List<DataType<?>> dataTypeList, ExecutionContext executionContext,
                                            OSSTableScan ossTableScan) {
        this.columnProviders =
            dataTypeList.stream()
                .map(t -> ColumnProviders.getProvider(t)).collect(Collectors.toList());

        LogicalAggregate agg = ossTableScan.getAgg();
        if (agg != null) {
            this.aggCalls = agg.getAggCallList();
            this.groupSet = agg.getGroupSet();
            this.dataType = agg.getRowType();
        } else {
            this.aggCalls = null;
            this.groupSet = null;
            this.dataType = null;
        }
        this.sessionProperties = SessionProperties.fromExecutionContext(executionContext);
    }

    public Chunk next(VectorizedRowBatch batch,
                      OSSColumnTransformer ossColumnTransformer,
                      List<DataType<?>> inProjectDataTypeList,
                      BlockBuilder[] blockBuilders,
                      VectorizedExpression condition,
                      MutableChunk preAllocatedChunk,
                      int[] filterBitmap,
                      int[] outProject,
                      ExecutionContext context) {
        int blockCount = blockBuilders.length;
        final int resultRows = batch.size;
        RandomAccessBlock[] blocksForCompute = new RandomAccessBlock[filterBitmap.length];

        int inProjectCount = inProjectDataTypeList.size();

        // todo: fix missing column and datatype convert here(shengyu)

        // make block for pre-filter
        for (int i = 0; i < inProjectCount; i++) {
            if (filterBitmap[i] == 1) {
                DataType dataType = inProjectDataTypeList.get(i);
                BlockBuilder blockBuilder = BlockBuilders.create(dataType, context);

                blocksForCompute[i] = (RandomAccessBlock) transformDataType(
                    blockBuilder,
                    inProjectDataTypeList.get(i),
                    batch.cols,
                    i,
                    null,
                    0,
                    resultRows,
                    context,
                    ossColumnTransformer
                );
            }
        }

        // pre-filter
        Pair<Integer, int[]> sel =
            preFilter(condition, preAllocatedChunk, filterBitmap, context,
                resultRows, blocksForCompute, inProjectCount);

        int selSize = sel.getKey();
        int[] selection = sel.getValue();
        if (selSize == 0) {
            return null;
        }

        // buffer to block builders
        if (!withAgg()) {
            Block[] blocks = new Block[blockBuilders.length];
            for (int i = 0; i < blockCount; i++) {
                int colId = outProject[i];
                blocks[i] = transformDataType(
                    blockBuilders[i],
                    inProjectDataTypeList.get(colId),
                    batch.cols,
                    colId,
                    selection,
                    selSize,
                    0,
                    context,
                    ossColumnTransformer
                );
            }
            return new Chunk(blocks);
        }

        // deal with agg with filter
        Block[] blocks = new Block[outProject.length];
        for (int i = 0; i < outProject.length; i++) {
            int colId = outProject[i];
            DataType dataType = inProjectDataTypeList.get(colId);
            BlockBuilder blockBuilder = BlockBuilders.create(dataType, context);

            blocks[i] = transformDataType(
                blockBuilder,
                dataType,
                batch.cols,
                colId,
                selection,
                selSize,
                0,
                context,
                ossColumnTransformer
            );
        }
        return aggExec(new Chunk(blocks), inProjectDataTypeList, outProject, context);
    }

    public Chunk next(VectorizedRowBatch batch,
                      OSSColumnTransformer ossColumnTransformer,
                      List<DataType<?>> inProjectDataTypeList,
                      BlockBuilder[] blockBuilders,
                      ExecutionContext context) {
        final int resultRows = batch.size;
        // buffer to block builders
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < inProjectDataTypeList.size(); i++) {
            DataType dataType = inProjectDataTypeList.get(i);
            BlockBuilder blockBuilder = BlockBuilders.create(dataType, context);

            blocks[i] = transformDataType(
                blockBuilder,
                inProjectDataTypeList.get(i),
                batch.cols,
                i,
                null,
                0,
                resultRows,
                context,
                ossColumnTransformer
            );
        }

        if (withAgg()) {
            return aggExec(new Chunk(blocks), inProjectDataTypeList, null, context);
        }
        return new Chunk(blocks);
    }

    /**
     * @param targetBlockBuilder target blockBuilder
     * @param targetDataType target datatype
     * @param columnVectors all column vector read from orc
     * @param colId the column id of target column
     * @param selection selection array of columnVector, null if without filter
     * @param selSize length of selection array
     * @param resultRows length of columnVectors, useful when selection is null
     * @param context ExecutionContext
     * @param ossColumnTransformer datatype transformer
     * @return a block with targetDataType
     */
    Block transformDataType(BlockBuilder targetBlockBuilder,
                            DataType<?> targetDataType,
                            ColumnVector[] columnVectors,
                            int colId,
                            int[] selection,
                            int selSize,
                            int resultRows,
                            ExecutionContext context,
                            OSSColumnTransformer ossColumnTransformer) {
        ColumnMeta sourceColumnMeta = ossColumnTransformer.getSourceColumnMeta(colId);
        ColumnMeta targetColumnMeta = ossColumnTransformer.getTargetColumnMeta(colId);

        TypeComparison ossColumnCompare = ossColumnTransformer.getCompareResult(colId);

        // target column is missing
        if (ossColumnCompare == TypeComparison.MISSING_EQUAL) {
            int rowCount = (selection == null) ? resultRows : selSize;
            return OSSColumnTransformer.fillDefaultValue(
                targetDataType,
                ossColumnTransformer.getInitColumnMeta(colId),
                ossColumnTransformer.getTimeStamp(colId),
                rowCount,
                context);
        }
        if (ossColumnCompare == TypeComparison.MISSING_NO_EQUAL) {
            int rowCount = (selection == null) ? resultRows : selSize;
            return OSSColumnTransformer.fillDefaultValueAndTransform(
                targetColumnMeta,
                ossColumnTransformer.getInitColumnMeta(colId),
                rowCount,
                context);
        }
        Integer colIdInOrc = ossColumnTransformer.getLocInOrc(colId);

        ColumnVector columnVector = columnVectors[colIdInOrc];
        // same data type
        if (ossColumnCompare == TypeComparison.IS_EQUAL_YES) {
            this.columnProviders.get(colId).transform(
                columnVector,
                targetBlockBuilder,
                selection,
                selSize,
                0,
                resultRows,
                sessionProperties
            );
            return targetBlockBuilder.build();
        }

        // data type transform
        return OSSColumnTransformer.transformDataType(
            targetColumnMeta,
            sourceColumnMeta,
            columnVector,
            selection,
            selSize,
            resultRows,
            sessionProperties,
            context);
    }

    @NotNull
    protected Pair<Integer, int[]> preFilter(VectorizedExpression condition, MutableChunk preAllocatedChunk,
                                             int[] filterBitmap, ExecutionContext context, int resultRows,
                                             RandomAccessBlock[] blocksForCompute, int inProjectCount) {
        for (int i = 0; i < inProjectCount; i++) {
            if (filterBitmap[i] == 1) {
                preAllocatedChunk.setSlotAt(blocksForCompute[i], i);
            }
        }
        preAllocatedChunk.reallocate(resultRows, inProjectCount, true);
        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        condition.eval(evaluationContext);

        // get filtered block
        RandomAccessBlock filteredBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());

        boolean[] nulls = filteredBlock.nulls();
        boolean[] inputArray = null;
        if (filteredBlock instanceof LongBlock) {
            long[] longInputArray = ((LongBlock) filteredBlock).longArray();
            inputArray = new boolean[longInputArray.length];
            for (int i = 0; i < inputArray.length; i++) {
                inputArray[i] = longInputArray[i] == 1;
            }
        } else if (filteredBlock instanceof IntegerBlock) {
            int[] intInputArray = ((IntegerBlock) filteredBlock).intArray();
            inputArray = new boolean[intInputArray.length];
            for (int i = 0; i < inputArray.length; i++) {
                inputArray[i] = intInputArray[i] == 1;
            }
        } else {
            GeneralUtil.nestedException("Invalid result block: " + filteredBlock.getClass());
        }

        // convert to selection (need cache)
        int[] selection = new int[inputArray.length];
        int selSize = 0;
        for (int pos = 0; pos < selection.length; pos++) {
            if (nulls[pos] == false && inputArray[pos] == true) {
                selection[selSize++] = pos;
            }
        }

        return Pair.of(selSize, selection);
    }

    protected Chunk aggExec(Chunk chunk, List<DataType<?>> inProjectDataTypeList, int[] outProject,
                            ExecutionContext context) {
        List<DataType> columns = outProject == null ?
            IntStream.range(0, inProjectDataTypeList.size()).mapToObj(i -> inProjectDataTypeList.get(i)).collect(
                Collectors.toList())
            :
            IntStream.range(0, outProject.length).mapToObj(i -> inProjectDataTypeList.get(outProject[i])).collect(
                Collectors.toList());
        int[] groups = AggregateUtils.convertBitSet(groupSet);
        MemoryAllocatorCtx memoryAllocator = context.getMemoryPool().getMemoryAllocatorCtx();
        List<Aggregator> aggregators =
            AggregateUtils.convertAggregators(columns, CalciteUtils.getTypes(dataType), aggCalls, context, memoryAllocator);
        HashAggExec aggExec = new HashAggExec(columns, groups, aggregators, CalciteUtils.getTypes(dataType),
            1, null, context);
        aggExec.openConsume();
        aggExec.consumeChunk(chunk);
        aggExec.buildConsume();
        aggExec.open();
        Chunk ret = aggExec.nextChunk();
        aggExec.close();
        return ret;
    }

    private boolean withAgg() {
        return aggCalls != null;
    }
}
