package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluatorBuilder;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import org.apache.calcite.rex.RexNode;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultLazyEvaluatorBuilder implements LazyEvaluatorBuilder<Chunk, BitSet> {

    /**
     * Input types of table scan including filter and project columns.
     */
    List<DataType<?>> inputTypes;

    /**
     * Contain the parameters list in expression.
     */
    ExecutionContext context;

    /**
     * Represent the abstract tree structure of expression.
     */
    RexNode rexNode;

    /**
     * The ratio to distinguish between partial-selected case and post-intersection case.
     */
    double ratio;

    public DefaultLazyEvaluatorBuilder setRexNode(RexNode rexNode) {
        this.rexNode = rexNode;
        return this;
    }

    public DefaultLazyEvaluatorBuilder setRatio(double ratio) {
        this.ratio = ratio;
        return this;
    }

    /**
     * In normal case, InputTypes includes only referenced columns
     */
    public DefaultLazyEvaluatorBuilder setInputTypes(List<DataType<?>> inputTypes) {
        this.inputTypes = inputTypes;
        return this;
    }

    public DefaultLazyEvaluatorBuilder setContext(ExecutionContext context) {
        this.context = context;
        return this;
    }

    @Override
    public LazyEvaluator<Chunk, BitSet> build() {
        RexNode root = VectorizedExpressionBuilder.rewriteRoot(rexNode, true);

        InputRefTypeChecker inputRefTypeChecker = new InputRefTypeChecker(inputTypes);
        root = root.accept(inputRefTypeChecker);

        Rex2VectorizedExpressionVisitor converter =
            new Rex2VectorizedExpressionVisitor(context, inputTypes.size());
        VectorizedExpression condition = root.accept(converter);

        // Data types of intermediate results or final results.
        List<DataType<?>> filterOutputTypes = converter.getOutputDataTypes();

        // placeholder for input and output blocks
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(inputTypes)
            .addEmptySlots(filterOutputTypes)
            .addChunkLimit(context.getExecutorChunkLimit())
            .addOutputIndexes(new int[] {condition.getOutputIndex()})
            .addLiteralBitmap(converter.getLiteralBitmap())
            .build();

        // prepare filter bitmap
        List<Integer> inputVectorIndex = VectorizedExpressionUtils.getInputIndex(condition);

        // filterVectorBitmap means which positions of vectors should be replaced by input-blocks.
        boolean[] filterVectorBitmap = new boolean[inputTypes.size() + filterOutputTypes.size()];
        Arrays.fill(filterVectorBitmap, false);
        for (int i : inputVectorIndex) {
            filterVectorBitmap[i] = true;
        }

        DefaultLazyEvaluator lazyEvaluator = new DefaultLazyEvaluator(
            condition,
            preAllocatedChunk,
            true,
            inputTypes.size(),
            filterVectorBitmap,
            context,
            inputTypes.stream().map(DataType.class::cast).collect(Collectors.toList()),
            ratio
        );

        return lazyEvaluator;
    }
}
