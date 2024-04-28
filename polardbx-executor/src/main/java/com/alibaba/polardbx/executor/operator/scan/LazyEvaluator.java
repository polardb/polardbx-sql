package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import org.roaringbitmap.RoaringBitmap;

import java.util.BitSet;

/**
 * Evaluate push-down predicate and store the result into batch and selection.
 *
 * @param <BATCH> class of column batch
 * @param <BITMAP> class of bitmap to store the filtered positions.
 */
public interface LazyEvaluator<BATCH, BITMAP> {

    /**
     * Get bound vectorized expression tree.
     */
    VectorizedExpression getCondition();

    /**
     * Evaluate push-down predicate and store the result into selection array.
     *
     * @param batch the batch produced by row-group reader.
     * @param startPosition the start position of this batch in total file.
     * @param positionCount the position count of this batch.
     * @param deletion the file-level deletion bitmap.
     * @return selection array for input batch.
     */
    BITMAP eval(BATCH batch, int startPosition, int positionCount, RoaringBitmap deletion);

    int eval(BATCH batch, int startPosition, int positionCount, RoaringBitmap deletion, boolean[] bitmap);

    /**
     * Check if this evaluator is a constant expression.
     */
    boolean isConstantExpression();
}
