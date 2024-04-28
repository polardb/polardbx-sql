package com.alibaba.polardbx.executor.operator.scan;

/**
 * Build the evaluator for push-down predicate.
 * The column data will not be fetched util first access.
 *
 * @param <BATCH> class of column batch
 * @param <BITMAP> class of bitmap to store the filtered positions.
 */
public interface LazyEvaluatorBuilder<BATCH, BITMAP> {

    LazyEvaluator<BATCH, BITMAP> build();
}
