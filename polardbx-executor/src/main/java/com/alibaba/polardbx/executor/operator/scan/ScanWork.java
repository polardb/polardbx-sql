package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * A splittable scan work represent a file fragment (range <= chunk threshold) in Columnar Split
 * which can be scheduled among different Drivers.
 *
 * @param <SplitT> class of columnar split
 */
public interface ScanWork<SplitT extends ColumnarSplit, BATCH> {
    String EVALUATION_TIMER = "Evaluation.Timer";

    /**
     * Allocate the IO thread resource and invoke the scan work processing.
     *
     * @param executor IO thread.
     */
    void invoke(ExecutorService executor);

    void cancel();

    /**
     * Get the IO status of current IO task.
     */
    IOStatus<BATCH> getIOStatus();

    /**
     * Get the unique identifier of scan work.
     */
    String getWorkId();

    /**
     * Get the metrics of this scan work.
     */
    RuntimeMetrics getMetrics();

    void close() throws IOException;

    void close(boolean force);
}
