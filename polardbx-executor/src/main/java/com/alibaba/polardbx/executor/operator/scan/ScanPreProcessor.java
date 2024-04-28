package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.operator.scan.impl.PreheatFileMeta;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.RoaringBitmap;

import java.util.SortedMap;
import java.util.concurrent.ExecutorService;

/**
 * Any time-consuming processing for preparation of columnar table-scan will converge here.
 */
public interface ScanPreProcessor {

    void addFile(Path filePath);

    /**
     * Prepare necessary data, like pruning
     */
    ListenableFuture<?> prepare(ExecutorService executor, String traceId, ColumnarTracer tracer);

    /**
     * Check if preparation is done.
     */
    boolean isPrepared();

    /**
     * Get pruning result represented by row-group matrix with given file path.
     */
    SortedMap<Integer, boolean[]> getPruningResult(Path filePath);

    /**
     * Get preheated file meta by file path.
     */
    PreheatFileMeta getPreheated(Path filePath);

    /**
     * Get deletion bitmap by file path.
     */
    RoaringBitmap getDeletion(Path filePath);

    /**
     * Throw runtime exception if precessing is failed.
     */
    default void throwIfFailed() {
    }
}

