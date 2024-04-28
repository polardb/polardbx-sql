package com.alibaba.polardbx.executor.operator.scan;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public interface IOStatus<BATCH> {
    /**
     * The unique identifier of the scan work.
     */
    String workId();

    ScanState state();

    ListenableFuture<?> isBlocked();

    void addResult(BATCH batch);

    void addResults(List<BATCH> batches);

    BATCH popResult();

    void addException(Throwable t);

    void throwIfFailed();

    void finish();

    void close();

    long rowCount();
}
