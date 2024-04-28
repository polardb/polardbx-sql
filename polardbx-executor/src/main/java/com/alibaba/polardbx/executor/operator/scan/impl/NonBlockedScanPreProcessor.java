package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.ScanPreProcessor;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.RoaringBitmap;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;

/**
 * A non-blocked implementation of ScanPreProcessor with prepared meta and pruning result.
 */
public class NonBlockedScanPreProcessor implements ScanPreProcessor {
    private final Set<Path> filePaths;
    private final PreheatFileMeta preheatFileMeta;
    private final SortedMap<Integer, boolean[]> matrix;
    private final RoaringBitmap deletion;

    public NonBlockedScanPreProcessor(PreheatFileMeta preheatFileMeta,
                                      SortedMap<Integer, boolean[]> matrix,
                                      RoaringBitmap deletion) {
        this.filePaths = new HashSet<>();
        this.preheatFileMeta = preheatFileMeta;
        this.matrix = matrix;
        this.deletion = deletion;
    }

    @Override
    public void addFile(Path filePath) {
        filePaths.add(filePath);
    }

    @Override
    public ListenableFuture<?> prepare(ExecutorService executor, String traceId, ColumnarTracer tracer) {
        return Futures.immediateFuture(null);
    }

    @Override
    public boolean isPrepared() {
        return true;
    }

    @Override
    public SortedMap<Integer, boolean[]> getPruningResult(Path filePath) {
        if (filePath != null && filePaths.contains(filePath)) {
            return matrix;
        }
        return null;
    }

    @Override
    public PreheatFileMeta getPreheated(Path filePath) {
        if (filePath != null && filePaths.contains(filePath)) {
            return preheatFileMeta;
        }
        return null;
    }

    @Override
    public RoaringBitmap getDeletion(Path filePath) {
        if (filePath != null && filePaths.contains(filePath)) {
            return deletion;
        }
        return null;
    }
}
