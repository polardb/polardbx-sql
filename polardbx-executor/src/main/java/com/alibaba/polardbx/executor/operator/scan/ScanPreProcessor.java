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

