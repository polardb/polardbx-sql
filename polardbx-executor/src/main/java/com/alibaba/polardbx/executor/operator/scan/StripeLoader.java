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

import org.apache.orc.impl.InStream;
import org.apache.orc.impl.StreamName;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A Stripe loader is related to stripe-level IO processing.
 * We must merge IO tasks of several row groups in one stream, and manage them in stripe-level.
 * <p>
 * These methods should be called by ColumnReader or some external IO task scheduler.
 */
public interface StripeLoader extends Closeable {
    void open();

    /**
     * Load several columns with different row group bitmaps.
     *
     * @param columnIds column id list.
     * @param rowGroupBitmaps row group bitmaps of columns
     * @return A future of mapping from stream name to InStream object which hold the buffered compressed data.
     */
    default CompletableFuture<Map<StreamName, InStream>> load(List<Integer> columnIds,
                                                              Map<Integer, boolean[]> rowGroupBitmaps) {
        return load(columnIds, rowGroupBitmaps, null);
    }

    /**
     * invoke Stripe-level IO processing.
     * It's a matrix of stream * row_group.
     * Mapping: stream name <-> stream information <-> InStream
     * stream-manager to hold the whole stream-information within stripe.
     *
     * @param columnId target column to load.
     * @param targetRowGroups target row group range to load.
     * @return A future of mapping from stream name to InStream object which hold the buffered compressed data.
     */
    default CompletableFuture<Map<StreamName, InStream>> load(int columnId, boolean[] targetRowGroups) {
        return load(columnId, targetRowGroups, null);
    }

    CompletableFuture<Map<StreamName, InStream>> load(List<Integer> columnIds, Map<Integer, boolean[]> rowGroupBitmaps,
                                                      Supplier<Boolean> controller);

    CompletableFuture<Map<StreamName, InStream>> load(int columnId, boolean[] targetRowGroups,
                                                      Supplier<Boolean> controller);

    /**
     * Clear the memory resources of given stream.
     *
     * @return Released bytes.
     */
    long clearStream(StreamName streamName);

}
