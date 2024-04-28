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

package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;

/**
 * Columnar store management for columnar instance.
 */
public interface ColumnarManager extends ColumnarSchemaTransformer, Purgeable {

    static ColumnarManager getInstance() {
        return DynamicColumnarManager.getInstance();
    }

    /**
     * Reload the columnar manager of current CN, clear all cache and snapshot
     */
    void reload();

    /**
     * Get the latest tso at which the columnar snapshot is visible.
     *
     * @return latest tso value.
     */
    long latestTso();

    /**
     * Set the latest tso to this node.
     *
     * @param tso latest tso
     */
    void setLatestTso(long tso);

    /**
     * Garbage collection for version management:
     * 1. merge all old bitmaps before given tso
     * 2. remove useless csv data cache
     * 3. remove file metas of invisible files.
     *
     * @param tso before which purge the old version.
     */
    void purge(long tso);

    /**
     * Get all the visible file-metas at the version whose tso <= given tso
     * In columnar mode, we only need file name, rather than the whole file meta
     * So this API is only used for OSS cold-data compatibility
     *
     * @param tso may larger than latest tso.
     * @param logicalTable logical table name.
     * @param partName part name.
     * @return Collection of file-metas: orc and csv
     */
    @Deprecated
    Pair<List<FileMeta>, List<FileMeta>> findFiles(long tso, String logicalSchema, String logicalTable,
                                                   String partName);

    /**
     * Get all the visible files at the version whose tso <= given tso
     * In columnar mode, we only need file name, rather than the whole file meta.
     *
     * @param tso may larger than latest tso.
     * @param logicalTable logical table name.
     * @param partName part name.
     * @return Collection of file names
     */
    Pair<List<String>, List<String>> findFileNames(long tso, String logicalSchema, String logicalTable,
                                                   String partName);

    /**
     * Get csv data cache of given file name in snapshot of tso.
     *
     * @param tso may larger than latest tso.
     * @param csvFileName csv file name.
     * @return Collection of csv cache data (in format of chunk)
     */
    List<Chunk> csvData(long tso, String csvFileName);

    default List<Chunk> rawCsvData(long tso, String csvFileName, ExecutionContext context) {
        return null;
    }

    /**
     * Fill the given selection array according to snapshot of tso, file name and position block
     * USED FOR OLD COLUMNAR TABLE SCAN ONLY
     *
     * @param fileName file name.
     * @param tso may larger than latest tso.
     * @param selection selection array.
     * @param positionBlock position block (default column in columnar store).
     * @return filled size of selection array.
     */
    @Deprecated
    int fillSelection(String fileName, long tso, int[] selection, IntegerBlock positionBlock);

    @Deprecated
    int fillSelection(String fileName, long tso, int[] selection, LongColumnVector longColumnVector, int batchSize);

    /**
     * Generate delete bitmap of given file name in snapshot of tso.
     *
     * @param tso snapshot tso
     * @param fileName file name of ORC/CSV
     * @return copy of corresponding delete bitmap
     */
    RoaringBitmap getDeleteBitMapOf(long tso, String fileName);
}
