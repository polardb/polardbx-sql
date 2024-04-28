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

import com.alibaba.polardbx.executor.archive.schemaevolution.ColumnMetaWithTs;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ColumnarSchemaTransformer {
    /**
     * @return The physical column index of the file from logical columnar table column index for certain tso
     */
    @Deprecated
    List<Integer> getPhysicalColumnIndexes(long tso, String fileName, List<Integer> columnIndexes);

    /**
     * @return The physical column index map of the file from logical columnar table column index for certain field id
     */
    Map<Long, Integer> getPhysicalColumnIndexes(String fileName);

    /**
     * @param tso TSO
     * @param logicalTable logical name of columnar table
     * @return The sort key column index of logical table, start from 0
     */
    List<Integer> getSortKeyColumns(long tso, String logicalSchema, String logicalTable);

    /**
     * Get the physical column indexes of primary key for certain file, start from 1.
     */
    int[] getPrimaryKeyColumns(String fileName);

    Optional<String> fileNameOf(String logicalSchema, long tableId, String partName, int columnarFileId);

    FileMeta fileMetaOf(String fileName);

    @NotNull
    List<Long> getColumnFieldIdList(long versionId, long tableId);

    @NotNull
    List<ColumnMeta> getColumnMetas(long schemaTso, String logicalSchema, String logicalTable);

    @NotNull
    List<ColumnMeta> getColumnMetas(long schemaTso, long tableId);

    @NotNull
    Map<Long, Integer> getColumnIndex(long schemaTso, long tableId);

    @NotNull
    ColumnMetaWithTs getInitColumnMeta(long tableId, long fieldId);
}
