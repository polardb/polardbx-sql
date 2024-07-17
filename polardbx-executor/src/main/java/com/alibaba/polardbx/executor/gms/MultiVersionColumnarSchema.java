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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.archive.schemaevolution.ColumnMetaWithTs;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexesAccessor;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.gms.DynamicColumnarManager.MAXIMUM_SIZE_OF_SNAPSHOT_CACHE;

public class MultiVersionColumnarSchema implements Purgeable {

    private final DynamicColumnarManager columnarManager;

    /**
     * Cache mapping from logical table name to table id
     */
    private final LoadingCache<Pair<String, String>, Long> tableMappingCache;

    /**
     * Cache mapping from table id to multi-version columnar table meta
     */
    private final LoadingCache<Long, MultiVersionColumnarTableMeta> columnarTableMetas;

    /**
     * Cache indexes column of logical table
     */
    private final LoadingCache<Pair<String, String>, List<Integer>> indexesColumnCache;

    public MultiVersionColumnarSchema(DynamicColumnarManager columnarManager) {
        this.columnarManager = columnarManager;

        this.columnarTableMetas = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_SIZE_OF_SNAPSHOT_CACHE)
            .build(new CacheLoader<Long, MultiVersionColumnarTableMeta>() {
                @Override
                public MultiVersionColumnarTableMeta load(@NotNull Long tableId) {
                    return new MultiVersionColumnarTableMeta(tableId);
                }
            });

        this.tableMappingCache = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_SIZE_OF_SNAPSHOT_CACHE)
            .build(new CacheLoader<Pair<String, String>, Long>() {
                @Override
                public Long load(@NotNull Pair<String, String> key) throws Exception {
                    String logicalSchema = key.getKey();
                    String logicalTableName = key.getValue();

                    List<ColumnarTableMappingRecord> records;
                    try (Connection connection = MetaDbUtil.getConnection()) {
                        ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
                        accessor.setConnection(connection);

                        // TODO(siyun): hack now, using newest table mapping
                        records = accessor.querySchemaIndex(logicalSchema, logicalTableName);
                    }

                    if (records != null && !records.isEmpty()) {
                        ColumnarTableMappingRecord record = records.get(0);
                        return record.tableId;
                    }

                    return null;
                }
            });

        this.indexesColumnCache = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_SIZE_OF_SNAPSHOT_CACHE)
            .build(new CacheLoader<Pair<String, String>, List<Integer>>() {
                @Override
                public List<Integer> load(@NotNull Pair<String, String> key) throws Exception {
                    String logicalSchema = key.getKey();
                    String indexName = key.getValue();
                    try (Connection connection = MetaDbUtil.getConnection()) {
                        IndexesAccessor indexesAccessor = new IndexesAccessor();
                        indexesAccessor.setConnection(connection);
                        List<IndexesRecord> indexRecords =
                            indexesAccessor.queryColumnarIndexColumnsByName(logicalSchema, indexName);
                        TableInfoManager tableInfoManager = new TableInfoManager();
                        tableInfoManager.setConnection(connection);
                        List<ColumnsRecord> columnsRecords =
                            tableInfoManager.queryVisibleColumns(logicalSchema, indexName);
                        return indexRecords.stream().map(indexesRecord -> {
                            for (int i = 0; i < columnsRecords.size(); i++) {
                                if (indexesRecord.columnName.equalsIgnoreCase(columnsRecords.get(i).columnName)) {
                                    return i;
                                }
                            }
                            return -1;
                        }).collect(Collectors.toList());
                    }
                }
            });
    }

    public List<Integer> getSortKeyColumns(long tso, String logicalSchema, String logicalTable)
        throws ExecutionException {
        return indexesColumnCache.get(Pair.of(logicalSchema, logicalTable));
    }

    public Long getTableId(long tso, String logicalSchema, String logicalTable) throws ExecutionException {
        return tableMappingCache.get(Pair.of(logicalSchema, logicalTable));
    }

    private MultiVersionColumnarTableMeta getColumnarTableMeta(long tableId) {
        try {
            return columnarTableMetas.get(tableId);
        } catch (ExecutionException e) {
            columnarTableMetas.invalidate(tableId);
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SCHEMA, e.getCause(),
                String.format("Failed to fetch column meta of table, table id: %d", tableId));
        }
    }

    @NotNull
    public List<ColumnMeta> getColumnMetas(long schemaTso, long tableId) {
        MultiVersionColumnarTableMeta columnarTableMeta = getColumnarTableMeta(tableId);

        List<ColumnMeta> columnMetas = columnarTableMeta.getColumnMetaListByTso(schemaTso);
        if (columnMetas != null) {
            return columnMetas;
        }

        // Case: column meta cache missed
        Lock writeLock = columnarTableMeta.getLock();
        writeLock.lock();
        try {
            columnarTableMeta.loadUntilTso(schemaTso);
            return Objects.requireNonNull(columnarTableMeta.getColumnMetaListByTso(schemaTso));
        } finally {
            writeLock.unlock();
        }
    }

    @NotNull
    public Map<Long, Integer> getColumnIndexMap(long schemaTso, long tableId) {
        MultiVersionColumnarTableMeta columnarTableMeta = getColumnarTableMeta(tableId);

        Map<Long, Integer> columnIndex = columnarTableMeta.getFieldIdMapByTso(schemaTso);
        if (columnIndex != null) {
            return columnIndex;
        }

        // Case: column cache missed
        Lock writeLock = columnarTableMeta.getLock();
        writeLock.lock();
        try {
            columnarTableMeta.loadUntilTso(schemaTso);
            return Objects.requireNonNull(columnarTableMeta.getFieldIdMapByTso(schemaTso));
        } finally {
            writeLock.unlock();
        }
    }

    @NotNull
    public List<Long> getColumnFieldIdList(long versionId, long tableId) {
        MultiVersionColumnarTableMeta columnarTableMeta = getColumnarTableMeta(tableId);

        List<Long> fieldIdList = columnarTableMeta.getColumnFieldIdList(versionId);
        if (fieldIdList != null) {
            return fieldIdList;
        }

        // Case:columns cache missed
        Lock writeLock = columnarTableMeta.getLock();
        writeLock.lock();
        try {
            columnarTableMeta.loadUntilTso(Long.MAX_VALUE);
            return Objects.requireNonNull(columnarTableMeta.getColumnFieldIdList(versionId));
        } finally {
            writeLock.unlock();
        }
    }

    @NotNull
    public ColumnMetaWithTs getInitColumnMeta(long tableId, long fieldId) {
        MultiVersionColumnarTableMeta columnarTableMeta = getColumnarTableMeta(tableId);

        ColumnMetaWithTs columnMeta = columnarTableMeta.getInitColumnMeta(fieldId);
        if (columnMeta != null) {
            return columnMeta;
        }

        // Case: default column meta cache missed
        Lock writeLock = columnarTableMeta.getLock();
        writeLock.lock();
        try {
            columnarTableMeta.loadUntilTso(Long.MAX_VALUE);
            return Objects.requireNonNull(columnarTableMeta.getInitColumnMeta(fieldId));
        } finally {
            writeLock.unlock();
        }
    }

    public int @NotNull [] getPrimaryKeyColumns(long schemaTso, long tableId) {
        MultiVersionColumnarTableMeta columnarTableMeta = getColumnarTableMeta(tableId);

        int[] primaryKeyColumns = columnarTableMeta.getPrimaryKeyColumns(schemaTso);
        if (primaryKeyColumns != null) {
            return primaryKeyColumns;
        }

        Lock writeLock = columnarTableMeta.getLock();
        writeLock.lock();
        try {
            columnarTableMeta.loadUntilTso(Long.MAX_VALUE);
            return Objects.requireNonNull(columnarTableMeta.getPrimaryKeyColumns(schemaTso));
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void purge(long tso) {
        // TODO(siyun):
        throw new NotSupportException("purge columnar schema not supported now!");
    }
}
