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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.schemaevolution.ColumnMetaWithTs;
import com.alibaba.polardbx.gms.metadb.table.ColumnarColumnEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarColumnEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.calcite.sql.type.SqlTypeName;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils.TYPE_FACTORY;

public class MultiVersionColumnarTableMeta implements Purgeable {
    private static final Logger LOGGER = LoggerFactory.getLogger("COLUMNAR_TRANS");
    // this size is based on columnar schema compaction frequency
    private static final int MAX_VERSION_SCHEMA_CACHE_SIZE = 16;

    private final long tableId;
    private final ColumnMeta TSO_COLUMN;
    private final ColumnMeta POSITION_COLUMN;

    // column_id -> columnMeta
    // this column_id is the id of columnar_column_evolution, ranther than field_id
    private final Map<Long, ColumnMetaWithTs> allColumnMetas = new ConcurrentHashMap<>();

    private final Set<Long> primaryKeySet = ConcurrentHashMap.newKeySet();

    // schema_ts -> version_id
    // THE schema_ts COULD BE NULL, which means that the columnar has not processed with this version
    // notice that only schema_ts is ordered, while version_id may be not
    private final SortedMap<Long, Long> versionIdMap = new ConcurrentSkipListMap<>();

    // version_id -> [column_id1 ...]
    // notice that columnar_table_evolution stores id rather than field_id of columnar_column_evolution
    private final Map<Long, List<Long>> multiVersionColumns = new ConcurrentHashMap<>();

    // version_id -> [partition_id ...]
    private final Map<Long, List<Long>> multiVersionPartitions = new ConcurrentHashMap<>();

    // for columnar_column_evoluion: id -> field_id
    private final Map<Long, Long> columnFieldIdMap = new ConcurrentHashMap<>();

    // field_id -> [column_id for version 1, column_id for version 2, ...]
    private final Map<Long, SortedSet<Long>> multiVersionColumnIds = new ConcurrentHashMap<>();

    // schema_ts -> partition_info
    private final SortedMap<Long, PartitionInfo> multiVersionPartitionInfos = new ConcurrentSkipListMap<>();

    private final Map<Long, TablePartitionRecord> allPartitions = new ConcurrentHashMap<>();

    private final LoadingCache<Long, List<ColumnMeta>> columnMetaListByTso;
    private final LoadingCache<Long, Map<Long, Integer>> fieldIdMapByTso;

    private final Lock lock = new ReentrantLock();

    public MultiVersionColumnarTableMeta(long tableId) {
        this.tableId = tableId;

        String tableName = String.valueOf(this.tableId);
        TSO_COLUMN = new ColumnMeta(tableName, "tso", null,
            new Field(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT)));
        POSITION_COLUMN = new ColumnMeta(tableName, "position", null,
            new Field(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT)));

        columnMetaListByTso = CacheBuilder.newBuilder()
            .maximumSize(MAX_VERSION_SCHEMA_CACHE_SIZE)
            .build(new CacheLoader<Long, List<ColumnMeta>>() {
                @Override
                public List<ColumnMeta> load(@NotNull Long schemaTso) {
                    long versionId = versionIdMap.get(schemaTso);
                    List<Long> columns = multiVersionColumns.get(versionId);

                    List<ColumnMeta> columnMetas = new ArrayList<>();
                    columnMetas.add(TSO_COLUMN);
                    columnMetas.add(POSITION_COLUMN);
                    columnMetas.addAll(
                        columns.stream().map(columnId -> allColumnMetas.get(columnId).getMeta())
                            .collect(Collectors.toList())
                    );

                    return columnMetas;
                }
            });

        fieldIdMapByTso = CacheBuilder.newBuilder()
            .maximumSize(MAX_VERSION_SCHEMA_CACHE_SIZE)
            .build(new CacheLoader<Long, Map<Long, Integer>>() {
                @Override
                public Map<Long, Integer> load(@NotNull Long schemaTso) {
                    long versionId = versionIdMap.get(schemaTso);
                    List<Long> columns = multiVersionColumns.get(versionId);

                    Map<Long, Integer> targetColumnIndex = new HashMap<>();

                    for (int i = 0; i < columns.size(); i++) {
                        targetColumnIndex.put(columnFieldIdMap.get(columns.get(i)),
                            i + ColumnarStoreUtils.IMPLICIT_COLUMN_CNT);
                    }

                    return targetColumnIndex;
                }
            });
    }

    @Nullable
    public List<Long> getColumnFieldIdList(long versionId) {
        if (multiVersionColumns.isEmpty() || !multiVersionColumns.containsKey(versionId)) {
            return null;
        }

        return multiVersionColumns.get(versionId).stream().map(columnFieldIdMap::get)
            .collect(Collectors.toList());
    }

    /**
     * Get the map from field_id to the position of corresponding file for certain tso
     *
     * @param schemaTso schema tso
     * @return Map: field_id -> column position in file
     */
    @Nullable
    public Map<Long, Integer> getFieldIdMapByTso(long schemaTso) {
        if (versionIdMap.isEmpty() || versionIdMap.lastKey() < schemaTso) {
            return null;
        }

        try {
            return fieldIdMapByTso.get(schemaTso);
        } catch (ExecutionException e) {
            throw new RuntimeException(
                String.format(
                    "Failed to build field id map for CCI, table id: %d, schema tso: %d", tableId, schemaTso
                ),
                e
            );
        }
    }

    @Nullable
    public List<ColumnMeta> getColumnMetaListByTso(long schemaTso) {
        if (versionIdMap.isEmpty() || versionIdMap.lastKey() < schemaTso) {
            return null;
        }

        try {
            return columnMetaListByTso.get(schemaTso);
        } catch (ExecutionException e) {
            throw new RuntimeException(
                String.format(
                    "Failed to build column meta for CCI, table id: %d, schema tso: %d", tableId, schemaTso
                ),
                e
            );
        }
    }

    @Nullable
    public PartitionInfo getPartitionInfoByTso(long schemaTso) {
        if (versionIdMap.isEmpty() || versionIdMap.lastKey() < schemaTso) {
            return null;
        }

        Long validSchemaTso = multiVersionPartitionInfos.headMap(schemaTso + 1).lastKey();
        return multiVersionPartitionInfos.get(validSchemaTso);
    }

    @Nullable
    public SortedMap<Long, PartitionInfo> getPartitionInfos(long schemaTso) {
        if (versionIdMap.isEmpty() || versionIdMap.lastKey() < schemaTso) {
            return null;
        }

        return multiVersionPartitionInfos.headMap(schemaTso + 1);
    }

    public int @Nullable [] getPrimaryKeyColumns(long schemaTso) {
        if (versionIdMap.isEmpty() || versionIdMap.lastKey() < schemaTso) {
            return null;
        }

        long versionId = versionIdMap.get(schemaTso);
        List<Long> columns = multiVersionColumns.get(versionId);

        return IntStream.range(0, columns.size())
            .filter(index -> primaryKeySet.contains(columns.get(index)))
            .map(index -> index + ColumnarStoreUtils.IMPLICIT_COLUMN_CNT + 1)
            .toArray();
    }

    @Nullable
    public ColumnMetaWithTs getInitColumnMeta(long fieldId) {
        if (multiVersionColumnIds.containsKey(fieldId)) {
            return allColumnMetas.get(multiVersionColumnIds.get(fieldId).first());
        } else {
            return null;
        }
    }

    public void loadUntilTso(long schemaTso) {
        if (!versionIdMap.isEmpty() && versionIdMap.lastKey() >= schemaTso) {
            return;
        }

        long latestTso = versionIdMap.isEmpty() ? Long.MIN_VALUE : versionIdMap.lastKey();
        // this latest version id in current memory cache is nonsense, since version id may be out of order

        List<ColumnarTableEvolutionRecord> tableEvolutionRecordList;
        List<ColumnarColumnEvolutionRecord> columnEvolutionRecordList;
        List<ColumnarPartitionEvolutionRecord> partitionEvolutionRecordList;

        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarTableEvolutionAccessor accessor = new ColumnarTableEvolutionAccessor();
            accessor.setConnection(connection);
            tableEvolutionRecordList = accessor.queryTableIdAndGreaterThanTso(tableId, latestTso);

            ColumnarColumnEvolutionAccessor columnAccessor = new ColumnarColumnEvolutionAccessor();
            columnAccessor.setConnection(connection);
            columnEvolutionRecordList = columnAccessor.queryTableIdAndVersionIdsOrderById(
                tableId,
                tableEvolutionRecordList.stream()
                    // For those versions which have not been loaded
                    .filter(r -> !multiVersionColumns.containsKey(r.versionId))
                    .map(r -> r.versionId)
                    .collect(Collectors.toList())
            );

            ColumnarPartitionEvolutionAccessor partitionAccessor = new ColumnarPartitionEvolutionAccessor();
            partitionAccessor.setConnection(connection);
            partitionEvolutionRecordList = partitionAccessor.queryTableIdAndVersionIdsOrderById(
                tableId,
                tableEvolutionRecordList.stream()
                    // For those versions which have not been loaded
                    .filter(r -> !multiVersionPartitions.containsKey(r.versionId))
                    .map(r -> r.versionId)
                    .collect(Collectors.toList())
            );

        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                String.format("Failed to generate columnar schema of tso: %d", schemaTso));
        }

        for (ColumnarColumnEvolutionRecord columnEvolutionRecord : columnEvolutionRecordList) {
            Long id = columnEvolutionRecord.id;
            Long fieldId = columnEvolutionRecord.fieldId;
            ColumnsRecord columnsRecord = columnEvolutionRecord.columnsRecord;
            ColumnMeta columnMeta = GmsTableMetaManager.buildColumnMeta(
                columnsRecord,
                tableEvolutionRecordList.get(0).indexName,
                columnsRecord.collationName,
                columnsRecord.characterSetName);
            ColumnMetaWithTs columnMetaWithTs = new ColumnMetaWithTs(columnEvolutionRecord.create, columnMeta);
            if ("PRI".equalsIgnoreCase(columnsRecord.columnKey)) {
                primaryKeySet.add(id);
            }
            allColumnMetas.put(id, columnMetaWithTs);
            columnFieldIdMap.put(id, fieldId);
            multiVersionColumnIds.compute(fieldId, (k, v) -> {
                if (v == null) {
                    SortedSet<Long> newSet = new ConcurrentSkipListSet<>();
                    newSet.add(id);
                    return newSet;
                } else {
                    v.add(id);
                    return v;
                }
            });
        }

        for (ColumnarPartitionEvolutionRecord columnarPartitionRecord : partitionEvolutionRecordList) {
            Long id = columnarPartitionRecord.id;
            TablePartitionRecord partitionRecord = columnarPartitionRecord.partitionRecord;
            allPartitions.put(id, partitionRecord);
        }

        for (ColumnarTableEvolutionRecord tableRecord : tableEvolutionRecordList) {
            long commitTs = tableRecord.commitTs;
            long versionId = tableRecord.versionId;
            multiVersionColumns.putIfAbsent(versionId, tableRecord.columns);
            multiVersionPartitions.putIfAbsent(versionId, tableRecord.partitions);

            if (commitTs != Long.MAX_VALUE) {
                versionIdMap.put(commitTs, versionId);

                List<Long> lastPartitions =
                    latestTso != Long.MIN_VALUE ? multiVersionPartitions.get(versionIdMap.get(latestTso)) : null;

                // If two partition definitions are identical, skip
                if (lastPartitions == null ||
                    (lastPartitions != tableRecord.partitions &&
                        !lastPartitions.equals(tableRecord.partitions))) {
                    multiVersionPartitionInfos.put(commitTs, buildPartitionInfo(commitTs, tableRecord.partitions));
                }

                latestTso = commitTs;
            }
        }
    }

    /**
     * Purge schema for outdated columnar table schema
     * Noted that this tso is different from low watermark,
     * since those schemas which haven't been compacted should all be reserved.
     */
    @Override
    public void purge(long schemaTso) {

    }

    public Lock getLock() {
        return lock;
    }

    private PartitionInfo buildPartitionInfo(long schemaTso, List<Long> partitionIds) {
        return PartitionInfoManager.generatePartitionInfo(
            getColumnMetaListByTso(schemaTso),
            partitionIds.stream()
                .map(allPartitions::get)
                .collect(Collectors.toList()),
            null,
            false,
            false
        );
    }
}
