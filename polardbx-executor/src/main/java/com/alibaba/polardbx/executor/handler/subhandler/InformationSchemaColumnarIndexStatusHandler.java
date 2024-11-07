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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Like;
import com.alibaba.polardbx.optimizer.view.InformationSchemaColumnarIndexStatus;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class InformationSchemaColumnarIndexStatusHandler extends BaseVirtualViewSubClassHandler {

    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaColumnarIndexStatusHandler.class);

    private static final String QUERY_ORC_FILES_SQL_FORMAT =
        "SELECT logical_table_name AS table_id,"
            + " count(*) AS file_count, sum(table_rows) AS row_count, sum(extent_size) AS file_size FROM files"
            + " WHERE logical_table_name IN (%s) AND RIGHT(file_name, 3) = 'orc'"
            + " GROUP BY table_id";

    private static final String QUERY_CSV_DEL_FILES_SQL_FORMAT =
        "SELECT f.logical_table_name AS table_id, RIGHT(f.file_name, 3) AS suffix, COUNT(*) AS file_count, "
            + "SUM(caf.total_rows) AS row_count, SUM(caf.append_offset + caf.append_length) AS file_size FROM files f "
            + "INNER JOIN (SELECT file_name, MAX(checkpoint_tso) AS max_checkpoint_tso FROM columnar_appended_files "
            + "GROUP BY file_name) latest_checkpoint ON f.file_name = latest_checkpoint.file_name "
            + "INNER JOIN columnar_appended_files caf ON f.file_name = caf.file_name "
            + "AND caf.checkpoint_tso = latest_checkpoint.max_checkpoint_tso "
            + "WHERE f.logical_table_name IN (%s) AND RIGHT(f.file_name, 3) IN ('csv', 'del') "
            + "GROUP BY f.logical_table_name, RIGHT(f.file_name, 3)";

    public InformationSchemaColumnarIndexStatusHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaColumnarIndexStatus;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        final int schemaIndex = InformationSchemaColumnarIndexStatus.getTableSchemaIndex();
        final int tableIndex = InformationSchemaColumnarIndexStatus.getTableNameIndex();
        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();
        // only new partitioning db
        Set<String> schemaNames = new TreeSet<>(String::compareToIgnoreCase);
        schemaNames.addAll(StatsUtils.getDistinctSchemaNames());

        schemaNames = virtualView.applyFilters(schemaIndex, params, schemaNames);

        // tableIndex
        Set<String> indexTableNames = virtualView.getEqualsFilterValues(tableIndex, params);
        // tableLike
        String tableLike = virtualView.getLikeString(tableIndex, params);

        queryColumnarIndexStatus(schemaNames, indexTableNames, tableLike, cursor, executionContext);

        return cursor;
    }

    private void queryColumnarIndexStatus(Set<String> schemaNames, Set<String> logicalTableNames, String tableLike,
                                          ArrayResultCursor cursor, ExecutionContext executionContext) {

        List<ColumnarIndexInfo> columnarIndexInfoList =
            getColumnarIndexInfoList(schemaNames, logicalTableNames, tableLike, executionContext);

        Map<Long, ColumnarIndexInfo> indexInfoMap = queryTableIdMap(columnarIndexInfoList);

        if (indexInfoMap.isEmpty()) {
            return;
        }

        Map<Long, ColumnarIndexStatus> indexStatusMap = queryFullStatus(indexInfoMap);

        fillIndexStatus(indexInfoMap, indexStatusMap, cursor);
    }

    private void fillIndexStatus(Map<Long, ColumnarIndexInfo> indexInfoMap,
                                 Map<Long, ColumnarIndexStatus> indexStatusMap, ArrayResultCursor cursor) {
        indexStatusMap.forEach((tableId, indexStatus) -> {
            ColumnarIndexInfo indexInfo = indexInfoMap.get(tableId);

            if (indexInfo == null) {
                return;
            }

            ColumnarFilesStatus orcStatus = indexStatus.orcStatus;
            ColumnarFilesStatus csvStatus = indexStatus.csvStatus;
            ColumnarFilesStatus delStatus = indexStatus.delStatus;

            long orcSize = orcStatus == null ? 0 : orcStatus.fileSize;
            long csvSize = csvStatus == null ? 0 : csvStatus.fileSize;
            long delSize = delStatus == null ? 0 : delStatus.fileSize;

            cursor.addRow(new Object[] {
                indexInfo.tableSchema,
                indexInfo.tableName,
                indexInfo.indexName,
                indexInfo.indexStatus.name(),
                orcStatus == null ? 0L : orcStatus.fileCount,
                orcStatus == null ? 0L : orcStatus.rowCount,
                orcSize,
                csvStatus == null ? 0L : csvStatus.fileCount,
                csvStatus == null ? 0L : csvStatus.rowCount,
                csvSize,
                delStatus == null ? 0L : delStatus.fileCount,
                delStatus == null ? 0L : delStatus.rowCount,
                delSize,
                orcSize + csvSize + delSize
            });
        });
    }

    @NotNull
    private Map<Long, ColumnarIndexInfo> queryTableIdMap(List<ColumnarIndexInfo> columnarIndexInfoList) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarTableMappingAccessor tableMappingAccessor = new ColumnarTableMappingAccessor();
            tableMappingAccessor.setConnection(connection);

            return tableMappingAccessor.querySchemaTableIndexes(
                columnarIndexInfoList.stream().map(ColumnarIndexInfo::toKey).collect(Collectors.toList())
            ).stream().collect(Collectors.toMap(
                record -> record.tableId,
                record -> new ColumnarIndexInfo(record.tableSchema, record.tableName, record.indexName,
                    IndexStatus.valueOf(record.status))
            ));
        } catch (Throwable t) {
            throw GeneralUtil.nestedException(t);
        }
    }

    @NotNull
    private Map<Long, ColumnarIndexStatus> queryFullStatus(Map<Long, ColumnarIndexInfo> indexInfoMap) {
        Map<Long, ColumnarIndexStatus> indexStatusMap = new HashMap<>();
        try (Connection connection = MetaDbUtil.getConnection()) {
            // query orc status
            String sql = String.format(QUERY_ORC_FILES_SQL_FORMAT, StringUtils.join(indexInfoMap.keySet(), ","));

            try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    Long tableId = Long.valueOf(rs.getString("table_id"));
                    long fileCount = rs.getLong("file_count");
                    long rowCount = rs.getLong("row_count");
                    long fileSize = rs.getLong("file_size");

                    indexStatusMap.computeIfAbsent(tableId,
                        id -> new ColumnarIndexStatus()
                    ).orcStatus = new ColumnarFilesStatus(fileCount, rowCount, fileSize);
                }
            }

            sql = String.format(QUERY_CSV_DEL_FILES_SQL_FORMAT, StringUtils.join(indexInfoMap.keySet(), ","));

            // query csv and del status, this may cost a lot
            try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    Long tableId = Long.valueOf(rs.getString("table_id"));
                    String suffix = rs.getString("suffix");
                    long fileCount = rs.getLong("file_count");
                    long rowCount = rs.getLong("row_count");
                    long fileSize = rs.getLong("file_size");

                    ColumnarFilesStatus filesStatus = new ColumnarFilesStatus(fileCount, rowCount, fileSize);
                    ColumnarIndexStatus indexStatus =
                        indexStatusMap.computeIfAbsent(tableId, id -> new ColumnarIndexStatus());

                    switch (ColumnarFileType.of(suffix)) {
                    case CSV:
                        indexStatus.csvStatus = filesStatus;
                        break;
                    case DEL:
                        indexStatus.delStatus = filesStatus;
                        break;
                    default:
                    }
                }
            }

        } catch (Throwable t) {
            throw GeneralUtil.nestedException(t);
        }

        // fill zero status if there are no files
        for (Long tableId : indexInfoMap.keySet()) {
            indexStatusMap.putIfAbsent(tableId, new ColumnarIndexStatus());
        }
        return indexStatusMap;
    }

    @NotNull
    private List<ColumnarIndexInfo> getColumnarIndexInfoList(Set<String> schemaNames, Set<String> logicalTableNames,
                                                             String tableLike, ExecutionContext executionContext) {
        final GsiMetaManager metaManager =
            ExecutorContext.getContext(executionContext.getSchemaName()).getGsiManager().getGsiMetaManager();

        // TODO(siyun): should fetch index that already dropped?
        final GsiMetaManager.GsiMetaBean meta = metaManager.getAllGsiMetaBean(schemaNames, logicalTableNames);

        List<ColumnarIndexInfo> columnarIndexInfoList = new ArrayList<>();

        for (GsiMetaManager.GsiTableMetaBean tableMetaBean : meta.getTableMeta().values()) {
            GsiMetaManager.GsiIndexMetaBean indexMetaBean = tableMetaBean.gsiMetaBean;
            if (indexMetaBean != null && indexMetaBean.columnarIndex) {
                if (tableLike != null && !new Like().like(indexMetaBean.tableName, tableLike)) {
                    continue;
                }

                columnarIndexInfoList.add(new ColumnarIndexInfo(
                    indexMetaBean.tableSchema,
                    indexMetaBean.tableName,
                    indexMetaBean.indexName,
                    indexMetaBean.indexStatus)
                );
            }
        }

        return columnarIndexInfoList;
    }

    private static class ColumnarIndexInfo {
        final public String tableSchema;
        final public String tableName;
        final public String indexName;
        final public IndexStatus indexStatus;

        private ColumnarIndexInfo(String tableSchema, String tableName, String indexName, IndexStatus indexStatus) {
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.indexName = indexName;
            this.indexStatus = indexStatus;
        }

        public Pair<String, Pair<String, String>> toKey() {
            return Pair.of(tableSchema, Pair.of(tableName, indexName));
        }
    }

    private static class ColumnarIndexStatus {
        public ColumnarFilesStatus orcStatus;
        public ColumnarFilesStatus csvStatus;
        public ColumnarFilesStatus delStatus;
    }

    private static class ColumnarFilesStatus {
        final public long fileCount;
        final public long rowCount;
        final public long fileSize;

        private ColumnarFilesStatus(long fileCount, long rowCount, long fileSize) {
            this.fileCount = fileCount;
            this.rowCount = rowCount;
            this.fileSize = fileSize;
        }
    }
}
