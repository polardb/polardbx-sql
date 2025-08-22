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

package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.sync.RequestColumnarSnapshotSeqSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.OrcFileStatusRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class ColumnarTransactionUtils {

    private final static Logger LOGGER = LoggerFactory.getLogger("ColumnarTransactionUtils");
    private final static String COLUMNAR_PURGE_LIMIT_TSO = "columnar_purge_limit_tso";

    @NotNull
    public static Long getMinColumnarSnapshotTime() {
        IGmsSyncAction action = new RequestColumnarSnapshotSeqSyncAction();
        List<List<Map<String, Object>>> results =
            SyncManagerHelper.sync(action, SystemDbHelper.DEFAULT_DB_NAME, SyncScope.ALL);

        // must >= 0
        long minSnapshotKeepTime = Math.max(DynamicConfig.getInstance().getMinSnapshotKeepTime(), 0L);
        long minSnapshotTime = ColumnarManager.getInstance().latestTso() - (minSnapshotKeepTime << 22);

        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }
            for (Map<String, Object> row : nodeRows) {
                Long time = DataTypes.LongType.convertFrom(row.get("TSO"));
                if (time == null) {
                    continue;
                }

                minSnapshotTime = Math.min(minSnapshotTime, time);
            }
        }
        return minSnapshotTime;
    }

    public static Long getLatestTsoFromGms() {
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
            checkpointsAccessor.setConnection(connection);

            return checkpointsAccessor.queryLatestTso();
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                "Failed to fetch latest columnar tso");
        }
    }

    public static Long getLatestTsoFromGmsWithDelay(long delayMicroseconds) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
            checkpointsAccessor.setConnection(connection);

            return checkpointsAccessor.queryLatestTsoWithDelay(delayMicroseconds);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                "Failed to fetch latest columnar tso");
        }
    }

    /**
     * @return latest tso of checkpoint that only contains orc files but not csv files,
     * in format (Innodb tso, Columnar tso)
     */
    public static Pair<Long, Long> getLatestOrcCheckpointTsoFromGms() {
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
            checkpointsAccessor.setConnection(connection);

            return checkpointsAccessor.queryLatestTsoPair();
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                "Failed to fetch latest columnar tso");
        }
    }

    /**
     * ShowColumnarStatus 包含显示正在创建的进度，tso 对应的 checkpoint type更丰富
     */
    public static Long getLatestShowColumnarStatusTsoFromGms() {
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
            checkpointsAccessor.setConnection(connection);

            return checkpointsAccessor.queryLatestTsoByShowColumnarStatus();
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                "Failed to fetch latest columnar tso");
        }
    }

    public static int updateColumnarPurgeWatermark(long purgeWatermark) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarConfigAccessor configAccessor = new ColumnarConfigAccessor();
            configAccessor.setConnection(connection);
            // Only leader CN could reach hear, so we don't need to select for update
            List<ColumnarConfigRecord> records = configAccessor.queryGlobalByConfigKey(COLUMNAR_PURGE_LIMIT_TSO);

            boolean shouldUpdate = false;
            if (GeneralUtil.isEmpty(records)) {
                shouldUpdate = true;
            } else {
                ColumnarConfigRecord configRecord = records.get(0);

                if (configRecord.configValue == null || Long.parseLong(configRecord.configValue) < purgeWatermark) {
                    shouldUpdate = true;
                }
            }

            if (shouldUpdate) {
                return configAccessor.updateGlobalParamValue(COLUMNAR_PURGE_LIMIT_TSO, Long.toString(purgeWatermark));
            }

            return 0;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                "Failed to update purge watermark");
        }
    }

    public static class ColumnarIndexStatusRow {
        public long tso;
        public String tableSchema;
        public String tableName;
        public String indexName;
        public long indexId;
        public long partitionNum;

        public String status;
        public long orcFileNum;
        public long orcRows;
        public long orcFileSize;
        public long csvFileNum;
        public long csvRows;
        public long csvFileSize;
        public long delFileNum;
        public long delRows;
        public long delFileSize;
    }

    public static List<ColumnarIndexStatusRow> queryColumnarIndexStatus(Long tso,
                                                                        List<ColumnarTableMappingRecord> columnarRecords) {
        if (tso == null) {
            return ImmutableList.of();
        }

        List<ColumnarIndexStatusRow> result = new ArrayList<>(columnarRecords.size());
        //获取每个列存索引表的统计信息
        for (ColumnarTableMappingRecord record : columnarRecords) {
            Long tableId = record.tableId;
            String schemaName = record.tableSchema;
            String tableName = record.tableName;
            String indexName = record.indexName;
            //a、获取每个表的列存分区名
            List<String> partitionNameList;
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
                tablePartitionAccessor.setConnection(metaDbConn);
                TablePartitionConfig tablePartitionConfig =
                    tablePartitionAccessor.getPublicTablePartitionConfig(schemaName, indexName);
                partitionNameList = tablePartitionConfig.getPartitionSpecConfigs().stream()
                    .map(partitionSpecConfig -> partitionSpecConfig.getSpecConfigInfo().partName)
                    .collect(Collectors.toList());
            } catch (Exception e) {
                //暂时忽略该列存索引表分区获取失败的情况：
                //1.正在创建的表可能rollback了，分区信息找不到，忽略就好
                //2.public的表可能删除了，分区信息找不到，也忽略
                LOGGER.warn(
                    "fail to fetch partition info by columnar index: " + record.tableSchema + "." + record.tableName
                        + "(" + record.indexName + "(" + record.tableId + "))", e);
                continue;
            }
            ColumnarIndexStatusRow row = new ColumnarIndexStatusRow();
            row.tso = tso;
            row.tableSchema = schemaName;
            row.tableName = tableName;
            row.indexName = indexName;
            row.indexId = record.tableId;
            row.partitionNum = partitionNameList.size();

            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                FilesAccessor filesAccessor = new FilesAccessor();
                filesAccessor.setConnection(metaDbConn);

                //orc 文件统计
                List<OrcFileStatusRecord> orcFileStatusRecords =
                    filesAccessor.queryOrcFileStatusByTsoAndTableId(tso, schemaName, String.valueOf(tableId));
                if (!orcFileStatusRecords.isEmpty()) {
                    row.orcFileNum = orcFileStatusRecords.get(0).fileCounts;
                    row.orcRows = orcFileStatusRecords.get(0).rowCounts;
                    row.orcFileSize = orcFileStatusRecords.get(0).fileSizes;
                }
                ColumnarAppendedFilesAccessor appendedFilesAccessor = new ColumnarAppendedFilesAccessor();
                appendedFilesAccessor.setConnection(metaDbConn);

                //快照表csv没有append记录的统计
                List<OrcFileStatusRecord> csvFileStatusRecords =
                    filesAccessor.querySnapshotCSVFileStatusByTsoAndTableId(tso, schemaName, String.valueOf(tableId));
                if (!csvFileStatusRecords.isEmpty()) {
                    row.csvFileNum += csvFileStatusRecords.get(0).fileCounts;
                    row.csvRows += csvFileStatusRecords.get(0).rowCounts;
                    row.csvFileSize += csvFileStatusRecords.get(0).fileSizes;
                }

                //csv/del文件appended统计
                boolean useSubQuery = DynamicConfig.getInstance().isShowColumnarStatusUseSubQuery();
                List<ColumnarAppendedFilesRecord> appendedFilesRecords;
                if (useSubQuery) {
                    //子查询方式
                    appendedFilesRecords =
                        appendedFilesAccessor.queryLastValidAppendByTsoAndTableIdSubQuery(tso, schemaName,
                            String.valueOf(tableId));
                } else {
                    //join方式
                    appendedFilesRecords = appendedFilesAccessor.queryLastValidAppendByTsoAndTableId(tso, schemaName,
                        String.valueOf(tableId));
                }

                //按照tso降序排序的，del文件总删除的行数只需要统计各分区最新文件的最新append记录就行，防止多个del文件
                Set<String> haveRecordDelRowPartitions = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                for (ColumnarAppendedFilesRecord appendRecord : appendedFilesRecords) {
                    String fileName = appendRecord.fileName;
                    ColumnarFileType columnarFileType = FileSystemUtils.getFileType(fileName);
                    switch (columnarFileType) {
                    case CSV:
                        row.csvFileNum++;
                        row.csvFileSize += appendRecord.appendOffset + appendRecord.appendLength;
                        row.csvRows += appendRecord.totalRows;
                        break;
                    case DEL:
                        row.delFileNum++;
                        row.delFileSize += appendRecord.appendOffset + appendRecord.appendLength;
                        if (!haveRecordDelRowPartitions.contains(appendRecord.partName)) {
                            //del文件的行数记录的是该分区总的删除行数，所以只需要最新的那条
                            row.delRows += appendRecord.totalRows;
                            haveRecordDelRowPartitions.add(appendRecord.partName);
                        }
                    default:
                        break;
                    }
                }

            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    "fail to fetch files for Index: " + record.indexName, e);
            }

            String status = record.status;
            //正在创建的表，显示进度
            if (record.status.equalsIgnoreCase(ColumnarTableStatus.CREATING.name())) {
                StringBuilder str = new StringBuilder();
                str.append(ColumnarTableStatus.CREATING.name()).append(" -> { ");
                try (Connection metaDbConn = MetaDbUtil.getConnection()) {

                    ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
                    checkpointsAccessor.setConnection(metaDbConn);
                    //获取select进度
                    List<ColumnarCheckpointsRecord> selectRecords =
                        checkpointsAccessor.queryLastRecordByTableAndTsoAndTypes(schemaName, tableId.toString(), tso,
                            ImmutableList.of(
                                ColumnarCheckpointsAccessor.CheckPointType.SNAPSHOT,
                                ColumnarCheckpointsAccessor.CheckPointType.SNAPSHOT_END));
                    if (selectRecords.isEmpty()) {
                        //说明还未开始，或者第一次提交都还没有
                        str.append("select: 0/1(0%);");
                    } else {
                        str.append(selectRecords.get(0).extra).append(";");
                    }
                    //获取compaction完成进度
                    List<ColumnarCheckpointsRecord> compactionRecords =
                        checkpointsAccessor.queryRecordsByTableAndTsoAndTypes(schemaName, tableId.toString(), tso,
                            ImmutableList.of(ColumnarCheckpointsAccessor.CheckPointType.SNAPSHOT_FINISHED));
                    int finishedCompaction = Math.min(compactionRecords.size(), partitionNameList.size());
                    int percent = 100 * finishedCompaction / partitionNameList.size();
                    str.append(" compaction: ").append(finishedCompaction).append('/').append(partitionNameList.size());
                    str.append("(").append(percent).append("%)");

                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        "fail to fetch create status by columnar index: " + record.tableSchema + "." + record.tableName
                            + "(" + record.indexName + "(" + record.tableId + "))", e);
                }
                str.append(" }");
                status = str.toString();
            }
            row.status = status;
            result.add(row);
        }
        return result;
    }
}
