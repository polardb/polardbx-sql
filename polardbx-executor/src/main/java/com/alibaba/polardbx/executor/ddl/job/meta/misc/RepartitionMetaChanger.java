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

package com.alibaba.polardbx.executor.ddl.job.meta.misc;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.partition.PartitionTableType;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.TableType.BROADCAST;
import static com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.TableType.GSI;
import static com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.TableType.SHARDING;
import static com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.TableType.SINGLE;

/**
 * @author guxu
 */
public class RepartitionMetaChanger {

    /**
     * 1. 交换逻辑主表和逻辑目标表的路由
     * 2. 修改indexes的规则，指向原表。使得原表变成GSI
     */
    public static void cutOver(Connection metaDbConn,
                               final String schemaName,
                               final String sourceTableName,
                               final String targetTableName,
                               final boolean isSingle,
                               final boolean isBroadcast,
                               final boolean isAuto,
                               final boolean isGsi) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);

        try {
            final GsiMetaManager.TableType primaryTableType;
            if (isSingle) {
                primaryTableType = SINGLE;
            } else if (isBroadcast) {
                primaryTableType = BROADCAST;
            } else if (isGsi) {
                primaryTableType = GSI;
            } else {
                primaryTableType = SHARDING;
            }
            if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                // do cut over
                doPartitionTableCutOver(schemaName, sourceTableName, targetTableName, tableInfoManager,
                    primaryTableType);
                if (isAuto) {
                    // recover AutoPartition Flag
                    tableInfoManager.changeTablePartitionsPartFlag(schemaName, sourceTableName,
                        TablePartitionRecord.FLAG_AUTO_PARTITION);
                }
            } else {
                doCutOver(schemaName, sourceTableName, targetTableName, tableInfoManager, primaryTableType);
            }
        } finally {
            tableInfoManager.setConnection(null);
        }
    }

    public static void doCutOver(
        final String schemaName,
        final String sourceTableName,
        final String targetTableName,
        final TableInfoManager tableInfoManager,
        final GsiMetaManager.TableType primaryTableType) {

        String random = UUID.randomUUID().toString();

        TablesExtRecord sourceTableExt =
            tableInfoManager.queryTableExt(schemaName, sourceTableName, false);
        if (sourceTableExt == null) {
            String msgContent = String.format("Table '%s.%s' doesn't exist", schemaName, sourceTableName);
            throw new TddlNestableRuntimeException(msgContent);
        }
        TablesExtRecord targetTableExt =
            tableInfoManager.queryTableExt(schemaName, targetTableName, false);
        if (targetTableExt == null) {
            String msgContent = String.format("Table '%s.%s' doesn't exist", schemaName, targetTableName);
            throw new TddlNestableRuntimeException(msgContent);
        }

        long sourceFlag = sourceTableExt.flag;
        long targetFlag = targetTableExt.flag;

        long newVersion = Math.max(sourceTableExt.version, targetTableExt.version) + 1;

        tableInfoManager
            .alterTableExtNameAndTypeAndFlag(schemaName, sourceTableName, random, GSI.getValue(), targetFlag);
        switch (primaryTableType) {
        case SINGLE:
            tableInfoManager
                .alterTableExtNameAndTypeAndFlag(schemaName, targetTableName, sourceTableName, SINGLE.getValue(),
                    sourceFlag);
            break;
        case BROADCAST:
            tableInfoManager
                .alterTableExtNameAndTypeAndFlag(schemaName, targetTableName, sourceTableName, BROADCAST.getValue(),
                    sourceFlag);
            break;
        case GSI:
            tableInfoManager
                .alterTableExtNameAndTypeAndFlag(schemaName, targetTableName, sourceTableName, GSI.getValue(),
                    sourceFlag);
            break;
        case SHARDING:
            tableInfoManager
                .alterTableExtNameAndTypeAndFlag(schemaName, targetTableName, sourceTableName, SHARDING.getValue(),
                    sourceFlag);
            break;
        default:
            throw new TddlNestableRuntimeException("unknown primary table type");
        }
        tableInfoManager
            .alterTableExtNameAndTypeAndFlag(schemaName, random, targetTableName, GSI.getValue(), targetFlag);
        tableInfoManager.updateTablesExtVersion(schemaName, sourceTableName, newVersion);
        tableInfoManager.updateTablesExtVersion(schemaName, targetTableName, newVersion);
    }

    public static void doPartitionTableCutOver(
        final String schemaName,
        final String sourceTableName,
        final String targetTableName,
        final TableInfoManager tableInfoManager,
        final GsiMetaManager.TableType primaryTableType) {


        String random = UUID.randomUUID().toString();

        List<TablePartitionRecord> sourceTablePartition =
            tableInfoManager.queryTablePartitions(schemaName, sourceTableName, false);
        if (sourceTablePartition == null || sourceTablePartition.isEmpty()) {
            String msgContent = String.format("Table '%s.%s' doesn't exist", schemaName, sourceTableName);
            throw new TddlNestableRuntimeException(msgContent);
        }
        List<TablePartitionRecord> targetTablePartition =
            tableInfoManager.queryTablePartitions(schemaName, targetTableName, false);
        if (targetTablePartition == null || targetTablePartition.isEmpty()) {
            String msgContent = String.format("Table '%s.%s' doesn't exist", schemaName, targetTableName);
            throw new TddlNestableRuntimeException(msgContent);
        }

        long newVersion =
            Math.max(sourceTablePartition.get(0).metaVersion, targetTablePartition.get(0).metaVersion) + 1;

        tableInfoManager.repartitionCutOver(schemaName, sourceTableName, random,
            PartitionTableType.GSI_TABLE.getTableTypeIntValue());

        Long tableId = tableInfoManager.queryTable(schemaName, sourceTableName, false).id;
        switch (primaryTableType) {
        case SINGLE:
            tableInfoManager.repartitionCutOver(schemaName, targetTableName, sourceTableName,
                PartitionTableType.SINGLE_TABLE.getTableTypeIntValue());
            break;
        case BROADCAST:
            tableInfoManager.repartitionCutOver(schemaName, targetTableName, sourceTableName,
                PartitionTableType.BROADCAST_TABLE.getTableTypeIntValue());
            break;
        case GSI:
            tableInfoManager.repartitionCutOver(schemaName, targetTableName, sourceTableName,
                PartitionTableType.GSI_TABLE.getTableTypeIntValue());
            break;
        case SHARDING:
            tableInfoManager.repartitionCutOver(schemaName, targetTableName, sourceTableName,
                PartitionTableType.PARTITION_TABLE.getTableTypeIntValue());
            break;
        default:
            throw new TddlNestableRuntimeException("unknown primary table type");
        }

        tableInfoManager.repartitionCutOver(schemaName, random, targetTableName,
            PartitionTableType.GSI_TABLE.getTableTypeIntValue());

        tableInfoManager.updateTablePartitionsVersion(schemaName, sourceTableName, newVersion);
        tableInfoManager.updateTablePartitionsVersion(schemaName, targetTableName, newVersion);
    }

    public static void changeTableMeta4RepartitionKey(Connection metaDbConn,
                                                      final String schemaName,
                                                      final String tableName,
                                                      List<String> changeShardColumns) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);

        List<TablePartitionRecord> tablePartition =
            tableInfoManager.queryTablePartitions(schemaName, tableName, false);
        if (tablePartition == null || tablePartition.isEmpty()) {
            String msgContent = String.format("Table '%s.%s' doesn't exist", schemaName, tableName);
            throw new TddlNestableRuntimeException(msgContent);
        }

        tableInfoManager.addShardColumns4RepartitionKey(schemaName, tableName, changeShardColumns);
        tableInfoManager.updateTablePartitionsVersion(schemaName, tableName, tablePartition.get(0).metaVersion + 1);
    }

    /**
     * for alter table partitions count
     */
    public static void alterPartitionCountCutOver(Connection metaDbConn,
                                                  final String schemaName,
                                                  final String logicalTableName,
                                                  Map<String, String> tableNameMap) {
        // 1. cut over table_partitions meta and local indexes meta
        // 2. cut over global indexes meta
        // 3. recover primary table auto_partition flag
        tableNameMap.forEach((tableName, newTableName) -> {
            if (!StringUtils.equalsIgnoreCase(tableName, logicalTableName)) {
                // index table
                cutOver(metaDbConn, schemaName, tableName, newTableName, false, false, false, true);
                cutOverIndexes(metaDbConn, schemaName, logicalTableName, tableName, newTableName);
            } else {
                // primary table
                cutOver(metaDbConn, schemaName, tableName, newTableName, false, false, true,false);
            }
        });
    }

    public static void cutOverIndexes(Connection metaDbConn,
                                      final String schemaName,
                                      final String logicalTableName,
                                      final String sourceIndexName,
                                      final String targetIndexName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);

        try {
            String random = UUID.randomUUID().toString();

            // validate
            List<IndexesRecord> sourceTableIndex =
                tableInfoManager.queryIndexes(schemaName, logicalTableName, sourceIndexName);
            if (sourceTableIndex == null || sourceTableIndex.isEmpty()) {
                String msgContent = String.format("Table'%s.%s' doesn't exist index '%s'", schemaName, logicalTableName,
                    sourceIndexName);
                throw new TddlNestableRuntimeException(msgContent);
            }

            List<IndexesRecord> targetTableIndex =
                tableInfoManager.queryIndexes(schemaName, logicalTableName, targetIndexName);
            if (targetTableIndex == null || targetTableIndex.isEmpty()) {
                String msgContent = String.format("Table'%s.%s' doesn't exist index '%s'", schemaName, logicalTableName,
                    targetIndexName);
                throw new TddlNestableRuntimeException(msgContent);
            }

            // cut over
            tableInfoManager.alterPartitionCountCutOver(schemaName, sourceIndexName, random);
            tableInfoManager.alterPartitionCountCutOver(schemaName, targetIndexName, sourceIndexName);
            tableInfoManager.alterPartitionCountCutOver(schemaName, random, targetIndexName);

            // update version
            long newVersion =
                Math.max(sourceTableIndex.get(0).version, targetTableIndex.get(0).version) + 1;

            tableInfoManager.updateIndexesVersion(schemaName, sourceIndexName, newVersion);
            tableInfoManager.updateIndexesVersion(schemaName, targetIndexName, newVersion);

        } finally {
            tableInfoManager.setConnection(null);
        }
    }
}