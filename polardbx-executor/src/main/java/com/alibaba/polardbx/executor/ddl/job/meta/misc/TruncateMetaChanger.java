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
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;

public class TruncateMetaChanger {

    /**
     * 1. 交换逻辑主表和逻辑目标表的路由
     */
    public static void cutOver(Connection metaDbConn,
                               final String schemaName,
                               final String sourceTableName,
                               final String targetTableName,
                               final boolean isNewPartDb) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);

        try {
            if (isNewPartDb) {
                doNewPartDbCutOver(schemaName, sourceTableName, targetTableName, tableInfoManager);
            } else {
                doCutOver(schemaName, sourceTableName, targetTableName, tableInfoManager);
            }
        } finally {
            tableInfoManager.setConnection(null);
        }
    }

    public static void doCutOver(
        final String schemaName,
        final String sourceTableName,
        final String targetTableName,
        final TableInfoManager tableInfoManager) {

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

        long newVersion = Math.max(sourceTableExt.version, targetTableExt.version) + 1;

        tableInfoManager.alterTableExtNameAndIndexes(schemaName, sourceTableName, random);
        tableInfoManager.alterTableExtNameAndIndexes(schemaName, targetTableName, sourceTableName);
        tableInfoManager.alterTableExtNameAndIndexes(schemaName, random, targetTableName);

        tableInfoManager.updateTablesExtVersion(schemaName, sourceTableName, newVersion);
        tableInfoManager.updateTablesExtVersion(schemaName, targetTableName, newVersion);
    }

    public static void doNewPartDbCutOver(
        final String schemaName,
        final String sourceTableName,
        final String targetTableName,
        final TableInfoManager tableInfoManager) {

        String random = UUID.randomUUID().toString();

        List<TablePartitionRecord> sourceTablePartitions =
            tableInfoManager.queryTablePartitions(schemaName, sourceTableName, false);
        if (sourceTablePartitions.size() == 0) {
            String msgContent = String.format("Table '%s.%s' doesn't exist", schemaName, sourceTableName);
            throw new TddlNestableRuntimeException(msgContent);
        }
        List<TablePartitionRecord> targetTablePartitions =
            tableInfoManager.queryTablePartitions(schemaName, targetTableName, false);
        if (targetTablePartitions.size() == 0) {
            String msgContent = String.format("Table '%s.%s' doesn't exist", schemaName, targetTableName);
            throw new TddlNestableRuntimeException(msgContent);
        }

        long newVersion =
            Math.max(sourceTablePartitions.get(0).metaVersion, targetTablePartitions.get(0).metaVersion) + 1;

        tableInfoManager.truncateTableCutOver(schemaName, sourceTableName, random);
        tableInfoManager.truncateTableCutOver(schemaName, targetTableName, sourceTableName);
        tableInfoManager.truncateTableCutOver(schemaName, random, targetTableName);

        tableInfoManager.updateTablePartitionVersion(schemaName, sourceTableName, newVersion);
        tableInfoManager.updateTablePartitionVersion(schemaName, targetTableName, newVersion);
    }

}