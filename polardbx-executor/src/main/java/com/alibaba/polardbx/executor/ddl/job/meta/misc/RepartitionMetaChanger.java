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
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;

import java.sql.Connection;
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
                               final boolean isBroadcast) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);

        try {
            final GsiMetaManager.TableType primaryTableType;
            if (isSingle) {
                primaryTableType = SINGLE;
            } else if (isBroadcast) {
                primaryTableType = BROADCAST;
            } else {
                primaryTableType = SHARDING;
            }
            doCutOver(schemaName, sourceTableName, targetTableName, tableInfoManager, primaryTableType);
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

}