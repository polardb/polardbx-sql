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

package com.alibaba.polardbx.executor.ddl.job.meta;

import com.google.common.collect.Lists;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;

import java.sql.Connection;
import java.util.List;

public class GsiMetaChanger {

    public static void addIndexMeta(Connection metaDbConnection,
                                    String schemaName,
                                    List<GsiMetaManager.IndexRecord> indexRecords) {
        ExecutorContext
            .getContext(schemaName)
            .getGsiManager()
            .getGsiMetaManager()
            .insertIndexMetaForPolarX(metaDbConnection, indexRecords);
    }

    public static void changeTableToGsi(Connection metaDbConnection,
                                        String schemaName,
                                        String indexName) {

        ExecutorContext
            .getContext(schemaName)
            .getGsiManager()
            .getGsiMetaManager()
            .changeTablesExtType(metaDbConnection, schemaName, indexName, GsiMetaManager.TableType.GSI.getValue());
    }

    public static void updateIndexStatus(Connection metaDbConnection,
                                         String schemaName,
                                         String primaryTableName,
                                         String indexName,
                                         IndexStatus beforeIndexStatus,
                                         IndexStatus afterIndexStatus) {
        ExecutorContext
            .getContext(schemaName)
            .getGsiManager()
            .getGsiMetaManager()
            .updateIndexStatus(metaDbConnection, schemaName, primaryTableName, indexName, beforeIndexStatus,
                afterIndexStatus);
    }

    public static void removeIndexMeta(Connection metaDbConnection,
                                       String schemaName,
                                       String primaryTableName,
                                       String indexName) {
        ExecutorContext
            .getContext(schemaName)
            .getGsiManager()
            .getGsiMetaManager()
            .removeIndexMeta(metaDbConnection, schemaName, primaryTableName, indexName);
    }

    public static void addIndexColumnMeta(Connection metaDbConnection,
                                          String schemaName,
                                          String primaryTableName,
                                          List<GsiMetaManager.IndexRecord> indexRecords) {
        ExecutorContext
            .getContext(schemaName)
            .getGsiManager()
            .getGsiMetaManager()
            .insertIndexMetaByAddColumn(metaDbConnection, schemaName, primaryTableName, indexRecords);
    }

    public static void updateIndexColumnMeta(Connection metaDbConnection,
                                             String schemaName,
                                             String primaryTableName,
                                             String indexName,
                                             String oldColumnName,
                                             String newColumnName,
                                             String nullable) {
        ExecutorContext.getContext(schemaName)
            .getGsiManager()
            .getGsiMetaManager()
            .changeColumnMeta(
                metaDbConnection,
                schemaName,
                primaryTableName,
                indexName,
                oldColumnName,
                newColumnName,
                nullable
            );
    }

    public static void updateIndexColumnStatusMeta(Connection metaDbConnection,
                                                   String schemaName,
                                                   String primaryTableName,
                                                   String indexName,
                                                   List<String> columns,
                                                   TableStatus beforeIndexStatus,
                                                   TableStatus afterIndexStatus) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        //update column status in gsi table
        tableInfoManager.updateColumnsStatus(
            schemaName,
            Lists.newArrayList(indexName),
            Lists.newArrayList(columns),
            beforeIndexStatus.getValue(),
            afterIndexStatus.getValue()
        );
        //update index status in primary table
        tableInfoManager.updateIndexesColumnsStatus(
            schemaName,
            primaryTableName,
            Lists.newArrayList(indexName),
            Lists.newArrayList(columns),
            beforeIndexStatus.getValue(),
            afterIndexStatus.getValue()
        );
    }
}
