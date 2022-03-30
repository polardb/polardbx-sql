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

package com.alibaba.polardbx.gms.partition;

import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class TablePartitionConfigUtil {

    public static List<TablePartitionConfig> getAllTablePartitionConfigs(Connection metaDbConn, String dbName) {
        return MetaDbUtil.queryMetaDbWrapper(metaDbConn, (conn) -> {
            TablePartitionAccessor tpa = new TablePartitionAccessor();
            tpa.setConnection(conn);
            return tpa.getAllTablePartitionConfigs(dbName);
        });
    }

    public static List<TablePartitionConfig> getAllTablePartitionConfigs(String dbName) {
        return getAllTablePartitionConfigs(null, dbName);
    }

    public static TablePartitionConfig getTablePartitionConfig(Connection metaDbConn,
                                                               String dbName, String tbName,
                                                               boolean fromDeltaTable) {
        return MetaDbUtil.queryMetaDbWrapper(metaDbConn, (conn) -> {
            TablePartitionAccessor tpa = new TablePartitionAccessor();
            tpa.setConnection(conn);
            return tpa.getTablePartitionConfig(dbName, tbName, fromDeltaTable);
        });
    }

    public static TablePartitionConfig getTablePartitionConfig(String dbName, String tbName, boolean fromDeltaTable) {
        return getTablePartitionConfig(null, dbName, tbName, fromDeltaTable);
    }

    public static TablePartitionConfig getPublicTablePartitionConfig(Connection metaDbConn,
                                                                     String dbName, String tbName,
                                                                     boolean fromDeltaTable) {
        return MetaDbUtil.queryMetaDbWrapper(metaDbConn, (conn) -> {
            TablePartitionAccessor tpa = new TablePartitionAccessor();
            tpa.setConnection(conn);
            return tpa.getPublicTablePartitionConfig(dbName, tbName);
        });
    }

    public static TablePartitionConfig getPublicTablePartitionConfig(String dbName, String tbName,
                                                                     boolean fromDeltaTable) {
        return getPublicTablePartitionConfig(null, dbName, tbName, fromDeltaTable);
    }

    public static Map<String, TablePartitionConfig> getPublicTablePartitionConfigs(Connection metaDbConn,
                                                                                   String dbName,
                                                                                   List<String> tableNames,
                                                                                   boolean fromDeltaTable) {
        return MetaDbUtil.queryMetaDbWrapper(metaDbConn, (conn) -> {
            Map<String, TablePartitionConfig> result = new HashMap<>();
            TablePartitionAccessor tpa = new TablePartitionAccessor();
            tpa.setConnection(conn);

            for (String tableName : tableNames) {
                TablePartitionConfig tableConfig = tpa.getPublicTablePartitionConfig(dbName, tableName);
                result.put(tableName, tableConfig);
            }

            return result;
        });
    }

    public static Map<String, TablePartitionConfig> getPublicTablePartitionConfigs(String dbName,
                                                                                   List<String> tableNames,
                                                                                   boolean fromDeltaTable) {
        return getPublicTablePartitionConfigs(null, dbName, tableNames, fromDeltaTable);
    }

}
