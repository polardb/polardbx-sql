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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;

import java.sql.Connection;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class TablePartitionConfigUtil {

    public static List<TablePartitionConfig> getAllTablePartitionConfigs(String dbName) {

        List<TablePartitionConfig> result = null;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            TablePartitionAccessor tpa = new TablePartitionAccessor();
            tpa.setConnection(conn);
            result = tpa.getAllTablePartitionConfigs(dbName);
            conn.setAutoCommit(true);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return result;
    }

    public static TablePartitionConfig getTablePartitionConfig(String dbName, String tbName, boolean fromDeltaTable) {

        TablePartitionConfig result = null;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            TablePartitionAccessor tpa = new TablePartitionAccessor();
            tpa.setConnection(conn);
            result = tpa.getTablePartitionConfig(dbName, tbName, fromDeltaTable);
            conn.setAutoCommit(true);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return result;
    }

    public static TablePartitionConfig getPublicTablePartitionConfig(String dbName, String tbName,
                                                                     boolean fromDeltaTable) {

        TablePartitionConfig result = null;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            TablePartitionAccessor tpa = new TablePartitionAccessor();
            tpa.setConnection(conn);
            result = tpa.getPublicTablePartitionConfig(dbName, tbName);
            conn.setAutoCommit(true);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return result;
    }
}
