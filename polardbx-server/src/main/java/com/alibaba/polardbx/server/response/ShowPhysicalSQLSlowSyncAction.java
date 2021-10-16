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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.statis.SQLRecord;

/**
 * 物理慢sql
 *
 * @author agapple 2016年2月19日 下午2:39:20
 * @since 5.1.25-2
 */
public class ShowPhysicalSQLSlowSyncAction implements ISyncAction {

    private String db;

    public ShowPhysicalSQLSlowSyncAction() {
    }

    public ShowPhysicalSQLSlowSyncAction(String db) {
        this.db = db;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("RULE");

        result.addColumn("GROUP_NAME", DataTypes.StringType);
        result.addColumn("DBKEY_NAME", DataTypes.StringType);
        result.addColumn("START_TIME", DataTypes.LongType);
        result.addColumn("EXECUTE_TIME", DataTypes.LongType);
        result.addColumn("SQL_EXECUTE_TIME", DataTypes.LongType);
        result.addColumn("GETLOCK_CONNECTION_TIME", DataTypes.LongType);
        result.addColumn("CREATE_CONNECTION_TIME", DataTypes.LongType);

        result.addColumn("AFFECT_ROW", DataTypes.LongType);
        result.addColumn("SQL", DataTypes.StringType);
        result.addColumn("TRACE_ID", DataTypes.StringType);

        SQLRecord[] records = CobarServer.getInstance()
            .getConfig()
            .getSchemas()
            .get(db)
            .getDataSource()
            .getPhysicalRecorder()
            .getRecords();

        for (int i = records.length - 1; i >= 0; i--) {
            if (records[i] != null) {
                result.addRow(new Object[] {
                    records[i].dataNode, records[i].dbKey, records[i].startTime,
                    records[i].executeTime, records[i].sqlTime, records[i].getLockConnectionTime,
                    records[i].createConnectionTime, records[i].affectRow, records[i].statement, records[i].traceId});
            }
        }

        return result;
    }
}
