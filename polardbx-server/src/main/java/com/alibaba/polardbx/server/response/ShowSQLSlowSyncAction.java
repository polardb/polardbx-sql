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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author mengshi.sunmengshi 2015年5月12日 下午1:28:16
 * @since 5.1.0
 */
public class ShowSQLSlowSyncAction implements ISyncAction {

    private String db;

    public ShowSQLSlowSyncAction() {
    }

    public ShowSQLSlowSyncAction(String db) {
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
        result.addColumn("USER", DataTypes.StringType);
        result.addColumn("HOST", DataTypes.StringType);
        result.addColumn("PORT", DataTypes.StringType);
        result.addColumn("SCHEMA", DataTypes.StringType);
        result.addColumn("START_TIME", DataTypes.LongType);
        result.addColumn("EXECUTE_TIME", DataTypes.LongType);
        result.addColumn("AFFECT_ROW", DataTypes.LongType);
        result.addColumn("SQL", DataTypes.StringType);
        result.addColumn("TRACE_ID", DataTypes.StringType);

        List<SQLRecord> sqlRecordList = CobarServer.getInstance()
            .getAllSchemaConfigs()
            .stream()
            .map(config -> Stream.of(config.getDataSource().getRecorder().getRecords()).collect(Collectors.toList()))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
        // 此处记录仅根据schema进行分组 由调用方根据实际需求进行排序
        for (SQLRecord record : sqlRecordList) {
            if (record != null) {
                result.addRow(new Object[] {
                    record.user, record.host, record.port, record.schema, record.startTime,
                    record.executeTime, record.affectRow, record.statement, record.traceId});
            }
        }

        return result;
    }
}
