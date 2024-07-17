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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.common.GsiStatisticsManager;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class GsiStatisticsSyncAction implements ISyncAction {

    final static public int INSERT_RECORD = 0;
    final static public int DELETE_RECORD = 1;
    final static public int RENAME_RECORD = 2;
    final static public int QUERY_RECORD = 3;

    final static public int DELETE_SCHEMA = 4;

    final static public int WRITE_BACK_ALL_SCHEMA = 5;
    final private String schemaName;
    final private String gsiName;
    final private String newValue;
    final private int alterKind;

    public GsiStatisticsSyncAction(String schemaName, String gsiName, String newValue, int alterKind) {
        this.schemaName = schemaName;
        this.gsiName = gsiName;
        this.newValue = newValue;
        this.alterKind = alterKind;
    }

    @Override
    public ResultCursor sync() {
        if (ConfigDataMode.needDNResource()) {
            GsiStatisticsManager statisticsManager = GsiStatisticsManager.getInstance();
            if (alterKind == QUERY_RECORD || alterKind == INSERT_RECORD || alterKind == DELETE_RECORD) {
                statisticsManager.writeBackSchemaLevelGsiStatistics(schemaName);
                statisticsManager.reLoadSchemaLevelGsiStatisticsInfoFromMetaDb(schemaName);
            } else if (alterKind == DELETE_SCHEMA) {
                statisticsManager.removeSchemaLevelRecordFromCache(schemaName);
            } else if (alterKind == RENAME_RECORD) {
                statisticsManager.renameGsiRecordFromCache(schemaName, gsiName, newValue);
                statisticsManager.writeBackSchemaLevelGsiStatistics(schemaName);
                statisticsManager.reLoadSchemaLevelGsiStatisticsInfoFromMetaDb(schemaName);
            } else if (alterKind == WRITE_BACK_ALL_SCHEMA) {
                statisticsManager.writeBackAllGsiStatistics();
                statisticsManager.loadFromMetaDb();
            }
        }
        return null;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getGsiName() {
        return gsiName;
    }

    public String getNewValue() {
        return newValue;
    }

    public int getAlterKind() {
        return alterKind;
    }
}
