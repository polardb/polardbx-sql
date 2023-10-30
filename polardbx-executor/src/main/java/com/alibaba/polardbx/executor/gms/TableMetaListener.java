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

package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

public class TableMetaListener implements ConfigListener {

    private String schemaName;
    private String tableName;

    public TableMetaListener(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public void onHandleConfig(String dataId, long newOpVersion) {
        MDC.put(MDC.MDC_KEY_APP, schemaName.toLowerCase());

        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        // TODO(pt.luoyanxin) optimize single-version schema change
        SQLRecorderLogger.ddlMetaLogger.info(
            String.format("tableName: %s; dataId: %s; start to new version by table meta listener", tableName, dataId));
        sm.toNewVersionForTableGroup(tableName, true);
    }
}
