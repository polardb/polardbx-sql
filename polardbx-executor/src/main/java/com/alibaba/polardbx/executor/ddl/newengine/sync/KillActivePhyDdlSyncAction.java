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

package com.alibaba.polardbx.executor.ddl.newengine.sync;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.sync.ISyncAction;

public class KillActivePhyDdlSyncAction implements ISyncAction {

    private static final Logger logger = LoggerFactory.getLogger(KillActivePhyDdlSyncAction.class);

    private String schemaName;
    private String traceId;

    public KillActivePhyDdlSyncAction() {
    }

    public KillActivePhyDdlSyncAction(String schemaName, String traceId) {
        this.schemaName = schemaName;
        this.traceId = traceId;
    }

    @Override
    public ResultCursor sync() {
        DdlHelper.killActivePhyDDLsUntilNone(schemaName, traceId);
        return null;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

}
