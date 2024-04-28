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
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutorMap;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import org.apache.commons.collections.CollectionUtils;

public class DdlInterruptSyncAction implements IGmsSyncAction {

    private static final Logger logger = LoggerFactory.getLogger(DdlInterruptSyncAction.class);

    private DdlRequest ddlRequest;

    public DdlInterruptSyncAction() {
    }

    public DdlInterruptSyncAction(DdlRequest request) {
        this.ddlRequest = request;
    }

    @Override
    public Object sync() {
        if (ddlRequest == null || ddlRequest.getSchemaName() == null || CollectionUtils
            .isEmpty(ddlRequest.getJobIds())) {
            return null;
        }
        final String schemaName = ddlRequest.getSchemaName();
        for (Long jobId : ddlRequest.getJobIds()) {
            DdlEngineDagExecutor ddlEngineDagExecutor = DdlEngineDagExecutorMap.get(schemaName, jobId);
            if (ddlEngineDagExecutor == null) {
                logger.warn(
                    String.format("The ddl job %s on schema %s does not exits", jobId, schemaName));
                continue;
            }
            ddlEngineDagExecutor.interrupt();
            logger.warn(
                String.format("The ddl job %s on schema %s has been interrupted by DdlInterruptSyncAction", jobId,
                    schemaName));
        }

        return null;
    }

    public DdlRequest getDdlRequest() {
        return ddlRequest;
    }

    public void setDdlRequest(DdlRequest ddlRequest) {
        this.ddlRequest = ddlRequest;
    }

}
