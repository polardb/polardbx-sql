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

package com.alibaba.polardbx.executor.sync.ddl;

import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class RemoteDdlTaskSyncAction implements ISyncAction {

    private String schemaName;
    private long jobId;
    private long taskId;

    private static final String SUCCESS = "SUCCESS";
    private static final String MSG = "MSG";

    public RemoteDdlTaskSyncAction(final String schemaName, final long jobId, final long taskId) {
        this.schemaName = schemaName;
        this.jobId = jobId;
        this.taskId = taskId;
    }

    /**
     * @return SUCCESS: 'TRUE'/'FALSE'
     * MSG: 'XXXXXX'
     */
    @Override
    public ResultCursor sync() {

        ArrayResultCursor result = new ArrayResultCursor("DDL_REMOTE_TASK_RESULT");
        result.addColumn(SUCCESS, DataTypes.StringType);
        result.addColumn(MSG, DataTypes.StringType);

        try {
            LoggerUtil.buildMDC(schemaName);
            IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
            serverConfigManager.remoteExecuteDdlTask(schemaName, jobId, taskId);
            buildSuccessResult(result);
        } catch (Exception e) {
            SQLRecorderLogger.ddlEngineLogger.error("execute/rollback DDL TASK failed", e);
            buildFailureResult(result, e.getMessage());
        }
        return result;
    }

    private void buildSuccessResult(ArrayResultCursor result) {
        result.addRow(new Object[] {
            String.valueOf(Boolean.TRUE),
            ""
        });
    }

    private void buildFailureResult(ArrayResultCursor result, String errMsg) {
        result.addRow(new Object[] {
            String.valueOf(Boolean.FALSE),
            errMsg
        });
    }

    public static boolean isRemoteDdlTaskSyncActionSuccess(List<Map<String, Object>> result) {
        if (CollectionUtils.isEmpty(result)) {
            return false;
        }
        return StringUtils.equalsIgnoreCase(
            (CharSequence) result.get(0).get(SUCCESS),
            Boolean.TRUE.toString()
        );
    }

    public static String getMsgFromResult(List<Map<String, Object>> result) {
        if (CollectionUtils.isEmpty(result)) {
            return "";
        }
        return (String) result.get(0).get(MSG);
    }

    public String getSchemaName() {
        return this.schemaName;
    }

    public void setSchemaName(final String schemaName) {
        this.schemaName = schemaName;
    }

    public long getJobId() {
        return this.jobId;
    }

    public void setJobId(final long jobId) {
        this.jobId = jobId;
    }

    public long getTaskId() {
        return this.taskId;
    }

    public void setTaskId(final long taskId) {
        this.taskId = taskId;
    }
}