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

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.QueryInfo;
import com.alibaba.polardbx.executor.mpp.execution.QueryManager;
import com.alibaba.polardbx.executor.mpp.execution.TaskInfo;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.executor.utils.ExecUtils.addAllHosts;
import static com.alibaba.polardbx.executor.utils.ExecUtils.getRootFailedTask;

public class ShowQueryInfoListSyncAction implements ISyncAction {

    public ShowQueryInfoListSyncAction() {
    }

    @Override
    public ResultCursor sync() {

        ArrayResultCursor result = new ArrayResultCursor("queryinfolist");
        result.addColumn("ID", DataTypes.StringType);
        result.addColumn("DB", DataTypes.StringType);
        result.addColumn("COMMAND", DataTypes.StringType);
        result.addColumn("STATE", DataTypes.StringType);
        result.addColumn("TASK", DataTypes.IntegerType);
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("ERROR", DataTypes.StringType);
        result.addColumn("FAILED_TASK", DataTypes.StringType);
        result.addColumn("START", DataTypes.StringType);
        result.addColumn("TIME", DataTypes.LongType);

        result.initMeta();

        QueryManager queryManager = ServiceProvider.getInstance().getServer().getQueryManager();
        for (QueryInfo queryInfo : queryManager.getAllQueryInfo()) {
            Set<String> hosts = new LinkedHashSet<>();
            hosts.add(ServiceProvider.getInstance().getServer().getLocalNode().getHostPort());
            if (queryInfo != null && queryInfo.getOutputStage().isPresent()) {
                addAllHosts(queryInfo.getOutputStage(), hosts);
            }
            String worker = String.join(",", hosts);

            String error = "";
            String failedTask = "";
            if (queryInfo != null && queryInfo.getFailureInfo() != null) {
                try {
                    error = queryInfo.getFailureInfo().toString();
                } catch (Exception e) {
                    //ignore
                    error = "query is failed";
                }

                try {
                    AtomicReference<TaskInfo> fail = new AtomicReference<>();
                    getRootFailedTask(queryInfo.getOutputStage(), fail);
                    if (fail.get() != null) {
                        failedTask = fail.get().getTaskStatus().getSelf().getNodeServer().toString();
                    }
                } catch (Exception e) {
                    //ignore
                }
            }

            result.addRow(new Object[] {
                queryInfo.getQueryId(), queryInfo.getSession().getSchema(), queryInfo.getQuery(),
                queryInfo.getState().toString(), queryInfo.getQueryStats().getTotalTasks(),
                worker, error, failedTask, queryInfo.getQueryStats().getCreateTime().toString(),
                queryInfo.getQueryStats().getElapsedTime().toMillis()});
        }

        return result;

    }
}
