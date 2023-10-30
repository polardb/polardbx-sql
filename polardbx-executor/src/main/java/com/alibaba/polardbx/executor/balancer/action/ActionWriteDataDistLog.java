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

package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.executor.balancer.serial.DataDistInfo;
import com.alibaba.polardbx.executor.ddl.job.task.rebalance.WriteDataDistLogTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.Objects;

/**
 * Action that just lock some resource
 *
 * @author moyi
 * @since 2021/10
 */
public class ActionWriteDataDistLog implements BalanceAction {

    private String schema;

    private DataDistInfo dataDistInfo;

    public ActionWriteDataDistLog(String schema, DataDistInfo dataDistInfo) {
        this.schema = schema;
        this.dataDistInfo = dataDistInfo;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getName() {
        return "WriteDataDistLog";
    }

    @Override
    public String getStep() {
        return "WriteDataDistLog";
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        ExecutableDdlJob job = new ExecutableDdlJob();
        WriteDataDistLogTask task = new WriteDataDistLogTask(schema, dataDistInfo);
        job.addTask(task);
        job.labelAsHead(task);
        job.labelAsTail(task);
        return job;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionLockResource)) {
            return false;
        }
        ActionWriteDataDistLog that = (ActionWriteDataDistLog) o;
        return Objects.equals(schema, that.schema) && Objects.equals(dataDistInfo, that.dataDistInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, dataDistInfo);
    }

    @Override
    public String toString() {
        return "ActionWriteDataDistLog{" +
            "schema='" + schema + '\'' +
            ", dataDist='" + JSON.toJSONString(dataDistInfo) + '\'' +
            '}';
    }
}