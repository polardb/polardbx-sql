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

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.List;

/**
 * Drain CDC binlog after move-out all data
 *
 * @author moyi
 * @since 2021/07
 */
@TaskName(name = "ActionDrainCDCStorage")
public class ActionDrainCDCStorage extends BaseDdlTask implements BalanceAction {

    final private static Logger LOG = SQLRecorderLogger.ddlLogger;

    @Getter
    @Setter
    @JSONField
    private List<String> dataNodes;

    @JSONCreator
    public ActionDrainCDCStorage(String schema, List<String> dataNodes) {
        super(schema);
        this.dataNodes = dataNodes;
    }

    @Override
    @JSONField
    public String getSchema() {
        return null;
    }

    @Override
    public String getName() {
        return "ScaleInDataNode";
    }

    @Override
    public String getStep() {
        return String.format("drain cdc binlog of data-node: %s", StringUtils.join(dataNodes, ","));
    }

    @Override
    public String toString() {
        return this.getStep();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        try {
            CdcManagerHelper.getInstance()
                .checkCdcBeforeStorageRemove(new HashSet<>(dataNodes), String.valueOf(getJobId()));
            LOG.info("Finish " + this.toString());
        } catch (Exception e) {
            LOG.error("Failed to " + this.toString());
            throw GeneralUtil.nestedException(
                String.format("Failed to drain CDC binlog of data-nodes(%s)", StringUtils.join(dataNodes, ",")),
                e);
        }
    }

}
