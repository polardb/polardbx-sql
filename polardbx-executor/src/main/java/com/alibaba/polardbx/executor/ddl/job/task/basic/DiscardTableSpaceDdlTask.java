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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import lombok.Getter;

import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.DN_SYSTEM_LOCK;

@Getter
@TaskName(name = "DiscardTableSpaceDdlTask")
public class DiscardTableSpaceDdlTask extends BasePhyDdlTask {

    private String logicalTableName;

    private String storageInst;

    private List<Pair<String, Integer>> hostIpAndPorts;

    @JSONCreator
    public DiscardTableSpaceDdlTask(String schemaName, String logicalTableName, String storageInst,
                                    PhysicalPlanData physicalPlanData, List<Pair<String, Integer>> hostIpAndPorts) {
        super(schemaName, physicalPlanData);
        this.logicalTableName = logicalTableName;
        this.storageInst = storageInst;
        this.hostIpAndPorts = hostIpAndPorts;
        setResourceAcquired(buildResourceRequired(storageInst, hostIpAndPorts));
        onExceptionTryRecoveryThenRollback();
    }

    DdlEngineResources buildResourceRequired(String storageInst, List<Pair<String, Integer>> hostIpAndPorts) {
        DdlEngineResources resourceRequired = new DdlEngineResources();
        String owner = "DiscardTableSpace:" + logicalTableName + ":" + storageInst;
        for (Pair<String, Integer> hostIpAndPort : hostIpAndPorts) {
            String fullDnResourceName = DdlEngineResources.concateDnResourceName(hostIpAndPort, storageInst);
            resourceRequired.requestForce(fullDnResourceName + DN_SYSTEM_LOCK, 55L, owner);
        }
        return resourceRequired;
    }

    public void executeImpl(ExecutionContext executionContext) {
        super.executeImpl(executionContext);
    }

    @Override
    protected QueryConcurrencyPolicy getConcurrencyPolicy(ExecutionContext executionContext) {
        boolean useGroupConcurrent = executionContext.getParamManager()
            .getBoolean(ConnectionParams.DISCARD_TABLESPACE_USE_GROUP_CONCURRENT_BLOCK);
        if (useGroupConcurrent) {
            return QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK;
        } else {
            return super.getConcurrencyPolicy(executionContext);
        }
    }

    @Override
    public String remark() {
        StringBuilder sb = new StringBuilder();
        sb.append("|discard tablespace detail: ");
        sb.append("table_schema [");
        sb.append(schemaName);
        sb.append("] ");
        sb.append("table [");
        sb.append(logicalTableName);
        sb.append("] ");
        sb.append("target physical table info [");
        int j = 0;
        for (Map.Entry<String, List<List<String>>> entry : physicalPlanData.getTableTopology().entrySet()) {
            if (j > 0) {
                sb.append(", ");
            }
            j++;
            sb.append("(");
            sb.append(entry.getKey()).append(":");
            int i = 0;
            for (List<String> tbNames : entry.getValue()) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(tbNames.get(0));
                i++;
            }
            sb.append(")");
        }
        sb.append("]");
        return sb.toString();
    }
}
