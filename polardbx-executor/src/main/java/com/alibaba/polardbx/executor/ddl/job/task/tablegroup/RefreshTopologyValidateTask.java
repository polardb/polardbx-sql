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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
@TaskName(name = "RefreshTopologyValidateTask")
public class RefreshTopologyValidateTask extends BaseValidateTask {

    private Map<String, List<Pair<String, String>>> instGroupDbInfo;

    @JSONCreator
    public RefreshTopologyValidateTask(String schemaName, Map<String, List<Pair<String, String>>> instGroupDbInfo) {
        super(schemaName);
        this.instGroupDbInfo = instGroupDbInfo;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        for (Map.Entry<String, List<Pair<String, String>>> entry : instGroupDbInfo.entrySet()) {
            String storageInst = entry.getKey();
            if (GeneralUtil.isNotEmpty(entry.getValue())) {
                boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
                if (isNewPart) {
                    List<GroupDetailInfoRecord> records = DbTopologyManager.getGroupDetails(schemaName, storageInst);
                    if (GeneralUtil.isNotEmpty(records)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PHYSICAL_TOPOLOGY_CHANGING,
                            String.format("the dn[%s] is changing, please retry this command later",
                                storageInst));
                    }
                }
            }
        }
    }

    @Override
    protected String remark() {
        return "| " + getDescription();
    }

    @Override
    public String getDescription() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<Pair<String, String>>> entry : instGroupDbInfo.entrySet()) {
            sb.append("[dn:");
            sb.append(entry.getKey());
            sb.append(",");
            for (Pair<String, String> pair : entry.getValue()) {
                sb.append("(");
                sb.append(pair.getKey());
                sb.append(",");
                sb.append(pair.getValue());
                sb.append("),");
            }
            sb.append("],");
        }
        return sb.toString();
    }
}
