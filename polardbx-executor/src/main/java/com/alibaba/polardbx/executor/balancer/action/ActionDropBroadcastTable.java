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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.balancer.policy.PolicyUtils;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Drop broadcast table when scale-in
 *
 * @author moyi
 * @since 2021/10
 */
public class ActionDropBroadcastTable implements BalanceAction {

    private String schema;
    private List<String> storageInstList;

    /**
     * Remove broadcast table in the `schema` on specified `storageInstList`
     */
    public ActionDropBroadcastTable(String schema, List<String> storageInstList) {
        this.schema = schema;
        this.storageInstList = storageInstList;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getName() {
        return "ActionDropBroadcastTable";
    }

    @Override
    public String getStep() {
        return "drop broadcast table on storage: " + storageInstList;
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        TableGroupConfig broadcastTg =
            OptimizerContext.getContext(schema).getTableGroupInfoManager().getBroadcastTableGroupConfig();

        if (broadcastTg == null || CollectionUtils.isEmpty(broadcastTg.getPartitionGroupRecords())) {
            return new TransientDdlJob();
        }
        // get partition-groups
        List<String> removedPgList = new ArrayList<>();
        Map<String, GroupDetailInfoRecord> groupMap = PolicyUtils.getGroupDetails(schema);
        for (PartitionGroupRecord pg : GeneralUtil.emptyIfNull(broadcastTg.getPartitionGroupRecords())) {
            String groupName = GroupInfoUtil.buildGroupNameFromPhysicalDb(pg.getPhy_db());
            GroupDetailInfoRecord group = groupMap.get(groupName);

            if (storageInstList.contains(group.getStorageInstId())) {
                removedPgList.add(pg.getPartition_name());
            }
        }

        if (CollectionUtils.isEmpty(removedPgList)) {
            return new TransientDdlJob();
        }

        // drop partition-groups
        String tgName = TStringUtil.backQuote(broadcastTg.getTableGroupRecord().getTg_name());
        String partitions = removedPgList.stream().map(TStringUtil::backQuote).collect(Collectors.joining(","));
        String sqlDropPartition = String.format("ALTER TABLEGROUP %s DROP PARTITION %s", tgName, partitions);
        ExecutableDdlJob job = ActionUtils.convertToDDLJob(ec, schema, sqlDropPartition);

        return job;
    }
}
