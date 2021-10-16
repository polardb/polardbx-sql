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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.factory.RefreshTopologyFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRefreshTopology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogicalRefreshTopologyHandler extends LogicalCommonDdlHandler {

    public LogicalRefreshTopologyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalRefreshTopology logicalRefreshTopology =
            (LogicalRefreshTopology) logicalDdlPlan;
        List<DbInfoRecord> newPartDbInfoRecords =
            DbTopologyManager.getNewPartDbInfoFromMetaDb();
        if (GeneralUtil.isEmpty(newPartDbInfoRecords)) {
            return new TransientDdlJob();
        }

        Map<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> dbTableGroupAndInstGroupInfo =
            new HashMap<>();
        for (DbInfoRecord dbInfoRecord : newPartDbInfoRecords) {
            Map<String, List<Pair<String, String>>> instGroupDbInfo =
                DbTopologyManager.generateDbAndGroupNewConfigInfo(dbInfoRecord.dbName);
            TableGroupConfig tableGroupConfig =
                OptimizerContext.getContext(dbInfoRecord.dbName).getTableGroupInfoManager()
                    .getBroadcastTableGroupConfig();
            if (GeneralUtil.isNotEmpty(instGroupDbInfo)) {
                final boolean shareStorageMode =
                    executionContext.getParamManager().getBoolean(ConnectionParams.SHARE_STORAGE_MODE);
                //for local debug
                if (shareStorageMode) {
                    Map<String, List<Pair<String, String>>> copyInstGroupDbInfo = new HashMap<>();
                    for (Map.Entry<String, List<Pair<String, String>>> entry : instGroupDbInfo.entrySet()) {
                        for (Pair<String, String> pair : entry.getValue()) {
                            copyInstGroupDbInfo.computeIfAbsent(entry.getKey(), o -> new ArrayList<>()).add(Pair.of(
                                GroupInfoUtil.buildGroupNameFromPhysicalDb(pair.getValue() + "S"),
                                pair.getValue() + "S"));
                        }
                    }
                    instGroupDbInfo = copyInstGroupDbInfo;
                }
                dbTableGroupAndInstGroupInfo.put(dbInfoRecord.dbName, new Pair<>(tableGroupConfig, instGroupDbInfo));
            }
        }
        if (GeneralUtil.isEmpty(dbTableGroupAndInstGroupInfo)) {
            // nothing to do, all the new storage insts are inited or not prepared
            return new TransientDdlJob();
        }
        logicalRefreshTopology.preparedData(dbTableGroupAndInstGroupInfo);
        return RefreshTopologyFactory
            .create(logicalRefreshTopology.relDdl, logicalRefreshTopology.getPreparedData(),
                executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        // do nothing
        return false;
    }

}
