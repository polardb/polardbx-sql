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
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "DropDbGroupHideMetaTask")
public class DropDbGroupHideMetaTask extends BaseDdlTask {

    private final static Logger LOG = SQLRecorderLogger.ddlEngineLogger;
    private List<String> targetGroupNames;

    @JSONCreator
    public DropDbGroupHideMetaTask(String schemaName, List<String> targetGroupNames) {
        super(schemaName);
        this.targetGroupNames = targetGroupNames;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
    }

    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {

        // Check if the group is empty
        final DbGroupInfoManager dgm = DbGroupInfoManager.getInstance();
        List<TableGroupConfig> tableGroupConfigs = TableGroupUtils.getAllTableGroupInfoByDb(schemaName);
        for (String groupName : targetGroupNames) {
            DbGroupInfoRecord groupInfo = dgm.queryGroupInfo(schemaName, groupName);
            if (groupInfo != null) {
                String phyDbName = groupInfo.phyDbName;

                for (TableGroupConfig tg : tableGroupConfigs) {
                    for (PartitionGroupRecord pg : tg.getPartitionGroupRecords()) {
                        String phyDb = pg.getPhy_db();
                        if (TStringUtil.equalsIgnoreCase(phyDb, phyDbName)) {
                            throw DdlHelper.logAndThrowError(LOG,
                                String.format(
                                    "Non-Empty group could not be hide: schema=%s,group=%s,tg=%d, pg=%s,pgId=%d",
                                    schemaName, groupName, pg.getTg_id(), pg.getPartition_name(), pg.getId()));
                        }
                    }
                }
            }
        }

        /**
         * Update the status of all the to-removed group to "REMOVING" from the "BEFORE_REMOVED"
         */
        DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
        dbGroupInfoAccessor.setConnection(metaDbConnection);
        String dbName = this.schemaName;
        for (int i = 0; i < targetGroupNames.size(); i++) {
            String grpName = targetGroupNames.get(i);
            dbGroupInfoAccessor.updateGroupTypeByDbAndGroup(dbName, grpName, DbGroupInfoRecord.GROUP_TYPE_REMOVING);
        }

        /**
         * Update the opVersion of the topology of the schema
         * so all the cn nodes can be clean the to-removed group data sources
         */
        String topologyDataId = MetaDbDataIdBuilder.getDbTopologyDataId(dbName);
        MetaDbConfigManager.getInstance().notify(topologyDataId, metaDbConnection);

        /**
         * Not allowed to rollback
         */
        updateSupportedCommands(true, false, metaDbConnection);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        /**
         * Update the status of all the to-removed group to "BEFORE_REMOVED" from the "REMOVING"
         */
        DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
        dbGroupInfoAccessor.setConnection(metaDbConnection);
        String dbName = this.schemaName;
        for (int i = 0; i < targetGroupNames.size(); i++) {
            String grpName = targetGroupNames.get(i);
            dbGroupInfoAccessor.updateGroupTypeByDbAndGroup(dbName, grpName,
                DbGroupInfoRecord.GROUP_TYPE_BEFORE_REMOVE);
        }

        /**
         * Update the opVersion of the topology of the schema
         * so all the cn nodes can be clean the to-removed group data sources
         */
        String topologyDataId = MetaDbDataIdBuilder.getDbTopologyDataId(dbName);
        MetaDbConfigManager.getInstance().notify(topologyDataId, metaDbConnection);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }
}
