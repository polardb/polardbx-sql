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
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.List;

/**
 * Drop a physical db
 *
 * @author chenhui.lch
 */
@TaskName(name = "DropPhysicalDbTask")
@Getter
public class DropPhysicalDbTask extends BaseDdlTask {

    private final static Logger LOG = SQLRecorderLogger.ddlEngineLogger;
    private final String groupName;

    @JSONCreator
    public DropPhysicalDbTask(String schemaName, String groupName) {
        super(schemaName);
        this.groupName = groupName;
        setExceptionAction(DdlExceptionAction.PAUSE);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        // Check if the group is empty
        final DbGroupInfoManager dgm = DbGroupInfoManager.getInstance();
        List<TableGroupConfig> tableGroupConfigs = TableGroupUtils.getAllTableGroupInfoByDb(schemaName);

        DbGroupInfoRecord groupInfo = dgm.queryGroupInfo(schemaName, groupName);
        if (groupInfo == null) {
            LOG.warn(String.format("group %s.%s already not exists", schemaName, groupName));
            return;
        }
        String phyDbName = groupInfo.phyDbName;

        for (TableGroupConfig tg : tableGroupConfigs) {
            for (PartitionGroupRecord pg : tg.getPartitionGroupRecords()) {
                String phyDb = pg.getPhy_db();
                if (TStringUtil.equalsIgnoreCase(phyDb, phyDbName)) {
                    throw DdlHelper.logAndThrowError(LOG,
                        String.format("Non-Empty group could not be dropped: schema=%s,group=%s,pg=%s,pgId=%d",
                            schemaName, groupName, pg.getPartition_name(), pg.getId()));
                }
            }
        }

        PartitionGroupAccessor pgAccessor = new PartitionGroupAccessor();
        pgAccessor.setConnection(metaDbConnection);
        List<PartitionGroupRecord> records = pgAccessor.getPartitionGroupsByPhyDb(phyDbName);
        if (CollectionUtils.isNotEmpty(records)) {
            throw DdlHelper.logAndThrowError(LOG,
                String.format("Non-empty group could not be dropped: schema=%s,group=%s,pg=%s",
                    schemaName, groupName, records));
        }

        SQLRecorderLogger.ddlEngineLogger.info(getDescription());
        updateSupportedCommands(true, false, metaDbConnection);

        Long socketTimeout = executionContext.getParamManager().getLong(ConnectionParams.SOCKET_TIMEOUT);
        long socketTimeoutVal = socketTimeout != null ? socketTimeout : -1;
        DbTopologyManager.cleanPhyDbOfRemovingDbGroup(schemaName, groupName, metaDbConnection, socketTimeoutVal);
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public String getDescription() {
        return String.format("Drop group %s.%s", schemaName, groupName);
    }

}
