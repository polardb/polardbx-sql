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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterJoinGroupJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.MergeTableGroupJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterJoinGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalMergeTableGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MergeTableGroupPreparedData;
import org.apache.calcite.sql.SqlAlterTableGroup;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class LogicalMergeTableGroupHandler extends LogicalCommonDdlHandler {

    public static Logger logger = LoggerFactory.getLogger(LogicalMergeTableGroupHandler.class);

    public LogicalMergeTableGroupHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalMergeTableGroup logicalMergeTableGroup = (LogicalMergeTableGroup) logicalDdlPlan;
        if (logicalMergeTableGroup.getPreparedData() == null) {
            logicalMergeTableGroup.preparedData(executionContext);
        }
        MergeTableGroupPreparedData preparedData = logicalMergeTableGroup.getPreparedData();

        boolean emptySourceTableGroup = true;
        for (String sourceTableGroup : preparedData.getSourceTableGroups()) {
            if (!GeneralUtil.isEmpty(preparedData.getTablesVersion().get(sourceTableGroup))) {
                emptySourceTableGroup = false;
                break;
            }
        }
        if (emptySourceTableGroup) {
            return new TransientDdlJob();
        }

        return MergeTableGroupJobFactory.create(preparedData, executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(executionContext.getSchemaName());
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "can't execute the merge tableGroup command in non-partitioning database");
        }
        LogicalMergeTableGroup logicalMergeTableGroup = (LogicalMergeTableGroup) logicalDdlPlan;
        if (logicalMergeTableGroup.getPreparedData() == null) {
            logicalMergeTableGroup.preparedData(executionContext);
        }
        MergeTableGroupPreparedData preparedData = logicalMergeTableGroup.getPreparedData();

        AlterTableGroupUtils.mergeTableGroupCheck(preparedData, executionContext);
        String tableGroupName = preparedData.getTargetTableGroupName();
        String schemaName = preparedData.getSchemaName();
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        assert tableGroupConfig != null;
        final boolean onlyManualTableGroupAllowed =
            executionContext.getParamManager().getBoolean(ConnectionParams.ONLY_MANUAL_TABLEGROUP_ALLOW);
        if (!tableGroupConfig.isManuallyCreated() && onlyManualTableGroupAllowed) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_IS_AUTO_CREATED,
                String.format(
                    "only the tablegroup create by user manually could by use explicitly, the table group[%s] is created internally",
                    tableGroupName));
        }
        return false;
    }
}
