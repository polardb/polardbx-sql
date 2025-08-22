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

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.balancer.policy.PolicyUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupSetPartitionsLocalityChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.BackgroupRebalanceTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.locality.LocalityDetailInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.PreemptiveTime;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSetPartitionsLocalityPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author taojinkun
 */
public class AlterTableGroupSetPartitionsLocalityJobFactory extends DdlJobFactory {

    @Deprecated
    protected final DDL ddl;
    protected final AlterTableGroupSetPartitionsLocalityPreparedData preparedData;
    protected final ExecutionContext executionContext;

    public AlterTableGroupSetPartitionsLocalityJobFactory(DDL ddl,
                                                          AlterTableGroupSetPartitionsLocalityPreparedData preparedData,
                                                          ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.ddl = ddl;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        //TODO: create task of modify locality.
        //TODO: create Subjob of rebalance.
        //TODO: create task of recover locality.
        PreemptiveTime preemptiveTime = PreemptiveTime.getPreemptiveTimeFromExecutionContext(executionContext,
            ConnectionParams.PREEMPTIVE_MDL_INITWAIT, ConnectionParams.PREEMPTIVE_MDL_INTERVAL);


        Map<String, Long> tablesVersion = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        List<String> logicalTableNames = new ArrayList<>();
        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager()
                .getTableGroupConfigByName(preparedData.getTableGroupName());
        for (String tableName : tableGroupConfig.getAllTables()) {
            String primaryTableName;
            TableMeta tableMeta = executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(tableName);
            if (tableMeta.isGsi()) {
                //all the gsi table version change will be behavior by primary table
                assert
                    tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                tableMeta = executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(primaryTableName);
            } else {
                primaryTableName = tableName;
            }
            logicalTableNames.add(primaryTableName);
            tablesVersion.put(primaryTableName, tableMeta.getVersion());
        }

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<LocalityDetailInfoRecord> toChangeMetaLocalityItems =
            PolicyUtils.getLocalityDetails(preparedData.getSchemaName(), preparedData.getTableGroupName(),
                preparedData.getPartition());
        String localityInfo = LocalityInfoUtils.parse(preparedData.getTargetLocality()).toString();
        DdlTask changeMetaTask = new AlterTableGroupSetPartitionsLocalityChangeMetaTask(preparedData.getSchemaName(),
            preparedData.getTableGroupName(), logicalTableNames, preparedData.getPartition(),
            localityInfo,
            toChangeMetaLocalityItems);
        DdlTask tablesSyncTask =
            new TablesSyncTask(preparedData.getSchemaName(), logicalTableNames, true, preemptiveTime);

        DdlTask validateTask =
            new AlterTableGroupValidateTask(preparedData.getSchemaName(), preparedData.getTableGroupName(),
                tablesVersion,
                true, null, false);

        BackgroupRebalanceTask rebalanceTask =
            new BackgroupRebalanceTask(preparedData.getSchemaName(), preparedData.getRebalanceSql());
        DdlTask tableGroupSyncTask =
            new TableGroupSyncTask(preparedData.getSchemaName(), preparedData.getTableGroupName());
        if (!preparedData.getWithRebalance()) {
            executableDdlJob.addSequentialTasks(Lists.newArrayList(
                validateTask,
                changeMetaTask,
                tablesSyncTask,
                tableGroupSyncTask
            ));
        } else {
            executableDdlJob.addSequentialTasks(Lists.newArrayList(
                validateTask,
                changeMetaTask,
                tablesSyncTask,
                tableGroupSyncTask,
                rebalanceTask
            ));
//            executableDdlJob.addTaskRelationship(rebalanceTask, changeMetaTask);
        }
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableGroupSetPartitionsLocalityPreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new AlterTableGroupSetPartitionsLocalityJobFactory(ddl, preparedData, executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
//        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()));
//        TableGroupConfig tableGroupConfig =
//            OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager()
//                .getTableGroupConfigByName(preparedData.getTableGroupName());
//        for (TablePartRecordInfoContext tablePartRecordInfoContext : tableGroupConfig.getAllTables()) {
//            String tableName = tablePartRecordInfoContext.getLogTbRec().getTableName();
//            resources.add(concatWithDot(preparedData.getSchemaName(), tableName));
//        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
