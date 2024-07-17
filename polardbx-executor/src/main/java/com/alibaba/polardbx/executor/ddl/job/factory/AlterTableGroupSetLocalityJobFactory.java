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
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupSetLocalityChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.BackgroupRebalanceTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.locality.LocalityDetailInfoRecord;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSetLocalityPreparedData;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * @author taojinkun
 */
public class AlterTableGroupSetLocalityJobFactory extends DdlJobFactory {

    @Deprecated
    protected final DDL ddl;
    protected final AlterTableGroupSetLocalityPreparedData preparedData;
    protected final ExecutionContext executionContext;

    public AlterTableGroupSetLocalityJobFactory(DDL ddl, AlterTableGroupSetLocalityPreparedData preparedData,
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
        Long initWait = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INITWAIT);
        Long interval = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INTERVAL);

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
            PolicyUtils.getLocalityDetails(preparedData.getSchemaName(), preparedData.getTableGroupName());
        DdlTask changeMetaTask = new AlterTableGroupSetLocalityChangeMetaTask(preparedData.getSchemaName(),
            preparedData.getTableGroupName(), logicalTableNames, preparedData.getTargetLocality(),
            toChangeMetaLocalityItems);
        DdlTask tablesSyncTask =
            new TablesSyncTask(preparedData.getSchemaName(), logicalTableNames, true, initWait, interval,
                TimeUnit.MILLISECONDS);

        DdlTask validateTask =
            new AlterTableGroupValidateTask(preparedData.getSchemaName(), preparedData.getTableGroupName(),
                tablesVersion,
                true, null, false);

        DdlTask rebalanceTask =
            new BackgroupRebalanceTask(preparedData.getSchemaName(), preparedData.getRebalanceSql());
        DdlTask tableGroupSyncTask =
            new TableGroupSyncTask(preparedData.getSchemaName(), preparedData.getTableGroupName());

        boolean needToGetCreateTableGroupLock =
            executionContext.getParamManager().getBoolean(ConnectionParams.ACQUIRE_CREATE_TABLE_GROUP_LOCK);
        if (needToGetCreateTableGroupLock) {
            executableDdlJob.addSequentialTasks(Lists.newArrayList(
                generateAlterTableGroupSetLocalityJob()
            ));
            preparedData.setNeedToGetTableGroupLock(true);
        } else {
            preparedData.setNeedToGetTableGroupLock(false);
            if (!preparedData.getWithRebalance()) {
                //case expand locality, change meta info only.
                executableDdlJob.addSequentialTasks(Lists.newArrayList(
                    validateTask,
                    changeMetaTask,
                    tablesSyncTask,
                    tableGroupSyncTask
                ));
            } else {
                //case shrink or expire locality, call meta.
                executableDdlJob.addSequentialTasks(Lists.newArrayList(
                    validateTask,
                    changeMetaTask,
                    tablesSyncTask,
                    tableGroupSyncTask,
                    rebalanceTask
                ));
            }
        }
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableGroupSetLocalityPreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new AlterTableGroupSetLocalityJobFactory(ddl, preparedData, executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        if (preparedData.getNeedToGetTableGroupLock()) {
            resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()));
            TableGroupConfig tableGroupConfig =
                OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager()
                    .getTableGroupConfigByName(preparedData.getTableGroupName());
            for (String tableName : tableGroupConfig.getAllTables()) {
                resources.add(concatWithDot(preparedData.getSchemaName(), tableName));
            }
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    private SubJobTask generateAlterTableGroupSetLocalityJob() {
        String sql = genSubJobSql();
        SubJobTask subJobTask = new SubJobTask(preparedData.getSchemaName(), sql, null);
        subJobTask.setParentAcquireResource(true);
        return subJobTask;
    }

    private String genSubJobSql() {
        List<String> params = Lists.newArrayList(
            ConnectionParams.ACQUIRE_CREATE_TABLE_GROUP_LOCK.getName() + "=false"
        );
        String hint = String.format("/*+TDDL:CMD_EXTRA(%s)*/", StringUtils.join(params, ","));
        return String.format(hint + preparedData.getSourceSql());
    }

}
