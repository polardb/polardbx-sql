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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupAddTablePreparedData;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author luoyanxin
 */
public class AlterTableGroupAddTableJobFactory extends DdlJobFactory {

    final AlterTableGroupAddTablePreparedData preparedData;
    final ExecutionContext executionContext;
    final static String SET_TABLEGROUP_SQL = "%s alter table %s set tablegroup=%s";
    final static String SET_TABLEGROUP_SQL_FORCE = "%s alter table %s set tablegroup=%s force";

    public AlterTableGroupAddTableJobFactory(DDL ddl, AlterTableGroupAddTablePreparedData preparedData,
                                             ExecutionContext executionContext) {
        ;
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        if (GeneralUtil.isEmpty(preparedData.getTables())) {
            return new TransientDdlJob();
        }
        return toDdlJob();
    }

    public ExecutableDdlJob toDdlJob() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        Set<String> sourceTables = new HashSet<>();
        sourceTables.addAll(preparedData.getTables());
        DdlTask curDdl;
        if (preparedData.getReferenceTable() == null) {
            //validate the tablegroup is not empty
            curDdl = new AlterTableGroupValidateTask(preparedData.getSchemaName(),
                preparedData.getTableGroupName(), preparedData.getTableVersions(), false, null);
        } else {
            curDdl = new ValidateTableVersionTask(preparedData.getSchemaName(), preparedData.getTableVersions());
        }
        executableDdlJob.addTask(curDdl);
        if (preparedData.getReferenceTable() != null) {
            sourceTables.remove(preparedData.getReferenceTable());
            DdlTask ddlTask = generateSetTableGroupJob(preparedData.getReferenceTable());
            executableDdlJob.addTask(ddlTask);
            executableDdlJob.addTaskRelationship(curDdl, ddlTask);
            curDdl = ddlTask;
        }
        EmptyTask lastTask = new EmptyTask(preparedData.getSchemaName());
        for (String tableName : sourceTables) {
            DdlTask ddlTask = generateSetTableGroupJob(tableName);
            executableDdlJob.addTask(ddlTask);
            executableDdlJob.addTaskRelationship(curDdl, ddlTask);
            executableDdlJob.addTaskRelationship(ddlTask, lastTask);
        }

        return executableDdlJob;
    }

    private SubJobTask generateSetTableGroupJob(String tableName) {
        String sql = genSubJobSql(tableName);
        SubJobTask subJobTask = new SubJobTask(preparedData.getSchemaName(), sql, null);
        subJobTask.setParentAcquireResource(true);
        return subJobTask;
    }

    private String genSubJobSql(String tableName) {
        List<String> params = Lists.newArrayList(
            ConnectionParams.SKIP_TABLEGROUP_VALIDATOR.getName() + "=true"
        );
        String hint = String.format("/*+TDDL:CMD_EXTRA(%s)*/", StringUtils.join(params, ","));
        if (preparedData.isForce()) {
            return String.format(SET_TABLEGROUP_SQL_FORCE, hint, tableName, preparedData.getTableGroupName());
        } else {
            return String.format(SET_TABLEGROUP_SQL, hint, tableName, preparedData.getTableGroupName());
        }
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableGroupAddTablePreparedData preparedData,
                                          ExecutionContext executionContext) {

        return new AlterTableGroupAddTableJobFactory(ddl, preparedData, executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        for (Map.Entry<String, Long> entry : preparedData.getTableVersions().entrySet()) {
            resources.add(concatWithDot(preparedData.getSchemaName(), entry.getKey()));
        }
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager();
        SchemaManager schemaManager = executionContext.getSchemaManager(preparedData.getSchemaName());
        for (String table : preparedData.getTables()) {
            TableMeta tableMeta = schemaManager.getTable(table);
            if (tableMeta.getPartitionInfo() == null || tableMeta.getPartitionInfo().getTableGroupId() < 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_META_TOO_OLD, preparedData.getSchemaName(), table);
            }
            TableGroupConfig tableGroupConfig =
                tableGroupInfoManager.getTableGroupConfigById(tableMeta.getPartitionInfo().getTableGroupId());
            if (tableGroupConfig == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_META_TOO_OLD, preparedData.getSchemaName(), table);
            }
            resources.add(concatWithDot(preparedData.getSchemaName(), tableGroupConfig.getTableGroupRecord().tg_name));
        }
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()));
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

}
