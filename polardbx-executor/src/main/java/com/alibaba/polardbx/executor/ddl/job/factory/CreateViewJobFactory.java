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

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateViewAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcCreateViewMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateViewSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ValidateCreateViewTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateView;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateView;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.TDDLSqlSelect;

import java.util.List;
import java.util.Set;

public class CreateViewJobFactory extends DdlJobFactory {

    private final LogicalCreateView logicalCreateView;

    ExecutionContext executionContext;

    public CreateViewJobFactory(LogicalCreateView logicalCreateView, ExecutionContext ec) {
        this.logicalCreateView = logicalCreateView;
        this.executionContext = ec;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {

        String schemaName = logicalCreateView.getSchemaName();
        String viewName = logicalCreateView.getViewName();
        boolean isReplace = logicalCreateView.isReplace();
        boolean isAlter = logicalCreateView.isAlter();
        List<String> columnList = logicalCreateView.getColumnList();
        String viewDefinition = RelUtils.toNativeSql(logicalCreateView.getDefinition(), DbType.MYSQL);
        String planString = null;
        String planType = null;

        if (logicalCreateView.getDefinition() instanceof TDDLSqlSelect) {
            TDDLSqlSelect tddlSqlSelect = (TDDLSqlSelect) logicalCreateView.getDefinition();
            if (tddlSqlSelect.getHints() != null && tddlSqlSelect.getHints().size() != 0) {
                String withHintSql =
                    ((SQLCreateViewStatement) FastsqlUtils.parseSql(executionContext.getSql()).get(0)).getSubQuery()
                        .toString();
                // FIXME: by now only support SMP plan.
                executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_MPP, false);
                executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_PARAMETER_PLAN, false);
                ExecutionPlan executionPlan =
                    Planner.getInstance().plan(withHintSql, executionContext.copy());
                if (PlanManagerUtil.canConvertToJson(executionPlan, executionContext.getParamManager())) {
                    planString = PlanManagerUtil.relNodeToJson(executionPlan.getPlan());
                    planType = "SMP";
                }
            }
        }

        DdlTask validateTask = new ValidateCreateViewTask(schemaName, viewName, isReplace);
        DdlTask addMetaTask = new CreateViewAddMetaTask(schemaName, viewName,
            isReplace, columnList, viewDefinition, planString, planType);

        DdlTask cdcMarkTask = new CdcCreateViewMarkTask(schemaName, viewName, isAlter);
        DdlTask syncTask = new CreateViewSyncTask(schemaName, viewName);

        ExecutableDdlJob4CreateView executableDdlJob4CreateView = new ExecutableDdlJob4CreateView();
        executableDdlJob4CreateView.setValidateCreateViewTask((ValidateCreateViewTask) validateTask);
        executableDdlJob4CreateView.setCreateViewAddMetaTask((CreateViewAddMetaTask) addMetaTask);
        executableDdlJob4CreateView.setCdcCreateViewMarkTask((CdcCreateViewMarkTask) cdcMarkTask);
        executableDdlJob4CreateView.setCreateViewSyncTask((CreateViewSyncTask) syncTask);
        executableDdlJob4CreateView.addSequentialTasks(
            Lists.newArrayList(validateTask, addMetaTask, cdcMarkTask, syncTask));

        return executableDdlJob4CreateView;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(logicalCreateView.getSchemaName(), logicalCreateView.getViewName()));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }
}