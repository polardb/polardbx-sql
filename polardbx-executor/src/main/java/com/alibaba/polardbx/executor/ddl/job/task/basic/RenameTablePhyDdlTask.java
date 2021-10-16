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
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.rel.RelNode;

import java.util.List;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;

@TaskName(name = "RenameTablePhyDdlTask")
public class RenameTablePhyDdlTask extends BasePhyDdlTask {

    @JSONCreator
    public RenameTablePhyDdlTask(String schemaName, PhysicalPlanData physicalPlanData) {
        super(schemaName, physicalPlanData);
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        checkTableNamePatternForRename(schemaName, physicalPlanData.getLogicalTableName(), executionContext);
        super.executeImpl(executionContext);
    }

    @Override
    protected List<RelNode> genRollbackPhysicalPlans(ExecutionContext executionContext) {
        return getPhysicalPlans(executionContext);
    }

    private void checkTableNamePatternForRename(String schemaName, String logicalTableName,
                                                ExecutionContext executionContext) {
        boolean hasRandomSuffixInTableNamePattern = true;

        try {
            TableRule tableRule =
                OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(logicalTableName);
            FailPoint.injectRandomExceptionFromHint(executionContext);
            FailPoint.injectRandomSuspendFromHint(executionContext);
            if (tableRule != null && executionContext.isRandomPhyTableEnabled()) {
                String tableNamePattern = tableRule.getTbNamePattern();
                if (TStringUtil.isEmpty(tableNamePattern)
                    || tableNamePattern.length() <= RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME) {
                    // Must be single or broadcast table.
                    hasRandomSuffixInTableNamePattern = false;
                } else if (TStringUtil.startsWithIgnoreCase(tableNamePattern, logicalTableName)) {
                    // Not renamed yet.
                    String randomSuffix = tableRule.extractRandomSuffix();
                    hasRandomSuffixInTableNamePattern = TStringUtil.isNotEmpty(randomSuffix);
                } else {
                    // The table may have been renamed when logical table name
                    // is supported, so that the table name pattern's prefix is
                    // not the logical table name, so it should be safe to
                    // contain random string.
                    hasRandomSuffixInTableNamePattern = true;
                }
            }
        } catch (Throwable ignored) {
        }

        if (executionContext.isRandomPhyTableEnabled() && hasRandomSuffixInTableNamePattern) {
            executionContext.setPhyTableRenamed(false);
        }
    }

}
