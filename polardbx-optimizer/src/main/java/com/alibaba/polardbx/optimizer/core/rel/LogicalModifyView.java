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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author lingce.ldm 2018-01-31 13:48
 */
public class LogicalModifyView extends LogicalView {

    public LogicalModifyView(LogicalView logicalView) {
        super(logicalView);
        this.tableNames = logicalView.getTableNames();
        this.pushDownOpt = logicalView.getPushDownOpt().copy(this, logicalView.getPushedRelNode());
        this.finishShard = logicalView.getFinishShard();
        this.hints = logicalView.getHints();
        this.hintContext = logicalView.getHintContext();
    }

    @Override
    public RelToSqlConverter getConverter() {
        return super.getConverter();
    }

    @Override
    public List<RelNode> getInput(ExecutionContext executionContext) {
        Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);
        List<String> logTbls = new ArrayList<>();
        logTbls.addAll(this.tableNames);

        PhyTableModifyViewBuilder phyTableModifyBuilder =
            new PhyTableModifyViewBuilder(getSqlTemplate(executionContext),
                targetTables,
                executionContext,
                this,
                dbType,
                logTbls,
                getSchemaName());
        return phyTableModifyBuilder.build();
    }

    @Override
    public List<RelNode> getInputWithoutCache(ExecutionContext executionContext) {
        return getInput(executionContext);
    }

    /**
     * getInput with sqlTemplate. Why not cache sqlTemplate: LogicalModifyView
     * is put into planCache, so sqlTemplate should be recreated.
     */
    public List<RelNode> getInput(SqlNode sqlTemplate, ExecutionContext executionContext) {
        Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);
        List<String> logTbls = new ArrayList<>();
//        if (this.getTableModify() instanceof LogicalInsert) {
//            logTbls.addAll(this.getTableModify().getTargetTableNames());
//        }
//        logTbls.addAll(this.getTableModify().getSourceTableNames());
        logTbls.addAll(this.tableNames);
        PhyTableModifyViewBuilder phyTableModifyBuilder = new PhyTableModifyViewBuilder(sqlTemplate,
            targetTables,
            executionContext,
            this,
            dbType,
            logTbls,
            getSchemaName()
        );
        return phyTableModifyBuilder.build();
    }

    @Override
    public String explainNodeName() {
        return "LogicalModifyView";
    }

    public TableModify getTableModify() {
        return this.pushDownOpt.getTableModify();
    }

    public boolean hasHint() {
        return targetTablesHintCache != null && !targetTablesHintCache.isEmpty();
    }

    @Override
    public LogicalModifyView copy(RelTraitSet traitSet) {
        LogicalModifyView logicalModifyView = new LogicalModifyView(this);
        logicalModifyView.traitSet = traitSet;
        logicalModifyView.pushDownOpt = pushDownOpt.copy(this, this.getPushedRelNode());
        return logicalModifyView;
    }
}
