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

package com.alibaba.polardbx.optimizer.view;

import com.alibaba.polardbx.common.privilege.PrivilegeVerifyItem;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dylan
 */
public class DrdsViewExpander implements RelOptTable.ToRelContext {

    private RelOptCluster cluster;

    private final String schemaName;

    private SqlConverter sqlConverter;

    private CalciteCatalogReader catalog;

    public DrdsViewExpander(RelOptCluster cluster, SqlConverter sqlConverter, CalciteCatalogReader catalog) {
        this.schemaName = PlannerContext.getPlannerContext(cluster).getSchemaName();
        this.cluster = cluster;
        this.sqlConverter = sqlConverter;
        this.catalog = catalog;
    }

    @Override
    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        PlannerContext plannerContext = PlannerContext.getPlannerContext(getCluster());
        plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_POST_PLANNER, false);
        plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_DIRECT_PLAN, false);
        if (SystemTableView.Row.isVirtual(queryString)) {
            VirtualView virtualView =
                VirtualView.create(getCluster(), SystemTableView.Row.getVirtualViewType(queryString));
            return RelRoot.of(virtualView, SqlKind.VIRTUAL_VIEW);
        } else {
            SqlNode ast = new FastsqlParser().parse(queryString, plannerContext.getExecutionContext()).get(0);
            SqlNode validatedNode = sqlConverter.validate(ast);
            return RelRoot.of(sqlConverter.toRel(validatedNode, getCluster()), ast.getKind());
        }
    }

    public RelRoot expandDrdsView(RelDataType rowType, RelOptTableImpl relOptTableImpl, List<String> schemaPath,
                                  List<String> viewPath) {
        DrdsViewTable drdsViewTable = CBOUtil.getDrdsViewTable(relOptTableImpl);
        assert drdsViewTable != null;
        SystemTableView.Row row = drdsViewTable.getRow();
        String queryString = row.getViewDefinition();
        PlannerContext plannerContext = PlannerContext.getPlannerContext(getCluster());
        plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_POST_PLANNER, false);
        plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_DIRECT_PLAN, false);
        ExecutionContext executionContext = plannerContext.getExecutionContext();

        SqlConverter viewSqlConverter = SqlConverter.getInstance(row.getSchemaName(), executionContext);

        if (SystemTableView.Row.isVirtual(queryString)) {
            VirtualView virtualView =
                VirtualView.create(getCluster(), SystemTableView.Row.getVirtualViewType(queryString));
            return RelRoot.of(virtualView, SqlKind.VIRTUAL_VIEW);
        } else if (row.getPlan() != null && row.getPlanType() != null) {
            List<PrivilegeVerifyItem> originalItems = null;
            if (executionContext.getPrivilegeContext() != null) {
                originalItems = executionContext.getPrivilegeContext().getPrivilegeVerifyItems();
            }
            if (executionContext.getPrivilegeContext() != null) {
                executionContext.getPrivilegeContext().setPrivilegeVerifyItems(null);
            }
            SqlNode ast = new FastsqlParser().parse(queryString, executionContext).get(0);
            if (executionContext.getPrivilegeContext() != null) {
                executionContext.getPrivilegeContext().setPrivilegeVerifyItems(originalItems);
            }
            if (row.isMppPlanType()) {
                plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_MPP, true);
            } else {
                plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_MPP, false);
            }
            String jsonPlan = row.getPlan();
            RelNode plan;
            try {
                plan = PlanManagerUtil.jsonToRelNode(jsonPlan, getCluster(), catalog);
                ViewPlan viewPlan = ViewPlan.create(
                    getCluster(),
                    relOptTableImpl,
                    plan);
                return RelRoot.of(viewPlan, ast.getKind());
            } catch (Exception e) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                String planError = sw.toString();
                ViewManager viewManager = OptimizerContext.getContext(row.getSchemaName()).getViewManager();
                viewManager.recordPlanError(row.getSchemaName(), row.getViewName(), planError);
                viewManager.invalidate(row.getViewName());
                throw e;
            }
        } else if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_CROSS_VIEW_OPTIMIZE)) {
            List<PrivilegeVerifyItem> originalItems = null;
            if (executionContext.getPrivilegeContext() != null) {
                originalItems = executionContext.getPrivilegeContext().getPrivilegeVerifyItems();
            }
            if (executionContext.getPrivilegeContext() != null) {
                executionContext.getPrivilegeContext().setPrivilegeVerifyItems(null);
            }
            SqlNode ast = new FastsqlParser().parse(queryString, executionContext).get(0);
            if (executionContext.getPrivilegeContext() != null) {
                executionContext.getPrivilegeContext().setPrivilegeVerifyItems(originalItems);
            }
            RelNode plan = Planner.getInstance().getPlan(ast, plannerContext).getPlan();
            ViewPlan viewPlan = ViewPlan.create(getCluster(), relOptTableImpl, plan);
            return RelRoot.of(viewPlan, ast.getKind());
        } else {
            List<PrivilegeVerifyItem> originalItems = null;
            if (executionContext.getPrivilegeContext() != null) {
                originalItems = executionContext.getPrivilegeContext().getPrivilegeVerifyItems();
            }
            if (executionContext.getPrivilegeContext() != null) {
                executionContext.getPrivilegeContext().setPrivilegeVerifyItems(null);
            }
            SqlNode ast = new FastsqlParser().parse(queryString, executionContext).get(0);
            if (executionContext.getPrivilegeContext() != null) {
                executionContext.getPrivilegeContext().setPrivilegeVerifyItems(originalItems);
            }
            SqlNode validatedNode = viewSqlConverter.validate(ast);
            return RelRoot.of(viewSqlConverter.toRel(validatedNode, getCluster()), ast.getKind());
        }
    }

    @Override
    public RelOptCluster getCluster() {
        return cluster;
    }
}
