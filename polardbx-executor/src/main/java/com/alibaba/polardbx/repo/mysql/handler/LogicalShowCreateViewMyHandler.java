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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.view.InformationSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.MysqlSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowCreateView;

/**
 * @author dylan
 */
public class LogicalShowCreateViewMyHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalShowCreateViewMyHandler.class);

    public LogicalShowCreateViewMyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowCreateView showCreateView = (SqlShowCreateView) show.getNativeSqlNode();
        final String viewName = RelUtils.lastStringValue(showCreateView.getTableName());
        //DBeaver use SHOW CREATE VIEW db_name.view_name to fetch view information
        //we need to fetch schemaName by LogicalShow instead of ExecutionContext
        String schemaName = show.getSchemaName();
        if (schemaName == null) {
            schemaName = executionContext.getSchemaName();
        }
        ViewManager viewManager;
        if (InformationSchema.NAME.equalsIgnoreCase(schemaName)) {
            viewManager = InformationSchemaViewManager.getInstance();
        } else if (RelUtils.informationSchema(showCreateView.getViewName())) {
            viewManager = InformationSchemaViewManager.getInstance();
        } else if (RelUtils.mysqlSchema(showCreateView.getViewName())) {
            viewManager = MysqlSchemaViewManager.getInstance();
        } else {
            viewManager = OptimizerContext.getContext(schemaName).getViewManager();
        }

        SystemTableView.Row row = viewManager.select(viewName);
        if (row != null) {
            ArrayResultCursor resultCursor = new ArrayResultCursor(viewName);
            // | View | Create View | character_set_client | collation_connection |
            resultCursor.addColumn("View", DataTypes.StringType);
            resultCursor.addColumn("Create View", DataTypes.StringType);
            resultCursor.addColumn("character_set_client", DataTypes.StringType);
            resultCursor.addColumn("collation_connection", DataTypes.StringType);
            boolean printPlan = row.getPlan() != null && row.getPlanType() != null;
            if (printPlan) {
                // | PLAN | PLAN_TYPE |
                resultCursor.addColumn("PLAN", DataTypes.StringType);
                resultCursor.addColumn("PLAN_TYPE", DataTypes.StringType);
            }

            String createView = row.isVirtual() ? "[VIRTUAL_VIEW] " + row.getViewDefinition() :
                "CREATE VIEW `" + viewName + "` AS " + row.getViewDefinition();

            if (printPlan) {
                String explainString;
                RelOptSchema relOptSchema = SqlConverter.getInstance(executionContext).getCatalog();
                try {
                    explainString = RelUtils.toString(PlanManagerUtil
                        .jsonToRelNode(row.getPlan(), logicalPlan.getCluster(), relOptSchema));
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                    explainString = throwable.getMessage();
                }

                resultCursor.addRow(new Object[] {
                    viewName,
                    createView,
                    "utf8",
                    "utf8_general_ci", "\n" + explainString, row.getPlanType()});
            } else {
                resultCursor.addRow(new Object[] {
                    viewName,
                    createView,
                    "utf8",
                    "utf8_general_ci"});
            }
            return resultCursor;
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW, viewName + " is not VIEW");
        }
    }
}

