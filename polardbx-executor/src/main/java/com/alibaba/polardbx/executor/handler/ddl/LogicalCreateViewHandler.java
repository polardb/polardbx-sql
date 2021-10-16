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

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.CreateViewSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateView;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.TDDLSqlSelect;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author dylan
 */
public class LogicalCreateViewHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalCreateViewHandler.class);

    private static final int MAX_VIEW_NAME_LENGTH = 64;

    private static final int MAX_VIEW_NUMBER = 10000;

    public LogicalCreateViewHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(final RelNode logicalPlan, ExecutionContext executionContext) {

        LogicalCreateView logicalCreateView = (LogicalCreateView) logicalPlan;

        String schemaName = logicalCreateView.getSchemaName();
        String viewName = logicalCreateView.getViewName();
        boolean isReplace = logicalCreateView.isReplace();
        List<String> columnList = logicalCreateView.getColumnList();
        String viewDefinition = RelUtils.toNativeSql(logicalCreateView.getDefinition(), DbType.MYSQL);
        String planString = null;
        String planType = null;

        if (!checkUtf8(viewName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW,
                "View name should be encoded as utf8");
        } else if (viewName.length() > MAX_VIEW_NAME_LENGTH) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW,
                "View name length " + viewName.length() + " > MAX_VIEW_NAME_LENGTH " + MAX_VIEW_NAME_LENGTH);
        }

        ViewManager viewManager = OptimizerContext.getContext(schemaName).getViewManager();

        if (viewManager.count(schemaName) > MAX_VIEW_NUMBER) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "View number at most " + MAX_VIEW_NUMBER);
        }

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

        if (columnList != null) {
            SqlNode ast = new FastsqlParser().parse(viewDefinition).get(0);
            SqlConverter converter = SqlConverter.getInstance(schemaName, executionContext);
            SqlNode validatedNode = converter.validate(ast);
            RelDataType rowType = converter.toRel(validatedNode).getRowType();
            if (rowType.getFieldCount() != columnList.size()) {
                throw new TddlRuntimeException(ErrorCode.ERR_VIEW,
                    "View's SELECT and view's field list have different column counts");
            }
        }

        // check view name
        TableMeta tableMeta;
        try {
            tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(viewName);
        } catch (Throwable throwable) {
            // pass
            tableMeta = null;
        }

        if (tableMeta != null) {
            if (isReplace) {
                throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "'" + viewName + "' is not VIEW ");
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "table '" + viewName + "' already exists ");
            }
        }

        boolean success = false;

        if (isReplace) {
            success = viewManager
                .replace(viewName, columnList, viewDefinition, executionContext.getConnection().getUser(), planString,
                    planType);
        } else {
            if (viewManager.select(viewName) != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "table '" + viewName + "' already exists ");
            }
            success = viewManager
                .insert(viewName, columnList, viewDefinition, executionContext.getConnection().getUser(), planString,
                    planType);
        }

        if (!success) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW,
                "create view fail for " + viewManager.getSystemTableView().getTableName() + " can not "
                    + "write");

        }

        SyncManagerHelper.sync(new CreateViewSyncAction(schemaName, viewName), schemaName);

        return new AffectRowCursor(new int[] {0});
    }

    private boolean checkUtf8(String s) {
        try {
            s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            return false;
        }
        return true;
    }
}

