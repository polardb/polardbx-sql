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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.config.InstanceRoleManager;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ExecutorCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.operator.FilterExec;
import com.alibaba.polardbx.executor.operator.ResultSetCursorExec;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlShowVariables;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.Map.Entry;

/**
 * @author chenmo.cm
 */
public class LogicalShowVariablesHandler extends HandlerCommon {

    public LogicalShowVariablesHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;

        ArrayResultCursor cursor = new ArrayResultCursor("VARIABLES");
        cursor.addColumn("Variable_name", DataTypes.StringType);
        cursor.addColumn("Value", DataTypes.StringType);
        cursor.initMeta();

        if (executionContext.getServerVariables() != null) {
            // SHOW VARIABLES
            for (Entry<String, Object> entry : executionContext.getServerVariables().entrySet()) {
                cursor.addRow(new Object[] {entry.getKey(), entry.getValue()});
            }
        }

        if (executionContext.getExtraServerVariables() != null) {
            // SHOW EXTRA VARIABLES
            for (Entry<String, Object> entry : executionContext.getExtraServerVariables().entrySet()) {
                cursor.addRow(new Object[] {entry.getKey(), entry.getValue()});
            }
        }

        // DRDS_TRANSACTION_POLICY
        cursor.addRow(new Object[] {
            TransactionAttribute.DRDS_TRANSACTION_POLICY,
            executionContext.getConnection().getTrxPolicy().toString()});

        // BATCH_INSERT_POLICY
        cursor.addRow(new Object[] {
            BatchInsertPolicy.getVariableName(),
            executionContext.getConnection().getBatchInsertPolicy(executionContext.getExtraCmds()).getName()});

        // DRDS_INSTANCE_ROLE
        cursor.addRow(new Object[] {
            InstanceRoleManager.INSTANCE_ROLE_VARIABLE,
            InstanceRoleManager.INSTANCE.getInstanceRole()});

        // SHARE_READ_VIEW
        cursor.addRow(new Object[] {
            TransactionAttribute.SHARE_READ_VIEW,
            executionContext.isShareReadView()});

        final SqlShowVariables showVariables = (SqlShowVariables) show.getNativeSqlNode();
        if (showVariables.like != null) {
            final String pattern = RelUtils.stringValue(showVariables.like);
            RexBuilder rexBuilder = new RexBuilder(new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance()));
            RexNode likeCondition = rexBuilder.makeCall(
                SqlStdOperatorTable.LIKE,
                Arrays
                    .asList(rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0),
                        rexBuilder.makeLiteral(pattern)));
            IExpression expression = RexUtils.buildRexNode(likeCondition, executionContext);

            FilterExec filterExec =
                new FilterExec(new ResultSetCursorExec(cursor, executionContext, Long.MAX_VALUE), expression,
                    null, executionContext);
            return new ExecutorCursor(filterExec, cursor.getMeta());
        }

        return cursor;
    }
}
