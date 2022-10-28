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

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.properties.SystemPropertiesHelper;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ExecutorCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.LogicalShowVariablesHandler;
import com.alibaba.polardbx.executor.operator.FilterExec;
import com.alibaba.polardbx.executor.operator.ResultSetCursorExec;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.config.impl.MetaDbVariableConfigManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.ssl.SSLVariables;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlShowVariables;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import static com.alibaba.polardbx.common.constants.ServerVariables.SUPPORT_SET_GLOBAL_VARIABLES;

/**
 * @author chenmo.cm
 */
public class LogicalShowVariablesMyHandler extends LogicalShowVariablesHandler {

    private static final Logger logger = LoggerFactory.getLogger(LogicalShowVariablesMyHandler.class);

    public LogicalShowVariablesMyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        final LogicalShow show = (LogicalShow) logicalPlan;
        final List<Throwable> exceptions = new ArrayList<>();
        final TreeMap<String, Object> variables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final SqlShowVariables showVariables = (SqlShowVariables) show.getNativeSqlNode();
        final boolean isGlobal = showVariables.isGlobal();

        Cursor cursor = null;
        boolean showAllParams = executionContext.getParamManager().getBoolean(ConnectionParams.SHOW_ALL_PARAMS);
        try {
            cursor = super.handle(logicalPlan, executionContext);
            extractVariableFromCursor(variables, cursor, isGlobal, showAllParams);
            cursor.close(exceptions);

            cursor = repo.getCursorFactory().repoCursor(executionContext, show);
            extractVariableFromCursor(variables, cursor, isGlobal, showAllParams);
        } finally {
            // 关闭cursor
            if (cursor != null) {
                cursor.close(exceptions);
            }
        }

        // For ssl configurations
        SSLVariables.fill(variables);

        if (variables.containsKey("max_allowed_packet")) {
            String maxAllowedPacket = System.getProperty("maxAllowedPacket", String.valueOf(1024 * 1024));
            String maxAllowedPacketCustom =
                String.valueOf(executionContext.getParamManager().getLong(ConnectionParams.MAX_ALLOWED_PACKET));
            if (StringUtils.isNotEmpty(maxAllowedPacketCustom)) {
                maxAllowedPacket = maxAllowedPacketCustom;
            }

            variables.put("max_allowed_packet", maxAllowedPacket);
        }

        if (variables.containsKey("max_user_connections")) {
            variables.put("max_user_connections", System.getProperty("maxConnection", "20000"));
        }

        if (variables.containsKey("max_connections")) {
            variables.put("max_connections", System.getProperty("maxConnection", "20000"));
        }

        if (variables.containsKey("autocommit")) {
            if (executionContext.isAutoCommit()) {
                variables.put("autocommit", "ON");
            } else {
                variables.put("autocommit", "OFF");
            }
        }

        if (variables.containsKey("read_only")) {
            if (ConfigDataMode.isMasterMode()) {
                variables.put("read_only", "OFF");
            } else if (ConfigDataMode.isSlaveMode()) {
                variables.put("read_only", "ON");
            }
        }

        if (variables.containsKey("auto_increment_increment") && !SequenceManagerProxy.getInstance()
            .areAllSequencesSameType(executionContext.getSchemaName(), SequenceAttribute.Type.SIMPLE)) {
            // Since the steps of Group and Time-based Sequence are fixed to 1,
            // so we have to override auto_increment_increment set on RDS for
            // correct behavior of generated keys, unless all sequence types
            // are SIMPLE which allows custom step/increment.
            variables.put("auto_increment_increment", 1);
        }

        ArrayResultCursor result = new ArrayResultCursor("Show Variables");
        result.addColumn("Variable_name", DataTypes.StringType);
        result.addColumn("Value", DataTypes.StringType);
        result.initMeta();

        for (Map.Entry<String, Object> entry : variables.entrySet()) {
            result.addRow(new Object[] {entry.getKey(), entry.getValue()});
        }

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
                new FilterExec(new ResultSetCursorExec(
                    result, executionContext, Long.MAX_VALUE), expression, null, executionContext);
            return new ExecutorCursor(filterExec, result.getMeta());
        }

        return result;
    }

    private void extractVariableFromCursor(TreeMap<String, Object> variables,
                                           Cursor cursor, boolean isGlobal, boolean showAllParams) {
        Row row;
        while ((row = cursor.next()) != null) {
            String variableName = row.getString(0);
            String variableValue = row.getString(1);
            variables.put(variableName, variableValue);
        }

        Properties cnVariableConfigMap = MetaDbInstConfigManager.getInstance().getCnVariableConfigMap();
        Map<String, Object> dnVariableConfigMap = MetaDbVariableConfigManager.getInstance().getDnVariableConfigMap();
        if (isGlobal) {
            if (showAllParams) {
                Set<String> connectionProperties = SystemPropertiesHelper.getConnectionProperties();
                for (String variableName : connectionProperties) {
                    if (cnVariableConfigMap.containsKey(variableName)) {
                        variables.put(MetaDbInstConfigManager.getOriginalName(variableName),
                            cnVariableConfigMap.getProperty(variableName));

                    }
                }
            }
            // Add all supported "set-global" variables into result if they are absent.
            final ParamManager paramManager = new ParamManager(cnVariableConfigMap);
            SUPPORT_SET_GLOBAL_VARIABLES.forEach((paramName) -> {
                final String cnParamName = paramName.toUpperCase();
                if (null != variables.get(cnParamName)) {
                    // Already exists in result set, return.
                    return;
                }
                String val = cnVariableConfigMap.getProperty(cnParamName);
                if (null == val) {
                    // Try to get the default value.
                    try {
                        val = paramManager.get(cnParamName);
                    } catch (Throwable t) {
                        // Ignore.
                        logger.error("Error getting " + cnParamName + ", cause: ", t);
                    }
                }
                if (null != val) {
                    variables.put(cnParamName, val);
                }
            });

            for (String variableName : dnVariableConfigMap.keySet()) {
                variables.put(variableName, dnVariableConfigMap.get(variableName));
            }

        }
    }
}
