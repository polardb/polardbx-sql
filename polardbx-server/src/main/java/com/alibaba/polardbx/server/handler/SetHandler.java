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

package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.atom.CacheVariables;
import com.alibaba.polardbx.common.SQLMode;
import com.alibaba.polardbx.common.constants.IsolationLevel;
import com.alibaba.polardbx.common.constants.ServerVariables;
import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.ddl.Attribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.SystemPropertiesHelper;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.encrypt.aes.BlockEncryptionMode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.Balancer;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.privilege.ActiveRoles;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.InstConfigAccessor;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.topology.VariableConfigAccessor;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.server.QueryResultHandler;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.IsolationUtil;
import com.alibaba.polardbx.transaction.utils.ParamValidationUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSet;
import org.apache.calcite.sql.SqlSetNames;
import org.apache.calcite.sql.SqlSetRole;
import org.apache.calcite.sql.SqlSetTransaction;
import org.apache.calcite.sql.SqlSystemVar;
import org.apache.calcite.sql.SqlUserDefVar;
import org.apache.calcite.sql.SqlUserName;
import org.apache.calcite.sql.TDDLSqlSelect;
import org.apache.calcite.sql.VariableScope;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_CHECK_PRIVILEGE_FAILED;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPoint.FP_CLEAR;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPoint.FP_SHOW;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPoint.SET_PREFIX;

/**
 * SET 语句处理
 *
 * @author xianmao.hexm
 */
public final class SetHandler {

    private static final Logger logger = LoggerFactory.getLogger(SetHandler.class);
    static FastsqlParser fastsqlParser = new FastsqlParser();
    static Object IGNORE_VALUE = new Object();
    static Object RETURN_VALUE = new Object();

    /**
     * @return true:no error packet
     */
    public static boolean handleV2(ByteString stmt, ServerConnection c, int offset, boolean hasMore) {
        return handleV2(stmt, c, offset, hasMore, false);
    }

    /**
     * @return true:no error packet
     */
    public static boolean handleV2(ByteString stmt, ServerConnection c, int offset, boolean hasMore,
                                   boolean inProcedureCall) {
        SqlNodeList results = fastsqlParser.parse(stmt);
        boolean ret = c.initOptimizerContext();
        if (!ret) {
            return true;
        }
        assert results.size() == 1;
        SqlNode result = results.get(0);

        if (result instanceof SqlSet) {
            SqlSet statement = (SqlSet) result;
            // 不在server层处理，需要继续下推的set设置
            List<Pair<SqlNode, SqlNode>> globalDNVariables = new ArrayList<>();
            List<Pair<String, String>> globalCnVariables = new ArrayList<>();
            for (Pair<SqlNode, SqlNode> variable : statement.getVariableAssignmentList()) {
                final SqlNode oriValue = variable.getValue();
                if (variable.getKey() instanceof SqlUserDefVar) {
                    final SqlUserDefVar key = (SqlUserDefVar) variable.getKey();
                    String lowerCaseKey = key.getName().toLowerCase();
                    if (oriValue instanceof SqlCharStringLiteral) {
                        String value = RelUtils.stringValue(oriValue);
                        c.getUserDefVariables().put(lowerCaseKey, value);
                        //FailPoint command, only works in java -ea mode
                        if (FailPoint.isAssertEnable() && StringUtils.startsWith(lowerCaseKey, SET_PREFIX)
                            && StringUtils.length(lowerCaseKey) >= 3) {
                            FailPoint.enable(lowerCaseKey, value);
                            c.getUserDefVariables().put(FP_SHOW, FailPoint.show());
                        }
                    } else if (oriValue instanceof SqlNumericLiteral) {
                        c.getUserDefVariables().put(lowerCaseKey, ((SqlNumericLiteral) oriValue).getValue());
                    } else if (oriValue instanceof SqlLiteral
                        && ((SqlLiteral) oriValue).getTypeName() == SqlTypeName.BOOLEAN) {
                        c.getUserDefVariables().put(lowerCaseKey, RelUtils.booleanValue(oriValue));
                        //FailPoint command, only works in java -ea mode
                        if (FailPoint.isAssertEnable() && StringUtils.equalsIgnoreCase(lowerCaseKey, FP_CLEAR)) {
                            FailPoint.clear();
                            c.getUserDefVariables().put(FP_SHOW, FailPoint.show());
                        }
                    } else if (oriValue instanceof SqlLiteral
                        && ((SqlLiteral) oriValue).getTypeName() == SqlTypeName.NULL && oriValue.toString()
                        .equalsIgnoreCase("NULL")) {
                        c.getUserDefVariables().remove(lowerCaseKey);
                        //FailPoint command, only works in java -ea mode
                        if (FailPoint.isAssertEnable() && StringUtils.startsWith(lowerCaseKey, SET_PREFIX)) {
                            FailPoint.disable(lowerCaseKey);
                            c.getUserDefVariables().put(FP_SHOW, FailPoint.show());
                        }
                    } else if (oriValue instanceof SqlUserDefVar) {
                        String value = ((SqlUserDefVar) oriValue).getName();
                        c.getUserDefVariables().put(lowerCaseKey, c.getUserDefVariables().get(value.toLowerCase()));
                    } else if (oriValue instanceof TDDLSqlSelect) {
                        String sql = RelUtils.toNativeSql(oriValue);
                        UserDefVarProcessingResult resultSet = getSelectResult(c, sql);
                        if (checkResultSuccess(c, resultSet, inProcedureCall, true)) {
                            c.getUserDefVariables().put(lowerCaseKey, resultSet.value);
                        } else {
                            return true;
                        }
                    } else if (oriValue instanceof SqlSystemVar) {
                        final SqlSystemVar var = (SqlSystemVar) oriValue;
                        if (!ServerVariables.contains(var.getName()) && !ServerVariables.isExtra(var.getName())) {
                            if (inProcedureCall) {
                                throw new RuntimeException("Unknown system variable '" + var.getName() + "'");
                            }
                            c.writeErrMessage(ErrorCode.ER_UNKNOWN_SYSTEM_VARIABLE,
                                "Unknown system variable '" + var.getName() + "'");
                            return false;
                        }
                        Object sysVarValue = c.getSysVarValue(var);
                        c.getUserDefVariables().put(lowerCaseKey, sysVarValue);
                    } else if (oriValue instanceof SqlBasicCall) {
                        String sql = "select " + RelUtils.toNativeSql(oriValue);
                        UserDefVarProcessingResult resultSet = getSelectResult(c, sql);
                        if (checkResultSuccess(c, resultSet, inProcedureCall, true)) {
                            c.getUserDefVariables().put(lowerCaseKey, resultSet.value);
                        } else {
                            return true;
                        }
                    } else {
                        if (inProcedureCall) {
                            throw new RuntimeException(
                                "Variable " + key.getName() + " can't be set to the value of " + RelUtils.stringValue(
                                    oriValue));
                        }
                        c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                            "Variable " + key.getName() + " can't be set to the value of " + RelUtils.stringValue(
                                oriValue));
                        return false;
                    }
                } else if (variable.getKey() instanceof SqlSystemVar) {
                    final SqlSystemVar key = (SqlSystemVar) variable.getKey();
                    if (c.getTddlConnection() == null) {
                        c.initTddlConnection();
                    }

                    boolean enableSetGlobal = true;
                    if (ConnectionProperties.ENABLE_SET_GLOBAL.equalsIgnoreCase(key.getName())
                        || ConfigDataMode.isFastMock()) {
                        enableSetGlobal = true;
                    } else {
                        Object globalValue = MetaDbInstConfigManager.getInstance().getCnVariableConfigMap()
                            .getProperty(ConnectionProperties.ENABLE_SET_GLOBAL);
                        if (globalValue != null) {
                            enableSetGlobal = Boolean.parseBoolean(globalValue.toString());
                        }
                        Map<String, Object> connectionVariables = c.getConnectionVariables();
                        Object sessionValue = connectionVariables.get(ConnectionProperties.ENABLE_SET_GLOBAL);
                        if (sessionValue != null) {
                            enableSetGlobal = Boolean.parseBoolean(sessionValue.toString());
                        }
                    }

                    if ("NAMES".equalsIgnoreCase(key.getName())) {
                        String charset = c.getVarStringValue(oriValue);
                        if (!setCharset(charset, c)) {
                            return true;
                        }
                        c.getExtraServerVariables().put(key.getName().toLowerCase(), charset);
                    } else if ("SOCKETTIMEOUT".equalsIgnoreCase(key.getName())) {
                        if (!(oriValue instanceof SqlNumericLiteral) && !(oriValue instanceof SqlUserDefVar)
                            && !(oriValue instanceof SqlSystemVar)) {
                            if (inProcedureCall) {
                                throw new RuntimeException(
                                    "Variable 'socketTimeout' can't be set to the value of " + RelUtils.stringValue(
                                        oriValue));
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable 'socketTimeout' can't be set to the value of " + RelUtils.stringValue(
                                    oriValue));
                            return false;
                        }
                        int milliseconds = c.getVarIntegerValue(oriValue);
                        c.setSocketTimeout(milliseconds);
                        c.getExtraServerVariables().put(key.getName().toLowerCase(), milliseconds);
                    } else if ("COLLATE".equalsIgnoreCase(key.getName())) {
                        // not implemented
                    } else if ("AUTOCOMMIT".equalsIgnoreCase(key.getName())) {
                        boolean autocommit = true;
                        String stipVal = StringUtils.strip(RelUtils.stringValue(oriValue), "'\"");
                        if (oriValue instanceof SqlLiteral
                            && ((SqlLiteral) oriValue).getTypeName() == SqlTypeName.BOOLEAN) {
                            autocommit = RelUtils.booleanValue(variable.getValue());
                        } else if (variable.getValue() instanceof SqlNumericLiteral) {
                            autocommit = RelUtils.integerValue((SqlLiteral) variable.getValue()) != 0;
                        } else if (oriValue instanceof SqlSystemVar || oriValue instanceof SqlUserDefVar) {
                            Boolean b = c.getVarBooleanValue(oriValue);
                            if (b == null) {
                                if (inProcedureCall) {
                                    throw new RuntimeException(
                                        "Variable 'autocommit' can't be set to the value of " + RelUtils.stringValue(
                                            variable.getValue()));
                                }
                                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                    "Variable 'autocommit' can't be set to the value of " + RelUtils.stringValue(
                                        variable.getValue()));
                                return false;
                            }
                            autocommit = b;
                        } else if ("ON".equalsIgnoreCase(stipVal)) {
                            autocommit = true;
                        } else if ("OFF".equalsIgnoreCase(stipVal)) {
                            autocommit = false;
                        } else {
                            if (inProcedureCall) {
                                throw new RuntimeException(
                                    "Variable 'autocommit' can't be set to the value of " + RelUtils.stringValue(
                                        variable.getValue()));
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable 'autocommit' can't be set to the value of " + RelUtils.stringValue(
                                    variable.getValue()));
                            return false;
                        }

                        if (autocommit) {
                            if (!c.isAutocommit()) {
                                c.setAutocommit(true);
                            }
                        } else {
                            if (c.isAutocommit()) {
                                c.setAutocommit(false);
                            }
                        }
                        c.getExtraServerVariables().put(key.getName().toLowerCase(), autocommit);
                    } else if (ConnectionProperties.PURE_ASYNC_DDL_MODE.equalsIgnoreCase(key.getName())) {
                        Boolean asyncDDLPureMode = false;
                        String stipVal = StringUtils.strip(RelUtils.stringValue(oriValue), "'\"");
                        if (oriValue instanceof SqlLiteral
                            && ((SqlLiteral) oriValue).getTypeName() == SqlTypeName.BOOLEAN) {
                            asyncDDLPureMode = RelUtils.booleanValue(variable.getValue());
                        } else if (variable.getValue() instanceof SqlNumericLiteral) {
                            asyncDDLPureMode = RelUtils.integerValue((SqlLiteral) variable.getValue()) != 0;
                        } else if (oriValue instanceof SqlSystemVar || oriValue instanceof SqlUserDefVar) {
                            asyncDDLPureMode = c.getVarBooleanValue(oriValue);
                        } else if ("ON".equalsIgnoreCase(stipVal)) {
                            asyncDDLPureMode = true;
                        } else if ("OFF".equalsIgnoreCase(stipVal)) {
                            asyncDDLPureMode = false;
                        } else if ("DEFAULT".equalsIgnoreCase(stipVal)) {
                            asyncDDLPureMode = false;
                        } else {
                            if (inProcedureCall) {
                                throw new RuntimeException("Variable '" + ConnectionProperties.PURE_ASYNC_DDL_MODE
                                    + "' can't be set to the value of " + RelUtils.stringValue(variable.getValue()));
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable '" + ConnectionProperties.PURE_ASYNC_DDL_MODE
                                    + "' can't be set to the value of " + RelUtils.stringValue(variable.getValue()));
                            return false;
                        }
                        //c.setAsyncDDLPureModeSession(asyncDDLPureMode);
                        c.getExtraServerVariables().put(key.getName().toLowerCase(), asyncDDLPureMode);
                        if (enableSetGlobal && (key.getScope() == VariableScope.GLOBAL)) {
                            globalCnVariables.add(new Pair<String, String>(key.getName(), asyncDDLPureMode.toString()));
                        }
                    } else if ("SQL_SAFE_UPDATES".equalsIgnoreCase(key.getName())) {
                        // ignore update不带主键就会报错
                    } else if ("NET_WRITE_TIMEOUT".equalsIgnoreCase(key.getName())) {
                        // ignore超时参数,规避DRDS数据导出时,服务端主动关闭连接
                    } else if ("SQL_LOG_BIN".equalsIgnoreCase(key.getName())) {
                        // ignore SQL_LOG_BIN，MySQL
                        // Dump会加入这种语句，下面的MySQL未必有权限
                    } else if ("TIMESTAMP".equalsIgnoreCase(key.getName())) {
                        // ignore max_statement_time for 2.0
                    } else if ("MAX_STATEMENT_TIME".equalsIgnoreCase(key.getName())) {
                        // ignore max_statement_time for 2.0
                    } else if ("PARTITION_HINT".equalsIgnoreCase(key.getName())) {
                        c.setPartitionHint(RelUtils.stringValue(oriValue));
                    } else if ("TRANSACTION POLICY".equalsIgnoreCase(key.getName())) {
                        if (!(oriValue instanceof SqlNumericLiteral) && !(oriValue instanceof SqlUserDefVar)
                            && !(oriValue instanceof SqlSystemVar)) {
                            if (inProcedureCall) {
                                throw new RuntimeException("Variable 'transaction policy' can't be set to the value of "
                                    + RelUtils.stringValue(oriValue));
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable 'transaction policy' can't be set to the value of " + RelUtils.stringValue(
                                    oriValue));
                            return false;
                        }
                        int policy = c.getVarIntegerValue(oriValue);
                        String strPolicy;
                        switch (policy) {
                        case 3:
                            c.setTrxPolicy(ITransactionPolicy.ALLOW_READ_CROSS_DB);
                            strPolicy = "ALLOW_READ";
                            break;
                        case 4:
                            c.setTrxPolicy(ITransactionPolicy.NO_TRANSACTION);
                            strPolicy = "NO_TRANSACTION";
                            break;
                        case 6:
                        case 7:
                            c.setTrxPolicy(ITransactionPolicy.XA);
                            strPolicy = "XA";
                            break;
                        case 8:
                            c.setTrxPolicy(ITransactionPolicy.TSO);
                            strPolicy = "TSO";
                            break;
                        default:
                            if (inProcedureCall) {
                                throw new RuntimeException("Variable 'transaction policy' can't be set to the value of "
                                    + RelUtils.stringValue(oriValue));
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable 'transaction policy' can't be set to the value of " + RelUtils.stringValue(
                                    oriValue));
                            return false;
                        }
                        c.getExtraServerVariables().put(key.getName().toLowerCase(), policy);
                        c.getExtraServerVariables().put("TRANS.POLICY".toLowerCase(), strPolicy);
                        c.getExtraServerVariables()
                            .put(TransactionAttribute.DRDS_TRANSACTION_POLICY.toLowerCase(), strPolicy);
                    } else if ("TRANS.POLICY".equalsIgnoreCase(key.getName())
                        || TransactionAttribute.DRDS_TRANSACTION_POLICY.equalsIgnoreCase(key.getName())) {
                        // 自动提交模式
                        if (c.isAutocommit()) {
                            if (!enableSetGlobal || key.getScope().equals(VariableScope.SESSION)) {
                                if (inProcedureCall) {
                                    throw new RuntimeException("Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' is read only on auto-commit mode");
                                }
                                c.writeErrMessage(ErrorCode.ER_VARIABLE_IS_READONLY,
                                    "Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' is read only on auto-commit mode");
                                return false;
                            }
                        } else {
                            String policy = StringUtils.strip(c.getVarStringValue(oriValue), "'\"");
                            int intPolicy = 0;
                            // 设置 DRDS 事务策略
                            // SET TRANS.POLICY = TDDL | FLEXIBLE | ...
                            if ("ALLOW_READ".equalsIgnoreCase(policy)) {
                                c.setTrxPolicy(ITransactionPolicy.ALLOW_READ_CROSS_DB);
                                intPolicy = 3;
                            } else if ("NO_TRANSACTION".equalsIgnoreCase(policy)) {
                                c.setTrxPolicy(ITransactionPolicy.NO_TRANSACTION);
                                intPolicy = 4;
                            } else if ("XA".equalsIgnoreCase(policy)
                                || "BEST_EFFORT".equalsIgnoreCase(policy)
                                || "2PC".equalsIgnoreCase(policy)
                                || "FLEXIBLE".equalsIgnoreCase(policy)) {
                                // to keep compatible
                                c.setTrxPolicy(ITransactionPolicy.XA);
                                intPolicy = 6;
                            } else if ("TSO".equalsIgnoreCase(policy)) {
                                c.setTrxPolicy(ITransactionPolicy.TSO);
                                intPolicy = 8;
                            } else if ("BEST_EFFORT".equalsIgnoreCase(policy) || "2PC".equalsIgnoreCase(policy)
                                || "FLEXIBLE".equalsIgnoreCase(policy)) {
                                // to keep compatible
                                c.setTrxPolicy(ITransactionPolicy.XA);
                                intPolicy = 6;
                            } else {
                                if (inProcedureCall) {
                                    throw new RuntimeException("Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' can't be set to the value of " + policy);
                                }
                                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                    "Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' can't be set to the value of " + policy);
                                return false;
                            }
                            c.getExtraServerVariables().put("TRANSACTION POLICY".toLowerCase(), intPolicy);
                            c.getExtraServerVariables().put("TRANS.POLICY".toLowerCase(), policy.toUpperCase());
                            c.getExtraServerVariables()
                                .put(TransactionAttribute.DRDS_TRANSACTION_POLICY.toLowerCase(), policy.toUpperCase());
                        }
                    } else if (TransactionAttribute.SHARE_READ_VIEW.equalsIgnoreCase(key.getName())) {
                        String stripVal = StringUtils.strip(RelUtils.stringValue(oriValue), "'\"");
                        boolean shareReadView;
                        if (oriValue instanceof SqlLiteral
                            && ((SqlLiteral) oriValue).getTypeName() == SqlTypeName.BOOLEAN) {
                            shareReadView = RelUtils.booleanValue(variable.getValue());
                        } else if (variable.getValue() instanceof SqlNumericLiteral) {
                            shareReadView = RelUtils.integerValue((SqlLiteral) variable.getValue()) != 0;
                        } else if (oriValue instanceof SqlSystemVar || oriValue instanceof SqlUserDefVar) {
                            Boolean b = c.getVarBooleanValue(oriValue);
                            if (b == null) {
                                if (inProcedureCall) {
                                    throw new RuntimeException("Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' can't be set to the value of " + RelUtils.stringValue(
                                        variable.getValue()));
                                }
                                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                    "Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' can't be set to the value of " + RelUtils.stringValue(
                                        variable.getValue()));
                                return false;
                            }
                            shareReadView = b;
                        } else if ("ON".equalsIgnoreCase(stripVal)) {
                            shareReadView = true;
                        } else if ("OFF".equalsIgnoreCase(stripVal)) {
                            shareReadView = false;
                        } else {
                            if (inProcedureCall) {
                                throw new RuntimeException("Variable '" + StringUtils.lowerCase(key.getName())
                                    + "' can't be set to the value of " + RelUtils.stringValue(variable.getValue()));
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable '" + StringUtils.lowerCase(key.getName()) + "' can't be set to the value of "
                                    + RelUtils.stringValue(variable.getValue()));
                            return false;
                        }
                        if (c.isAutocommit()) {
                            if (!enableSetGlobal || key.getScope().equals(VariableScope.SESSION)) {
                                if (inProcedureCall) {
                                    throw new RuntimeException("Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' is read only on auto-commit mode");
                                }
                                c.writeErrMessage(ErrorCode.ER_VARIABLE_IS_READONLY,
                                    "Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' is read only on auto-commit mode");
                                return false;
                            }
                        } else {
                            c.setShareReadView(shareReadView);
                        }
                        if (enableSetGlobal && key.getScope() == VariableScope.GLOBAL) {
                            globalCnVariables.add(new Pair<>(key.getName(), Boolean.toString(shareReadView)));
                        }
                    } else if (TransactionAttribute.GROUP_PARALLELISM.equalsIgnoreCase(key.getName())) {

                        final String value = c.getVarStringValue(oriValue);
                        try {
                            Integer val = Integer.parseInt(value);
                            if (val <= 0) {
                                if (inProcedureCall) {
                                    throw new RuntimeException("Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' can't be set to the value of " + RelUtils.stringValue(
                                        variable.getValue()));
                                }
                                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                    "Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' can't be set to the value of " + RelUtils.stringValue(
                                        variable.getValue()));
                                return false;
                            }
                            long newGrpParallelism = Long.valueOf(val);
                            c.setGroupParallelism(newGrpParallelism);
                            if (enableSetGlobal && key.getScope() == VariableScope.GLOBAL) {
                                globalCnVariables.add(new Pair<>(key.getName(), Long.toString(newGrpParallelism)));
                            }
                        } catch (NumberFormatException e) {
                            if (inProcedureCall) {
                                throw new RuntimeException("Variable '" + StringUtils.lowerCase(key.getName())
                                    + "' can't be set to the value of " + RelUtils.stringValue(variable.getValue()));
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable '" + StringUtils.lowerCase(key.getName()) + "' can't be set to the value of "
                                    + RelUtils.stringValue(variable.getValue()));
                            return false;
                        }
                    } else if (TransactionAttribute.DRDS_TRANSACTION_TIMEOUT.equalsIgnoreCase(key.getName())) {
                        final String val = c.getVarStringValue(oriValue);
                        try {
                            final long lval = Long.parseLong(val); // ms -> s
                            c.getServerVariables().put("max_trx_duration", lval < 1000 ? 1 : lval / 1000);
                        } catch (NumberFormatException e) {
                            if (inProcedureCall) {
                                throw new RuntimeException("Variable '" + StringUtils.lowerCase(key.getName())
                                    + "' can't be set to the value of " + val);
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable '" + StringUtils.lowerCase(key.getName()) + "' can't be set to the value of "
                                    + val);
                            return false;
                        }
                    } else if (ConnectionProperties.TX_ISOLATION.equalsIgnoreCase(key.getName()) ||
                        ConnectionProperties.TRANSACTION_ISOLATION.equalsIgnoreCase(key.getName())
                    ) {
                        int isolationCode;
                        IsolationLevel isolation;
                        String value;
                        if (isDefault(oriValue)) {
                            isolationCode = DynamicConfig.getInstance().getTxIsolation();
                            isolation = IsolationLevel.fromInt(isolationCode);
                            if (isolation == null) {
                                throw new AssertionError("Invalid global tx_isolation");
                            }
                            value = isolation.nameWithHyphen();
                        } else {
                            value = c.getVarStringValue(oriValue);
                            isolation = IsolationLevel.parse(value);
                            if (isolation == null) {
                                if (inProcedureCall) {
                                    throw new RuntimeException(
                                        "Variable 'tx_isolation' can't be set to the value of '" + value + "'");
                                }
                                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                    "Variable 'tx_isolation' can't be set to the value of '" + value + "'");
                                return false;
                            }
                            isolationCode = isolation.getCode();
                        }
                        c.setTxIsolation(isolationCode);
                        c.getExtraServerVariables().put(key.getName().toLowerCase(), value);
                        // tx_isolation is deprecated in 8.0, use transaction_isolation instead.
                        c.getServerVariables().put(ConnectionProperties.TRANSACTION_ISOLATION.toLowerCase(), value);
                        if (enableSetGlobal && key.getScope() == VariableScope.GLOBAL) {
                            globalCnVariables.add(
                                new Pair<>(ConnectionProperties.TRANSACTION_ISOLATION, value));
                            globalCnVariables.add(
                                new Pair<>(ConnectionProperties.TX_ISOLATION, value));

                            // tx_isolation is deprecated in 8.0, use transaction_isolation instead.
                            globalDNVariables.add(new Pair<>(
                                SqlSystemVar.create(((SqlSystemVar) variable.getKey()).getScope(),
                                    ConnectionProperties.TRANSACTION_ISOLATION, variable.getKey().getParserPosition()),
                                variable.getValue()));
                        }
                    } else if ("READ".equalsIgnoreCase(key.getName())) {
                        if (!(oriValue instanceof SqlCharStringLiteral) && !(oriValue instanceof SqlUserDefVar)
                            && !(oriValue instanceof SqlSystemVar)) {
                            if (inProcedureCall) {
                                throw new RuntimeException(
                                    "unexpected token for SET TRANSACTION statement " + RelUtils.stringValue(oriValue));
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "unexpected token for SET TRANSACTION statement " + RelUtils.stringValue(oriValue));
                            return false;
                        }
                        String readValue = TStringUtil.upperCase(c.getVarStringValue(oriValue));
                        if (TStringUtil.equalsIgnoreCase("WRITE", readValue)) {
                            c.setReadOnly(false);
                        } else if (TStringUtil.equalsIgnoreCase("ONLY", readValue)) {
                            c.setReadOnly(true);
                        } else {
                            if (inProcedureCall) {
                                throw new RuntimeException(
                                    "unexpected token for SET TRANSACTION statement " + RelUtils.stringValue(oriValue));
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "unexpected token for SET TRANSACTION statement " + RelUtils.stringValue(oriValue));
                            return false;
                        }
                        c.getExtraServerVariables().put(key.getName().toLowerCase(), readValue);
                    } else if ("CHARACTER_SET_RESULTS".equalsIgnoreCase(key.getName())
                        || "CHARACTER_SET_CONNECTION".equalsIgnoreCase(key.getName())) {
                        String strVal = RelUtils.stringValue(oriValue);
                        String charset = null;
                        if (oriValue instanceof SqlCharStringLiteral) {
                            charset = RelUtils.stringValue(oriValue);
                        } else if (oriValue instanceof SqlNumericLiteral) {
                            charset = RelUtils.stringValue(oriValue);
                        } else if (oriValue instanceof SqlUserDefVar || oriValue instanceof SqlSystemVar) {
                            charset = c.getVarStringValue(oriValue);
                        } else if ((oriValue instanceof SqlLiteral
                            && ((SqlLiteral) oriValue).getTypeName() == SqlTypeName.NULL) || "NULL".equalsIgnoreCase(
                            strVal) || StringUtils.isEmpty(strVal) || StringUtils.isEmpty(StringUtils.strip("'\""))) {
                            charset = null;
                        } else if (isDefault(oriValue)) {
                            charset = "utf8";
                        } else if (oriValue instanceof SqlIdentifier) {
                            charset = oriValue.toString();
                        } else {
                            if (inProcedureCall) {
                                throw new RuntimeException(
                                    "Variable '" + key.getName() + "' can't be set to the value of "
                                        + RelUtils.stringValue(oriValue));
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable '" + key.getName() + "' can't be set to the value of " + RelUtils.stringValue(
                                    oriValue));
                            return false;
                        }

                        if (!setCharset(charset, c)) {
                            return true;
                        }
                        c.getExtraServerVariables().put("CHARACTER_SET_RESULTS".toLowerCase(), charset);
                        c.getExtraServerVariables().put("CHARACTER_SET_CONNECTION".toLowerCase(), charset);
                    } else if ("CHARACTER_SET_CLIENT".equalsIgnoreCase(key.getName())) {

                        /* 忽略client属性设置 */
                        // 忽略这个？
                    } else if (BatchInsertPolicy.getVariableName().equalsIgnoreCase(key.getName())) {
                        if (!(oriValue instanceof SqlCharStringLiteral) && !(oriValue instanceof SqlUserDefVar)
                            && !(oriValue instanceof SqlSystemVar)) {
                            if (inProcedureCall) {
                                throw new RuntimeException(
                                    "unexpected token for SET BATCH_INSERT_POLICY statement " + RelUtils.stringValue(
                                        oriValue));
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "unexpected token for SET BATCH_INSERT_POLICY statement " + RelUtils.stringValue(
                                    oriValue));
                            return false;
                        }

                        String policyValue = TStringUtil.upperCase(c.getVarStringValue(oriValue));
                        BatchInsertPolicy policy = BatchInsertPolicy.getPolicyByName(policyValue);
                        if (policy == null) {
                            if (inProcedureCall) {
                                throw new RuntimeException(
                                    "unexpected token for SET BATCH_INSERT_POLICY statement " + RelUtils.stringValue(
                                        oriValue));
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "unexpected token for SET BATCH_INSERT_POLICY statement " + RelUtils.stringValue(
                                    oriValue));
                            return false;
                        }

                        c.setBatchInsertPolicy(policy);
                        c.getExtraServerVariables().put(key.getName().toLowerCase(), policyValue);
                    } else if (ConnectionProperties.ENABLE_BALANCER.equalsIgnoreCase(key.getName())) {
                        // TODO(moyi) make this variable instance scope
                        boolean enable = RelUtils.booleanValue(oriValue);
                        Balancer.getInstance().enableBalancer(enable);
                        c.getExtraServerVariables().put(key.getName().toLowerCase(Locale.ROOT), enable);
                    } else if (ConnectionProperties.BALANCER_MAX_PARTITION_SIZE.equalsIgnoreCase(key.getName())) {
                        // TODO(moyi) make this variable instance scope
                        long value = RelUtils.longValue(oriValue);
                        BalanceOptions.setMaxPartitionSize(value);
                        c.getExtraServerVariables().put(key.getName().toLowerCase(), value);
                    } else if (ConnectionProperties.GROUP_CONCAT_MAX_LEN.equalsIgnoreCase(key.getName())) {
                        if (key.getScope() != null && key.getScope() == VariableScope.GLOBAL) {
                            if (inProcedureCall) {
                                throw new RuntimeException("not support set global group_concat_max_len");
                            }
                        }
                        try {
                            int v = c.getVarIntegerValue(oriValue);
                            if (v < 4) {
                                v = 4;
                            }
                            c.getServerVariables().put("group_concat_max_len", v);
                            c.getConnectionVariables().put(ConnectionProperties.GROUP_CONCAT_MAX_LEN, v);
                            if (enableSetGlobal && key.getScope() == VariableScope.GLOBAL) {
                                globalCnVariables.add(
                                    new Pair(ConnectionProperties.GROUP_CONCAT_MAX_LEN, String.valueOf(v)));
                                globalDNVariables.add(variable);
                            }
                        } catch (Exception e) {
                            if (inProcedureCall) {
                                throw new RuntimeException(
                                    "Incorrect argument type to variable 'group_concat_max_len'");
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR,
                                "Incorrect argument type to variable 'group_concat_max_len'");
                            return false;
                        }
                    } else if ("sql_mock".equalsIgnoreCase(key.getName())) {
                        String val = TStringUtil.upperCase(c.getVarStringValue(oriValue));
                        if ("ON".equalsIgnoreCase(val)) {
                            c.setSqlMock(true);
                        } else {
                            c.setSqlMock(false);
                        }
                        c.getExtraServerVariables().put(key.getName().toLowerCase(), val);
                    } else if ("tx_read_only".equalsIgnoreCase(key.getName())) {
                        boolean val = RelUtils.booleanValue(oriValue);
                        c.setReadOnly(val);
                    } else if ("polardbx_server_id".equalsIgnoreCase(key.getName())) {
                        if (key.getScope() != null && key.getScope().name().equalsIgnoreCase("global")) {
                            if (inProcedureCall) {
                                throw new RuntimeException("not support set global polardbx_server_id");
                            }
                            c.writeErrMessage(ErrorCode.ER_NOT_SUPPORTED_YET,
                                "not support set global polardbx_server_id");
                            return false;
                        }
                        try {
                            int v = c.getVarIntegerValue(oriValue);
                            if (v <= 0) {
                                throw new RuntimeException("polardbx_server_id can`t be less than zero.");
                            }
                            c.getExtraServerVariables().put("polardbx_server_id", v);
                        } catch (Exception | Error e) {
                            if (inProcedureCall) {
                                throw new RuntimeException("Incorrect argument type to variable 'polardbx_server_id'");
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR,
                                "Incorrect argument type to variable 'polardbx_server_id'");
                            return false;
                        }
                    } else if ("time_zone".equalsIgnoreCase(key.getName())) {
                        //在内部添加到customizeVar中
                        c.setTimeZone(c.getVarStringValue(oriValue));
                        Object parserValue = parserValue(oriValue, key, c);
                        if (parserValue == RETURN_VALUE) {
                            return true;
                        } else if (parserValue != IGNORE_VALUE) {
                            c.getServerVariables().put(key.getName().toLowerCase(), parserValue);
                        }
                    } else if (ConnectionProperties.SUPPORT_INSTANT_ADD_COLUMN.equalsIgnoreCase(key.getName())) {
                        String value = StringUtils.strip(c.getVarStringValue(oriValue), "'\"");
                        Boolean iacSupported;
                        if ("ON".equalsIgnoreCase(value)) {
                            iacSupported = Boolean.TRUE;
                        } else if ("OFF".equalsIgnoreCase(value)) {
                            iacSupported = Boolean.FALSE;
                        } else {
                            if (inProcedureCall) {
                                throw new RuntimeException(
                                    "Invalid value '" + RelUtils.stringValue(variable.getValue()) + "' for variable '"
                                        + ConnectionProperties.SUPPORT_INSTANT_ADD_COLUMN + "'. Please use ON or OFF.");
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Invalid value '" + RelUtils.stringValue(variable.getValue()) + "' for variable '"
                                    + ConnectionProperties.SUPPORT_INSTANT_ADD_COLUMN + "'. Please use ON or OFF.");
                            return false;
                        }
                        // global only
                        if (enableSetGlobal && key.getScope() == VariableScope.GLOBAL) {
                            globalCnVariables.add(new Pair<>(key.getName(), iacSupported.toString()));
                            if (TableInfoManager.isXdbInstantAddColumnSupported()) {
                                SqlSystemVar dnKey =
                                    SqlSystemVar.create(key.getScope(), Attribute.XDB_VARIABLE_INSTANT_ADD_COLUMN,
                                        SqlParserPos.ZERO);
                                globalDNVariables.add(new Pair<>(dnKey, variable.getValue()));
                            }
                        }
                    } else if (ConnectionProperties.ENABLE_CHANGESET.equalsIgnoreCase(key.getName())) {
                        Boolean enableChangeSet;
                        String value = StringUtils.strip(RelUtils.stringValue(oriValue), "'\"");

                        if ("ON".equalsIgnoreCase(value) || "TRUE".equalsIgnoreCase(value)) {
                            enableChangeSet = Boolean.TRUE;
                        } else if ("OFF".equalsIgnoreCase(value) || "FALSE".equalsIgnoreCase(value)) {
                            enableChangeSet = Boolean.FALSE;
                        } else {
                            enableChangeSet = Boolean.FALSE;
                        }

                        if (enableSetGlobal && key.getScope() == VariableScope.GLOBAL) {
                            globalCnVariables.add(
                                new Pair<>(ConnectionProperties.CN_ENABLE_CHANGESET, enableChangeSet.toString()));
                            globalDNVariables.add(variable);
                        }
                    } else if (
                        ConnectionProperties.ENABLE_PHYSICAL_TABLE_PARALLEL_BACKFILL.equalsIgnoreCase(key.getName())
                            || ConnectionProperties.ENABLE_SLIDE_WINDOW_BACKFILL.equalsIgnoreCase(key.getName())) {
                        String value = StringUtils.strip(c.getVarStringValue(oriValue), "'\"");

                        Boolean enablePhyTblParallel;
                        if ("ON".equalsIgnoreCase(value)) {
                            enablePhyTblParallel = Boolean.TRUE;
                        } else if ("OFF".equalsIgnoreCase(value)) {
                            enablePhyTblParallel = Boolean.FALSE;
                        } else {
                            enablePhyTblParallel = Boolean.FALSE;
                        }

                        if (enableSetGlobal && key.getScope() == VariableScope.GLOBAL) {
                            globalCnVariables.add(new Pair<>(key.getName(), enablePhyTblParallel.toString()));
                        }
                    } else if (ConnectionProperties.PHYSICAL_TABLE_BACKFILL_PARALLELISM.equalsIgnoreCase(key.getName())
                        || ConnectionProperties.SLIDE_WINDOW_SPLIT_SIZE.equalsIgnoreCase(key.getName())
                        || ConnectionProperties.BACKFILL_PARALLELISM.equalsIgnoreCase(key.getName())
                        || ConnectionProperties.CHANGE_SET_APPLY_PARALLELISM.equalsIgnoreCase(key.getName())
                        || ConnectionProperties.PHYSICAL_TABLE_START_SPLIT_SIZE.equalsIgnoreCase(key.getName())
                        || ConnectionProperties.SLIDE_WINDOW_TIME_INTERVAL.equalsIgnoreCase(key.getName())) {
                        final String value = c.getVarStringValue(oriValue);
                        try {
                            Integer val = Integer.parseInt(value);
                            if (val <= 0) {
                                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                    "Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' can't be set to the value of " + RelUtils.stringValue(
                                        variable.getValue()));
                                return false;
                            }
                            if (enableSetGlobal && key.getScope() == VariableScope.GLOBAL) {
                                globalCnVariables.add(new Pair<>(key.getName(), val.toString()));
                            }
                        } catch (NumberFormatException e) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable '" + StringUtils.lowerCase(key.getName()) + "' can't be set to the value of "
                                    + RelUtils.stringValue(variable.getValue()));
                            return false;
                        }
                    } else if (ConnectionProperties.ENABLE_NEW_SEQ_GROUPING.equalsIgnoreCase(key.getName())
                        || ConnectionProperties.ENABLE_NEW_SEQ_CACHE_ON_CN.equalsIgnoreCase(key.getName())
                        || ConnectionProperties.ENABLE_NEW_SEQ_REQUEST_MERGING.equalsIgnoreCase(key.getName())) {
                        String value = StringUtils.strip(c.getVarStringValue(oriValue), "'\"");
                        Boolean newSeqGroupingEnabled;
                        if ("TRUE".equalsIgnoreCase(value)) {
                            newSeqGroupingEnabled = Boolean.TRUE;
                        } else if ("FALSE".equalsIgnoreCase(value)) {
                            newSeqGroupingEnabled = Boolean.FALSE;
                        } else {
                            newSeqGroupingEnabled = Boolean.FALSE;
                        }
                        if (enableSetGlobal && key.getScope() == VariableScope.GLOBAL) {
                            globalCnVariables.add(new Pair<>(key.getName(), newSeqGroupingEnabled.toString()));
                        }
                    } else if (ConnectionProperties.NEW_SEQ_CACHE_SIZE.equalsIgnoreCase(key.getName())
                        || ConnectionProperties.NEW_SEQ_CACHE_SIZE_ON_CN.equalsIgnoreCase(key.getName())
                        || ConnectionProperties.NEW_SEQ_GROUPING_TIMEOUT.equalsIgnoreCase(key.getName())
                        || ConnectionProperties.NEW_SEQ_TASK_QUEUE_NUM_PER_DB.equalsIgnoreCase(key.getName())
                        || ConnectionProperties.NEW_SEQ_VALUE_HANDLER_KEEP_ALIVE_TIME.equalsIgnoreCase(key.getName())
                        || ConnectionProperties.GROUP_SEQ_CHECK_INTERVAL.equalsIgnoreCase(key.getName())) {
                        final String value = c.getVarStringValue(oriValue);
                        try {
                            long longValue = Long.parseLong(value);
                            if (enableSetGlobal && key.getScope() == VariableScope.GLOBAL) {
                                globalCnVariables.add(new Pair<>(key.getName(), String.valueOf(longValue)));
                            }
                        } catch (NumberFormatException e) {
                            if (inProcedureCall) {
                                throw new RuntimeException("Variable '" + StringUtils.lowerCase(key.getName())
                                    + "' can't be set to the value of " + value);
                            }
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable '" + StringUtils.lowerCase(key.getName()) + "' can't be set to the value of "
                                    + value);
                            return false;
                        }
                    } else if (ConnectionProperties.SQL_SELECT_LIMIT.equalsIgnoreCase(key.getName())) {
                        String value = null;
                        if (isDefault(oriValue)) {
                            value = ConnectionParams.SQL_SELECT_LIMIT.getDefault();
                        } else if (oriValue instanceof SqlBasicCall) {
                            String sql = "select " + RelUtils.toNativeSql(oriValue);
                            UserDefVarProcessingResult resultSet = getSelectResult(c, sql);
                            if (checkResultSuccess(c, resultSet, inProcedureCall, false)) {
                                value = resultSet.value.toString();
                            } else {
                                value = ConnectionParams.SQL_SELECT_LIMIT.getDefault();
                            }
                        } else {
                            value = c.getVarStringValue(oriValue);
                        }
                        long sqlSelectLimit = -1L;
                        boolean parseFail = false;
                        try {
                            sqlSelectLimit = Long.parseLong(value);
                        } catch (NumberFormatException e) {
                            parseFail = true;
                        }
                        if (parseFail || sqlSelectLimit <= 0L) {
                            continue;
                        }
                        c.getConnectionVariables().put(key.getName().toUpperCase(Locale.ROOT), sqlSelectLimit);
                        if (enableSetGlobal && key.getScope() == VariableScope.GLOBAL) {
                            globalCnVariables.add(
                                new Pair<>(key.getName().toUpperCase(Locale.ROOT), String.valueOf(sqlSelectLimit)));
                        }
                    } else if (ConnectionProperties.MAX_JAVA_UDF_NUM.equalsIgnoreCase(key.getName())) {
                        String value = null;
                        if (isDefault(oriValue)) {
                            value = ConnectionParams.MAX_JAVA_UDF_NUM.getDefault();
                        } else if (oriValue instanceof SqlBasicCall) {
                            String sql = "select " + RelUtils.toNativeSql(oriValue);
                            UserDefVarProcessingResult resultSet = getSelectResult(c, sql);
                            if (checkResultSuccess(c, resultSet, inProcedureCall, false)) {
                                value = resultSet.value.toString();
                            } else {
                                value = ConnectionParams.MAX_JAVA_UDF_NUM.getDefault();
                            }
                        } else {
                            value = c.getVarStringValue(oriValue);
                        }
                        long maxUdfNum = -1L;
                        boolean parseFail = false;
                        try {
                            maxUdfNum = Long.parseLong(value);
                        } catch (NumberFormatException e) {
                            parseFail = true;
                        }
                        if (parseFail || maxUdfNum <= 0L) {
                            continue;
                        }
                        if (enableSetGlobal && key.getScope() == VariableScope.GLOBAL) {
                            globalCnVariables.add(
                                new Pair<>(key.getName().toUpperCase(Locale.ROOT), String.valueOf(maxUdfNum)));
                        } else {
                            if (inProcedureCall) {
                                throw new RuntimeException(
                                    "Variable 'max_java_udf_num' should be set with global scope");
                            }
                            c.writeErrMessage(ErrorCode.ER_GLOBAL_VARIABLE,
                                "Variable 'max_java_udf_num' should be set with global scope");
                            return false;
                        }
                    } else if ("block_encryption_mode".equalsIgnoreCase(key.getName())) {
                        BlockEncryptionMode encryptionMode;
                        boolean supportOpenSSL =
                            ExecutorContext.getContext(c.getSchema()).getStorageInfoManager().supportOpenSSL();

                        if (isDefault(oriValue)) {
                            encryptionMode = BlockEncryptionMode.DEFAULT_MODE;
                        } else {
                            String modeStr = c.getVarStringValue(oriValue);
                            try {
                                encryptionMode = new BlockEncryptionMode(modeStr, supportOpenSSL);
                            } catch (IllegalArgumentException exception) {
                                if (inProcedureCall) {
                                    throw new RuntimeException(
                                        "Variable 'block_encryption_mode' can't be set to the value of '" + modeStr
                                            + "'");
                                }
                                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                    "Variable 'block_encryption_mode' can't be set to the value of '" + modeStr + "'");
                                return false;
                            }
                        }
                        c.getExtraServerVariables()
                            .put(ConnectionProperties.BLOCK_ENCRYPTION_MODE, encryptionMode.nameWithHyphen());
                        c.getServerVariables().put(key.getName().toLowerCase(), c.getVarStringValue(oriValue));
                    } else if ("SQL_MODE".equalsIgnoreCase(key.getName())) {
                        String val = c.getVarStringValue(oriValue);
                        boolean enableANSIQuotes = false;

                        // sql_mode的值为空，不设置ANSI_QUOTES；
                        // sql_mode的值包含ANSI_QUOTES，设置ANSI_QUOTES；
                        // sql_mode的值包含组合sql_mode，该组合sql_mode包含ANSI_QUOTES，设置ANSI_QUOTES。
                        if (!StringUtils.isEmpty(val)) {
                            String[] sqlmodes = val.split(",");
                            for (String sqlmode : sqlmodes) {
                                if ("ANSI_QUOTES".equalsIgnoreCase(sqlmode) || (SQLMode.isCombSQLMode(sqlmode)
                                    && SQLMode.contains(sqlmode, SQLMode.ANSI_QUOTES))) {
                                    enableANSIQuotes = true;
                                    break;
                                }
                            }
                        }
                        c.setEnableANSIQuotes(enableANSIQuotes);
                        c.setSqlMode(val);
                        c.getExtraServerVariables().put(key.getName().toLowerCase(), val);
                        Object parserValue = parserValue(oriValue, key, c);
                        if (parserValue == RETURN_VALUE) {
                            return true;
                        } else if (parserValue != IGNORE_VALUE) {
                            c.getServerVariables().put(key.getName().toLowerCase(), parserValue);
                            if (enableSetGlobal && (key.getScope() == VariableScope.GLOBAL)) {
                                globalDNVariables.add(variable);
                            }
                        }
                    } else if (ConnectionProperties.FOREIGN_KEY_CHECKS.equalsIgnoreCase(key.getName())) {
                        Boolean foreignKeyChecks = parseBool(oriValue, c);
                        c.getServerVariables().put(ConnectionProperties.FOREIGN_KEY_CHECKS, foreignKeyChecks);
                        if (enableSetGlobal && (key.getScope() == VariableScope.GLOBAL)) {
                            globalCnVariables.add(new Pair<>(key.getName(), String.valueOf(foreignKeyChecks)));
                            globalDNVariables.add(variable);
                        }
                    } else if (ConnectionProperties.STATISTIC_CORRECTIONS.equalsIgnoreCase(key.getName())) {
                        String stipVal = StringUtils.strip(RelUtils.stringValue(oriValue), "'\"");
                        StatisticManager.getInstance().resetCorrectionMap(stipVal);
                    } else if (ConnectionProperties.SERVER_ID.equalsIgnoreCase(key.getName())) {
                        if (key.getScope() != org.apache.calcite.sql.VariableScope.GLOBAL) {
                            c.writeErrMessage(ErrorCode.ER_INCORRECT_GLOBAL_LOCAL_VAR,
                                "Variable '" + key.getName()
                                    + "' is a GLOBAL variable and should be set with SET GLOBAL");
                            return false;
                        }
                        try {
                            long v = Long.parseLong(c.getVarStringValue(oriValue));
                            c.getExtraServerVariables()
                                .put(ConnectionProperties.SERVER_ID.toLowerCase(), Long.toString(v));
                            globalCnVariables.add(new Pair<>(key.getName(), Long.toString(v)));
                        } catch (Throwable t) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR,
                                "Incorrect argument type to variable 'server_id'");
                            return false;
                        }
                    } else if (!isCnVariable(key.getName())) {
                        if (!ServerVariables.isWritable(key.getName())) {
                            if (enableSetGlobal && key.getScope() == org.apache.calcite.sql.VariableScope.GLOBAL) {
                                //ignore
                            } else {
                                if (!ServerVariables.contains(key.getName())) {
                                    if (inProcedureCall) {
                                        throw new RuntimeException("Unknown system variable '" + key.getName() + "'");
                                    }
                                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_SYSTEM_VARIABLE,
                                        "Unknown system variable '" + key.getName() + "'");
                                    return false;
                                }

                                if (ServerVariables.isReadonly(key.getName())) {
                                    if (inProcedureCall) {
                                        throw new RuntimeException("Variable '" + key.getName()
                                            + "' is a read only variable");
                                    }
                                    c.writeErrMessage(ErrorCode.ER_INCORRECT_GLOBAL_LOCAL_VAR,
                                        "Variable '" + key.getName() + "' is a read only variable");
                                    return false;
                                }
                                if (!enableSetGlobal && key.getScope() == org.apache.calcite.sql.VariableScope.GLOBAL) {
                                    if (inProcedureCall) {
                                        throw new RuntimeException("Don't support SET GLOBAL now!");
                                    }
                                    c.writeErrMessage(ErrorCode.ER_INCORRECT_GLOBAL_LOCAL_VAR,
                                        "Don't support SET GLOBAL now!");
                                    return false;
                                } else {
                                    if (inProcedureCall) {
                                        throw new RuntimeException("Variable '" + key.getName()
                                            + "' is a GLOBAL variable and should be set with SET GLOBAL");
                                    }
                                    c.writeErrMessage(ErrorCode.ER_INCORRECT_GLOBAL_LOCAL_VAR,
                                        "Variable '" + key.getName()
                                            + "' is a GLOBAL variable and should be set with SET GLOBAL");
                                    return false;
                                }
                            }
                        }
                        if (ServerVariables.isBanned(key.getName())) {
                            if (inProcedureCall) {
                                throw new RuntimeException("Not supported variable for now '" + key.getName() + "'");
                            }
                            c.writeErrMessage(ErrorCode.ER_UNKNOWN_SYSTEM_VARIABLE,
                                "Not supported variable for now '" + key.getName() + "'");
                            return false;
                        }
                        // ignore variables
                        if (isNeedIgnore(key.getName())) {
                            break;
                        }
                        Object parserValue = parserValue(oriValue, key, c);
                        if (parserValue == RETURN_VALUE) {
                            return true;
                        } else if (parserValue != IGNORE_VALUE) {
                            if (enableSetGlobal && (key.getScope() == VariableScope.GLOBAL)) {
                                globalDNVariables.add(variable);
                                if (ServerVariables.isWritable(key.getName())) {
                                    //global变量，只有属于writableVariables才属于同时set session
                                    c.getServerVariables().put(key.getName().toLowerCase(), parserValue);
                                }
                            } else {
                                c.getServerVariables().put(key.getName().toLowerCase(), parserValue);
                            }
                        }
                    } else {
                        Object parserValue = parserValue(oriValue, key, c);
                        if (parserValue == RETURN_VALUE) {
                            return true;
                        } else if (parserValue != IGNORE_VALUE && parserValue != null) {
                            String relVal = parserValue.toString();
                            c.getConnectionVariables().put(key.getName().toUpperCase(Locale.ROOT), relVal);
                            if (ConnectionProperties.ENABLE_STORAGE_TRIGGER.equalsIgnoreCase(key.getName())) {
                                boolean iacSupported = false;
                                if (relVal.equalsIgnoreCase(Boolean.TRUE.toString())) {
                                    iacSupported = true;
                                } else {
                                    iacSupported = false;
                                }
                                c.getConnectionVariables()
                                    .put(ConnectionProperties.ENABLE_RANDOM_PHY_TABLE_NAME, !iacSupported);
                            }
                            if (enableSetGlobal && (key.getScope() == VariableScope.GLOBAL)) {
                                globalCnVariables.add(new Pair<String, String>(key.getName(), parserValue.toString()));
                            }
                        }
                    }
                }
            }

            if (!GeneralUtil.isEmpty(globalDNVariables) || !GeneralUtil.isEmpty(globalCnVariables)) {
                boolean isOk = handleGlobalVariable(c, globalCnVariables, globalDNVariables);
                if (isOk) {
                    return true;
                }
            }
        } else if (result instanceof SqlSetTransaction) {
            SqlSetTransaction statement = (SqlSetTransaction) result;

            if (null != statement.getAccessModel()) {
                switch (statement.getAccessModel()) {
                case READ_ONLY:
                    c.setReadOnly(true);
                    break;
                case READ_WRITE:
                    c.setReadOnly(false);
                    break;
                default:
                    c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                        "unexpected token for SET TRANSACTION statement " + statement.getAccess());
                    return false;
                } // end of switch
            } // end of if

            if (null != statement.getIsolationLevel()) {
                if (statement.isGlobal()) {
                    c.writeErrMessage(ErrorCode.ER_INCORRECT_GLOBAL_LOCAL_VAR,
                        "Global isolation level must be set on DRDS console");
                    return false;
                }
                IsolationLevel isolation = IsolationUtil.convertCalcite(statement.getIsolationLevel());
                if (isolation == null) {
                    throw new AssertionError("impossible isolation null");
                }
                if (statement.isSession()) {
                    c.setTxIsolation(isolation.getCode());
                } else {
                    c.setStmtTxIsolation(isolation.getCode());
                }
            } // end of if

            if (null != statement.getPolicy()) {
                int policy = Integer.parseInt(statement.getPolicy());
                switch (policy) {
                case 3:
                    c.setTrxPolicy(ITransactionPolicy.ALLOW_READ_CROSS_DB);
                    break;
                case 4:
                    c.setTrxPolicy(ITransactionPolicy.NO_TRANSACTION);
                    break;
                case 6:
                case 7:
                    c.setTrxPolicy(ITransactionPolicy.XA);
                    break;
                case 8:
                    c.setTrxPolicy(ITransactionPolicy.TSO);
                    break;
                default:
                    c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                        "Variable 'transaction policy' can't be set to the value of " + String.valueOf(
                            statement.getPolicy()));
                    return false;
                } // end of switch
            } // end of if
        } else if (result instanceof SqlSetNames) {
            // not support collate
            final SqlNode charsetNode = ((SqlSetNames) result).getCharset();
            String charset = RelUtils.stringValue(charsetNode);
            if (!setCharset(charset, c)) {
                return true;
            }
        } else if (result instanceof SqlSetRole) {
            final SqlSetRole setRoleNode = (SqlSetRole) result;
            setRole(setRoleNode, c);
        } else {
            if (inProcedureCall) {
                throw new RuntimeException(stmt + " should not be set under procedure");
            }
            c.innerExecute(stmt, null, c.createResultHandler(hasMore), null);
            return true;
        }

        if (!inProcedureCall) {
            OkPacket ok = new OkPacket();
            ok.packetId = c.getNewPacketId();
            ok.insertId = 0;
            ok.affectedRows = 0;
            if (c.getTddlConnection() != null) {
                ok.warningCount = c.getTddlConnection().getWarningCount();
            }

            if (c.isAutocommit()) {
                ok.serverStatus = MySQLPacket.SERVER_STATUS_AUTOCOMMIT;
            } else {
                ok.serverStatus = MySQLPacket.SERVER_STATUS_IN_TRANS;
            }
            if (hasMore) {
                ok.serverStatus |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
            }
            ok.write(PacketOutputProxyFactory.getInstance().createProxy(c));
        }

        return true;
    }

    private static Boolean parseBool(SqlNode oriValue, ServerConnection c) {
        Boolean ret = null;
        String stripVal = StringUtils.strip(RelUtils.stringValue(oriValue), "'\"");
        if (oriValue instanceof SqlLiteral
            && ((SqlLiteral) oriValue).getTypeName() == SqlTypeName.BOOLEAN) {
            ret = RelUtils.booleanValue(oriValue);
        } else if (oriValue instanceof SqlNumericLiteral) {
            ret = RelUtils.integerValue((SqlLiteral) oriValue) != 0;
        } else if (oriValue instanceof SqlSystemVar || oriValue instanceof SqlUserDefVar) {
            ret = c.getVarBooleanValue(oriValue);
        } else if ("ON".equalsIgnoreCase(stripVal)) {
            ret = true;
        } else if ("OFF".equalsIgnoreCase(stripVal)) {
            ret = false;
        }
        return ret;
    }

    private static boolean isNeedIgnore(String variableName) {
        if (!ExecUtils.isMysql80Version()) {
            // ignore variables only supported by 8 version
            if ("information_schema_stats_expiry".equalsIgnoreCase(variableName)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isDefault(SqlNode oriValue) {
        return (oriValue instanceof SqlBasicCall && oriValue.getKind() == SqlKind.DEFAULT) || (
            oriValue instanceof SqlIdentifier && "DEFAULT".equals(oriValue.toString()));
    }

    private static UserDefVarProcessingResult userDefVarProcessingFunc(ResultSet resultSet) {
        boolean moreThanOneColumn = false;
        boolean moreThanOneRow = false;
        boolean otherError = false;
        Object value = null;
        if (resultSet != null) {
            try {
                int columnCount = resultSet.getMetaData().getColumnCount();
                if (columnCount == 1) {
                    if (resultSet.next()) {
                        int sqlType = resultSet.getMetaData().getColumnType(1);
                        switch (sqlType) {
                        case Types.BIT:
                        case Types.TINYINT:
                        case Types.SMALLINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                            value = resultSet.getLong(1);
                            break;
                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            value = resultSet.getDouble(1);
                            break;
                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.LONGVARBINARY:
                            value = resultSet.getBytes(1);
                            break;
                        default:
                            value = resultSet.getString(1);
                            break;
                        }

                        if (resultSet.next()) {
                            moreThanOneRow = true;
                        }
                    }
                } else {
                    moreThanOneColumn = true;
                }
            } catch (SQLException e) {
                otherError = true;
            }
        }
        return new UserDefVarProcessingResult(moreThanOneColumn, moreThanOneRow, otherError, value);
    }

    // return value demonstrates whether write packet
    private static boolean handleGlobalVariable(ServerConnection c, List<Pair<String, String>> globalCNVariableList,
                                                List<Pair<SqlNode, SqlNode>> globalDNVariableList) {

        List<Pair<SqlNode, SqlNode>> dnVariableAssignmentList = new ArrayList<>();

        Properties cnProps = new Properties();

        for (Pair<String, String> variable : globalCNVariableList) {

            String systemVarName = variable.getKey();
            String systemVarValue = variable.getValue();

            try {

                if (ServerVariables.isGlobalBanned(systemVarName)) {
                    c.writeErrMessage(ErrorCode.ER_NOT_SUPPORTED_YET,
                        "The global variable '" + systemVarName + "' is no supported setting using SET GLOBAL");
                    return true;
                }

                try {
                    // Check whether the variable value is valid.
                    extraCheck(systemVarName, systemVarValue);
                } catch (Throwable t) {
                    logger.warn(t);
                    c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, t.getMessage());
                    return true;
                }

                systemVarName = getParamNameInInstConfig(systemVarName);

                if (isCnVariable(systemVarName)) {
                    systemVarName = systemVarName.toUpperCase(Locale.ROOT);
                    cnProps.setProperty(systemVarName, systemVarValue);
                } else {
                    c.writeErrMessage(ErrorCode.ER_GLOBAL_VARIABLE, "Unsupported variable '" + systemVarName + "'");
                    return true;
                }
            } catch (Throwable t) {
                c.writeErrMessage(ErrorCode.ER_GLOBAL_VARIABLE, "Error occurred when setting global variables");
                logger.error(t.getMessage());
                return true;
            }
        }

        Properties dnProps = new Properties();

        for (Pair<SqlNode, SqlNode> variable : globalDNVariableList) {

            String systemVarName = ((SqlSystemVar) variable.getKey()).getName().toLowerCase(Locale.ROOT);
            String systemVarValue = c.getVarStringValue(variable.getValue());

            try {
                if (ServerVariables.isGlobalBanned(systemVarName)) {
                    c.writeErrMessage(ErrorCode.ER_NOT_SUPPORTED_YET,
                        "The global variable '" + systemVarName + "' is no supported setting using SET GLOBAL");
                    return true;
                }
                if (isDnVariable(systemVarName)) {
                    if ((ServerVariables.isMysqlBoth(systemVarName) || ServerVariables.isMysqlGlobal(systemVarName))
                        && !ServerVariables.isMysqlDynamic(systemVarName)) {
                        c.writeErrMessage(ErrorCode.ER_VARIABLE_IS_READONLY,
                            "The global variable '" + systemVarName + "' is readonly");
                        return true;
                    }
                    dnVariableAssignmentList.add(variable);
                    dnProps.setProperty(systemVarName, systemVarValue);
                } else {
                    c.writeErrMessage(ErrorCode.ER_GLOBAL_VARIABLE, "Unsupported variable '" + systemVarName + "'");
                    return true;
                }

            } catch (Throwable t) {
                c.writeErrMessage(ErrorCode.ER_GLOBAL_VARIABLE, "Error occurred when setting global variables");
                logger.error(t.getMessage());
                return true;
            }
        }

        // check superuser privileges before executing real set global
        if (!c.isSuperUser()) {
            if (c.getTddlConnection() == null) {
                c.initTddlConnection();
            }
            TConnection conn = c.getTddlConnection();
            String msg = "User " + c.getUser()
                + " trying to execute set global, which will be ignored";
            if (conn != null && conn.getExecutionContext() != null) {
                conn.getExecutionContext().addMessage(ExecutionContext.WARNING_MESSAGE,
                    new ExecutionContext.ErrorMessage(ERR_CHECK_PRIVILEGE_FAILED.getCode(), null, msg));
            }

            logger.warn("[" + c.getTraceId() + "]" + msg);
            return false;
        }

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            VariableConfigAccessor variableConfigAccessor = new VariableConfigAccessor();
            InstConfigAccessor instConfigAccessor = new InstConfigAccessor();
            variableConfigAccessor.setConnection(metaDbConn);
            instConfigAccessor.setConnection(metaDbConn);
            if (!GeneralUtil.isEmpty(dnVariableAssignmentList)) {
                SqlSet setStatement = new SqlSet(SqlParserPos.ZERO, dnVariableAssignmentList);
                Map<String, StorageInstHaContext> storageStatusMap =
                    StorageHaManager.getInstance().getStorageHaCtxCache();
                Iterator<StorageInstHaContext> iterator = storageStatusMap.values().stream().iterator();
                while (iterator.hasNext()) {
                    StorageInstHaContext instHaContext = iterator.next();
                    if (instHaContext != null && ServerInstIdManager.getInstance().getInstId()
                        .equalsIgnoreCase(instHaContext.getInstId()) && !instHaContext.isMetaDb()) {
                        try (Connection connection = DbTopologyManager.getConnectionForStorage(instHaContext)) {
                            PreparedStatement statement = connection.prepareStatement(setStatement.toString());
                            statement.execute();
                        } catch (Throwable e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                // refresh cache
                variableConfigAccessor.updateParamsValue(dnProps, InstIdUtil.getInstId());
                CacheVariables.invalidateAll();
                // FIXME: refresh cache when using X-Protocol
            }
            if (!GeneralUtil.isEmpty(cnProps)) {
                instConfigAccessor.updateInstConfigValue(InstIdUtil.getInstId(), cnProps);
            }
            return false;
        } catch (Throwable t) {
            c.writeErrMessage(ErrorCode.ER_GLOBAL_VARIABLE, "Error occurred when setting global variables");
            logger.error(t.getMessage());
            return true;
        }
    }

    private static boolean setCharset(String charset, ServerConnection c) {
        if ("default".equalsIgnoreCase(charset) || "null".equalsIgnoreCase(charset) || null == charset) {
            /* 忽略字符集为null的属性设置 */
        } else if (c.setCharset(charset)) {

        } else {
            try {
                if (c.setCharsetIndex(Integer.parseInt(charset))) {
                } else {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset :" + charset);
                    return false;
                }
            } catch (RuntimeException e) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset :" + charset);
                return false;
            }
        }

        return true;
    }

    /**
     * Set current session's active roles.
     *
     * @param sqlNode Sql node for set role statement.
     * @param c Current connection
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-role.html">set role</a>
     * @see ActiveRoles
     */
    private static void setRole(SqlSetRole sqlNode, ServerConnection c) {
        PolarPrivManager manager = PolarPrivManager.getInstance();

        PolarAccountInfo currentUser = manager.getAndCheckById(c.getMatchPolarUserInfo().getAccountId());

        ActiveRoles.ActiveRoleSpec activeRoleSpec = ActiveRoles.ActiveRoleSpec.from(sqlNode.getRoleSpec());

        List<PolarAccountInfo> roles =
            sqlNode.getUsers().stream().map(SqlUserName::toPolarAccount).map(manager::getAndCheckExactUser)
                .collect(Collectors.toList());

        c.setActiveRoles(currentUser.getRolePrivileges().checkAndGetActiveRoles(activeRoleSpec, roles));
    }

    private static UserDefVarProcessingResult getSelectResult(ServerConnection c, String sql) {
        SelectResultHandler handler = new SelectResultHandler();
        c.innerExecute(ByteString.from(sql), null, handler, null);

        UserDefVarProcessingResult r = handler.getResult();
        return r;
    }

    private static boolean checkResultSuccess(ServerConnection c, UserDefVarProcessingResult r, boolean inProcedureCall,
                                              boolean throwException) {
        if (r == null) {
            if (throwException) {
                if (inProcedureCall) {
                    throw new RuntimeException("ER_WRONG_VALUE_FOR_VAR: execute sql Error");
                }
                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "execute sql Error");
            }
            return false;
        } else if (r.moreThanOneColumn) {
            if (throwException) {
                if (inProcedureCall) {
                    throw new RuntimeException("ER_WRONG_VALUE_FOR_VAR: Operand should contain 1 column(s)");
                }
                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Operand should contain 1 column(s)");
            }
            return false;
        } else if (r.moreThanOneRow && !(ConfigDataMode.isFastMock())) {
            if (throwException) {
                if (inProcedureCall) {
                    throw new RuntimeException("ER_WRONG_VALUE_FOR_VAR: Subquery returns more than 1 row");
                }
                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Subquery returns more than 1 row");
            }
            return false;
        } else if (r.otherError) {
            if (throwException) {
                if (inProcedureCall) {
                    throw new RuntimeException("ER_WRONG_VALUE_FOR_VAR: execute sql Error");
                }
                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "execute sql Error");
            }
            return false;
        } else {
            return true;
        }
    }

    public static class SelectResultHandler implements QueryResultHandler {

        private UserDefVarProcessingResult result;

        @Override
        public void sendUpdateResult(long affectedRows) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void sendSelectResult(ResultSet resultSet, AtomicLong outAffectedRows, long sqlSelectLimit)
            throws Exception {
            result = userDefVarProcessingFunc(resultSet);
            outAffectedRows.set(1);
        }

        @Override
        public void sendPacketEnd(boolean hasMoreResults) {
            // do nothing
        }

        @Override
        public void handleError(Throwable ex, ByteString sql, boolean fatal) {
            // do nothing
        }

        public UserDefVarProcessingResult getResult() {
            return result;
        }
    }

    /**
     * Check whether the {@param systemVarValue} is valid for {@param systemVarName}.
     * If it is not valid, an exception will be thrown.
     */
    private static void extraCheck(String systemVarName, String systemVarValue) {
        if (systemVarName.equalsIgnoreCase(TransactionAttribute.DRDS_TRANSACTION_POLICY)) {
            if ("2PC".equalsIgnoreCase(systemVarValue) || "FLEXIBLE".equalsIgnoreCase(systemVarValue)) {
                return;
            }
            try {
                ITransactionPolicy.TransactionClass.valueOf(systemVarValue.toUpperCase());
            } catch (Exception e) {
                throw new TddlRuntimeException(com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_VALIDATE,
                    "The global variable '" + systemVarName + "' cannot be set to the value of '" + systemVarValue
                        + "'");
            }
            return;
        }

        // Check whether it is a timer task parameter, and validate it.
        if (ParamValidationUtils.isTimerTaskParam(systemVarName.toUpperCase(), systemVarValue)) {
            return;
        }

    }

    private static String getParamNameInInstConfig(String originParamName) {
        if (originParamName.equalsIgnoreCase(TransactionAttribute.DRDS_TRANSACTION_POLICY)) {
            return ConnectionProperties.TRANSACTION_POLICY;
        }
        return originParamName;
    }

    private static boolean isCnVariable(String variableName) {
        return SystemPropertiesHelper.getConnectionProperties().contains(variableName.toUpperCase(Locale.ROOT));
    }

    private static boolean isDnVariable(String variableName) {
        return ServerVariables.contains(variableName);
    }

    private static Object parserValue(SqlNode oriValue, SqlSystemVar key, ServerConnection c) {
        Object value = IGNORE_VALUE;
        if (oriValue instanceof SqlCharStringLiteral) {
            value = RelUtils.stringValue(oriValue);
        } else if (oriValue instanceof SqlNumericLiteral) {
            value = ((SqlNumericLiteral) oriValue).getValue();
        } else if (oriValue instanceof SqlUserDefVar) {
            value = c.getUserDefVariables().get(((SqlUserDefVar) oriValue).getName().toLowerCase());
            if (!c.getUserDefVariables().containsKey(((SqlUserDefVar) oriValue).getName().toLowerCase())) {
                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                    "Variable " + key.getName() + " can't be set to the value of " + RelUtils.stringValue(oriValue));
                return RETURN_VALUE;
            }
        } else if (oriValue instanceof SqlSystemVar) {
            SqlSystemVar var = (SqlSystemVar) oriValue;
            if (!ServerVariables.contains(var.getName()) && !ServerVariables.isExtra(var.getName())) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_SYSTEM_VARIABLE,
                    "Unknown system variable '" + var.getName() + "'");
                return RETURN_VALUE;
            }
            value = c.getSysVarValue(var);
        } else if (oriValue instanceof SqlLiteral && ((SqlLiteral) oriValue).getTypeName() == SqlTypeName.NULL) {
            value = null;
        } else if (oriValue instanceof SqlLiteral && ((SqlLiteral) oriValue).getTypeName() == SqlTypeName.BOOLEAN) {
            value = ((SqlLiteral) oriValue).booleanValue();
        } else if (isDefault(oriValue)) {
            value = "default";
        } else if (oriValue instanceof SqlIdentifier) {
            value = oriValue.toString();
            // } else if (oriValue instanceof SqlBasicCall
            // && oriValue.getKind() == SqlKind.MINUS) {
            // value =
            // ((SqlNode)oriValue).evaluation(Collections.emptyMap());
        }
        return value;
    }

    private static class UserDefVarProcessingResult {

        public boolean moreThanOneColumn;
        public boolean moreThanOneRow;
        public boolean otherError;
        public Object value;

        public UserDefVarProcessingResult(boolean moreThanOneColumn, boolean moreThanOneRow, boolean otherError,
                                          Object value) {
            this.moreThanOneColumn = moreThanOneColumn;
            this.moreThanOneRow = moreThanOneRow;
            this.otherError = otherError;
            this.value = value;
        }
    }
    
}
