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

import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.atom.CacheVariables;
import com.alibaba.polardbx.common.SQLMode;
import com.alibaba.polardbx.common.constants.IsolationLevel;
import com.alibaba.polardbx.common.constants.ServerVariables;
import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.ddl.Attribute;
import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
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
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.privilege.ActiveRoles;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.topology.InstConfigAccessor;
import com.alibaba.polardbx.gms.topology.VariableConfigAccessor;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.server.QueryResultHandler;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.IsolationUtil;
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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.utils.failpoint.FailPoint.FP_CLEAR;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPoint.FP_SHOW;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPoint.SET_PREFIX;

/**
 * SET 语句处理
 *
 * @author xianmao.hexm
 */
public final class SetHandler {

    static FastsqlParser fastsqlParser = new FastsqlParser();

    static Object IGNORE_VALUE = new Object();
    static Object RETURN_VALUE = new Object();

    private static final Logger logger = LoggerFactory.getLogger(SetHandler.class);

    public static void handleV2(ByteString stmt, ServerConnection c, int offset, boolean hasMore) {
        SqlNodeList results = fastsqlParser.parse(stmt);
        boolean ret = c.initOptimizerContext();
        if (!ret) {
            return;
        }
        assert results.size() == 1;
        SqlNode result = results.get(0);

        if (result instanceof SqlSet) {
            SqlSet statement = (SqlSet) result;
            // 不在server层处理，需要继续下推的set设置
            List<Pair<SqlNode, SqlNode>> globalDNVariables = new ArrayList<>();
            List<Pair<String, String>> globalCnVariables = new ArrayList<>();
            for (org.apache.calcite.util.Pair<SqlNode, SqlNode> variable : statement.getVariableAssignmentList()) {
                final SqlNode oriValue = variable.getValue();
                if (variable.getKey() instanceof SqlUserDefVar) {
                    final SqlUserDefVar key = (SqlUserDefVar) variable.getKey();
                    String lowerCaseKey = key.getName().toLowerCase();
                    if (oriValue instanceof SqlCharStringLiteral) {
                        String value = RelUtils.stringValue(oriValue);
                        c.getUserDefVariables().put(lowerCaseKey, value);
                        //FailPoint command, only works in java -ea mode
                        if (FailPoint.isAssertEnable()
                            && StringUtils.startsWith(lowerCaseKey, SET_PREFIX)
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
                        if (FailPoint.isAssertEnable()
                            && StringUtils.equalsIgnoreCase(lowerCaseKey, FP_CLEAR)) {
                            FailPoint.clear();
                            c.getUserDefVariables().put(FP_SHOW, FailPoint.show());
                        }
                    } else if (oriValue instanceof SqlLiteral
                        && ((SqlLiteral) oriValue).getTypeName() == SqlTypeName.NULL
                        && oriValue.toString().equalsIgnoreCase("NULL")) {
                        c.getUserDefVariables().remove(lowerCaseKey);
                        //FailPoint command, only works in java -ea mode
                        if (FailPoint.isAssertEnable()
                            && StringUtils.startsWith(lowerCaseKey, SET_PREFIX)) {
                            FailPoint.disable(lowerCaseKey);
                            c.getUserDefVariables().put(FP_SHOW, FailPoint.show());
                        }
                    } else if (oriValue instanceof SqlUserDefVar) {
                        String value = ((SqlUserDefVar) oriValue).getName();
                        c.getUserDefVariables().put(lowerCaseKey, c.getUserDefVariables().get(value.toLowerCase()));
                    } else if (oriValue instanceof TDDLSqlSelect) {

                        SelectResultHandler handler = new SelectResultHandler();
                        String sql = RelUtils.toNativeSql(oriValue);
                        c.innerExecute(ByteString.from(sql), null, handler, null);

                        UserDefVarProcessingResult r = handler.getResult();
                        if (r == null) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "execute sql Error");
                            return;
                        } else if (r.moreThanOneColumn) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Operand should contain 1 column(s)");
                            return;
                        } else if (r.moreThanOneRow && !(ConfigDataMode.isFastMock())) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Subquery returns more than 1 row");
                            return;
                        } else if (r.otherError) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "execute sql Error");
                            return;
                        } else {
                            c.getUserDefVariables().put(lowerCaseKey, r.value);
                        }
                    } else if (oriValue instanceof SqlSystemVar) {
                        final SqlSystemVar var = (SqlSystemVar) oriValue;
                        if (!ServerVariables.contains(var.getName()) && !ServerVariables.isExtra(var.getName())) {
                            c.writeErrMessage(ErrorCode.ER_UNKNOWN_SYSTEM_VARIABLE, "Unknown system variable '"
                                + var.getName() + "'");
                            return;
                        }
                        Object sysVarValue = c.getSysVarValue(var);
                        c.getUserDefVariables().put(lowerCaseKey, sysVarValue);
                    } else {
                        c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable " + key.getName()
                            + " can't be set to the value of "
                            + RelUtils.stringValue(oriValue));
                        return;
                    }
                } else if (variable.getKey() instanceof SqlSystemVar) {
                    final SqlSystemVar key = (SqlSystemVar) variable.getKey();
                    if (c.getTddlConnection() == null) {
                        c.initTddlConnection();
                    }

                    boolean enableSetGlobal = false;
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

                    if ("NAMES".equalsIgnoreCase(key.getName())) {
                        String charset = c.getVarStringValue(oriValue);
                        if (!setCharset(charset, c)) {
                            return;
                        }
                        c.getExtraServerVariables().put(key.getName().toLowerCase(), charset);
                    } else if ("SOCKETTIMEOUT".equalsIgnoreCase(key.getName())) {
                        if (!(oriValue instanceof SqlNumericLiteral) &&
                            !(oriValue instanceof SqlUserDefVar) && !(oriValue instanceof SqlSystemVar)) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable 'socketTimeout' can't be set to the value of "
                                    + RelUtils.stringValue(oriValue));
                            return;
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
                                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                    "Variable 'autocommit' can't be set to the value of "
                                        + RelUtils.stringValue(variable.getValue()));
                                return;
                            }
                            autocommit = b;
                        } else if ("ON".equalsIgnoreCase(stipVal)) {
                            autocommit = true;
                        } else if ("OFF".equalsIgnoreCase(stipVal)) {
                            autocommit = false;
                        } else {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable 'autocommit' can't be set to the value of "
                                    + RelUtils.stringValue(variable.getValue()));
                            return;
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
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable '" + ConnectionProperties.PURE_ASYNC_DDL_MODE
                                    + "' can't be set to the value of "
                                    + RelUtils.stringValue(variable.getValue()));
                            return;
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
                    } else if ("SQL_SELECT_LIMIT".equalsIgnoreCase(key.getName())) {
                        //SQL_SELECT_LIMIT 默认值为无符号64位整型的最大值 18446744073709551615，无法转Long
                        if (isDefault(oriValue)) {
                            c.setSqlSelectLimit(null);
                            c.getExtraServerVariables().put(key.getName().toLowerCase(), null);
                        } else if (oriValue instanceof SqlSystemVar || oriValue instanceof SqlUserDefVar) {
                            Long limit = Long.valueOf(String.valueOf(c.getVarValueBySqlNode(oriValue)));
                            c.setSqlSelectLimit(limit);
                            c.getExtraServerVariables().put(key.getName().toLowerCase(), limit);
                        }

                        // ignore 会截断数据
                        else if (!(oriValue instanceof SqlNumericLiteral)) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable 'sql_select_limit' can't be set to the value of "
                                    + RelUtils.stringValue(oriValue));
                            return;
                        } else {
                            Long limit = Long.parseUnsignedLong(((SqlNumericLiteral) oriValue).getValue().toString());
                            c.setSqlSelectLimit(limit);
                            c.getExtraServerVariables().put(key.getName().toLowerCase(), limit);
                        }
                    } else if ("TRANSACTION POLICY".equalsIgnoreCase(key.getName())) {
                        if (!(oriValue instanceof SqlNumericLiteral) &&
                            !(oriValue instanceof SqlUserDefVar) && !(oriValue instanceof SqlSystemVar)) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable 'transaction policy' can't be set to the value of "
                                    + RelUtils.stringValue(oriValue));
                            return;
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
                            c.setTrxPolicy(ITransactionPolicy.XA);
                            strPolicy = "XA";
                            break;
                        case 8:
                            c.setTrxPolicy(ITransactionPolicy.TSO);
                            strPolicy = "TSO";
                            break;
                        default:
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable 'transaction policy' can't be set to the value of "
                                    + RelUtils.stringValue(oriValue));
                            return;
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
                                c.writeErrMessage(ErrorCode.ER_VARIABLE_IS_READONLY,
                                    "Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' is read only on auto-commit mode");
                                return;
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
                            } else {
                                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                    "Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' can't be set to the value of " + policy);
                                return;
                            }
                            c.getExtraServerVariables().put("TRANSACTION POLICY".toLowerCase(), intPolicy);
                            c.getExtraServerVariables().put("TRANS.POLICY".toLowerCase(), policy.toUpperCase());
                            c.getExtraServerVariables()
                                .put(TransactionAttribute.DRDS_TRANSACTION_POLICY.toLowerCase(), policy.toUpperCase());
                        }
                    } else if (TransactionAttribute.SHARE_READ_VIEW.equalsIgnoreCase(key.getName())) {
                        if (c.isAutocommit()) {
                            if (!enableSetGlobal || key.getScope().equals(VariableScope.SESSION)) {
                                c.writeErrMessage(ErrorCode.ER_VARIABLE_IS_READONLY,
                                    "Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' is read only on auto-commit mode");
                                return;
                            }
                        }
                        boolean shareReadView;
                        String stripVal = StringUtils.strip(RelUtils.stringValue(oriValue), "'\"");
                        if (oriValue instanceof SqlLiteral
                            && ((SqlLiteral) oriValue).getTypeName() == SqlTypeName.BOOLEAN) {
                            shareReadView = RelUtils.booleanValue(variable.getValue());
                        } else if (variable.getValue() instanceof SqlNumericLiteral) {
                            shareReadView = RelUtils.integerValue((SqlLiteral) variable.getValue()) != 0;
                        } else if (oriValue instanceof SqlSystemVar || oriValue instanceof SqlUserDefVar) {
                            Boolean b = c.getVarBooleanValue(oriValue);
                            if (b == null) {
                                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                    "Variable '" + StringUtils.lowerCase(key.getName())
                                        + "' can't be set to the value of "
                                        + RelUtils.stringValue(variable.getValue()));
                                return;
                            }
                            shareReadView = b;
                        } else if ("ON".equalsIgnoreCase(stripVal)) {
                            shareReadView = true;
                        } else if ("OFF".equalsIgnoreCase(stripVal)) {
                            shareReadView = false;
                        } else {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable '" + StringUtils.lowerCase(key.getName())
                                    + "' can't be set to the value of "
                                    + RelUtils.stringValue(variable.getValue()));
                            return;
                        }
                        c.setShareReadView(shareReadView);
                    } else if (TransactionAttribute.DRDS_TRANSACTION_TIMEOUT.equalsIgnoreCase(key.getName())) {
                        final String val = c.getVarStringValue(oriValue);
                        try {
                            final long lval = Long.parseLong(val); // ms -> s
                            c.getServerVariables().put("max_trx_duration", lval < 1000 ? 1 : lval / 1000);
                        } catch (NumberFormatException e) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "Variable '" + StringUtils.lowerCase(key.getName())
                                    + "' can't be set to the value of " + val);
                            return;
                        }
                    } else if ("TX_ISOLATION".equals(key.getName().toUpperCase())) {
                        int isolationCode;
                        IsolationLevel isolation;
                        String value;
                        if (isDefault(oriValue)) {
                            isolationCode = ConfigDataMode.getTxIsolation();
                            isolation = IsolationLevel.fromInt(isolationCode);
                            if (isolation == null) {
                                throw new AssertionError("Invalid global tx_isolation");
                            }
                            value = isolation.nameWithHyphen();
                        } else {
                            value = c.getVarStringValue(oriValue);
                            isolation = IsolationLevel.parse(value);
                            if (isolation == null) {
                                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                    "Variable 'tx_isolation' can't be set to the value of '" + value + "'");
                                return;
                            }
                            isolationCode = isolation.getCode();
                        }
                        c.setTxIsolation(isolationCode);
                        c.getExtraServerVariables().put(key.getName().toLowerCase(), value);
                    } else if ("READ".equalsIgnoreCase(key.getName())) {
                        if (!(oriValue instanceof SqlCharStringLiteral) &&
                            !(oriValue instanceof SqlUserDefVar) && !(oriValue instanceof SqlSystemVar)) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "unexpected token for SET TRANSACTION statement " + RelUtils.stringValue(oriValue));
                            return;
                        }
                        String readValue = TStringUtil.upperCase(c.getVarStringValue(oriValue));
                        if (TStringUtil.equalsIgnoreCase("WRITE", readValue)) {
                            c.setReadOnly(false);
                        } else if (TStringUtil.equalsIgnoreCase("ONLY", readValue)) {
                            c.setReadOnly(true);
                        } else {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "unexpected token for SET TRANSACTION statement " + RelUtils.stringValue(oriValue));
                            return;
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
                            && ((SqlLiteral) oriValue).getTypeName() == SqlTypeName.NULL)
                            || "NULL".equalsIgnoreCase(strVal)
                            || StringUtils.isEmpty(strVal)
                            || StringUtils.isEmpty(StringUtils.strip("'\""))) {
                            charset = null;
                        } else if (isDefault(oriValue)) {
                            charset = "utf8";
                        } else if (oriValue instanceof SqlIdentifier) {
                            charset = oriValue.toString();
                        } else {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable '" + key.getName()
                                + "' can't be set to the value of "
                                + RelUtils.stringValue(oriValue));
                            return;
                        }

                        if (!setCharset(charset, c)) {
                            return;
                        }
                        c.getExtraServerVariables().put("CHARACTER_SET_RESULTS".toLowerCase(), charset);
                        c.getExtraServerVariables().put("CHARACTER_SET_CONNECTION".toLowerCase(), charset);
                    } else if ("CHARACTER_SET_CLIENT".equalsIgnoreCase(key.getName())) {

                        /* 忽略client属性设置 */
                        // 忽略这个？
                    } else if (BatchInsertPolicy.getVariableName().equalsIgnoreCase(key.getName())) {
                        if (!(oriValue instanceof SqlCharStringLiteral) &&
                            !(oriValue instanceof SqlUserDefVar) && !(oriValue instanceof SqlSystemVar)) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "unexpected token for SET BATCH_INSERT_POLICY statement "
                                    + RelUtils.stringValue(oriValue));
                            return;
                        }

                        String policyValue = TStringUtil.upperCase(c.getVarStringValue(oriValue));
                        BatchInsertPolicy policy = BatchInsertPolicy.getPolicyByName(policyValue);
                        if (policy == null) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                "unexpected token for SET BATCH_INSERT_POLICY statement "
                                    + RelUtils.stringValue(oriValue));
                            return;
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
                    } else if ("group_concat_max_len".equalsIgnoreCase(key.getName())) {
                        if (key.getScope() != null && key.getScope().name().equalsIgnoreCase("global")) {
                            c.writeErrMessage(ErrorCode.ER_NOT_SUPPORTED_YET,
                                "not support set global group_concat_max_len");
                            return;
                        }
                        try {
                            int v = c.getVarIntegerValue(oriValue);
                            if (v < 4) {
                                v = 4;
                            }
                            c.getServerVariables().put("group_concat_max_len", v);
                        } catch (Exception e) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR,
                                "Incorrect argument type to variable 'group_concat_max_len'");
                            return;
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
                            c.writeErrMessage(ErrorCode.ER_NOT_SUPPORTED_YET,
                                "not support set global polardbx_server_id");
                            return;
                        }
                        try {
                            int v = c.getVarIntegerValue(oriValue);
                            c.getExtraServerVariables().put("polardbx_server_id", v);
                        } catch (Exception | Error e) {
                            c.writeErrMessage(ErrorCode.ER_WRONG_TYPE_FOR_VAR,
                                "Incorrect argument type to variable 'polardbx_server_id'");
                            return;
                        }
                    } else if ("time_zone".equalsIgnoreCase(key.getName())) {
                        //在内部添加到customizeVar中
                        c.setTimeZone(c.getVarStringValue(oriValue));
                        Object parserValue = parserValue(oriValue, key, c);
                        if (parserValue == RETURN_VALUE) {
                            return;
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
                            iacSupported = Boolean.FALSE;
                        }
                        // global only
                        if (enableSetGlobal && key.getScope() == VariableScope.GLOBAL) {
                            globalCnVariables.add(new Pair<>(key.getName(), iacSupported.toString()));
                            if (TableInfoManager.isXdbInstantAddColumnSupported()) {
                                SqlSystemVar dnKey = SqlSystemVar.create(key.getScope(),
                                    Attribute.XDB_VARIABLE_INSTANT_ADD_COLUMN, SqlParserPos.ZERO);
                                globalDNVariables.add(new Pair<>(dnKey, variable.getValue()));
                            }
                        }
                    } else if ("block_encryption_mode".equalsIgnoreCase(key.getName())) {
                        BlockEncryptionMode encryptionMode;
                        boolean supportOpenSSL =
                            ExecutorContext.getContext(c.getSchema()).getStorageInfoManager()
                                .supportOpenSSL();

                        if (isDefault(oriValue)) {
                            encryptionMode = BlockEncryptionMode.DEFAULT_MODE;
                        } else {
                            String modeStr = c.getVarStringValue(oriValue);
                            try {
                                encryptionMode = new BlockEncryptionMode(modeStr, supportOpenSSL);
                            } catch (IllegalArgumentException exception) {
                                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                                    "Variable 'block_encryption_mode' can't be set to the value of '" + modeStr
                                        + "'");
                                return;
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
                                if ("ANSI_QUOTES".equalsIgnoreCase(sqlmode)
                                    || (SQLMode.isCombSQLMode(sqlmode) && SQLMode.contains(sqlmode,
                                    SQLMode.ANSI_QUOTES))) {
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
                            return;
                        } else if (parserValue != IGNORE_VALUE) {
                            c.getServerVariables().put(key.getName().toLowerCase(), parserValue);
                            if (enableSetGlobal && (key.getScope() == VariableScope.GLOBAL)) {
                                globalDNVariables.add(variable);
                            }
                        }
                    } else if (!isCnVariable(key.getName())) {
                        if (!ServerVariables.isWritable(key.getName())) {
                            if (enableSetGlobal && key.getScope() == org.apache.calcite.sql.VariableScope.GLOBAL) {
                                //ignore
                            } else {
                                if (!ServerVariables.contains(key.getName())) {
                                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_SYSTEM_VARIABLE, "Unknown system variable '"
                                        + key.getName() + "'");
                                    return;
                                }

                                if (ServerVariables.isReadonly(key.getName())) {
                                    c.writeErrMessage(ErrorCode.ER_INCORRECT_GLOBAL_LOCAL_VAR,
                                        "Variable '" + key.getName() + "' is a read only variable");
                                    return;
                                }
                                if (!enableSetGlobal && key.getScope() == org.apache.calcite.sql.VariableScope.GLOBAL) {
                                    c.writeErrMessage(ErrorCode.ER_INCORRECT_GLOBAL_LOCAL_VAR,
                                        "Don't support SET GLOBAL now!");
                                } else {
                                    c.writeErrMessage(ErrorCode.ER_INCORRECT_GLOBAL_LOCAL_VAR,
                                        "Variable '" + key.getName()
                                            + "' is a GLOBAL variable and should be set with SET GLOBAL");
                                }

                                return;
                            }
                        }
                        if (ServerVariables.isBanned(key.getName())) {
                            c.writeErrMessage(ErrorCode.ER_UNKNOWN_SYSTEM_VARIABLE,
                                "Not supported variable for now '" + key.getName() + "'");
                            return;
                        }
                        Object parserValue = parserValue(oriValue, key, c);
                        if (parserValue == RETURN_VALUE) {
                            return;
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
                            return;
                        } else if (parserValue != IGNORE_VALUE && parserValue != null) {
                            c.getConnectionVariables().put(
                                key.getName().toUpperCase(Locale.ROOT), parserValue.toString());
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
                    return;
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
                    return;
                } // end of switch
            } // end of if

            if (null != statement.getIsolationLevel()) {
                if (statement.isGlobal()) {
                    c.writeErrMessage(ErrorCode.ER_INCORRECT_GLOBAL_LOCAL_VAR,
                        "Global isolation level must be set on DRDS console");
                    return;
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
                    c.setTrxPolicy(ITransactionPolicy.XA);
                    break;
                case 8:
                    c.setTrxPolicy(ITransactionPolicy.TSO);
                    break;
                default:
                    c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                        "Variable 'transaction policy' can't be set to the value of "
                            + String.valueOf(statement.getPolicy()));
                    return;
                } // end of switch
            } // end of if
        } else if (result instanceof SqlSetNames) {
            // not support collate
            final SqlNode charsetNode = ((SqlSetNames) result).getCharset();
            String charset = RelUtils.stringValue(charsetNode);
            if (!setCharset(charset, c)) {
                return;
            }
        } else if (result instanceof SqlSetRole) {
            final SqlSetRole setRoleNode = (SqlSetRole) result;
            setRole(setRoleNode, c);
        } else {
            c.innerExecute(stmt, null, c.createResultHandler(hasMore), null);
            return;
        }

        OkPacket ok = new OkPacket();
        ok.packetId = c.getNewPacketId();
        ok.insertId = 0;
        ok.affectedRows = 0;

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

    public static boolean isDefault(SqlNode oriValue) {
        return (oriValue instanceof SqlBasicCall && oriValue.getKind() == SqlKind.DEFAULT)
            || (oriValue instanceof SqlIdentifier && "DEFAULT".equals(oriValue.toString()));
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
    private static boolean handleGlobalVariable(ServerConnection c,
                                                List<Pair<String, String>> globalCNVariableList,
                                                List<Pair<SqlNode, SqlNode>> globalDNVariableList) {
        List<Pair<SqlNode, SqlNode>> dnVariableAssignmentList = new ArrayList<>();

        Properties cnProps = new Properties();

        for (Pair<String, String> variable : globalCNVariableList) {

            String systemVarName = variable.getKey();
            String systemVarValue = variable.getValue();

            try {
                if (!extraCheck(systemVarName, systemVarValue)) {
                    c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR,
                        "The global variable '" + systemVarName + "' cannot be set to the value of '"
                            + systemVarValue + "'");
                    return true;
                }

                systemVarName = getParamNameInInstConfig(systemVarName);

                if (isCnVariable(systemVarName)) {
                    systemVarName = systemVarName.toUpperCase(Locale.ROOT);
                    cnProps.setProperty(systemVarName, systemVarValue);

                } else {
                    c.writeErrMessage(ErrorCode.ER_GLOBAL_VARIABLE,
                        "Unsupported variable '" + systemVarName + "'");
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
                    c.writeErrMessage(ErrorCode.ER_GLOBAL_VARIABLE,
                        "Unsupported variable '" + systemVarName + "'");
                    return true;
                }

            } catch (Throwable t) {
                c.writeErrMessage(ErrorCode.ER_GLOBAL_VARIABLE, "Error occurred when setting global variables");
                logger.error(t.getMessage());
                return true;
            }
        }

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            VariableConfigAccessor variableConfigAccessor = new VariableConfigAccessor();
            InstConfigAccessor instConfigAccessor = new InstConfigAccessor();
            variableConfigAccessor.setConnection(metaDbConn);
            instConfigAccessor.setConnection(metaDbConn);
            if (!GeneralUtil.isEmpty(dnVariableAssignmentList)) {
                SqlSet setStatement = new SqlSet(SqlParserPos.ZERO, dnVariableAssignmentList);
                Map<String, TGroupDataSource> instanceDataSources = new HashMap<>();
                TopologyHandler topologyHandler =
                    ExecutorContext.getContext(c.getConnectionInfo().getSchema()).getTopologyHandler();
                for (Group group : topologyHandler.getMatrix()
                    .getGroups()) {
                    String groupName = group.getName();
                    Object o = topologyHandler.get(groupName).getDataSource();
                    if (o instanceof TGroupDataSource) {
                        TGroupDataSource groupDataSource = (TGroupDataSource) o;
                        String instanceId = groupDataSource.getMasterSourceAddress();
                        instanceDataSources.putIfAbsent(instanceId, groupDataSource);
                    }
                }
                for (TGroupDataSource dataSource : instanceDataSources.values()) {
                    PreparedStatement statement =
                        dataSource.getConnection().prepareStatement(setStatement.toString());
                    statement.execute();
                }
                variableConfigAccessor.updateParamsValue(dnProps, InstIdUtil.getInstId());
                // refresh cache
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

        ActiveRoles.ActiveRoleSpec activeRoleSpec =
            ActiveRoles.ActiveRoleSpec.from(sqlNode.getRoleSpec());

        List<PolarAccountInfo> roles = sqlNode.getUsers()
            .stream()
            .map(SqlUserName::toPolarAccount)
            .map(manager::getAndCheckExactUser)
            .collect(Collectors.toList());

        c.setActiveRoles(currentUser.getRolePrivileges().checkAndGetActiveRoles(activeRoleSpec, roles));
    }

    private static class SelectResultHandler implements QueryResultHandler {

        private UserDefVarProcessingResult result;

        @Override
        public void sendUpdateResult(long affectedRows) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void sendSelectResult(ResultSet resultSet, AtomicLong outAffectedRows) throws Exception {
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

    private static boolean extraCheck(String systemVarName, String systemVarValue) {
        if (systemVarName.equalsIgnoreCase(TransactionAttribute.DRDS_TRANSACTION_POLICY)) {
            if ("2PC".equalsIgnoreCase(systemVarValue) || "FLEXIBLE".equalsIgnoreCase(systemVarValue)) {
                return true;
            }
            try {
                ITransactionPolicy.TransactionClass.valueOf(systemVarValue.toUpperCase());
            } catch (Exception e) {
                return false;
            }
            return true;
        }
        return true;
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
            if (!c.getUserDefVariables()
                .containsKey(((SqlUserDefVar) oriValue).getName().toLowerCase())) {
                c.writeErrMessage(ErrorCode.ER_WRONG_VALUE_FOR_VAR, "Variable " + key.getName()
                    + " can't be set to the value of "
                    + RelUtils.stringValue(oriValue));
                return RETURN_VALUE;
            }
        } else if (oriValue instanceof SqlSystemVar) {
            SqlSystemVar var = (SqlSystemVar) oriValue;
            if (!ServerVariables.contains(var.getName()) && !ServerVariables
                .isExtra(var.getName())) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_SYSTEM_VARIABLE, "Unknown system variable '"
                    + var.getName() + "'");
                return RETURN_VALUE;
            }
            value = c.getSysVarValue(var);
        } else if (oriValue instanceof SqlLiteral
            && ((SqlLiteral) oriValue).getTypeName() == SqlTypeName.NULL) {
            value = null;
        } else if (oriValue instanceof SqlLiteral
            && ((SqlLiteral) oriValue).getTypeName() == SqlTypeName.BOOLEAN) {
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
}
