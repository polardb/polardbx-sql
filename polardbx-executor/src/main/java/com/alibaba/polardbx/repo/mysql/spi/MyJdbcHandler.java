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

package com.alibaba.polardbx.repo.mysql.spi;

import com.alibaba.polardbx.atom.utils.LoadFileUtils;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ConnectionStats;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.convertor.ConvertorHelper;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.support.LogFormat;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.executor.Xprotocol.XRowSet;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.exception.ExecutorException;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.executor.utils.transaction.PhyOpTrxConnUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbConnectionProxy;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.group.utils.StatementStats;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.Xplan.XPlanTemplate;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectMultiDBTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectShardingKeyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.core.row.ResultSetRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.alibaba.polardbx.optimizer.statis.OperatorStatisticsExt;
import com.alibaba.polardbx.optimizer.statis.SQLRecord;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.repo.mysql.cursor.ResultSetCursor;
import com.alibaba.polardbx.rpc.XLog;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.compatible.XPreparedStatement;
import com.alibaba.polardbx.rpc.compatible.XResultSet;
import com.alibaba.polardbx.rpc.compatible.XStatement;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.rpc.result.XResultObject;
import com.alibaba.polardbx.statistics.ExecuteSQLOperation;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import com.googlecode.protobuf.format.JsonFormat;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import com.mysql.jdbc.Statement;
import com.mysql.jdbc.exceptions.MySQLQueryInterruptedException;
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jol.info.ClassLayout;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static com.alibaba.polardbx.common.TddlConstants.ANONAMOUS_DBKEY;
import static com.alibaba.polardbx.common.utils.ExceptionUtils.isMySQLIntegrityConstraintViolationException;
import static com.alibaba.polardbx.common.utils.GeneralUtil.listToMap;
import static com.alibaba.polardbx.common.utils.GeneralUtil.mapToList;
import static com.alibaba.polardbx.common.utils.GeneralUtil.prepareParam;
import static com.alibaba.polardbx.executor.utils.ExecUtils.buildDRDSTraceComment;
import static com.alibaba.polardbx.executor.utils.ExecUtils.buildDRDSTraceCommentBytes;
import static com.alibaba.polardbx.executor.utils.ExecUtils.useExplicitTransaction;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.TABLE_NAME_PARAM_INDEX;

/**
 * Created by chuanqin on 17/7/7.
 */
public class MyJdbcHandler implements GeneralQueryHandler {

    public static long INSTANCE_MEM_SIZE = ClassLayout.parseClass(MyJdbcHandler.class).instanceSize();

    private static final String UNKNOWN_COLUMN = "42S22";
    private static final Logger logger = LoggerFactory.getLogger(MyJdbcHandler.class);
    private static final int MAX_LOG_PARAM_COUNT = 500;

    private static final int LONG_ENOUGH_TIMEOUT_FOR_DDL_ON_XPROTO_CONN = 7 * 24 * 60 * 60 * 1000;

    private IConnection connection = null;
    private ResultSet resultSet = null;
    private java.sql.Statement ps = null;
    private ExecutionType executionType = null;
    private Row current = null;
    private CursorMeta cursorMeta;
    private boolean isStreaming = false;
    private String groupName = null;
    private ExecutionContext executionContext = null;
    private boolean closed = false;
    private MyRepository repo;
    private Long rowsAffect = 0L;
    private SQLRecord record;
    private int phyConnLastSocketTimeout = -1;
    protected RuntimeStatistics runtimeStat = null;
    protected ConnectionStats connectionStats = new ConnectionStats();
    protected StatementStats statementStats = new StatementStats();
    protected OperatorStatisticsExt operatorStatisticsExt = null;
    protected boolean enableTaskProfile = false;
    private XResult xResult = null;
    private boolean inTrans = false;
    private boolean phySqlExecuted = false;
    private boolean lessMy56Version = false;

    private int ddlTimeoutForTest = -1;

    public enum ExecutionType {
        PUT, GET
    }

    public MyJdbcHandler(ExecutionContext executionContext, MyRepository repo) {
        this.executionContext = executionContext;
        this.repo = repo;
        this.enableTaskProfile = ExecUtils.isSQLMetricEnabled(executionContext);
        this.runtimeStat = (RuntimeStatistics) this.executionContext.getRuntimeStatistics();
        this.inTrans = useExplicitTransaction(executionContext);
        try {
            lessMy56Version = ExecutorContext.getContext(
                executionContext.getSchemaName()).getStorageInfoManager().isLessMy56Version();
        } catch (Throwable ignored) {
        }
        FailPoint.inject(FailPointKey.FP_PHYSICAL_DDL_TIMEOUT, (k, v) -> {
            try {
                ddlTimeoutForTest = Integer.parseInt(v);
            } catch (Throwable ignored) {
            }
        });
    }

    private boolean isForceStream() {
        Map<String, Object> extraCmd = executionContext.getExtraCmds();
        boolean forceStreaming = false;
        if (extraCmd.containsKey(ConnectionProperties.CHOOSE_STREAMING)) {
            forceStreaming = executionContext.getParamManager().getBoolean(ConnectionParams.CHOOSE_STREAMING);
        } else {
            // fetchSize==MIN_VALUE，enable stream mode force
            if (Integer.MIN_VALUE == executionContext.getParamManager().getLong(ConnectionParams.FETCH_SIZE)) {
                forceStreaming = true;
            }
        }

        return forceStreaming;
    }

    protected Long getLastInsertIdFromResultSet(ResultSet rs) throws SQLException {
        long idVal = 0;

        Object lastInsertId = rs.getObject(1);
        if (lastInsertId instanceof Long) {
            idVal = (Long) lastInsertId;
        } else if (lastInsertId instanceof BigInteger) {
            // 当lastInsertId的值为负数的，JDBC直接强制将其当作 BIGINT UNSIGNED,
            // 并返回的对像BigInteger(该对像的符号值被置为正数),
            // 代码细节请参考JDBC：com.mysql.jdbc.StatementImpl.getGeneratedKeysInternal()
            // JDBC驱动返回的generated列的类型是BigInteger
            // 因为tddl的lastInsertId使用 Long 值来存，因而无法表 BIGINT UNSIGNED 的值，
            // 这里直接以负数形式返回，这跟JDBC的行为不一致
            idVal = ((BigInteger) lastInsertId).longValue();
        } else if (lastInsertId instanceof UInt64) {
            idVal = ((UInt64) lastInsertId).longValue();
        }
        return idVal;
    }

    void recordSqlLog(BytesSql bytesSql, long startTime, long nanoStartTime, long sqlCostTime,
                      long getConnectionCreateCostNano, long getConnectionWaitCostNano,
                      Map<Integer, ParameterContext> param, String groupName, ITransaction.RW rw) {
        long nanoTime = System.nanoTime() - nanoStartTime;
        if (bytesSql == null) {
            return;
        }
        if (rw == ITransaction.RW.WRITE) {
            MatrixStatistics.addWriteTimeCost(executionContext.getAppName(),
                this.groupName,
                getCurrentDbkey(rw),
                nanoTime / 1000);
            MatrixStatistics.addPhysicalWriteRequestPerAtom(executionContext.getAppName(),
                this.groupName,
                getCurrentDbkey(rw),
                1L,
                bytesSql.size());
        } else {
            MatrixStatistics.addReadTimeCost(executionContext.getAppName(),
                this.groupName,
                getCurrentDbkey(rw),
                nanoTime / 1000);
            MatrixStatistics.addPhysicalReadRequestPerAtom(executionContext.getAppName(),
                this.groupName,
                getCurrentDbkey(rw),
                1L,
                bytesSql.size());
        }
        long time = System.currentTimeMillis() - startTime;
        if (SQLRecorderLogger.physicalSlowLogger.isInfoEnabled()) {
            try {
                long thresold = this.executionContext.getPhysicalRecorder().getSlowSqlTime();

                // Use slow sql time of db level first
                if (executionContext.getExtraCmds().containsKey(ConnectionProperties.SLOW_SQL_TIME)) {
                    thresold = executionContext.getParamManager().getLong(ConnectionParams.SLOW_SQL_TIME);
                }
                if (thresold > 0L && time > thresold) {

                    boolean isBackfillTask = (executionContext.getDdlJobId() != null
                        && executionContext.getDdlJobId() > 0);
                    String sql = StringUtils.EMPTY;
                    if (!isBackfillTask) {
                        sql = bytesSql.display();
                    }
                    long length = executionContext.getPhysicalRecorder().getMaxSizeThreshold();

                    long remainingSize = 0;
                    if (sql.length() < length) {
                        remainingSize = length - sql.length();
                    }
                    StringBuilder paramsBuffer = new StringBuilder();

                    if (remainingSize <= 0 || isBackfillTask) {
                        //ignore the backfill physical slow sql's parameters
                        if (GeneralUtil.isNotEmpty(param)) {
                            paramsBuffer.append("{1=");
                            paramsBuffer.append(param.get(1));
                            paramsBuffer.append(",...,}");
                        }
                    } else {
                        paramsBuffer.append("{");
                        if (GeneralUtil.isNotEmpty(param)) {
                            for (int i = 1; i <= param.size(); i++) {
                                String val = String.valueOf(param.get(i));
                                if (val.length() + paramsBuffer.length() <= remainingSize) {
                                    paramsBuffer.append(i);
                                    paramsBuffer.append("=");
                                    paramsBuffer.append(val);
                                    paramsBuffer.append(",");
                                } else {
                                    paramsBuffer.append("...");
                                    break;
                                }
                            }
                        }
                        paramsBuffer.append("}");
                    }

                    String traceId = executionContext.getTraceId();

                    executionContext.getStats().physicalSlowRequest++;

                    String currentDbKey = getCurrentDbkey(rw);

                    if (sql.length() > length) {
                        StringBuilder newSql = new StringBuilder((int) length + 3);
                        newSql.append(sql, 0, (int) length);
                        newSql.append("...");
                        sql = newSql.toString();
                    }
                    String formatSql = LogFormat.formatLog(sql);
                    sqlCostTime = (sqlCostTime == -1) ? -1 : sqlCostTime / 1000;
                    getConnectionWaitCostNano =
                        (getConnectionWaitCostNano == -1) ? -1 : getConnectionWaitCostNano / 1000;
                    getConnectionCreateCostNano =
                        (getConnectionCreateCostNano == -1) ? -1 : getConnectionCreateCostNano / 1000;

                    this.record = new SQLRecord();
                    record.schema = executionContext.getSchemaName();
                    record.statement = formatSql;
                    record.startTime = startTime;
                    record.executeTime = time;
                    record.sqlTime = sqlCostTime;
                    record.getLockConnectionTime = getConnectionWaitCostNano;
                    record.createConnectionTime = getConnectionCreateCostNano;
                    record.dataNode = groupName;
                    record.dbKey = currentDbKey;
                    record.affectRow = rowsAffect;
                    record.physical = true;
                    record.traceId = traceId;

                    if (executionContext.getPhysicalRecorder().check(time)) {
                        executionContext.getPhysicalRecorder().recordSql(record);
                    }
                    SQLRecorderLogger.physicalSlowLogger.info(SQLRecorderLogger.physicalLogFormat.format(new Object[] {
                        formatSql, this.groupName, currentDbKey, time, sqlCostTime, getConnectionWaitCostNano,
                        getConnectionCreateCostNano, paramsBuffer, traceId}));
                }
            } catch (Throwable e) {
                logger.error("Error occurs when record SQL", e);
            }
        }
    }

    /**
     * <pre>
     * 兼容mysql5.6的时间精度,去掉毫秒.针对部分单表下推的sql在优化器层无法处理,只能在执行层做一次时间精度处理了
     * </pre>
     */
    public void convertParameters(Map<Integer, ParameterContext> params, ParamManager extraCmds) {
        if (extraCmds.getBoolean(ConnectionParams.ENABLE_COMPATIBLE_DATETIME_ROUNDDOWN)
            || extraCmds.getBoolean(ConnectionParams.ENABLE_COMPATIBLE_TIMESTAMP_ROUNDDOWN)) {
            for (ParameterContext paramContext : params.values()) {
                Object value = paramContext.getValue();
                if (value instanceof Date) {
                    long mills = ((Date) value).getTime();
                    if (mills % 1000 > 0) {
                        // 去掉精度
                        paramContext.setValue(ConvertorHelper.longToDate.convert(((mills / 1000) * 1000),
                            value.getClass()));
                    }
                }
            }
        }
        return;
    }

    @Override
    public Cursor getResultCursor() {
        if (xResult != null) {
            throw GeneralUtil.nestedException("Not supported.");
        }

        if (executionType == ExecutionType.PUT) {
            throw new IllegalAccessError("impossible");
        } else if (executionType == ExecutionType.GET) {
            // get的时候只会有一个结果集
            ResultSet rs = resultSet;
            return new ResultSetCursor(rs, this);
        } else {
            return null;
        }
    }

    protected void setStreamingForStatement(java.sql.Statement stat) throws SQLException {
        stat.setFetchSize(Integer.MIN_VALUE);
    }

    protected void setContext(CursorMeta meta, boolean isStreaming, String group) {
        if (cursorMeta == null) {
            cursorMeta = meta;
        }

        if (isStreaming != this.isStreaming) {
            this.isStreaming = isStreaming;
        }

        if (groupName == null) {
            this.groupName = group;
        }
    }

    @Override
    public void close() throws SQLException {
        if (this.closed) {
            return;
        }

        synchronized (this) {
            if (this.closed) {
                return;
            }
            this.closed = true;
        }
        long startCloseJdbcRsNano = System.nanoTime();
        boolean killStreaming = executionContext.getParamManager().getBoolean(
            ConnectionParams.KILL_CLOSE_STREAM);
        try {
            if (xResult != null) {
                if (!killStreaming) {
                    while (xResult.next() != null) {
                        ;
                    }
                }
                OptimizerAlertUtil.xplanAlert(executionContext, xResult);
                xResult.close();
                xResult = null;
            }

        } finally {
            try {
                if (ps != null) {
                    ps.setFetchSize(0);
                    ps.close();
                    ps = null;
                } else {
                    ps = null;
                }
            } catch (Exception e) {
                if (killStreaming && executionContext.isAutoCommit() && !inTrans && exceptionIsInterrupt(e)) {
                    logger.info("failed to close statement, ", e);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e, "error when close result");
                }
            } finally {
                if (this.connection != null) {
                    this.executionContext.getTransaction().tryClose(this.connection, this.groupName);
                }
            }
        }
        if (statementStats != null) {
            if (enableTaskProfile) {
                statementStats.addCloseResultSetNano(System.nanoTime() - startCloseJdbcRsNano);
            }
            collectStatementStatsForClose();
        }
        executionType = null;
    }

    private boolean exceptionIsInterrupt(Exception e) {
        return e instanceof MySQLQueryInterruptedException || TStringUtil.contains(e.getClass().getName(),
            "MySQLQueryInterruptedException");
    }

    @Override
    public Row next() throws SQLException {
        // Check XResult first.
        if (xResult != null) {
            try {
                final XResultObject res = xResult.next();
                if (null == res) {
                    if (this.record != null) {
                        this.record.affectRow = rowsAffect;
                    }
                    if (this.operatorStatisticsExt != null) {
                        this.operatorStatisticsExt.addRowCount(rowsAffect);
                    }
                    return null;
                }
                rowsAffect++;
                return new XRowSet(xResult, cursorMeta, xResult.getMetaData(), res.getRow(), true);
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        }

        checkInitedInRsNext();
        try {
            if (resultSet.isClosed()) {
                return null;
            }
        } catch (Exception ex) {
            if (this.record != null) {
                this.record.affectRow = rowsAffect;
            }
            if (this.operatorStatisticsExt != null) {
                this.operatorStatisticsExt.addRowCount(rowsAffect);
            }
            return null;
        }
        if (resultSet.next()) {
            current = new ResultSetRow(cursorMeta, resultSet);
            rowsAffect++;

        } else {
            if (this.record != null) {
                this.record.affectRow = rowsAffect;
            }
            if (this.operatorStatisticsExt != null) {
                this.operatorStatisticsExt.addRowCount(rowsAffect);
            }
            current = null;
        }

        return current;
    }

    protected PreparedStatement prepareStatement(
        BytesSql bytesSql, byte[] sqlBytesPrefix, IConnection conn, ExecutionContext executionContext,
        List<ParameterContext> parameterContexts, Map<Integer, ParameterContext> param, boolean isInsertOrUpdate,
        boolean isDDL,
        BaseQueryOperation rel)
        throws SQLException {
        long startInitStmtEnvNano = System.nanoTime();
        if (this.ps != null) {
            throw new IllegalStateException("上一个请求还未执行完毕");
        }
        if (conn == null) {
            throw new IllegalStateException("should not be here");
        }

        Executor socketTimeoutExecutor = TGroupDirectConnection.socketTimeoutExecutor;
        Long socketTimeout = executionContext.getParamManager().getLong(ConnectionParams.SOCKET_TIMEOUT);
        if (isDDL) {
            phyConnLastSocketTimeout = conn.getNetworkTimeout();
            int ddlTimeout = getDdlTimeout(conn, executionContext);
            conn.setNetworkTimeout(socketTimeoutExecutor, ddlTimeout);
        } else if (socketTimeout >= 0) {
            phyConnLastSocketTimeout = conn.getNetworkTimeout();
            conn.setNetworkTimeout(socketTimeoutExecutor, socketTimeout.intValue());
        } else if (executionContext.getSocketTimeout() >= 0) {
            phyConnLastSocketTimeout = conn.getNetworkTimeout();
            conn.setNetworkTimeout(socketTimeoutExecutor, executionContext.getSocketTimeout());
        }

        conn.setServerVariables(executionContext.getServerVariables());

        int autoGeneratedKeys = executionContext.getAutoGeneratedKeys();
        if (isInsertOrUpdate) {
            autoGeneratedKeys = java.sql.Statement.RETURN_GENERATED_KEYS;
        }

        int[] columnIndexes = executionContext.getColumnIndexes();
        String[] columnNames = executionContext.getColumnNames();
        int resultSetType = executionContext.getResultSetType();
        int resultSetConcurrency = executionContext.getResultSetConcurrency();
        int resultSetHoldability = executionContext.getResultSetHoldability();
        // 只处理设定过的txIsolation
        if (executionContext.getTxIsolation() >= 0) {
            conn.setTransactionIsolation(executionContext.getTxIsolation());
        }
        // 只处理设置过的编码
        if (executionContext.getEncoding() != null) {
            conn.setEncoding(executionContext.getEncoding());
        }
        // 只处理设置过的sqlMode
        if (executionContext.getSqlMode() != null) {
            conn.setSqlMode(executionContext.getSqlMode());
        }

        conn.setStressTestValid(executionContext.isStressTestValid());

        if (conn.isBytesSqlSupported()) {
            // bytes sql && raw string path
            ps = conn.prepareStatement(bytesSql, sqlBytesPrefix);
        } else {
            if (parameterContexts == null && param != null) {
                parameterContexts = mapToList(param);
            }
            String sql;
            if (conn.isWrapperFor(XConnection.class)) {
                // x-conn not support rawstring get sql without hint
                sql = bytesSql.toString(parameterContexts);
            } else {
                sql = new String(sqlBytesPrefix) + bytesSql.toString(parameterContexts);
            }

            if (autoGeneratedKeys != -1) {
                ps = conn.prepareStatement(sql, autoGeneratedKeys);
            } else if (columnIndexes != null) {
                ps = conn.prepareStatement(sql, columnIndexes);
            } else if (columnNames != null) {
                ps = conn.prepareStatement(sql, columnNames);
            } else if (resultSetType != -1 && resultSetConcurrency != -1 && resultSetHoldability != -1) {
                ps = conn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
            } else if (resultSetType != -1 && resultSetConcurrency != -1) {
                ps = conn.prepareStatement(sql, resultSetType, resultSetConcurrency);
            } else {
                ps = conn.prepareStatement(sql);
            }

            if (ps.isWrapperFor(XPreparedStatement.class)) {
                // x-prepare not support rawstring should set hint
                ps.unwrap(XPreparedStatement.class).setHint(sqlBytesPrefix);
            }
        }

        final boolean noDigest =
            (rel instanceof PhyTableOperation && ((PhyTableOperation) rel).getTableNames().size() != 1) ||
                (executionContext.getGroupHint() != null && !executionContext.getGroupHint().isEmpty()) ||
                (executionContext.getExplain() != null
                    && executionContext.getExplain().explainMode == ExplainResult.ExplainMode.EXECUTE);
        // denied if DDL or function not support or contain raw string or using returning
        if ((noDigest || isDDL || !rel.isSupportGalaxyPrepare() || (null == parameterContexts ?
            bytesSql.containRawString(param) : bytesSql.containRawString(parameterContexts))
            || executionContext.useReturning()) && ps.isWrapperFor(XPreparedStatement.class)) {
            ps.unwrap(XPreparedStatement.class).setUseGalaxyPrepare(false);
        }

        if (executionContext.getLocalInfileInputStream() != null) {
            LoadFileUtils.setLocalInfileInputStream(ps, executionContext.getLocalInfileInputStream());
        }
        this.statementStats.addCreateAndInitStmtNano(System.nanoTime() - startInitStmtEnvNano);
        return (PreparedStatement) ps;
    }

    private int getDdlTimeout(IConnection conn, ExecutionContext executionContext) throws SQLException {
        // Long enough (7-day) for X-Protocol or never (0) for JDBC.
        int ddlTimeout = conn.isWrapperFor(XConnection.class) ? LONG_ENOUGH_TIMEOUT_FOR_DDL_ON_XPROTO_CONN : 0;

        if (ddlTimeoutForTest > 0) {
            ddlTimeout = ddlTimeoutForTest;
        }

        return ddlTimeout;
    }

    protected java.sql.Statement createStatement(IConnection conn, ExecutionContext executionContext, boolean isDDL)
        throws SQLException {
        long startInitStmtEnvNano = System.nanoTime();
        if (this.ps != null) {
            throw new IllegalStateException("上一个请求还未执行完毕");
        }
        if (conn == null) {
            throw new IllegalStateException("should not be here");
        }

        Executor socketTimeoutExecutor = TGroupDirectConnection.socketTimeoutExecutor;
        Long socketTimeout = executionContext.getParamManager().getLong(ConnectionParams.SOCKET_TIMEOUT);
        if (isDDL) {
            phyConnLastSocketTimeout = conn.getNetworkTimeout();
            int ddlTimeout = getDdlTimeout(conn, executionContext);
            conn.setNetworkTimeout(socketTimeoutExecutor, ddlTimeout);
        } else if (socketTimeout >= 0) {
            phyConnLastSocketTimeout = conn.getNetworkTimeout();
            conn.setNetworkTimeout(socketTimeoutExecutor, socketTimeout.intValue());
        } else if (executionContext.getSocketTimeout() >= 0) {
            phyConnLastSocketTimeout = conn.getNetworkTimeout();
            conn.setNetworkTimeout(socketTimeoutExecutor, executionContext.getSocketTimeout());
        }

        int resultSetType = executionContext.getResultSetType();
        int resultSetConcurrency = executionContext.getResultSetConcurrency();
        int resultSetHoldability = executionContext.getResultSetHoldability();
        // 只处理设定过的txIsolation
        if (executionContext.getTxIsolation() >= 0) {
            conn.setTransactionIsolation(executionContext.getTxIsolation());
        }
        // 只处理设置过的编码
        if (executionContext.getEncoding() != null) {
            conn.setEncoding(executionContext.getEncoding());
        }
        // 只处理设置过的sqlMode
        if (executionContext.getSqlMode() != null) {
            conn.setSqlMode(executionContext.getSqlMode());
        }

        conn.setServerVariables(executionContext.getServerVariables());

        conn.setStressTestValid(executionContext.isStressTestValid());
        java.sql.Statement ps;
        if (resultSetType != -1 && resultSetConcurrency != -1 && resultSetHoldability != -1) {
            ps = conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        } else if (resultSetType != -1 && resultSetConcurrency != -1) {
            ps = conn.createStatement(resultSetType, resultSetConcurrency);
        } else {
            ps = conn.createStatement();
        }

        if (executionContext.getLocalInfileInputStream() != null) {
            LoadFileUtils.setLocalInfileInputStream(ps, executionContext.getLocalInfileInputStream());
        }
        this.ps = ps;
        this.statementStats.addCreateAndInitStmtNano(System.nanoTime() - startInitStmtEnvNano);
        return ps;
    }

    /**
     * 格式不能随意变化, 部分自定义格式的依赖这个结果
     */
    protected String buildSql(String sql, ExecutionContext executionContext) {
        StringBuilder append = new StringBuilder();

        String trace = buildDRDSTraceComment(executionContext);
        if (trace != null) {
            append.append(trace);
        }

        if (executionContext.getGroupHint() != null) {
            // 如果有group hint，传递一下hint
            append.append(executionContext.getGroupHint());
        }

        String ap = append.toString();
        if (StringUtils.isNotEmpty(ap)) {
            sql = ap + sql;
        }
        if (executionContext.getExplain() != null
            && executionContext.getExplain().explainMode == ExplainResult.ExplainMode.EXECUTE) {
            sql = "explain " + sql;
        }
        return sql;
    }

    @Override
    public Row getCurrent() {
        if (xResult != null) {
            final XResultObject res = xResult.current();
            if (null == res) {
                return null;
            }
            try {
                return new XRowSet(xResult, cursorMeta, xResult.getMetaData(), res.getRow(), true);
            } catch (SQLException e) {
                throw GeneralUtil.nestedException(e);
            }
        }
        return current;
    }

    private void checkInitedInRsNext() {
        if (!isInited()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "not inited");
        }
    }

    @Override
    public boolean isInited() {
        return xResult != null || resultSet != null;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    public ResultSet getResultSet() {
        if (xResult != null) {
            return new XResultSet(xResult);
        }
        return this.resultSet;
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    protected void resetPhyConnSocketTimeout() {
        if (phyConnLastSocketTimeout != -1 && connection != null) {
            try {
                connection.setNetworkTimeout(TGroupDirectConnection.socketTimeoutExecutor, phyConnLastSocketTimeout);
                phyConnLastSocketTimeout = -1;
            } catch (Throwable ex) {
                logger.warn("Reset conn socketTimeout failed, lastSocketTimeout is " + phyConnLastSocketTimeout, ex);
            }
        }
    }

    private boolean executeQueryX(XPlanTemplate XTemplate,
                                  List<String> tableNames,
                                  List<List<String>> allPhyTableNames,
                                  Map<Integer, ParameterContext> params,
                                  BaseQueryOperation phyTblOp,
                                  BytesSql nativeSql,
                                  boolean compactMeta) {
        long startPrepStmtEnvNano = 0;
        if (enableTaskProfile) {
            startPrepStmtEnvNano = ThreadCpuStatUtil.getThreadCpuTimeNano();
        }

        // Get DB info.
        final TGroupDataSource group = (TGroupDataSource) repo.getDataSource(groupName);
        final String dbName = group.getAtomDataSources().get(0).getDsConfHandle().getRunTimeConf().getDbName();
        ITransaction.RW rw = ITransaction.RW.READ; // Now XPlan only support pure read without lock.
        final byte[] trace = buildSqlPreFix(executionContext);
        if (executionContext.getGroupHint() != null && !executionContext.getGroupHint().isEmpty()) {
            return false; // Not support.
        }
        if (executionContext.getExplain() != null
            && executionContext.getExplain().explainMode == ExplainResult.ExplainMode.EXECUTE) {
            return false; // Not support.
        }

        // Generate final plan.
        boolean isPhyOp = phyTblOp instanceof PhyTableOperation;
        final PolarxExecPlan.ExecPlan.Builder execPlan =
            XTemplate.getXPlan(dbName, tableNames, params, executionContext);
        if (null == execPlan) {
            return false; // Forbidden by some reason.
        }
        final JsonFormat format = new JsonFormat();

        if (logger.isDebugEnabled()) {
            logger.debug("Native XPlan:" + format.printToString(execPlan.build()).replaceAll("\n", " "));
        }

        // Connect and exec.
        long startTime = System.currentTimeMillis();
        long nanoStartTime = System.nanoTime();
        long sqlCostTime = -1;
        long createCostTime = -1;
        long waitCostTime = -1;

        Long grpConnId = 0L;
        try {
            executionType = ExecutionType.GET;
            grpConnId =
                PhyTableOperationUtil.computeGrpConnIdByPhyOp(phyTblOp, groupName, allPhyTableNames, executionContext);
            connection = getPhyConnection(executionContext.getTransaction(), rw, groupName, grpConnId, null);
            final XConnection xConnection = connection.unwrap(XConnection.class);

            //
            // Prepare for execution.
            //

            final long startInitStmtEnvNano = System.nanoTime();

            // Check last.
            final XResult last = xConnection.getLastUserRequest();
            if (xResult != null || (last != null && !last.isGoodAndDone() && !last.isGoodIgnorable())) {
                try {
                    XResult probe = last;
                    while (probe != null) {
                        logger.warn(
                            xConnection + " last req: " + probe.getSql().toString() + " status:" + probe.getStatus()
                                .toString());
                        probe = probe.getPrevious();
                    }
                } catch (Throwable e) {
                    XLog.XLogLogger.error(e);
                }
                throw new IllegalStateException("Start XPlan with previous unfinished.");
            }

            // Reset timeout.
            Executor socketTimeoutExecutor = TGroupDirectConnection.socketTimeoutExecutor;
            Long socketTimeout = executionContext.getParamManager().getLong(ConnectionParams.SOCKET_TIMEOUT);
            if (socketTimeout >= 0) {// 优先自定义参数
                phyConnLastSocketTimeout = connection.getNetworkTimeout();
                connection.setNetworkTimeout(socketTimeoutExecutor, socketTimeout.intValue());
            } else if (executionContext.getSocketTimeout() >= 0) {
                phyConnLastSocketTimeout = connection.getNetworkTimeout();
                connection.setNetworkTimeout(socketTimeoutExecutor, executionContext.getSocketTimeout());
            }

            // Session variable.
            connection.setServerVariables(executionContext.getServerVariables());

            // Advanced setting.
            int[] columnIndexes = executionContext.getColumnIndexes();
            String[] columnNames = executionContext.getColumnNames();
            int resultSetType = executionContext.getResultSetType();
            int resultSetConcurrency = executionContext.getResultSetConcurrency();
            int resultSetHoldability = executionContext.getResultSetHoldability();

            if (columnIndexes != null || columnNames != null || resultSetType != -1 || resultSetConcurrency != -1
                || resultSetHoldability != -1) {
                throw new NotSupportException("Advanced statement setting not supported.");
            }

            // Isolation level.
            if (executionContext.getTxIsolation() >= 0) {
                connection.setTransactionIsolation(executionContext.getTxIsolation());
            }
            // Encoding.
            if (executionContext.getEncoding() != null) {
                connection.setEncoding(executionContext.getEncoding());
            }
            // Sql mode set in session variables, so this can remove.
            if (executionContext.getSqlMode() != null) {
                connection.setSqlMode(executionContext.getSqlMode());
            }

            connection.setStressTestValid(executionContext.isStressTestValid());

            if (executionContext.getLocalInfileInputStream() != null) {
                throw new NotSupportException("Local file input stream not supported.");
            }
            if (isStreaming) {
                xConnection.setStreamMode(true);
            }
            this.statementStats.addCreateAndInitStmtNano(System.nanoTime() - startInitStmtEnvNano);
            ps = null;

            //
            // Prepare done. Start execution.
            //

            executionContext.getStats().physicalRequest.incrementAndGet();
            if (enableTaskProfile) {
                this.statementStats.addPrepStmtEnvNano(ThreadCpuStatUtil.getThreadCpuTimeNano() - startPrepStmtEnvNano);
            }

            final long startTimeNano = System.nanoTime();
            xConnection.setTraceId(executionContext.getTraceId());
            connection.flushUnsent(); // Caution: This is important when use deferred sql.
            xConnection.getSession().setChunkResult(false);
            if (compactMeta) {
                execPlan.setCompactMetadata(true);
            }
            // Add feedback flag directly in execPlan.
            execPlan.setFeedBack(true);
            execPlan.setCapabilities(executionContext.getCapabilityFlags());
            phySqlExecuted = true;
            xResult = xConnection.execQuery(execPlan, nativeSql, trace);
            // Note: We skip here, so physical_slow never get slow of plan exec, but XRequest record this case.
            // xResult.getMetaData(); // Compatible with original time record.
            this.resultSet = null;

            // 这里统计的是mysql的执行时间
            long timeCostNano = System.nanoTime() - startTimeNano;
            sqlCostTime = (timeCostNano / 1000);
            if (enableTaskProfile) {
                this.statementStats.addExecuteStmtNano(timeCostNano);
            }
            executionContext.getStats().recordPhysicalTimeCost(sqlCostTime);

            if (executionContext.isEnableTrace()) {
                String sqlPrefix = "/*PolarDB-X Connection*/";
                String sql = sqlPrefix + format.printToString(execPlan.build());

                String currentDbKey = getCurrentDbkey(rw);
                ExecuteSQLOperation op =
                    new ExecuteSQLOperation(this.groupName, currentDbKey, sql, startTimeNano / 1000_000);
                op.setThreadName(Thread.currentThread().getName());
                op.setTimeCost(System.currentTimeMillis() - startTime);
                op.setRowsCount((long) xResult.getFetchCount());
                op.setGetConnectionTimeCost((startTimeNano - nanoStartTime) / (float) (1000 * 1000));
                op.setGrpConnId(grpConnId);
                op.setTraceId(executionContext.getTraceId());
                executionContext.getTracer().trace(op);
            }
        } catch (Throwable e) {
            logger.error(e);
            if (xResult != null) {
                xResult.close();
            }
            xResult = null;
            throw GeneralUtil.nestedException(e);
        } finally {
            executionContext.getTransaction().clearTrxContext();
            collectStatementStats();
        }

        return true;
    }

    private boolean isXDataSources() {
        try {
            if (groupName.equalsIgnoreCase(MetaDbDataSource.DEFAULT_META_DB_GROUP_NAME)) {
                return MetaDbDataSource.getInstance().getDataSource().isWrapperFor(XDataSource.class);
            }
            TGroupDataSource dataSource = repo.getDataSource(groupName);
            return dataSource != null && dataSource.isXDataSource();
        } catch (Exception ignore) {
            return false;
        }
    }

    private boolean executeQueryX(CursorMeta meta, BaseQueryOperation queryOperation,
                                  Pair<String, Map<Integer, ParameterContext>> dbIndexAndParam,
                                  List<List<String>> allPhyTableNames) {
        // Tnx check.
        if (!isXDataSources() || !XConnectionManager.getInstance().isEnableXplan()) {
            return false;
        }

        final XPlanTemplate XTemplate = queryOperation.getXTemplate();
        if (null == XTemplate) {
            return false;
        }

        //xplan don't support cross-schema query
        if (queryOperation instanceof DirectMultiDBTableOperation) {
            return false;
        }

        // Query with XPlan.
        final List<String> tableNames;
        if (queryOperation instanceof SingleTableOperation) {
            final SingleTableOperation singleTableOperation = (SingleTableOperation) queryOperation;

            String phyTableName = null;
            if (1 == XTemplate.getTableNames().size()) {
                // Find table name index.
                List<Integer> paramIdx = singleTableOperation.getParamIndex();
                boolean found = false;
                int tblNameIdx = 1;
                for (int i : paramIdx) {
                    if (i == TABLE_NAME_PARAM_INDEX) {
                        found = true;
                        break;
                    }
                    ++tblNameIdx;
                }
                if (!found) {
                    throw GeneralUtil.nestedException("Table name not found.");
                }
                phyTableName = (String) dbIndexAndParam.getValue().get(tblNameIdx).getValue();
                if (!phyTableName.isEmpty() && '`' == phyTableName.charAt(0) && '`' == phyTableName
                    .charAt(phyTableName.length() - 1)) {
                    phyTableName = phyTableName.substring(1, phyTableName.length() - 1);
                }
            } else if (XTemplate.getTableNames().size() > 1) {
                throw GeneralUtil.nestedException("Incorrect table name number.");
            }

            tableNames = null == phyTableName ? Collections.emptyList() : ImmutableList.of(phyTableName);
        } else if (queryOperation instanceof DirectTableOperation) {
            final DirectTableOperation directTableOperation = (DirectTableOperation) queryOperation;
            tableNames = directTableOperation.getTableNames();
        } else if (queryOperation instanceof DirectShardingKeyTableOperation) {
            tableNames = Collections.singletonList(executionContext.getDbIndexAndTableName().getValue());
        } else {
            tableNames = null;
        }

        if (tableNames != null) {
            // XPlan have individual parameter mapper.
            final Map<Integer, ParameterContext> params =
                null == executionContext.getParams() ? null : executionContext.getParams().getCurrentParameter();
            final boolean compactMeta =
                null == executionContext.getGroupHint() && null == executionContext.getExplain();
            return executeQueryX(XTemplate, tableNames, allPhyTableNames, params, queryOperation,
                queryOperation.getBytesSql(), compactMeta);
        }
        return false;
    }

    @Override
    public void executeQuery(CursorMeta meta, BaseQueryOperation queryOperation) throws SQLException {
        if (queryOperation instanceof PhyTableOperation
            && ((PhyTableOperation) queryOperation).getPhyOperationBuilder() != null) {
            this.executeQuery(meta, (PhyTableOperation) queryOperation);
            return;
        }
        long startPrepStmtEnvNano = 0;
        if (enableTaskProfile) {
            startPrepStmtEnvNano = ThreadCpuStatUtil.getThreadCpuTimeNano();
        }

        List<List<String>> phyTableNames = new ArrayList<>();
        Pair<String, Map<Integer, ParameterContext>> dbIndexAndParam =
            fetchDbIndexAndParams(queryOperation, phyTableNames);
        this.groupName = dbIndexAndParam.getKey();
        setContext(meta, isForceStream(), dbIndexAndParam.getKey());
        if (executionContext.getParams() != null) {
            // 查询语句不支持batch模式
            if (executionContext.getParams().isBatch()) {
                throw new ExecutorException("batch not supported query sql");
            }
        }

        if (executeQueryX(meta, queryOperation, dbIndexAndParam, phyTableNames)) {
            return;
        }

        SqlAndParam sqlAndParam = new SqlAndParam();
        ITransaction.RW rw = ITransaction.RW.READ;
        BytesSql bytesSql = queryOperation.getBytesSql();
        if (bytesSql != null) {
            //sqlAndParam.sql = nativeSql;
            if (queryOperation instanceof BaseTableOperation && ((BaseTableOperation) queryOperation).isForUpdate()) {
                rw = ITransaction.RW.WRITE;
            }
            if (dbIndexAndParam.getValue() != null) {
                sqlAndParam.param = dbIndexAndParam.getValue();
            } else {
                sqlAndParam.param = new HashMap<>();
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Native SQL:" + bytesSql.display().replaceAll("\n", " "));
                StringBuilder builder = new StringBuilder();
                if (dbIndexAndParam.getValue() != null) {
                    for (ParameterContext c : dbIndexAndParam.getValue().values()) {
                        builder.append(" Index ").append(c.getArgs()[0]).append(" Value:");
                        builder.append(c.getValue());
                    }
                    logger.debug("Param :" + builder.toString());
                }
            }
        }
        long startTime = System.currentTimeMillis();
        long nanoStartTime = System.nanoTime();
        long sqlCostTime = -1, createCostTime = -1, waitCostTime = -1;
        Long grpConnId = 0L;
        try {
            executionType = ExecutionType.GET;
            grpConnId = PhyTableOperationUtil.computeGrpConnIdByPhyOp(queryOperation, groupName, phyTableNames,
                executionContext);
            connection = getPhyConnection(executionContext.getTransaction(), rw, groupName, grpConnId, null);
            byte[] sqlBytesPrefix = buildSqlPreFix(executionContext);
            if (sqlAndParam.param.isEmpty()) {
                ps = createStatement(connection, executionContext, false);
            } else {
                ps = prepareStatement(bytesSql, sqlBytesPrefix, connection, executionContext, null, sqlAndParam.param,
                    false, false, queryOperation);
            }
            if (isStreaming) {
                // 当prev的时候 不能设置
                setStreamingForStatement(ps);
            }

            Map<Integer, ParameterContext> map = sqlAndParam.param;
            List<ParameterContext> parameterContexts = null;
            parameterContexts = handleParamsMap(map);
            ResultSet rs;

            executionContext.getStats().physicalRequest.incrementAndGet();
            if (enableTaskProfile) {
                this.statementStats.addPrepStmtEnvNano(ThreadCpuStatUtil.getThreadCpuTimeNano() - startPrepStmtEnvNano);
            }

            long startTimeNano = System.nanoTime();
            ResultSet realResultSet;
            if (sqlAndParam.param.isEmpty()) {
                if (ps.isWrapperFor(XStatement.class)) {
                    final XStatement xStatement = ps.unwrap(XStatement.class);
                    final boolean compactMeta = (queryOperation instanceof SingleTableOperation
                        || queryOperation instanceof DirectTableOperation)
                        && null == executionContext.getGroupHint() && null == executionContext.getExplain();
                    final XConnection xConnection = xStatement.getConnection().unwrap(XConnection.class);
                    xConnection.setTraceId(executionContext.getTraceId());
                    xConnection.setCompactMetadata(compactMeta);
                    // Add feedback here by set flag in XConnection.
                    xConnection.setWithFeedback(true);
                    // set client JDBC capability
                    xConnection.setCapabilities(executionContext.getCapabilityFlags());
                    connection.flushUnsent(); // Caution: This is important when use deferred sql.
                    xConnection.getSession().setChunkResult(false);
                    final boolean noDigest =
                        (executionContext.getGroupHint() != null && !executionContext.getGroupHint().isEmpty()) ||
                            (executionContext.getExplain() != null
                                && executionContext.getExplain().explainMode == ExplainResult.ExplainMode.EXECUTE);
                    phySqlExecuted = true;
                    xResult = xStatement.executeQueryX(bytesSql, sqlBytesPrefix,
                        noDigest ? null : queryOperation.getSqlDigest());
                    xResult.getMetaData(); // Compatible with original time record.
                    rs = null;
                } else {
                    throw new AssertionError("unreachable");
//                    phySqlExecuted = true;
//                    ps.execute(bytesSql.toString(Collections.emptyList()));
//                    realResultSet = ps.getResultSet();
//                    rs = new ResultSetWrapper(realResultSet, this);
                }
            } else {
                if (ps.isWrapperFor(XPreparedStatement.class)) {
                    final XPreparedStatement xPreparedStatement = ps.unwrap(XPreparedStatement.class);
                    final boolean compactMeta = (queryOperation instanceof SingleTableOperation
                        || queryOperation instanceof DirectTableOperation)
                        && null == executionContext.getGroupHint() && null == executionContext.getExplain();
                    final XConnection xConnection = xPreparedStatement.getConnection().unwrap(XConnection.class);
                    xConnection.setTraceId(executionContext.getTraceId());
                    xConnection.setCompactMetadata(compactMeta);
                    // Add feedback here by set flag in XConnection.
                    xConnection.setWithFeedback(true);
                    // set client JDBC capability
                    xConnection.setCapabilities(executionContext.getCapabilityFlags());
                    connection.flushUnsent(); // Caution: This is important when use deferred sql.
                    xConnection.getSession().setChunkResult(false);
                    final boolean noDigest =
                        (executionContext.getGroupHint() != null && !executionContext.getGroupHint().isEmpty()) ||
                            (executionContext.getExplain() != null
                                && executionContext.getExplain().explainMode == ExplainResult.ExplainMode.EXECUTE);
                    phySqlExecuted = true;
                    xResult = xPreparedStatement.executeQueryX(noDigest ? null :
                        xPreparedStatement.isUseGalaxyPrepare() ? queryOperation.getGalaxyPrepareDigest() :
                            queryOperation.getSqlDigest());
                    xResult.getMetaData(); // Compatible with original time record.
                    rs = null;
                } else {
                    // 因为这里的sq可能会因为被改造过而成为多语句sql
                    // 例如，select sql_calc_found_rows xxx; select found_rows();
                    // 这里使用ps.execute()执行，是因为这个方法返回的resultRet支持多结果集
                    throw new AssertionError("unreachable");
//                    phySqlExecuted = true;
//                    ((PreparedStatement) ps).execute();
//                    realResultSet = ps.getResultSet();
//                    rs = new ResultSetWrapper(realResultSet, this);
                }
            }

            long timeCostNano = System.nanoTime() - startTimeNano;
            long getConnectionCostNano = 0;
            if (connectionStats != null) {
                getConnectionCostNano = connectionStats.getGetConnectionTotalNano();
            }
            // 这里统计的是mysql的执行时间
            sqlCostTime = (timeCostNano / 1000);
            if (enableTaskProfile) {
                this.statementStats.addExecuteStmtNano(timeCostNano);
            }
            executionContext.getStats().recordPhysicalTimeCost(sqlCostTime);

            String currentDbKey = getCurrentDbkey(rw);
            if (executionContext.isEnableTrace()) {
                ExecuteSQLOperation op = null;
                if (connection.isBytesSqlSupported()) {
                    op = new ExecuteSQLOperation(this.groupName, currentDbKey,
                        new String(sqlBytesPrefix) + bytesSql.display(), startTime);
                } else {
                    op = new ExecuteSQLOperation(this.groupName, currentDbKey,
                        new String(sqlBytesPrefix) + new String(bytesSql.getBytes(parameterContexts)), startTime);
                }
                if (connection.isBytesSqlSupported()) {
                    op.setParams(new Parameters(map, false));
                } else {
                    op.setParams(new Parameters(listToMap(parameterContexts), false));
                }
                op.setThreadName(Thread.currentThread().getName());
                op.setTimeCost(System.currentTimeMillis() - startTime);
                op.setRowsCount(rowsAffect);
                op.setGetConnectionTimeCost(getConnectionCostNano / (float) (1000 * 1000));
                op.setGrpConnId(grpConnId);
                op.setTraceId(executionContext.getTraceId());
                executionContext.getTracer().trace(op);
            }

            this.resultSet = null;
        } catch (SQLException e) {
            if (UNKNOWN_COLUMN.equals(e.getSQLState())) {
                generalHandlerException(queryOperation, sqlAndParam, e, true, rw);
            } else {
                generalHandlerException(queryOperation, sqlAndParam, e, false, rw);
            }
        } catch (Throwable e) {
            generalHandlerException(queryOperation, sqlAndParam, e, false, rw);
        } finally {
            resetPhyConnSocketTimeout();
            recordSqlLog(bytesSql,
                startTime,
                nanoStartTime,
                sqlCostTime,
                createCostTime,
                waitCostTime,
                sqlAndParam.param,
                this.groupName,
                rw);
            executionContext.getTransaction().clearTrxContext();
            collectStatementStats();
        }
    }

    private Pair<String, Map<Integer, ParameterContext>> fetchDbIndexAndParams(BaseQueryOperation queryOperation,
                                                                               List<List<String>> phyTableNamesOutput) {
        Pair<String, Map<Integer, ParameterContext>> result;

        Map<Integer, ParameterContext> paramMap = executionContext.getParamMap();
        if (queryOperation instanceof BaseTableOperation) {
            result = queryOperation.getDbIndexAndParam(
                paramMap, phyTableNamesOutput, executionContext);
        } else {
            result = queryOperation.getDbIndexAndParam(
                paramMap, executionContext);
        }
        return result;
    }

    private boolean executeQueryX(CursorMeta meta, PhyTableOperation phyTableOperation) {
        // Flag check.
        if (!isXDataSources() || !XConnectionManager.getInstance().isEnableXplan()) {
            return false;
        }

        final XPlanTemplate XTemplate = phyTableOperation.getXTemplate();
        if (null == XTemplate) {
            return false;
        }

        // Query with XPlan.
        final List<List<String>> phyTableNames = phyTableOperation.getTableNames();
        // XPlan have individual parameter mapper.
        final Map<Integer, ParameterContext> params =
            null == executionContext.getParams() ? null : executionContext.getParams().getCurrentParameter();

        // Check table and param mapping.
        if (phyTableNames.size() != 1) {
            return false; // Merge union not supported.
        }

        final boolean compactMeta =
            null == executionContext.getGroupHint() && null == executionContext.getExplain();
        return executeQueryX(XTemplate, phyTableNames.get(0), phyTableNames, params, phyTableOperation,
            phyTableOperation.getBytesSql(), compactMeta);
    }

    private void executeQuery(CursorMeta meta, PhyTableOperation phyTableOperation) throws SQLException {
        long startPrepStmtEnvNano = 0;
        if (enableTaskProfile) {
            startPrepStmtEnvNano = ThreadCpuStatUtil.getThreadCpuTimeNano();
        }
        Pair<String, Map<Integer, ParameterContext>> dbIndexAndParam =
            phyTableOperation.getDbIndexAndParam(null, executionContext);
        groupName = dbIndexAndParam.getKey();
        setContext(meta, isForceStream(), groupName);

        if (executionContext.getParams() != null) {
            if (executionContext.getParams().isBatch()) {
                throw new ExecutorException("batch not supported query sql");
            }
        }

        // Try XPlan first.
        if (executeQueryX(meta, phyTableOperation)) {
            return;
        }

        ITransaction.RW rw = phyTableOperation.isForUpdate() ? ITransaction.RW.WRITE : ITransaction.RW.READ;
        PhyTableScanBuilder phyTableScanBuilder = (PhyTableScanBuilder) phyTableOperation.getPhyOperationBuilder();
        byte[] sqlBytesPrefix = buildSqlPreFix(executionContext);
        BytesSql bytesSql = phyTableScanBuilder.buildBytesSql(phyTableOperation);
        List<ParameterContext> originParamList =
            phyTableScanBuilder.buildParams(this.groupName, phyTableOperation.getTableNames());

        HashMap<Integer, ParameterContext> logParams = new HashMap<>();
        for (int i = 0; i < Math.min(MAX_LOG_PARAM_COUNT, originParamList.size()); i++) {
            logParams.put(i, originParamList.get(i));
        }

        // for logging and exception handling
        SqlAndParam sqlAndParam = new SqlAndParam();
        sqlAndParam.sql = "";
        sqlAndParam.param = logParams;

        if (logger.isDebugEnabled()) {
            logger.debug("Native SQL:" + bytesSql.display().replaceAll("\n", " "));
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < logParams.size(); i++) {
                builder.append(" Index ").append(i + 1).append(" Value:");
                builder.append(logParams.get(i).getValue());
            }
            logger.debug("Param :" + builder.toString());
        }

        long startTime = System.currentTimeMillis();
        long nanoStartTime = System.nanoTime();
        long sqlCostTime = -1;
        long createCostTime = -1;
        long waitCostTime = -1;
        Long grpConnId = 0L;
        try {
            executionType = ExecutionType.GET;
            grpConnId = PhyTableOperationUtil.computeGrpConnIdByPhyOp(phyTableOperation, null, null, executionContext);
            this.connection = getPhyConnection(executionContext.getTransaction(), rw, groupName, grpConnId, null);
            ps = prepareStatement(bytesSql, sqlBytesPrefix, connection, executionContext, originParamList, null, false,
                false,
                phyTableOperation);
            List<ParameterContext> paramList = handleParamsList(originParamList);
            if (isStreaming) {
                setStreamingForStatement(ps);
            }

            executionContext.getStats().physicalRequest.incrementAndGet();
            if (enableTaskProfile) {
                this.statementStats.addPrepStmtEnvNano(ThreadCpuStatUtil.getThreadCpuTimeNano() - startPrepStmtEnvNano);
            }

            long startTimeNano = System.nanoTime();
            if (ps.isWrapperFor(XPreparedStatement.class)) {
                final XPreparedStatement xPreparedStatement = ps.unwrap(XPreparedStatement.class);
                final boolean compactMeta =
                    null == executionContext.getGroupHint() && null == executionContext.getExplain();
                final XConnection xConnection = xPreparedStatement.getConnection().unwrap(XConnection.class);
                xConnection.setTraceId(executionContext.getTraceId());
                xConnection.setCompactMetadata(compactMeta);

                // Add feedback here by set flag in XConnection.
                xConnection.setWithFeedback(true);
                // set client JDBC capability
                xConnection.setCapabilities(executionContext.getCapabilityFlags());
                connection.flushUnsent(); // Caution: This is important when use deferred sql.
                xConnection.getSession().setChunkResult(false);
                final boolean noDigest =
                    phyTableOperation.getTableNames().size() != 1 ||
                        (executionContext.getGroupHint() != null && !executionContext.getGroupHint().isEmpty()) ||
                        (executionContext.getExplain() != null
                            && executionContext.getExplain().explainMode == ExplainResult.ExplainMode.EXECUTE);
                phySqlExecuted = true;
                xResult = xPreparedStatement.executeQueryX(noDigest ? null :
                    xPreparedStatement.isUseGalaxyPrepare() ? phyTableOperation.getGalaxyPrepareDigest() :
                        phyTableOperation.getSqlDigest());
                xResult.getMetaData(); // Compatible with original time record.
                this.resultSet = null;
            } else {
                // FIXME(moyi) figure out it
                throw new AssertionError("unreachable");
//                phySqlExecuted = true;
//                ((PreparedStatement) ps).execute();
//                this.resultSet = new ResultSetWrapper(ps.getResultSet(), this);
            }

            // 这里统计的是mysql的执行时间
            long timeCostNano = System.nanoTime() - startTimeNano;
            long getConnectionCostNano = 0;
            if (connectionStats != null) {
                getConnectionCostNano = connectionStats.getGetConnectionTotalNano();
            }
            sqlCostTime = (timeCostNano / 1000);
            if (enableTaskProfile) {
                this.statementStats.addExecuteStmtNano(timeCostNano);
            }
            executionContext.getStats().recordPhysicalTimeCost(sqlCostTime);

            String currentDbKey = getCurrentDbkey(rw);
            if (executionContext.isEnableTrace()) {
                ExecuteSQLOperation op = null;
                if (connection.isBytesSqlSupported()) {
                    op = new ExecuteSQLOperation(this.groupName, currentDbKey,
                        new String(sqlBytesPrefix) + bytesSql.display(), startTime);
                    op.setParams(new Parameters(listToMap(originParamList), false));
                } else {
                    op = new ExecuteSQLOperation(this.groupName, currentDbKey,
                        new String(sqlBytesPrefix) + new String(bytesSql.getBytes(originParamList)), startTime);
                    op.setParams(new Parameters(listToMap(paramList), false));
                }
                op.setThreadName(Thread.currentThread().getName());
                op.setTimeCost(System.currentTimeMillis() - startTime);
                op.setRowsCount(rowsAffect);
                op.setGetConnectionTimeCost(getConnectionCostNano / (float) (1000 * 1000));
                op.setGrpConnId(grpConnId);
                op.setTraceId(executionContext.getTraceId());
                executionContext.getTracer().trace(op);
            }
        } catch (SQLException e) {
            if (UNKNOWN_COLUMN.equals(e.getSQLState())) {
                generalHandlerException(phyTableOperation, sqlAndParam, e, true, rw);
            } else {
                generalHandlerException(phyTableOperation, sqlAndParam, e, false, rw);
            }
        } catch (Throwable e) {
            generalHandlerException(phyTableOperation, sqlAndParam, e, false, rw);
        } finally {
            recordSqlLog(bytesSql,
                startTime,
                nanoStartTime,
                sqlCostTime,
                createCostTime,
                waitCostTime,
                logParams,
                this.groupName,
                rw);
            executionContext.getTransaction().clearTrxContext();
            collectStatementStats();
        }
    }

    private byte[] buildSqlPreFix(ExecutionContext executionContext) {
        byte[] trace = buildDRDSTraceCommentBytes(executionContext);

        if (executionContext.getExplain() != null
            && executionContext.getExplain().explainMode == ExplainResult.ExplainMode.EXECUTE) {
            trace = Bytes.concat("explain ".getBytes(StandardCharsets.UTF_8), trace);
        }
        return trace;
    }

    @Override
    public int[] executeUpdate(BaseQueryOperation phyTableModify) throws SQLException {
        long startPrepStmtEnvNano = 0;
        List<List<String>> phyTableNames = new ArrayList<>();
        Pair<String, Map<Integer, ParameterContext>> dbIndexAndParam =
            fetchDbIndexAndParams(phyTableModify, phyTableNames);
        this.groupName = dbIndexAndParam.getKey();
        if (phyTableModify.isUseDbIndex()) {
            this.groupName = phyTableModify.getDbIndex();
        }
        this.closed = false;

        List<Map<Integer, ParameterContext>> batchParams = null;
        SqlAndParam sqlAndParam = new SqlAndParam();
        BytesSql bytesSql = phyTableModify.getBytesSql();
        if (bytesSql != null) {
            if (dbIndexAndParam.getValue() != null) {
                sqlAndParam.param = dbIndexAndParam.getValue();
            } else if (phyTableModify instanceof BaseTableOperation) {
                batchParams = ((BaseTableOperation) phyTableModify).getBatchParameters();
                sqlAndParam.param = new HashMap<>();
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Native SQL:" + bytesSql.display().replaceAll("\n", " "));
                StringBuilder builder = new StringBuilder();
                for (ParameterContext c : dbIndexAndParam.getValue().values()) {
                    builder.append(" Index ").append(c.getArgs()[0]).append(" Value:");
                    builder.append(c.getValue());
                }
                logger.debug("Param :" + builder.toString());
            }
        }

        interceptDMLAllTableSql(phyTableModify, sqlAndParam.sql);

        boolean isInsertOrUpdate = false;
        if (phyTableModify.getKind() == SqlKind.INSERT || phyTableModify.getKind() == SqlKind.REPLACE
            || phyTableModify.getKind() == SqlKind.UPDATE) {
            isInsertOrUpdate = true;
        }

        long startTime = System.currentTimeMillis();
        long nanoStartTime = System.nanoTime();
        long sqlCostTime = -1, createCostTime = -1, waitCostTime = -1;
        boolean createConnectionSuccess = false;
        Long grpConnId = null;
        ITransaction.RW rw = ITransaction.RW.WRITE;
        try {
            byte[] sqlBytesPrefix = buildSqlPreFix(executionContext);
            ITransaction transaction = executionContext.getTransaction();
            if (executionContext.getParamManager().getBoolean(ConnectionParams.BROADCAST_DML)) {
                rw = ITransaction.RW.READ;
            }

            grpConnId = PhyTableOperationUtil.computeGrpConnIdByPhyOp(phyTableModify, groupName, phyTableNames,
                executionContext);
            connection = getPhyConnection(transaction, rw, groupName, grpConnId, null);
            if (batchParams != null) {
                connection.flushUnsent();
            }
            if (connection.isWrapperFor(XConnection.class)) {
                connection.unwrap(XConnection.class).setTraceId(executionContext.getTraceId());
                // set client JDBC capability
                connection.unwrap(XConnection.class).setCapabilities(executionContext.getCapabilityFlags());
            }

            final boolean noDigest =
                (executionContext.getGroupHint() != null && !executionContext.getGroupHint().isEmpty()) ||
                    (executionContext.getExplain() != null
                        && executionContext.getExplain().explainMode == ExplainResult.ExplainMode.EXECUTE);
            int[] affectRows;
            int affRows = 0;
            long startTimeNano;

            if (batchParams != null) {

                if (executionContext.getLoadDataContext() != null &&
                    executionContext.getLoadDataContext().isUseBatch()) {
                    try {
                        ps = prepareStatement(bytesSql, sqlBytesPrefix, connection, executionContext,
                            null, batchParams.get(0), isInsertOrUpdate, false, phyTableModify);
                        if (!noDigest && ps.isWrapperFor(XPreparedStatement.class)) {
                            ps.unwrap(XPreparedStatement.class)
                                .setGalaxyDigest(phyTableModify.getGalaxyPrepareDigest());
                        }
                        for (Map<Integer, ParameterContext> param : batchParams) {
                            handleParamsMap(param);
                            ((PreparedStatement) ps).addBatch();
                        }

                        startTimeNano = System.nanoTime();
                        executionContext.getStats().physicalRequest.incrementAndGet();
                        phySqlExecuted = true;
                        ps.executeBatch();
                        //batch 模式下 获取的rowCnt 不准确
                        affectRows = new int[] {batchParams.size()};
                    } catch (Throwable t) {
                        throw t;
                    }
                } else {
                    ps = prepareStatement(bytesSql, sqlBytesPrefix, connection, executionContext,
                        null, batchParams.get(0), isInsertOrUpdate, false, phyTableModify);
                    if (!noDigest && ps.isWrapperFor(XPreparedStatement.class)) {
                        ps.unwrap(XPreparedStatement.class).setGalaxyDigest(phyTableModify.getGalaxyPrepareDigest());
                    }
                    for (Map<Integer, ParameterContext> param : batchParams) {
                        if (connection.isBytesSqlSupported()) {
                            convertParameters(param, executionContext.getParamManager());
                            ParameterMethod.setParameters(ps, param);
                        } else {
                            convertParameters(param, executionContext.getParamManager());
                            ParameterMethod.setParameters(ps, prepareParam(mapToList(param)));
                        }
                        ((PreparedStatement) ps).addBatch();
                    }

                    startTimeNano = System.nanoTime();
                    executionContext.getStats().physicalRequest.incrementAndGet();
                    phySqlExecuted = true;
                    affectRows = ps.executeBatch();
                    for (int i : affectRows) {
                        affRows += i;
                    }
                }
            } else {

                if (sqlAndParam.param.isEmpty()) {
                    ps = createStatement(connection, executionContext, false);
                } else {
                    ps = prepareStatement(bytesSql, sqlBytesPrefix, connection, executionContext, null,
                        sqlAndParam.param, isInsertOrUpdate, false, phyTableModify);
                    if (!noDigest && ps.isWrapperFor(XPreparedStatement.class)) {
                        ps.unwrap(XPreparedStatement.class).setGalaxyDigest(phyTableModify.getGalaxyPrepareDigest());
                    }
                }

                int affectRow;
                handleParamsMap(sqlAndParam.param);

                if (sqlAndParam.param.isEmpty()) {
                    int[] columnIndexes = executionContext.getColumnIndexes();
                    String[] columnNames = executionContext.getColumnNames();
                    int autoGeneratedKeys = executionContext.getAutoGeneratedKeys();
                    if (isInsertOrUpdate) {
                        autoGeneratedKeys = Statement.RETURN_GENERATED_KEYS;
                    }

                    startTimeNano = System.nanoTime();
                    executionContext.getStats().physicalRequest.incrementAndGet();
                    phySqlExecuted = true;
                    if (ps.isWrapperFor(XStatement.class)) {
                        final XConnection xConnection = ps.getConnection().unwrap(XConnection.class);
                        xConnection.setTraceId(executionContext.getTraceId());
                        xConnection.getSession().setChunkResult(false);
                        connection.flushUnsent();
                        affectRow = (int) ps.unwrap(XStatement.class).executeUpdateX(bytesSql, sqlBytesPrefix);
                    } else {
                        // jdbc path
                        if (autoGeneratedKeys != -1) {
                            affectRow = ps.executeUpdate(
                                new String(sqlBytesPrefix) + bytesSql.toString(Collections.emptyList()),
                                autoGeneratedKeys);
                        } else if (columnIndexes != null) {
                            affectRow = ps.executeUpdate(
                                new String(sqlBytesPrefix) + bytesSql.toString(Collections.emptyList()), columnIndexes);
                        } else if (columnNames != null) {
                            affectRow = ps.executeUpdate(
                                new String(sqlBytesPrefix) + bytesSql.toString(Collections.emptyList()), columnNames);
                        } else {
                            affectRow = ps.executeUpdate(
                                new String(sqlBytesPrefix) + bytesSql.toString(Collections.emptyList()));
                        }
                    }
                } else {

                    if (enableTaskProfile) {
                        this.statementStats.addPrepStmtEnvNano(ThreadCpuStatUtil.getThreadCpuTimeNano()
                            - startPrepStmtEnvNano);
                    }
                    startTimeNano = System.nanoTime();
                    executionContext.getStats().physicalRequest.incrementAndGet();
                    phySqlExecuted = true;

                    if (ps.isWrapperFor(XPreparedStatement.class) && executionContext.useReturning()) {
                        final XPreparedStatement xPreparedStatement = ps.unwrap(XPreparedStatement.class);
                        final XConnection xConnection = xPreparedStatement.getConnection().unwrap(XConnection.class);
                        xConnection.setTraceId(executionContext.getTraceId());
                        connection.flushUnsent(); // Caution: This is important when use deferred sql.
                        xConnection.getSession().setChunkResult(false);
                        xResult = xPreparedStatement.executeUpdateReturningX(executionContext.getReturning());
                        xResult.getMetaData(); // Compatible with original time record.
                        affectRow = -2;
                    } else {
                        affectRow = ((PreparedStatement) ps).executeUpdate();
                    }
                }

                this.rowsAffect = new Long(affectRow);
                affectRows = new int[] {affectRow};
                affRows = affectRow;
            }

            long timeCostNano = System.nanoTime() - startTimeNano;
            long getConnectionCostNano = 0;
            if (connectionStats != null) {
                getConnectionCostNano = connectionStats.getGetConnectionTotalNano();
            }
            // 这里统计的是mysql的执行时间
            sqlCostTime = timeCostNano / 1000;
            if (enableTaskProfile) {
                this.statementStats.addExecuteStmtNano(timeCostNano);
            }
            executionContext.getStats().recordPhysicalTimeCost(sqlCostTime);

            String currentDbKey = getCurrentDbkey(rw);
            if (executionContext.isEnableTrace()) {
                ExecuteSQLOperation op =
                    new ExecuteSQLOperation(this.groupName, currentDbKey,
                        new String(sqlBytesPrefix) + bytesSql.display(), startTime);
                if (batchParams != null) {
                    op.setParams(new Parameters(batchParams));
                } else {
                    op.setParams(new Parameters(sqlAndParam.param));
                }
                op.setTimeCost(System.currentTimeMillis() - startTime);
                op.setGetConnectionTimeCost(getConnectionCostNano / (1000 * 1000));
                op.setRowsCount((long) affectRows[0]);
                op.setGrpConnId(grpConnId);
                op.setTraceId(executionContext.getTraceId());
                executionContext.getTracer().trace(op);
            }

            UpdateResultWrapper urw = new UpdateResultWrapper(affectRows, this);
            this.resultSet = urw;
            executionType = ExecutionType.PUT;
            if (isInsertOrUpdate) {
                ResultSet lastInsertIdResult = null;
                try {
                    lastInsertIdResult = ps.getGeneratedKeys();
                    long lastInsertId = 0;
                    while (lastInsertIdResult.next()) {
                        long id = getLastInsertIdFromResultSet(lastInsertIdResult);
                        if (id != 0 && lastInsertId == 0) {
                            lastInsertId = id;
                        }
                    }
                    // Get the first key as last insert id.
                    if (lastInsertId != 0) {
                        executionContext.getConnection().setLastInsertId(lastInsertId);
                        executionContext.getConnection().setReturnedLastInsertId(lastInsertId);
                    }
                } finally {
                    if (lastInsertIdResult != null) {
                        lastInsertIdResult.close();
                    }
                }
            }
            phyTableModify.setAffectedRows(affRows);
            return affectRows;
        } catch (Throwable e) {
            if (!createConnectionSuccess) {
                MatrixStatistics.addConnErrorCount(executionContext.getAppName(),
                    this.groupName,
                    getCurrentDbkey(rw),
                    1L);
            } else {
                MatrixStatistics.addSqlErrorCount(executionContext.getAppName(),
                    this.groupName,
                    getCurrentDbkey(rw),
                    1L);
            }
            handleException(phyTableModify, sqlAndParam, e, false, rw);
            return null;
        } finally {
            try {
                resetPhyConnSocketTimeout();
                recordSqlLog(bytesSql,
                    startTime,
                    nanoStartTime,
                    sqlCostTime,
                    createCostTime,
                    waitCostTime,
                    sqlAndParam.param,
                    this.groupName,
                    rw);
            } finally {
                if (!executionContext.useReturning()) {
                    close();
                }
                executionContext.getTransaction().clearTrxContext();
                collectStatementStats();
            }
        }
    }

    /**
     * handle params map, if connection support bytesSql, then just setParam;
     * else must transform map to list, and extend rawString params by prepareParam method
     */
    @Nullable
    private List<ParameterContext> handleParamsMap(Map<Integer, ParameterContext> params) throws SQLException {
        convertParameters(params, executionContext.getParamManager());
        if (connection.isBytesSqlSupported()) {
            ParameterMethod.setParameters(ps, params);
            return null;
        } else {
            List<ParameterContext> paramList = prepareParam(mapToList(params));
            ParameterMethod.setParameters(ps, paramList);
            return paramList;
        }
    }

    /**
     * handle params list, if connection support bytesSql, then just setParam; else must prepareParam params first
     */
    @Nullable
    private List<ParameterContext> handleParamsList(List<ParameterContext> originParamList) throws SQLException {
        List<ParameterContext> paramList = null;
        if (connection.isBytesSqlSupported()) {
            paramList = originParamList;
        } else {
            paramList = prepareParam(originParamList);
        }
        ParameterMethod.setParameters(ps, paramList);
        return paramList;
    }

    public int executeTableDdl(PhyDdlTableOperation tableOperation) throws SQLException {
        long startPrepStmtEnvNano = 0;
        if (enableTaskProfile) {
            startPrepStmtEnvNano = ThreadCpuStatUtil.getThreadCpuTimeNano();
        }
        this.groupName = tableOperation.getDbIndex();
        this.closed = false;

        SqlAndParam sqlAndParam = new SqlAndParam();
        sqlAndParam.param = tableOperation.getParam();
        BytesSql bytesSql = tableOperation.getBytesSql();
        byte[] sqlBytesPrefix = buildSqlPreFix(executionContext);

        long startTime = System.currentTimeMillis();
        long nanoStartTime = System.nanoTime();

        long sqlCostTime = -1, createCostTime = -1, waitCostTime = -1;
        boolean createConnectionSuccess = false;
        boolean isBatch = executionContext.getParams() != null ? executionContext.getParams().isBatch() : false;
        try {
            int affectRows = 0;

            // 可能执行过程有失败，需要释放链接
            connection = getPhyConnection(executionContext.getTransaction(),
                ITransaction.RW.WRITE,
                tableOperation.getDbIndex(), null, null);
            if (connection.isWrapperFor(XConnection.class)) {
                connection.unwrap(XConnection.class).setTraceId(executionContext.getTraceId());
            }

            createConnectionSuccess = true;
            if (sqlAndParam.param.isEmpty() && !isBatch) {
                ps = createStatement(connection, executionContext, true);
            } else {
                ps = prepareStatement(bytesSql, sqlBytesPrefix, connection, executionContext, null, sqlAndParam.param,
                    false, true, tableOperation);
                setParameters(ps, sqlAndParam.param);
            }

            executionContext.getStats().physicalRequest.incrementAndGet();
            if (enableTaskProfile) {
                this.statementStats.addPrepStmtEnvNano(ThreadCpuStatUtil.getThreadCpuTimeNano() - startPrepStmtEnvNano);
            }
            long startTimeNano = System.nanoTime();
            try {
                FailPoint.injectFromHint(FailPointKey.FP_BEFORE_PHYSICAL_DDL_EXCEPTION, executionContext, () -> {
                    FailPoint.injectException(FailPointKey.FP_BEFORE_PHYSICAL_DDL_EXCEPTION);
                });
                FailPoint.injectFromHint(FailPointKey.FP_BEFORE_PHYSICAL_DDL_PARTIAL_EXCEPTION, executionContext,
                    () -> {
                        long taskId = executionContext.getPhyDdlExecutionRecord().getTaskId();
                        if (!executionContext.getDdlContext().compareAndSetPhysicalDdlInjectionFlag(taskId)) {
                            FailPoint.injectException(FailPointKey.FP_BEFORE_PHYSICAL_DDL_PARTIAL_EXCEPTION);
                        }
                    });

                /**
                 * 真正将SQL发向物理数据源并执行：一定需要返回是否真正对DB产生了影响，后面会根据这个来决定是否上推新规则
                 * mysql对于直接收到的DDL也是返回0 rows affected,所以执行成功也是0 affectRows
                 * 后面直接判断是否为0就可以判断是否可以上推规则了。
                 */
                phySqlExecuted = true;
                if (ps instanceof PreparedStatement) {
                    affectRows = ((PreparedStatement) ps).executeUpdate();
                } else {
                    sqlAndParam.sql = tableOperation.getNativeSql();
                    String sql = buildSql(sqlAndParam.sql, executionContext);
                    if (tableOperation.isExplain()) {
                        sqlAndParam.sql = "explain " + sqlAndParam.sql;
                    }
                    affectRows = ps.executeUpdate(sql);
                }

                FailPoint.injectFromHint(FailPointKey.FP_AFTER_PHYSICAL_DDL_EXCEPTION, executionContext, () -> {
                    FailPoint.injectException(FailPointKey.FP_AFTER_PHYSICAL_DDL_EXCEPTION);
                });
                FailPoint.injectFromHint(FailPointKey.FP_AFTER_PHYSICAL_DDL_PARTIAL_EXCEPTION, executionContext,
                    () -> {
                        long taskId = executionContext.getPhyDdlExecutionRecord().getTaskId();
                        if (!executionContext.getDdlContext().compareAndSetPhysicalDdlInjectionFlag(taskId)) {
                            FailPoint.injectException(FailPointKey.FP_AFTER_PHYSICAL_DDL_PARTIAL_EXCEPTION);
                        }
                    });

                long timeCostNano = System.nanoTime() - startTimeNano;
                long getConnectionCostNano = 0;
                if (connectionStats != null) {
                    getConnectionCostNano = connectionStats.getGetConnectionTotalNano();
                }
                // 这里统计的是mysql的执行时间
                sqlCostTime = timeCostNano / 1000;
                if (enableTaskProfile) {
                    this.statementStats.addExecuteStmtNano(timeCostNano);
                }
                executionContext.getStats().recordPhysicalTimeCost(sqlCostTime);
                String currentDbKey = getCurrentDbkey(ITransaction.RW.WRITE);

                if (executionContext.isEnableTrace() || executionContext.isEnableDdlTrace()) {
                    ExecuteSQLOperation op = new ExecuteSQLOperation(this.groupName,
                        currentDbKey,
                        new String(sqlBytesPrefix) + bytesSql.display(),
                        startTimeNano / 1000_000);
                    op.setParams(new Parameters(null, false));
                    op.setTimeCost(System.currentTimeMillis() - startTime);
                    op.setGetConnectionTimeCost(getConnectionCostNano / (float) (1000 * 1000));
                    executionContext.getTracer().trace(op);
                }

                UpdateResultWrapper urw = new UpdateResultWrapper(new int[] {affectRows}, this);
                executionType = ExecutionType.PUT;
                this.resultSet = urw;

                /**
                 * 对于alter因为是顺序执行的，而且executionContext是重用的，所以可以将每次成功执行后
                 * 的结构放入到executionContext中，这样如果发现失败就可以打印当时成功的记录了,并且可以
                 * 抛出异常告诉客户手工执行
                 */
                storeSuccessDDLRecords(tableOperation, this.groupName);
            } catch (Throwable t) {
                MatrixStatistics.addSqlErrorCount(executionContext.getAppName(),
                    this.groupName,
                    getCurrentDbkey(ITransaction.RW.WRITE),
                    1L);
                throw t;
            }

            // Some warnings should be recorded in ExecutionContext.
            storeWarningsIfNeeded(tableOperation);

            return affectRows;
        } catch (Throwable e) {
            if (!createConnectionSuccess) {
                MatrixStatistics.addConnErrorCount(executionContext.getAppName(),
                    this.groupName,
                    getCurrentDbkey(ITransaction.RW.WRITE),
                    1L);
            }

            handlePhyDDLAfterFailure(tableOperation, e);

            try {
                if (tableOperation.isPartitioned()) {
                    // 分库情况下,忽略中间异常,全部执行所有分库
                    int code = 0;
                    if (e instanceof TddlNestableRuntimeException) {
                        code = ((TddlNestableRuntimeException) e).getErrorCode();
                    } else if (e instanceof SQLException) {
                        code = ((SQLException) e).getErrorCode();
                    }

                    storeFailedDDLRecords(tableOperation, this.groupName, code, e.getMessage());

                    return 0;
                } else {
                    if (e instanceof TddlRuntimeException || e instanceof TddlException) {
                        throw GeneralUtil.nestedException(e);
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL,
                            e,
                            this.groupName,
                            getCurrentDbkey(ITransaction.RW.WRITE),
                            e.getMessage());
                    }
                }
            } finally {
                if (logger.isWarnEnabled()) {
                    logger.warn("Execute error on group: " + this.groupName + ", sql is: " + sqlAndParam.sql
                        + ", param is: " + sqlAndParam.param.values(), e);
                }
            }
        } finally {
            try {
                resetPhyConnSocketTimeout();
                recordSqlLog(bytesSql,
                    startTime,
                    nanoStartTime,
                    sqlCostTime,
                    createCostTime,
                    waitCostTime,
                    sqlAndParam.param,
                    this.groupName,
                    ITransaction.RW.WRITE);
            } finally {
                close();
                executionContext.getTransaction().clearTrxContext();
            }
        }
    }

    /**
     * If the physical ddl failed due to timeout or connection reset,
     * then it may be still running on physical instance. We should
     * check the physical process until it's done or kill it, so that
     * ddl executor can determine later if the physical ddl is already done.
     */
    private void handlePhyDDLAfterFailure(PhyDdlTableOperation tableOperation, Throwable t) {
        boolean isTimeoutOnJdbc = t instanceof CommunicationsException &&
            TStringUtil.equalsIgnoreCase(((CommunicationsException) t).getSQLState(), "08S01") &&
            TStringUtil.containsIgnoreCase(t.getMessage(), "Communications link failure");

        boolean isTimeoutOnJdbc2 = t instanceof com.mysql.jdbc.CommunicationsException &&
            TStringUtil.equalsIgnoreCase(((com.mysql.jdbc.CommunicationsException) t).getSQLState(), "08S01") &&
            TStringUtil.containsIgnoreCase(t.getMessage(), "Communications link failure");

        boolean isTimeoutOnXProtocol = t instanceof TddlRuntimeException &&
            ((TddlRuntimeException) t).getErrorCode() == 10004 &&
            TStringUtil.containsIgnoreCase(t.getMessage(), "Query timeout");

        boolean isUnfinishedOnXProtocol = t instanceof TddlRuntimeException &&
            ((TddlRuntimeException) t).getErrorCode() == 10002 &&
            TStringUtil.containsIgnoreCase(t.getMessage(), "previous unfinished");

        String schemaName = executionContext.getSchemaName();
        String traceId = executionContext.getTraceId() + "";

        Pair<String, String> phyTableInfo = DdlHelper.genPhyTablePair(tableOperation, executionContext.getDdlContext());

        String groupName = phyTableInfo.getKey();
        String phyTableName = phyTableInfo.getValue();

        String warningMsg =
            String.format("The physical DDL %%s on %s:%s:%s, then we will %%s if it's still active", groupName,
                phyTableName, traceId);

        if (isTimeoutOnJdbc || isTimeoutOnJdbc2 || isTimeoutOnXProtocol || isUnfinishedOnXProtocol) {
            // Unexpected timeout, so let's wait until the physical DDL completes.
            logger.warn(String.format(warningMsg, "timed out due to unexpected connection reset",
                "wait until it completes"));
            DdlHelper.waitUntilPhyDDLDone(schemaName, groupName, phyTableName, traceId);
        } else {
            // Kill the physical DDL right now for other exceptions.
            logger.warn(String.format(warningMsg, "failed due to " + t.getMessage(), "kill it"));
            DdlHelper.killUntilPhyDDLGone(schemaName, groupName, phyTableName, traceId);
        }
    }

    private boolean storeWarningsIfNeeded(PhyDdlTableOperation tableOperation) {
        if (tableOperation.getKind() == SqlKind.CREATE_TABLE && tableOperation.isIfNotExists()) {
            SQLWarning warning = null;
            try {
                warning = ps.getWarnings();
            } catch (SQLException ignored) {
            }
            // Only the 'table already exists' warning won't be ignored.
            // And we only care about the first warning.
            if (warning != null && warning.getErrorCode() == DdlConstants.ERROR_TABLE_EXISTS) {
                storeFailedDDLRecords(tableOperation, this.groupName, warning.getErrorCode(), warning.getMessage());
                return true;
            }
        }
        return false;
    }

    private void generalHandlerException(BaseQueryOperation queryOperation, SqlAndParam sqlAndParam, Throwable e,
                                         Boolean unkownColumn, ITransaction.RW rw) throws SQLException {
        MatrixStatistics.addSqlErrorCount(executionContext.getAppName(),
            this.groupName,
            getCurrentDbkey(rw),
            1L);
        try {
            // 关闭自提交的链接
            close();
        } finally {
            handleException(queryOperation, sqlAndParam, e, unkownColumn, rw);
        }
    }

    private void handleException(BaseQueryOperation queryOperation, SqlAndParam sqlAndParam, Throwable e,
                                 Boolean unKownColunm, ITransaction.RW rw) {
        try {
            if (e instanceof TddlRuntimeException || e instanceof TddlException) {
                throw GeneralUtil.nestedException(e);
            } else if (unKownColunm) {
                TddlRuntimeException tddlRuntimeException2 =
                    new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL_UNKNOWN_COLUMN,
                        e,
                        this.groupName,
                        e.getMessage());
                throw tddlRuntimeException2;
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL,
                    e,
                    this.groupName,
                    getCurrentDbkey(rw),
                    e.getMessage());
            }
        } finally {
            if (logger.isWarnEnabled()) {
                Object traceId = executionContext.getTraceId();

                StringBuilder sb = new StringBuilder();
                sb.append("[").append(traceId).append("]");
                sb.append("Execute ERROR on GROUP: ").append(this.groupName);
                sb.append(", ATOM: ").append(getCurrentDbkey(rw));

                if (queryOperation != null && queryOperation.getKind() == SqlKind.SELECT) {
                    sb.append(", MERGE_UNION_SIZE:").append((queryOperation).getUnionSize());
                }

                sb.append(", SQL: ").append(sqlAndParam.sql);

                if (!GeneralUtil.isEmpty(sqlAndParam.param.values())) {
                    sb.append(", PARAM: ").append(String.valueOf(sqlAndParam.param.values()));
                }
                sb.append(", ERROR: ").append(e.getMessage());

                if (isMySQLIntegrityConstraintViolationException(e)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(sb.toString(), e);
                    }
                } else {
                    logger.warn(sb.toString(), e);
                }
            }
        }
    }

    /**
     * 拦截全表删或者全表更新操作
     * <p>
     * 下面的语句是极其危险的操作，默认禁止掉
     * statusStage=1+2&openTaskId=10554339&。 1. DELETE 语句不带 WHERE 条件    2.
     * UPDATE 语句不带 WHERE 条件   3. 当
     * ENABLE_DELETE_WITH_LIMIT、ENABLE_UPDATE_WITH_LIMIT 开启的时候， 如果 DELETE、UPDATE
     * 操作既没有提供 where 条件也没有提供 limit 条件。
     * </p>
     * 用户如果确实想要执行上述两种语句怎么办？     1. 通过使用 HINT
     * 方式：TDDL:ENABLE_DELETE_WITHOUT_WHERE_FILTER
     * 、TDDL:ENABLE_UPDATE_WITHOUT_WHERE_FILTER* 2. 实例级别允许在 diamond 上定义
     * ENABLE_DELETE_WITHOUT_WHERE_FILTER、ENABLE_UPDATE_WITHOUT_WHERE_FILTER 开关，
     * 来关闭上述禁止。
     */
    protected void interceptDMLAllTableSql(BaseQueryOperation phyTableModify, String sql) {
        boolean forbidDmlAll = executionContext.getParamManager().getBoolean(ConnectionParams.FORBID_EXECUTE_DML_ALL);
        if (forbidDmlAll) {
            if (phyTableModify.getKind() == SqlKind.DELETE || phyTableModify.getKind() == SqlKind.UPDATE) {
                RelUtils.forbidDMLAllTableSql(phyTableModify.getNativeSqlNode());
            }
        }
    }

    public static void setParameters(java.sql.Statement stmt, Map<Integer, ParameterContext> parameterSettings)
        throws SQLException {
        if (!(stmt instanceof PreparedStatement)) {
            return;
        }

        if (null != parameterSettings) {
            for (ParameterContext context : parameterSettings.values()) {
                context.getParameterMethod().setParameter((PreparedStatement) stmt, context.getArgs());
            }
        }
    }

    private void storeFailedDDLRecords(PhyDdlTableOperation ddl, String group, int code, String message) {
        String tableName = null;

        tableName = DdlHelper.genPhyTablePair(ddl, executionContext.getDdlContext()).getValue();

        if (TStringUtil.isNotEmpty(tableName)) {
            String pureTableName = tableName.replaceAll(DdlConstants.BACKTICK, DdlConstants.EMPTY_CONTENT);
            String pureMessage = message.replaceAll(DdlConstants.BACKTICK, DdlConstants.EMPTY_CONTENT);
            if (!pureMessage.toLowerCase().contains(pureTableName.toLowerCase())) {
                // Make sure that the error message contains physical table name
                // so that logical ddl executor can determine if the physical DDL
                // has been executed successfully later.
                message += " on " + tableName;
            }
        }

        executionContext.addMessage(ExecutionContext.FAILED_MESSAGE,
            new ExecutionContext.ErrorMessage(code, group, message));
    }

    private void storeSuccessDDLRecords(PhyDdlTableOperation ddl, String group) {
        executionContext.addMessage(ExecutionContext.SUCCESS_MESSAGE,
            new ExecutionContext.ErrorMessage(0, group, "Success"));
    }

    private static class SqlAndParam {
        public String sql;
        public Map<Integer, ParameterContext> param;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("sql: ").append(sql);

            if (GeneralUtil.isNotEmpty(param)) {
                sb.append("\n").append("param: ").append(param).append("\n");
            }
            return sb.toString();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    protected String getCurrentDbkey(ITransaction.RW rw) {
        TGroupDataSource dataSource = repo.getDataSource(groupName);
        String currentDbKey = ANONAMOUS_DBKEY;
        if (dataSource != null) {
            if (connection != null) {
                IConnection realConneciton = connection.getRealConnection();
                if (realConneciton instanceof TGroupDirectConnection) {
                    currentDbKey = ((TGroupDirectConnection) realConneciton).getDbKey();
                }
            }
            if (currentDbKey == ANONAMOUS_DBKEY) {
                MasterSlave masterSlave =
                    ExecUtils.getMasterSlave(inTrans, rw.equals(ITransaction.RW.WRITE), executionContext);
                currentDbKey =
                    dataSource.getConfigManager().getDataSource(
                        masterSlave).getDsConfHandle().getDbKey();
            }
            if (StringUtils.isEmpty(currentDbKey)) {
                currentDbKey = ANONAMOUS_DBKEY;
            }
        }
        return currentDbKey;
    }

    protected IConnection getPhyConnection(ITransaction trans, ITransaction.RW rw, String groupName, Long grpConnId,
                                           DataSource ds)
        throws SQLException {
        if (groupName.equalsIgnoreCase(MetaDbDataSource.DEFAULT_META_DB_GROUP_NAME) ||
            SystemDbHelper.INFO_SCHEMA_DB_GROUP_NAME.equalsIgnoreCase(groupName)) {
            IConnection metaDbConn = new MetaDbConnectionProxy();
            return metaDbConn;
        }

        DataSource dataSource = null == ds ? repo.getDataSource(groupName) : ds;

        IConnection conn = (IConnection) PhyOpTrxConnUtils.getConnection(trans, repo.getSchemaName(), groupName,
            (TGroupDataSource) dataSource, rw, executionContext, grpConnId);

        if (conn.getRealConnection() instanceof TGroupDirectConnection) {
            this.connectionStats = conn.getConnectionStats();
            collectConnectionStats();
        }
        return conn;

    }

    public void setOperatorStatistics(OperatorStatistics operatorStatisticsExt) {
        this.operatorStatisticsExt = (OperatorStatisticsExt) operatorStatisticsExt;
    }

    protected void collectStatementStats() {
        if (!enableTaskProfile) {
            return;
        }

        if (runtimeStat != null) {
            if (phySqlExecuted) {
                runtimeStat
                    .addPhySqlTimecost(statementStats.getCreateAndInitStmtNano() + statementStats.getExecuteStmtNano());
                runtimeStat.addPhySqlCount(1);
            }
        }

        if (operatorStatisticsExt == null) {
            return;
        }

        operatorStatisticsExt.setPrepateStmtEnvDuration(operatorStatisticsExt.getPrepateStmtEnvDuration()
            + statementStats.getPrepStmtEnvNano());
        operatorStatisticsExt.setCreateAndInitJdbcStmtDuration(operatorStatisticsExt.getCreateAndInitJdbcStmtDuration()
            + statementStats.getCreateAndInitStmtNano());
        if (phySqlExecuted) {
            operatorStatisticsExt.setExecJdbcStmtDuration(operatorStatisticsExt.getExecJdbcStmtDuration()
                + statementStats.getExecuteStmtNano());
        }

    }

    protected void collectStatementStatsForClose() {
        if (!enableTaskProfile) {
            return;
        }

        if (runtimeStat != null) {
            if (executionType == ExecutionType.GET) {
                runtimeStat.addPhyFetchRows(rowsAffect);
            } else {
                runtimeStat.addPhyAffectedRows(rowsAffect);
            }
        }

        if (operatorStatisticsExt == null) {
            return;
        }
        operatorStatisticsExt.setCloseJdbcResultSetDuration(operatorStatisticsExt.getCloseJdbcResultSetDuration()
            + statementStats.getCloseResultSetNano());

    }

    protected void collectConnectionStats() {
        if (!enableTaskProfile) {
            return;
        }

        long createConn = connectionStats.getCreateConnectionNano();
        long waitConn = connectionStats.getWaitConnectionNano();
        long initConn = connectionStats.getInitConnectionNano();

        if (runtimeStat != null) {
            runtimeStat.addPhyConnTimecost(createConn + waitConn + initConn);
        }

        if (operatorStatisticsExt == null) {
            return;
        }
        operatorStatisticsExt.setCreateConnDuration(operatorStatisticsExt.getCreateConnDuration() + createConn);
        operatorStatisticsExt.setWaitConnDuration(operatorStatisticsExt.getWaitConnDuration() + waitConn);
        operatorStatisticsExt.setInitConnDuration(operatorStatisticsExt.getInitConnDuration() + initConn);

    }
}
