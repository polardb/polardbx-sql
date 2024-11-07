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

package com.alibaba.polardbx.server.conn;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.IInnerConnection;
import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.TrxIdGenerator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.mdl.MdlContext;
import com.alibaba.polardbx.executor.mdl.MdlManager;
import com.alibaba.polardbx.gms.privilege.PolarPrivUtil;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.matrix.jdbc.TPreparedStatement;
import com.alibaba.polardbx.matrix.jdbc.TResultSet;
import com.alibaba.polardbx.net.ClusterAcceptIdGenerator;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Maps;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 1. 一个精简版的Jdbc Connection实现，用于封装和屏蔽TConnection的细节，方便编写业务逻辑代码，提供原生JDBC的使用体验
 * 2. 如果有在Server内部使用逻辑库表、分布式事务等的需求，可以使用该Connection
 * * 3. 维护方式：够用就好，随时有新需求，随时添加代码
 *
 * @author ziyang.lb 2020-12-05
 */
public class InnerConnection implements IInnerConnection {
    private final static Logger logger = LoggerFactory.getLogger(InnerConnection.class);

    private final Long id;
    private final String schemaName;
    private final TConnection connection;
    private MdlContext mdlContext;
    private final Object mdlContextLock;

    private volatile Long txId = 0L;
    private Long sqlId = 0L;
    private Long phySqlId = 0L;
    private String traceId = null;
    private volatile boolean autoCommit = true;
    private ITransactionPolicy trxPolicy = ITransactionPolicy.TSO;
    private final List<Consumer<Object>> executionContextInjectHooks = new ArrayList<>();
    private final AtomicBoolean statementExecuting = new AtomicBoolean(false);
    private long lastActiveTime = System.nanoTime();
    private String user = PolarPrivUtil.POLAR_ROOT + '@' + "127.0.0.1";
    private String sqlSample = null;
    final private Map<String, Object> extraServerVariables = new HashMap<>();
    private AtomicBoolean isClosed = new AtomicBoolean(false);

    private boolean recordCommitTso = false;

    private long commitTso = -1L;

    public InnerConnection() throws SQLException {
        this(SystemDbHelper.DEFAULT_DB_NAME);
    }

    public InnerConnection(String schemaName) throws SQLException {
        this(schemaName, false, Maps.newHashMap());
    }

    public InnerConnection(String schemaName, boolean recordCommitTso) throws SQLException {
        this(schemaName, recordCommitTso, Maps.newHashMap());
    }

    public InnerConnection(String schemaName,
                           boolean recordCommitTso,
                           Map<String, Object> sessionVariables) throws SQLException {
        this.schemaName = schemaName;
        this.id = ClusterAcceptIdGenerator.getInstance().nextId();
        this.mdlContext = MdlManager.addContext(id);
        this.mdlContextLock = new Object();
        this.recordCommitTso = recordCommitTso;

        // JDBC会改成STRICT_TRANS_TABLES，为与MySQL兼容，需要改成global的设置
        Map<String, Object> serverVariables = new HashMap<>();
        serverVariables.put("sql_mode", "default");
        serverVariables.put("net_write_timeout", (long) (8 * 60 * 60));

        // 系统库在server启动阶段会进行初始化，不会出现在执行过程中被drop的情况，所以取到的schema一定不会为空
        SchemaConfig schema =
            CobarServer.getInstance().getConfig().getSchemas().get(schemaName);
        TDataSource ds = schema.getDataSource();
        if (!ds.isInited()) {
            ds.init();
        }

        // 对connection进行初始化，部分参数直接写死，使用root用户
        OptimizerContext.setContext(ds.getConfigHolder().getOptimizerContext());
        this.connection = (TConnection) ds.getConnection();
        this.connection.setMdlContext(mdlContext);
        this.connection.setUser(user);
        this.connection.setFrontendConnectionInfo(user + ':' + "1111");
        this.connection.setId(id);
        this.connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        this.connection.setAutoCommit(autoCommit);
        this.connection.setEncoding("utf8");

        this.connection.setTrxPolicy(trxPolicy, false);
        this.connection.setServerVariables(serverVariables);
        this.connection.setExtraServerVariables(extraServerVariables);

        if (null != InnerConnectionManager.getActiveConnections().putIfAbsent(id, this)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Found duplicate inner connection id.");
        }
        initConnBySessionVariables(sessionVariables);
    }

    /**
     * Init the InnerConnection by the session variables,
     * so, its functions are added as needed,
     */
    protected void initConnBySessionVariables(Map<String, Object> sessionVariables) {

        if (sessionVariables == null || sessionVariables.isEmpty()) {
            return;
        }

        Map<String, Object> sessionVariablesToBeSetOnDn = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, Object> variableItem : sessionVariables.entrySet()) {
            String key = variableItem.getKey();
            Object val = variableItem.getValue();
            if (key.equalsIgnoreCase("time_zone")) {
                String tzStr = (String) SQLUtils.normalizeNoTrim((String) val);
                if (!StringUtils.isEmpty(tzStr)) {
                    InternalTimeZone internalTz = TimeZoneUtils.convertFromMySqlTZ(tzStr);
                    this.connection.setTimeZone(internalTz);
                }
                continue;
            }

            if (key.equalsIgnoreCase("sql_mode")) {
                String sqlModeStr = (String) SQLUtils.normalizeNoTrim((String) val);
                if (!StringUtils.isEmpty(sqlModeStr)) {
                    this.connection.setSqlMode(sqlModeStr);
                }
                continue;
            }

            if (key.equalsIgnoreCase("names")) {
                String charsetStr = (String) SQLUtils.normalizeNoTrim((String) val);
                if (!StringUtils.isEmpty(charsetStr)) {
                    this.connection.setEncoding(charsetStr);
                }
                continue;
            }

            if (key.equalsIgnoreCase("group_parallelism")) {
                String groupParallelismStr = SQLUtils.normalizeNoTrim((String) val);
                if (!StringUtils.isEmpty(groupParallelismStr)) {
                    Long grpParallelismVal = Long.valueOf(groupParallelismStr);
                    if (grpParallelismVal > 0) {
                        this.connection.setGroupParallelism(grpParallelismVal);
                    }
                }
                continue;
            }

            if (key.equalsIgnoreCase("transaction_policy")) {
                String trxPolicyStr = (String) SQLUtils.normalizeNoTrim((String) val);
                if (!StringUtils.isEmpty(trxPolicyStr)) {
                    if (trxPolicyStr.equalsIgnoreCase("archive")) {
                        this.setTrxPolicy(ITransactionPolicy.ARCHIVE);
                        this.connection.setTrxPolicy(ITransactionPolicy.ARCHIVE, false);
                    }
                }
                continue;
            }
            sessionVariablesToBeSetOnDn.put(key, val);
        }

//        if (!sessionVariablesToBeSetOnDn.isEmpty()) {
//            this.connection.getServerVariables().putAll(sessionVariablesToBeSetOnDn);
//        }
    }

    public InnerConnection(TConnection connection) {
        this.id = 0L;
        this.connection = connection;
        this.schemaName = "polardbx";
        this.mdlContextLock = new Object();
    }

    public void setExtraServerVariables(String key, Object value) {
        if (null == this.connection.getExtraServerVariables()) {
            this.connection.setExtraServerVariables(new HashMap<>());
        }
        this.connection.getExtraServerVariables().put(key, value);
    }

    protected Object executeSql(String sql, List<Pair<Integer, ParameterContext>> params) throws SQLException {
        lastActiveTime = System.nanoTime();
        int sqlSimpleMaxLen = CobarServer.getInstance().getConfig().getSystem().getSqlSimpleMaxLen();
        sqlSample = sql.substring(0, Math.min(sqlSimpleMaxLen, sql.length()));
        boolean success = true;
        String traceIdToLog = "";
        Integer updateCountToLog = 0;
        try {
            genTxIdAndTraceId();
            CobarServer.getInstance().getServerExecutor().initTraceStats(traceId);
            traceIdToLog = this.traceId;

            // 设置TrxPolicy & ExecutionContext信息
            connection.setTrxPolicy(trxPolicy, false);

            // 变量定义
            boolean flag = false;
            Object result = null;
            Throwable exception = null;
            ITransaction trx = null;
            ExecutionContext ec = null;

            // In non Auto-commit mode, A DDL will commit the
            // current trans obj and create a new one. so we must
            // record the trans obj for checking.
            if (!autoCommit) {
                trx = connection.getTrx();
            }

            //统一使用PrepareStatement模式，只支持一次一个Sql
            try (TPreparedStatement stmt = (TPreparedStatement) connection.prepareStatement(sql)) {
                // Since prepareStatement will init a new execution context,
                // any operation on ec should start here.
                ec = this.connection.getExecutionContext();
                beforeExecution();
                processExecutionContextHooks(ec);
                fillParams(stmt, params);
                statementExecuting.set(true);
                flag = stmt.execute();
                if (flag) {
                    result = stmt.getResultSet();
                    result = buildResultSet((TResultSet) result);
                } else {
                    result = stmt.getUpdateCount();
                }
            } catch (Throwable t) {
                logger.error(t);
                exception = t;
                if (!flag) {
                    updateCountToLog = -1;
                }
            }

            if (!flag && result != null) {
                updateCountToLog = (Integer) result;
            }

            // 自动提交模式下，直接提交即可
            if (autoCommit && exception == null) {
                try {
                    connection.commit();
                } catch (Throwable ex) {
                    exception = ex;
                }
            }

            // 释放事务锁
            synchronized (mdlContextLock) {
                if (connection.getMdlContext() != null) {
                    if (autoCommit && exception != null) {
                        // Release mdl on autocommit transaction with execution err
                        mdlContext.releaseTransactionalLocks(txId);
                    }

                    if (autoCommit && exception != null && null != ec && ec.getTransaction() != null) {
                        // Release mdl on autocommit transaction with execution
                        // err
                        mdlContext.releaseTransactionalLocks(ec.getTransaction().getId());
                    }
                }
            }

            try {
                CobarServer.getInstance().getServerExecutor().closeByTraceId(traceId);
                CobarServer.getInstance().getServerExecutor().waitByTraceId(traceId);
            } catch (Throwable ex) {
                logger.error("Interrupted unexpectedly for " + ec.getTraceId(), ex);
            }

            // Checking the current trans obj, If it's changing, A DDL
            // maybe execute and committing the previous trans.
            if (!autoCommit && trx != null) {
                if (trx != connection.getTrx()) {
                    this.txId = null;
                }
            }

            try {
                connection.tryClose();
            } catch (Throwable e) {
                logger.error("Failed to close TConnection", e);
            }

            statementExecuting.set(false);

            try {
                connection.getExecutionContext().clearAllMemoryPool();
            } catch (Throwable e) {
                logger.warn("Failed to release memory of current request", e);
            }
            if (connection.getExecutionContext().getParams() != null) {
                connection.getExecutionContext().getParams().clear();
            }

            if (exception != null) {
                success = false;
                if (exception instanceof SQLException) {
                    throw (SQLException) exception;
                } else {
                    throw new RuntimeException("sql execute error!", exception);
                }
            } else {
                return result;
            }
        } finally {
            // Record sql.
            SQLRecorderLogger.innerSqlLogger.info(SQLRecorderLogger.innerSqlFormat.format(new Object[] {
                sql,
                success ? "0" : "1",
                // in milliseconds
                (System.nanoTime() - lastActiveTime) / 1_000_000,
                traceIdToLog,
                updateCountToLog
            }));
            // Reset.
            lastActiveTime = System.nanoTime();
        }
    }

    /**
     * release Transactional locks and remove context
     */
    private void releaseLockAndRemoveMdlContext() {
        synchronized (mdlContextLock) {
            // remove mdl context
            if (null != this.mdlContext) {
                this.mdlContext.releaseAllTransactionalLocks();
                MdlManager.removeContext(this.mdlContext);
                this.mdlContext = null;
            }
        }
    }

    private void beforeExecution() {
        connection.getExecutionContext().setClientIp("127.0.0.1");
        connection.getExecutionContext().setConnId(connection.getId());
        connection.getExecutionContext().setTxId(txId);
        connection.getExecutionContext().setTraceId(traceId);
        connection.getExecutionContext().setPhySqlId(phySqlId);
        connection.getExecutionContext().setSchemaName(schemaName);
        connection.getExecutionContext().renewMemoryPoolHolder();
        connection.getExecutionContext().setInternalSystemSql(false);
        connection.getExecutionContext().setLogicalSqlStartTimeInMs(System.currentTimeMillis());
        connection.getExecutionContext().setLogicalSqlStartTime(System.nanoTime());
    }

    @Override
    public ITransactionPolicy getTrxPolicy() {
        return trxPolicy;
    }

    @Override
    public void setTrxPolicy(ITransactionPolicy trxPolicy) {
        this.trxPolicy = trxPolicy;
    }

    private void genTxIdAndTraceId() {
        IdGenerator traceIdGen = TrxIdGenerator.getInstance().getIdGenerator();
        StringBuilder sb = new StringBuilder();

        if (this.autoCommit) {
            this.txId = traceIdGen.nextId();
            this.sqlId = 0L;
            sb.append(Long.toHexString(txId));
        } else {
            if (this.txId == null) {
                this.txId = traceIdGen.nextId();
                this.sqlId = 0L;
            }

            this.sqlId++;
            sb.append(Long.toHexString(txId)).append("-").append(this.sqlId);
        }

        this.phySqlId = 0L;
        this.traceId = sb.toString();
    }

    private TResultSet buildResultSet(TResultSet resultSet) {
        try {
            ResultCursor oldCursor = resultSet.getResultCursor();
            InnerResultCursor innerCursor = new InnerResultCursor(false);

            while (true) {
                Row row = oldCursor.doNext();
                if (row != null) {
                    ArrayRow arrayRow = new ArrayRow(row.getParentCursorMeta(), row.getValues().toArray());
                    innerCursor.addRow(arrayRow);
                    innerCursor.setCursorMeta(row.getParentCursorMeta());
                } else {
                    ResultCursor newCursor = new ResultCursor(innerCursor);
                    newCursor.setCursorMeta(innerCursor.getCursorMeta());
                    return new TResultSet(newCursor, resultSet.getExtraCmd());
                }
            }
        } finally {
            try {
                resultSet.close();
            } catch (Throwable e) {
                logger.error("Failed to close ResultSet", e);
            }
        }
    }

    private void fillParams(TPreparedStatement stmt, List<Pair<Integer, ParameterContext>> params) {
        if (params == null) {
            return;
        }
        for (Pair<Integer, ParameterContext> param : params) {
            stmt.setParam(param.getKey(), param.getValue());
        }
    }

    public ITransaction getTransaction() {
        return connection.getTrx();
    }

    @Override
    public void addExecutionContextInjectHook(Consumer<Object> hook) {
        executionContextInjectHooks.add(hook);
    }

    @Override
    public void clearExecutionContextInjectHooks() {
        executionContextInjectHooks.clear();
    }

    private void processExecutionContextHooks(ExecutionContext ec) {
        for (Consumer<Object> hook : executionContextInjectHooks) {
            hook.accept(ec);
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new InnerStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new InnerPreparedStatement(this, sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        // 新事务开始，清掉txId
        if (this.autoCommit != autoCommit) {
            this.txId = null;
        }

        // 自动提交, 清理事务参数
        if (autoCommit) {
            this.connection.setTrxPolicy(null, false);
        }

        this.autoCommit = autoCommit;
        if (this.connection != null) {
            try {
                this.connection.setAutoCommit(autoCommit);
            } catch (SQLException e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return this.autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        this.txId = null;

        if (this.connection != null) {
            connection.commit();

            //如果需要搜集commitTso，判断到是TsoTransaction时，搜集commitTso
            if (recordCommitTso) {
                commitTso = connection.getCommitTso();
            }
        }
    }

    @Override
    public void rollback() throws SQLException {
        this.txId = null;

        if (this.connection != null) {
            connection.rollback();
        }
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        try {
            if (this.statementExecuting.get()) {
                if (connection.isDdlStatement()) {
                    logger.warn("Inner connection Killed By Client While Executing DDL");
                    if (connection.getExecutionContext().getDdlContext() != null) {
                        connection.getExecutionContext().getDdlContext().setClientConnectionResetAsTrue();
                    }
                }
                CobarServer.getInstance().getKillExecutor().execute(() -> {
                    CobarServer.getInstance().getServerExecutor().closeByTraceId(traceId);
                    try {
                        int retry = 0;
                        do {
                            try {
                                connection.kill();
                            } catch (Exception ex) {
                                logger.warn("error when kill", ex);
                            }

                            try {
                                Thread.sleep(5 + retry * 10);
                            } catch (InterruptedException e) {
                                logger.warn(e);
                            }

                        } while (statementExecuting.get() && ++retry < 10);

                        try {
                            connection.close();
                        } catch (Exception ex) {
                            logger.warn("error when kill inner connection close", ex);
                        }

                        if (10 == retry) {
                            logger.error("KILL Inner Connection Failed, retry: " + retry);
                        } else {
                            logger.warn("Inner Connection Killed");
                        }
                    } finally {
                        releaseLockAndRemoveMdlContext();
                    }
                });
            } else {
                if (connection != null) {
                    CobarServer.getInstance().getKillExecutor().execute(() -> {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            logger.warn("error when close inner connection", e);
                        } finally {
                            releaseLockAndRemoveMdlContext();
                        }
                    });
                } else {
                    releaseLockAndRemoveMdlContext();
                }
            }
        } finally {
            InnerConnectionManager.getActiveConnections().remove(id);
        }
    }

    public boolean isStatementExecuting() {
        return statementExecuting.get();
    }

    public TConnection getTConnection() {
        return connection;
    }

    public long getLastActiveTime() {
        return lastActiveTime;
    }

    public long getId() {
        return id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTraceId() {
        return traceId;
    }

    public String getSqlSample() {
        return sqlSample;
    }

    @Override
    public void setTimeZone(String timeZoneId) {
        if ("SYSTEM".equalsIgnoreCase(timeZoneId)) {
            this.connection.resetTimeZone();
            this.extraServerVariables.put("time_zone", timeZoneId);
        } else {
            InternalTimeZone timeZone = TimeZoneUtils.convertFromMySqlTZ(timeZoneId);
            if (timeZone == null) {
                throw new TddlRuntimeException(com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_UNKNOWN_TZ,
                    timeZoneId);
            }
            this.extraServerVariables.put("time_zone", timeZoneId);
            this.connection.setTimeZone(timeZone);
        }
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    private static class InnerResultCursor extends AbstractCursor {
        private final List<Row> rows = new ArrayList<>();
        private Iterator<Row> iter = null;
        private CursorMeta cursorMeta;
        private boolean closed = false;

        public InnerResultCursor(boolean enableOperatorMetric) {
            super(enableOperatorMetric);
        }

        public void addRow(Row row) {
            rows.add(row);
        }

        @Override
        public Row doNext() {
            if (iter == null) {
                iter = rows.iterator();
            }
            if (iter.hasNext()) {
                return iter.next();
            }
            return null;
        }

        @Override
        public List<Throwable> doClose(List<Throwable> exceptions) {
            this.closed = true;
            if (exceptions == null) {
                exceptions = new ArrayList<>();
            }
            return exceptions;
        }

        public boolean isClosed() {
            return this.closed;
        }

        public List<Row> getRows() {
            return rows;
        }

        public CursorMeta getCursorMeta() {
            return cursorMeta;
        }

        public void setCursorMeta(CursorMeta cursorMeta) {
            if (null != cursorMeta) {
                this.returnColumns = cursorMeta.getColumns();
                this.cursorMeta = cursorMeta;
            }
        }
    }

    // ----------------------------------------------------------------------------------------------
    // ---------------------------------------根据需求，逐步实现----------------------------------------
    // ----------------------------------------------------------------------------------------------
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    public void releaseAutoSavepoint() {
        if (null != this.connection && null != this.connection.getTrx()) {
            this.connection.getTrx().releaseAutoSavepoint();
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public String getSchema() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("unsupported operation");
    }

    public void setRecordCommitTso(boolean recordCommitTso) {
        this.recordCommitTso = recordCommitTso;
    }

    public long getCommitTso() {
        return commitTso;
    }
}
