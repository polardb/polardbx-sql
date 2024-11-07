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

package com.alibaba.polardbx.matrix.jdbc;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.TrxIdGenerator;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.ShareReadViewPolicy;
import com.alibaba.polardbx.common.lock.LockingFunctionHandle;
import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.logical.ITPrepareStatement;
import com.alibaba.polardbx.common.logical.ITStatement;
import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.MergeHashMap;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.InsertSplitter;
import com.alibaba.polardbx.executor.PlanExecutor;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.MultiResultCursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.mdl.MdlContext;
import com.alibaba.polardbx.executor.mdl.MdlRequest;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.group.utils.GroupHintParser;
import com.alibaba.polardbx.matrix.jdbc.utils.ByteStringUtil;
import com.alibaba.polardbx.matrix.jdbc.utils.ExceptionUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.ccl.CclManager;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.schema.MysqlSchema;
import com.alibaba.polardbx.optimizer.config.schema.PerformanceSchema;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.MultiDdlContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ExecutionStrategy;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.DirectShardingKeyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateIndex;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.planmanager.PreparedStmtCache;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.alibaba.polardbx.optimizer.statis.SQLTracer;
import com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties;
import com.alibaba.polardbx.optimizer.utils.FailureInjectionFlag;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;
import com.alibaba.polardbx.optimizer.utils.IDistributedTransaction;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.InventoryMode;
import com.alibaba.polardbx.repo.mysql.cursor.ResultSetCursor;
import com.alibaba.polardbx.server.lock.LockingFunctionManager;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.alibaba.polardbx.transaction.trx.ITsoTransaction;
import com.alibaba.polardbx.transaction.trx.ReadOnlyTsoTransaction;
import com.alibaba.polardbx.transaction.trx.TsoTransaction;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.OptimizerHint;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang.StringUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.polardbx.common.utils.GeneralUtil.unixTimeStamp;
import static com.alibaba.polardbx.druid.sql.ast.SqlType.isDDL;
import static com.alibaba.polardbx.druid.sql.ast.SqlType.isDML;
import static com.alibaba.polardbx.druid.util.ByteStringUtil.findTraceIndex;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.changeParameterTypeByTableMetadata;
import static com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties.DDL_STATEMENT;
import static com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties.DML_STATEMENT;
import static com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties.MDL_REQUIRED_POLARDBX;
import static com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties.MODIFY_TABLE;
import static org.apache.calcite.sql.OptimizerHint.COMMIT_ON_SUCCESS;
import static org.apache.calcite.sql.OptimizerHint.ROLLBACK_ON_FAIL;
import static org.apache.calcite.sql.OptimizerHint.TARGET_AFFECT_ROW;
import static org.apache.calcite.sql.SqlKind.UPDATE;

/**
 * @author mengshi.sunmengshi 2013-11-22 下午3:26:06
 * @since 5.0.0
 */
public class TConnection implements ITConnection {

    protected final static Logger logger = LoggerFactory.getLogger(TConnection.class);
    private static final String TRACE = "trace ";
    private final TDataSource dataSource;
    private final List<TStatement> openedStatements = new ArrayList<TStatement>(2);
    private final ServerThreadPool executorService;
    private final ReentrantLock lock = new ReentrantLock();
    private PlanExecutor executor = null;
    private ExecutionContext executionContext = new ExecutionContext();                             // 记录上一次的执行上下文
    private boolean isAutoCommit = true;                                               // jdbc规范，新连接为true
    private boolean readOnly = false;
    private volatile boolean closed;
    private int transactionIsolation = -1;
    private ITransactionPolicy trxPolicy = null;
    // Only set by ServerConnection. For users, it's set by
    // "set batch_insert_policy='split'"
    // Null stands for not set by this connection, and the policy depends on
    // instance property.
    private BatchInsertPolicy batchInsertPolicy = null;
    private long lastExecutionBeginNano = -1;
    private long lastExecutionBeginUnixTime = -1;
    /**
     * 管理这个连接下用到的所有物理连接
     */
    private long lastInsertId;
    // last_insert_id for "getGeneratedKeys" is not necessarily the same as
    // last_insert_id for "select LAST_INSERT_ID()".
    private long returnedLastInsertId;
    private String encoding = null;
    private String sqlMode = null;
    private List<Long> generatedKeys = Collections.synchronizedList(new ArrayList<Long>());
    private ITransaction trx;
    /**
     * 保存 select sql_calc_found_rows 返回的结果
     */
    private long foundRows = 0;
    /**
     * Store the result of ROW_COUNT()
     */
    private long affectedRows = -1;
    private SQLTracer tracer;
    private ColumnarTracer columnarTracer;
    private int socketTimeout = -1;
    // 下推到下层的系统变量，全部小写
    private Map<String, Object> serverVariables = null;
    // 下推到下层的全局系统变量，全部小写
    private Map<String, Object> globalServerVariables = null;
    // 用户定义的变量，全部小写
    private Map<String, Object> userDefVariables = null;
    // 特殊处理的系统变量以及自定义的系统变量，全部小写
    private Map<String, Object> extraServerVariables = null;
    // ConnectionParams 中支持的其他系统变量，全部大写
    private Map<String, Object> connectionVariables = null;
    private String user = null;
    private MdlContext mdlContext = null;
    private String frontendConnectionInfo = null;
    private Boolean asyncDDLPureModeSession = null;
    private InternalTimeZone logicalTimeZone = null;
    /**
     * <pre>
     * For UPDATE statements, the affected-rows value by default is the number of rows actually changed.
     * If you specify the CLIENT_FOUND_ROWS flag to mysql_real_connect() when connecting to mysqld, the affected-rows value is the number of rows “found”; that is, matched by the WHERE clause.
     *
     * For INSERT ... ON DUPLICATE KEY UPDATE statements, the affected-rows value per row is 1 if the row is inserted as a new row, 2 if an existing row is updated, and 0 if an existing row is set to its current values.
     * If you specify the CLIENT_FOUND_ROWS flag, the affected-rows value is 1 (not 0) if an existing row is set to its current values.
     * </pre>
     */
    private boolean clientFoundRows = true;
    /**
     * 和show processlist里显示的ID一致，用于生成mpp的QueryId
     */
    private long id;
    /**
     * 分布式锁是连接级别（会话级别）
     */
    private LockingFunctionHandle lockHandle;
    private String traceId;
    /**
     * 如果是tso事务，保存commit tso
     */
    private long commitTso = -1L;

    /**
     * 事务级别
     */
    private ShareReadViewPolicy shareReadView = ShareReadViewPolicy.DEFAULT;
    /**
     * whether the current statement is a DDL
     */
    private boolean ddlStatement;
    /**
     * the intra group parallelism ,
     * when it is not null means user manually set the variable by "SET GROUP_PARALLELISM=xxx"
     */
    private Long groupParallelism;

    public TConnection(TDataSource ds) {
        this.dataSource = ds;
        this.executor = ds.getExecutor();
        this.executorService = ds.borrowExecutorService();
        this.logicalTimeZone = ds.getLogicalDbTimeZone();
    }

    private static int findTraceIndex(ByteString sql) {
        int i = 0;
        for (; i < sql.length(); ++i) {
            switch (sql.charAt(i)) {
            case ' ':
            case '\t':
            case '\r':
            case '\n':
                continue;
            }
            break;
        }

        if (sql.regionMatches(true, i, TRACE, 0, TRACE.length())) {
            return i + TRACE.length();
        } else {
            return -1;
        }
    }

    private static void checkTransactionParams(IDistributedTransaction trx, ExecutionContext executionContext) {
        // Check injected failure from hint (for test purpose)
        String injectedFailure =
            (String) executionContext.getExtraCmds().get(ConnectionProperties.FAILURE_INJECTION);
        if (injectedFailure != null) {
            trx.setFailureFlag(FailureInjectionFlag.parseString(injectedFailure));
        }
    }

    public boolean isDdlStatement() {
        return ddlStatement;
    }

    public void setDdlStatement(boolean ddlStatement) {
        this.ddlStatement = ddlStatement;
    }

    public boolean getShareReadView() {
        return shareReadView == ShareReadViewPolicy.ON;
    }

    public void setShareReadView(ShareReadViewPolicy shareReadView) {
        if (this.shareReadView == shareReadView) {
            return;
        }
        if (this.trx != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS,
                "Set share read view in the middle of transaction "
                    + "is not allowed. Please do this operation right after transaction begins.");
        }
        if (shareReadView == ShareReadViewPolicy.ON) {
            if (!dnSupportShareReadView()) {
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS, "Data node does not support share read view.");
            }
            ShareReadViewPolicy.checkTxIsolation(transactionIsolation);
        }
        this.shareReadView = shareReadView;
    }

    private ITransactionPolicy loadTrxPolicy(ExecutionContext executionContext) {
        ExecutorContext executorContext = dataSource.getConfigHolder().getExecutorContext();
        StorageInfoManager storageManager = executorContext.getStorageInfoManager();
        ITransactionManager transactionManager = executorContext.getTransactionManager();

        // Use transaction from schema config
        String policyName = executionContext.getParamManager().getString(ConnectionParams.TRANSACTION_POLICY);
        ITransactionPolicy policy = ITransactionPolicy.of(policyName);

        // Use default policy
        if (policy == null) {
            policy = transactionManager.getDefaultDistributedTrxPolicy(executionContext);
            if (policy == null) {
                policy = TransactionAttribute.DEFAULT_TRANSACTION_POLICY_MYSQL56;
            }
        }

        this.trxPolicy = policy;

        if (storageManager.isReadOnly() && (trxPolicy == ITransactionPolicy.XA)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS,
                "Distributed transaction is not supported in read-only PolarDB-X instances");
        }

        return trxPolicy;
    }

    private void loadShareReadView(ExecutionContext executionContext) {
        if (executionContext.getParamManager().getBoolean(ConnectionParams.SHARE_READ_VIEW)) {
            if (dnSupportShareReadView() && ShareReadViewPolicy.supportTxIsolation(transactionIsolation)) {
                this.shareReadView = ShareReadViewPolicy.ON;
            } else {
                this.shareReadView = ShareReadViewPolicy.OFF;
            }
        } else {
            this.shareReadView = ShareReadViewPolicy.OFF;
        }
    }

    /**
     * 执行sql语句的逻辑
     */
    public ResultSet executeSQL(ByteString sql, Parameters params, TStatement stmt,
                                ExecutionContext executionContext) throws SQLException {
        OptimizerContext.setContext(this.dataSource.getConfigHolder().getOptimizerContext());
        // 处理 trace
        try {
            int trace = findTraceIndex(sql);
            if (trace > 0) {
                sql = sql.slice(trace);
                this.tracer = new SQLTracer();
                InternalNodeManager manager = ServiceProvider.getInstance().getServer().getNodeManager();
                this.columnarTracer = new ColumnarTracer(manager.getCurrentNode().getHostPort());
                executionContext.setEnableTrace(true);
            } else if (executionContext.getLoadDataContext() != null && executionContext.getLoadDataContext()
                .getParamManager()
                .getBoolean(ConnectionParams.ENABLE_LOAD_DATA_TRACE)) {
                this.tracer = new SQLTracer();
                executionContext.setEnableTrace(true);
            } else {
                executionContext.setEnableTrace(false);
            }

            executionContext.setTracer(this.tracer);
            executionContext.setColumnarTracer(this.columnarTracer);
            returnedLastInsertId = 0;
            this.generatedKeys.clear();

            ResultCursor resultCursor;
            ResultSet rs = null;

            MergeHashMap<String, Object> extraCmd = new MergeHashMap<>(dataSource.getConnectionProperties());

            if (connectionVariables != null) {
                extraCmd.putAll(connectionVariables);
                if (executionContext.getHintCmds() != null) {
                    for (Map.Entry<String, Object> entry : connectionVariables.entrySet()) {
                        executionContext.getHintCmds().putIfAbsent(entry.getKey(), entry.getValue());
                    }
                }
            }

            buildExtraCommand(sql, extraCmd);

            if (serverVariables == null) {
                serverVariables = new HashMap<String, Object>();
            }
            if (globalServerVariables == null) {
                globalServerVariables = new HashMap<String, Object>();
            }
            if (userDefVariables == null) {
                userDefVariables = new HashMap<String, Object>();
            }
            if (extraServerVariables == null) {
                extraServerVariables = new HashMap<String, Object>();
            }

            // 设置逻辑库默认时区
            if (logicalTimeZone != null) {
                setTimeZoneVariable(serverVariables);
            }
            // 从连接属性中加载dbPriv，只要配了就覆盖其他渠道
            loadDbPriv(executionContext);

            // 处理下group hint
            String groupHint = GroupHintParser.extractTDDLGroupHint(sql);
            if (StringUtils.isNotEmpty(groupHint)) {
                OptimizerContext.getContext(executionContext.getSchemaName()).getStatistics().hintCount++;
                executionContext.setGroupHint(GroupHintParser.buildTddlGroupHint(groupHint));

                transformGroupIndexHintToMasterSlave(groupHint, extraCmd);
            } else {
                executionContext.setGroupHint(null);
            }

            executionContext.setAppName(dataSource.getAppName());
            executionContext.setSchemaName(dataSource.getSchemaName());

            executionContext.setExecutorService(executorService);
            executionContext.setParams(params);
            executionContext.setSql(sql);
            executionContext.setExtraCmds(extraCmd);
            executionContext.setTxIsolation(transactionIsolation);
            executionContext.setSqlMode(sqlMode);
            executionContext.setServerVariables(serverVariables);
            executionContext.setExtraServerVariables(extraServerVariables);
            executionContext.setUserDefVariables(userDefVariables);
            executionContext.setEncoding(encoding);
            executionContext.setConnection(this);
            executionContext.setStressTestValid(dataSource.isStressTestValid());
            executionContext.setSocketTimeout(socketTimeout);
            executionContext.setModifySelect(false);
            executionContext.setModifySelectParallel(false);
            executionContext.setTimeZone(this.logicalTimeZone);
            executionContext.getPrivilegeVerifyItems().clear();
            executionContext.setStorageInfoSupplier(
                (schema) -> ExecutorContext.getContext(schema).getStorageInfoManager().getMergedStorageInfo());
            if (executionContext.isInternalSystemSql()) {
                /**
                 * When the sql is labeled as internal system sql of drds, the
                 * traceId that is get by executionContext.getContextId() has
                 * been invalid （its val is null or the residual traceId of last
                 * sql ), so the traceId must be reset by geting new traceId.
                 */
                String internSqlTraceId = Long.toHexString(TrxIdGenerator.getInstance().nextId());
                executionContext.setTraceId(internSqlTraceId);
                executionContext.setPhySqlId(0L);
            }
            if (!DynamicConfig.getInstance().enableExtremePerformance()) {
                executionContext.setRuntimeStatistics(RuntimeStatHelper.buildRuntimeStat(executionContext));
            } else {
                executionContext.setRuntimeStatistics(RuntimeStatHelper.SHARE_RUNTIME_STATISTICS);
            }
            DdlContext ddlContext = executionContext.getDdlContext();
            if (ddlContext != null && ddlContext.getParentDdlContext() != null) {
                for (Map.Entry<String, Object> entry : ddlContext.getParentDdlContext().getServerVariables()
                    .entrySet()) {
                    executionContext.getServerVariables().putIfAbsent(entry.getKey(), entry.getValue());
                }
                for (Map.Entry<String, Object> entry : ddlContext.getParentDdlContext().getUserDefVariables()
                    .entrySet()) {
                    executionContext.getUserDefVariables().putIfAbsent(entry.getKey(), entry.getValue());
                }
                for (Map.Entry<String, Object> entry : ddlContext.getParentDdlContext().getExtraServerVariables()
                    .entrySet()) {
                    executionContext.getExtraServerVariables().putIfAbsent(entry.getKey(), entry.getValue());
                }
                for (Map.Entry<String, Object> entry : ddlContext.getParentDdlContext().getExtraCmds().entrySet()) {
                    executionContext.getExtraCmds().putIfAbsent(entry.getKey(), entry.getValue());
                }
                if (StringUtils.isNotEmpty(ddlContext.getParentDdlContext().getTimeZone())) {
                    executionContext.setTimeZone(
                        TimeZoneUtils.convertFromMySqlTZ(ddlContext.getParentDdlContext().getTimeZone()));
                }
                executionContext.setEncoding(ddlContext.getParentDdlContext().getEncoding());
            }
            if (this.trx == null || this.trx.isClosed()) {
                beginTransaction();
            } else {
                // In some cases, the transaction can't continue. Rollback
                // statement walks rollback(), not here.
                trx.checkCanContinue();
            }
            executionContext.setTransaction(trx);

            Throwable exOfResultCursor = null;
            try {
                AtomicBoolean trxPolicyModified = new AtomicBoolean(false);

                BatchInsertPolicy policy = getBatchInsertPolicy(extraCmd);
                if (InsertSplitter.needSplit(sql, policy, executionContext)) {
                    executionContext.setDoingBatchInsertBySpliter(true);
                    InsertSplitter insertSplitter = new InsertSplitter();
                    // In batch insert, update transaction policy in writing broadcast table is also needed.
                    resultCursor = insertSplitter.execute(sql,
                        executionContext,
                        policy,
                        (ByteString s) -> executeQuery(s, executionContext, trxPolicyModified),
                        (ByteString s) -> executeQuery(s, executionContext, null));
                } else {
                    resultCursor = executeQuery(sql, executionContext, trxPolicyModified);
                }

                if (trxPolicyModified.get()) {
                    this.trxPolicy = null;
                }
            } catch (Throwable e) {
                if (isRollbackOnFail()) {
                    rollback();
                }
                // 这里是优化器的异常
                exOfResultCursor = e;
                boolean collectSqlErrorInfo = executionContext.getParamManager().getBoolean(
                    ConnectionParams.COLLECT_SQL_ERROR_INFO);

                if (collectSqlErrorInfo) {
                    resultCursor = buildSqlErrorInfoCursor(e, sql.toString());
                } else {
                    throw GeneralUtil.nestedException(e);
                }
            }

            if (isCommitOnSuccess()) {
                commit();
            }

            if (resultCursor instanceof ResultSetCursor) {
                rs = ((ResultSetCursor) resultCursor).getResultSet();
            } else if (resultCursor instanceof MultiResultCursor) {
                rs = new TMultiResultSet((MultiResultCursor) resultCursor, extraCmd);
            } else {
                rs = new TResultSet(resultCursor, extraCmd);
            }

            if (exOfResultCursor == null) {
                boolean collectSqlErrorInfo =
                    executionContext.getParamManager().getBoolean(ConnectionParams.COLLECT_SQL_ERROR_INFO);
                if (collectSqlErrorInfo) {
                    Throwable exOfRs = null;
                    try {
                        // 这里有可能产生执行器的异常
                        rs.next();
                    } catch (Throwable e) {
                        exOfRs = e;
                    } finally {
                        if (rs != null) {
                            rs.close();
                        }
                    }
                    resultCursor = buildSqlErrorInfoCursor(exOfRs, sql.toString());
                    rs = new TResultSet(resultCursor, extraCmd);
                }
            }

            return rs;
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        } finally {
            if (trx != null && trx.getInventoryMode() != null) {
                trx.getInventoryMode().resetInventoryMode();
            }
        }
    }

    /**
     * Transforms a group index hint into Master/Slave connection properties.
     *
     * @param groupHint The group index hint string, e.g., "groupindex:0" or "groupindex:1".
     * @param extraCmd A map to store additional commands, including connection properties.
     */
    protected static void transformGroupIndexHintToMasterSlave(String groupHint, Map<String, Object> extraCmd) {
        // Check for non-null parameters
        if (groupHint == null || extraCmd == null) {
            return;
        }

        switch (groupHint.toLowerCase()) {
        case "groupindex:0":
            extraCmd.put(ConnectionProperties.MASTER, Boolean.TRUE);
            break;
        case "groupindex:1":
            extraCmd.put(ConnectionProperties.SLAVE, Boolean.TRUE);
            break;
        default:
        }
    }

    /**
     * Separate execute(sql, ec) into two parts: plan and execute. If it's
     * writing into broadcast table and has no transaction, a new transaction
     * will be open.
     */
    private ResultCursor executeQuery(ByteString sql, ExecutionContext executionContext,
                                      AtomicBoolean trxPolicyModified) {
        if (null == executionContext.getParams()) {
            executionContext.setParams(new Parameters());
        }

        // Get all meta version before optimization
        final long[] metaVersions = MdlContext.snapshotMetaVersions();

        final Parameters originParams = executionContext.getParams().clone();
        ExecutionPlan plan = Planner.getInstance().plan(sql, executionContext);
        if (!executionContext.isFlashbackArea()) {
            executionContext.setFlashbackArea(plan.isFlashbackArea());
        }

        databaseReadOnlyCheck(plan);
        instanceReadOnlyCheck();
        // [mysql behavior]
        // comment can be executed, sql example :  "-- I can execute"
        if (plan == null) {
            return new ResultCursor(new AffectRowCursor(0));
        }
        SqlNode ast = plan.getAst();
        if (ast != null) {
            SqlKind sqlKind = ast.getKind();
            boolean saveOriginSql =
                (sqlKind == SqlKind.CREATE_TABLE || sqlKind == SqlKind.CREATE_INDEX || sqlKind == SqlKind.ALTER_TABLE);
            if (saveOriginSql) {
                if (ast instanceof SqlCreateTable) {
                    ((SqlCreateTable) ast).setOriginalSql(sql.toString());
                } else if (ast instanceof SqlCreateIndex) {
                    ((SqlCreateIndex) ast).setOriginalSql(sql.toString());
                    RelNode relNode = plan.getPlan();
                    if (relNode != null && relNode instanceof LogicalCreateIndex) {
                        if (((LogicalCreateIndex) relNode).relDdl != null) {
                            SqlNode createIndex = ((LogicalCreateIndex) relNode).relDdl.sqlNode;
                            if (createIndex != null) {
                                ((SqlCreateIndex) createIndex).setOriginalSql(sql.toString());
                            }
                        }
                    }
                } else if (ast instanceof SqlAlterTable) {
                    ((SqlAlterTable) ast).setOriginalSql(sql.toString());
                }
            }
        }

        this.lastExecutionBeginNano = System.nanoTime();
        this.lastExecutionBeginUnixTime = unixTimeStamp();

        this.setDdlStatement(plan.is(DDL_STATEMENT));

        if (!isAutoCommit && this.isDdlStatement()) {
            try {
                // DDL statement causes an implicit commit
                commit();

                // Attention: the current txid has been 'used' so we need to generate one
                executionContext.setTxId(0L);

                // Start an auto-commit transaction ONLY for this DDL statement
                beginTransaction(true);
                executionContext.setTransaction(trx);
                executionContext.setAutoCommit(true);

            } catch (SQLException ex) {
                throw new RuntimeException("Failed to commit transaction implicitly", ex);
            }
        }

        // If parameter 'trxPolicyModified' is provided,
        // Update transaction policy for modify of broadcast table and
        // of table with global secondary index
        // Or Update transaction policy for columnar read
        if (trxPolicyModified != null) {
            trxPolicyModified.set(updateTransactionAndConcurrentPolicy(plan, executionContext));
            if (PlanManagerUtil.canOptByForcePrimary(plan, executionContext) && executionContext.isTsoTransaction()) {
                // If this plan can be optimized, rebuild plan.
                plan = rebuildPlan(sql, executionContext, originParams, false);
            }
        }

        final boolean enableMdl = executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_MDL);
        // For DML, meta data must not be modified after optimization and before mdl acquisition
        final boolean requireMdl;

        requireMdl = plan.is(MDL_REQUIRED_POLARDBX);
        // For test purpose
        final boolean testRebuild = executionContext.getParamManager().getBoolean(ConnectionParams.ALWAYS_REBUILD_PLAN);

        long mdlWaitTime = 0;
        if (requireMdl && enableMdl) {
            if (!isClosed()) {
                // Acquire meta data lock for each statement modifies table data
                mdlWaitTime = acquireTransactionalMdl(sql, plan, executionContext);
            }

            if (isClosed()) {
                // TConnection already closed before execute
                releaseTransactionalMdl(executionContext);
            }

            // If any meta is modified during optimization, rebuild plan
            if (metaVersionChanged(plan, metaVersions, executionContext) || testRebuild) {
                plan = rebuildPlan(sql, executionContext, originParams, true);

                // Update transaction policy for modify of broadcast table and
                // of table with global secondary index
                if (trxPolicyModified != null) {
                    trxPolicyModified.set(updateTransactionAndConcurrentPolicy(plan, executionContext));
                    if (PlanManagerUtil.canOptByForcePrimary(plan, executionContext)
                        && executionContext.isTsoTransaction()) {
                        // If this plan can be optimized, rebuild plan.
                        plan = rebuildPlan(sql, executionContext, originParams, false);
                    }
                }
            }
        }

        // Trx object SHOULD NOT be changed from here.
        checkSqlLogBinXForbiddenAutoCommit();

        trx.setMdlWaitTime(mdlWaitTime);
        if (0 == trx.getStartTimeInMs()) {
            trx.setStartTimeInMs(executionContext.getLogicalSqlStartTimeInMs());
            trx.setStartTime(executionContext.getLogicalSqlStartTime());
        }

        trx.setSqlStartTime(executionContext.getLogicalSqlStartTime());

        if (trx instanceof IDistributedTransaction) {
            checkTransactionParams((IDistributedTransaction) trx, executionContext);
        } else {
            final boolean forceCheckTrx = executionContext.getParamManager().getBoolean(
                ConnectionParams.DISTRIBUTED_TRX_REQUIRED);
            if (forceCheckTrx) {
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_DISTRIBUTED_TRX_REQUIRED);
            }
        }

        if (trx instanceof ReadOnlyTsoTransaction && plan.getPlanProperties().get(MODIFY_TABLE)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_CANNOT_EXECUTE_IN_RO_TRX);
        }

        if (trx instanceof ITsoTransaction && !plan.getTableSet().isEmpty()) {
            // Update read ts for READ-COMMITTED isolation level
            ((ITsoTransaction) trx).updateSnapshotTimestamp();
        }

        executionContext.setXplanStat(plan.isForbidXplan());
        executionContext.setFinalPlan(plan);

        changeParameterTypeByTableMetadata(executionContext, plan);

        if (!executionContext.isExecutingPreparedStmt()) {
            PolarPrivilegeUtils.checkPrivilege(plan, executionContext);
            PolarPrivilegeUtils.checkLBACColumnAccess(plan, executionContext);
        }
        ScaleOutPlanUtil.checkDDLPermission(plan, executionContext);
        invalidInventoryMode(plan);

        if (executionContext.getCclContext() == null) {
            CclManager.getService().begin(executionContext);
        }

        ResultCursor resultCursor = executor.execute(plan, executionContext);
        updateTableStatistic(plan, resultCursor, executionContext);
        return resultCursor;
    }

    private void invalidInventoryMode(ExecutionPlan executionPlan) {
        trx.setInventoryMode(null);
        if (executionPlan == null) {
            return;
        }
        final RelNode plan = executionPlan.getPlan();
        if (plan == null) {
            return;
        }

        if (plan instanceof AbstractRelNode) {
            final OptimizerHint hintContext = ((AbstractRelNode) plan).getHintContext();

            if (hintContext != null && trx != null) {
                InventoryMode inventoryMode = new InventoryMode();
                if (hintContext.containsHint(COMMIT_ON_SUCCESS)) {
                    inventoryMode.enableCommitOnSuccess();
                }

                if (hintContext.containsHint(ROLLBACK_ON_FAIL)) {
                    inventoryMode.enableRollbackOnFail();
                }

                if (hintContext.containsHint(TARGET_AFFECT_ROW)) {
                    inventoryMode.enableTargetAffectRow();
                }
                if (inventoryMode.isInventoryHint()) {
                    PlannerContext plannerContext = PlannerContext.getPlannerContext(plan);
                    if (plannerContext != null && plannerContext.getSqlKind() == UPDATE) {
                        if ((executionContext.isModifyCrossDb() || executionContext.isModifyBroadcastTable())) {
                            throw new TddlRuntimeException(ErrorCode.ERR_IVENTORY_HINT_NOT_SUPPORT_CROSS_SHARD,
                                "Inventory hint is not allowed when the transaction involves more than one group!");
                        }
                    } else {
                        throw new RuntimeException("Inventory hint can only be used within update!");
                    }
                    trx.setInventoryMode(inventoryMode);
                }
            }
        }
    }

    private boolean metaVersionChanged(ExecutionPlan plan, long[] metaVersions,
                                       ExecutionContext executionContext) {
        if (executionContext.getSchemaManagers().values().stream().anyMatch(s -> s.isExpired())) {
            return true;
        } else {
            return false;
        }
    }

    private ExecutionPlan rebuildPlan(ByteString sql, ExecutionContext executionContext,
                                      Parameters originParams,
                                      boolean causedByMetaChanged) {
        if (executionContext.isExecutingPreparedStmt() || originParams.getBatchSize() <= 0 && GeneralUtil
            .isEmpty(originParams.getFirstParameter())) {
            // rebuild plan during executing preparedStmt should reset origin params
            // Resume empty parameters for insert
            executionContext.setParams(originParams.clone());
        }

        if (causedByMetaChanged) {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format("[{0}] Rebuild plan by meta data modified, SQL: {1} , Param: {2}",
                    executionContext.getTraceId(),
                    sql,
                    GsiUtils.rowToString(executionContext.getParams())));

            executionContext.refreshTableMeta();
        }

        ExecutionPlan plan = Planner.getInstance().plan(sql, executionContext);
        this.lastExecutionBeginNano = System.nanoTime();
        this.lastExecutionBeginUnixTime = unixTimeStamp();
        return plan;
    }

    public void feedBackPlan(Throwable ex) {
        if (!DynamicConfig.getInstance().enableClearFailedPlan()) {
            return;
        }
        if (executionContext.getExplain() != null) {
            return;
        }
        if (ex != null) {
            ExecutionPlan plan = executionContext.getFinalPlan();
            if (plan != null && !plan.isDirectShardingKey()) {
                // only deal with plan not null and not direct sharding point select
                if (plan.getCacheKey() != null) {
                    PlanManager.getInstance().invalidateCache(plan, ex);
                }
            }
        }
    }

    public Pair<Integer, Integer> updatePlanManagementInfo(long lastExecuteUnixTime,
                                                           double executionTimeInSeconds, ExecutionContext ec,
                                                           Throwable ex) {
        if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_SPM)) {
            return null;
        }
        if (executionContext.getExplain() != null) {
            return null;
        }
        if (!ConfigDataMode.isMasterMode()) {
            return null;
        }
        ExecutionPlan executionPlan = executionContext.getFinalPlan();
        if (executionPlan != null && !executionPlan.isDirectShardingKey()) {
            // only deal with plan not null and not direct sharding point select
            RelNode plan = executionPlan.getPlan();
            PlannerContext plannerContext = PlannerContext.getPlannerContext(plan);

            BaselineInfo baselineInfo = plannerContext.getBaselineInfo();
            PlanInfo planInfo = plannerContext.getPlanInfo();
            if (planInfo != null && baselineInfo != null) {
                PlanCache.getInstance().xplanFeedBack(executionPlan, executionContext);
                synchronized (baselineInfo) {
                    PlanManager.getInstance().doEvolution(executionContext.getSchemaName(), baselineInfo, planInfo,
                        lastExecuteUnixTime, executionTimeInSeconds, ec, ex);
                    return new Pair<>(baselineInfo.getId(), planInfo.getId());
                }
            }
            // only deal with plan not null and not direct sharding point select
            if (executionPlan.getCacheKey() != null) {
                PlanManager.getInstance()
                    .feedBack(executionPlan, ex,
                        ((RuntimeStatistics) executionContext.getRuntimeStatistics()).toSketch(), executionContext);
            }
        }

        return null;
    }

    private void updateTableStatistic(ExecutionPlan executionPlan, ResultCursor resultCursor,
                                      ExecutionContext executionContext) {
        if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_STATISTIC_FEEDBACK)) {
            return;
        }
        if (executionContext.getExplain() != null) {
            return;
        }
        RelNode plan = executionPlan.getPlan();
        SqlKind sqlKind = PlannerContext.getPlannerContext(plan).getSqlKind();
        String logicalTableName;
        List<String> tableNameList;
        if (plan instanceof LogicalInsert) {
            tableNameList = ((LogicalInsert) plan).getTargetTableNames();
        } else if (plan instanceof LogicalModifyView) {
            tableNameList = ((LogicalModifyView) plan).getTableNames();
        } else if (plan instanceof DirectTableOperation) {
            tableNameList = ((DirectTableOperation) plan).getLogicalTableNames();
        } else if (plan instanceof BroadcastTableModify) {
            tableNameList = ((BroadcastTableModify) plan).getDirectTableOperation().getTableNames();
        } else if (plan instanceof SingleTableOperation) {
            tableNameList = ((SingleTableOperation) plan).getLogicalTableNames();
        } else {
            return;
        }

        if (tableNameList != null && tableNameList.size() == 1) {
            logicalTableName = tableNameList.get(0);
        } else {
            return;
        }

        long affectRow = 0;
        Cursor subCursor = resultCursor.getCursor();
        if (subCursor instanceof AffectRowCursor) {
            AffectRowCursor affectRowCursor = (AffectRowCursor) subCursor;
            int[] affectRowArr = affectRowCursor.getAffectRows();
            for (int j = 0; j < affectRowArr.length; j++) {
                affectRow += affectRowArr[j];
            }
        } else {
            return;
        }

        if (sqlKind == SqlKind.INSERT) {
            StatisticManager.getInstance()
                .addUpdateRowCount(executionContext.getSchemaName(), logicalTableName, affectRow);
        }
        if (sqlKind == SqlKind.DELETE) {
            StatisticManager.getInstance()
                .addUpdateRowCount(executionContext.getSchemaName(), logicalTableName, -affectRow);
        }
    }

    /**
     * update transaction and concurrent policy for autocommit broadcast/gsi
     * table write operation
     *
     * @return is transaction policy updated
     */
    private boolean updateTransactionAndConcurrentPolicyForDml(ExecutionPlan plan, ExecutionContext ec) {
        if (isAutoCommit && ec.isForbidAutoCommitTrx() && plan.is(DML_STATEMENT)) {
            // Init a non-autocommit trx for DML.
            ITransaction trx = forceInitTransaction(ec, false, true);
            ec.setTransaction(trx);
            return true;
        }

        final BitSet properties = plan.getPlanProperties();
        final boolean currentModifyBroadcast = properties.get(ExecutionPlanProperties.MODIFY_BROADCAST_TABLE);

        boolean modifyBroadcastTable = currentModifyBroadcast;
        boolean modifyGsiTable = properties.get(ExecutionPlanProperties.MODIFY_GSI_TABLE);
        boolean modifyShardingColumn = properties.get(ExecutionPlanProperties.MODIFY_SHARDING_COLUMN);
        boolean modifyScaleOutGroup = properties.get(ExecutionPlanProperties.MODIFY_SCALE_OUT_GROUP);
        boolean modifyCrossDb = properties.get(ExecutionPlanProperties.MODIFY_CROSS_DB);
        boolean modifyForeignKey = properties.get(ExecutionPlanProperties.MODIFY_FOREIGN_KEY);
        boolean dmlWithTransaction = ec.getParamManager().getBoolean(ConnectionParams.COMPLEX_DML_WITH_TRX);
        boolean modifyReplicateTable = plan.getPlanProperties().get(ExecutionPlanProperties.REPLICATE_TABLE);
        if (currentModifyBroadcast && !modifyGsiTable) {
            ec.getExtraCmds().put(ConnectionProperties.FIRST_THEN_CONCURRENT_POLICY, true);

        }
        boolean modifyTable = properties.get(ExecutionPlanProperties.MODIFY_TABLE);
        boolean selectWithLock = properties.get(ExecutionPlanProperties.SELECT_WITH_LOCK);
        boolean isSelect = plan.getAst().getKind() == SqlKind.SELECT;

        ExecutionStrategy executionStrategy = ExecutionStrategy.fromHint(ec);
        boolean useLogicalExecution = executionStrategy == ExecutionStrategy.LOGICAL;

        final boolean currentReadOnly = ec.isReadOnly();

        boolean readOnlyUpdated = false;
        if (!currentReadOnly && isAutoCommit && trxPolicy == ITransactionPolicy.TSO && !(modifyTable || selectWithLock)
            && isSelect) {
            readOnlyUpdated = true;
            ec.setReadOnly(true);
        }

        ec.setModifyBroadcastTable(modifyBroadcastTable);
        ec.setModifyGsiTable(modifyGsiTable);
        ec.setModifyShardingColumn(modifyShardingColumn);
        ec.setModifyScaleOutGroup(modifyScaleOutGroup);
        ec.setModifyCrossDb(modifyCrossDb);
        ec.setModifyForeignKey(modifyForeignKey);
        executionContext.setModifyReplicateTable(modifyReplicateTable);

        final boolean enableMultiWrite = modifyBroadcastTable || modifyGsiTable || modifyScaleOutGroup
            || modifyShardingColumn || modifyReplicateTable || useLogicalExecution || modifyForeignKey;

        if (!enableMultiWrite && !(modifyCrossDb && dmlWithTransaction) && !readOnlyUpdated) {
            return false;
        }

        // If it's already in a transaction
        if (!isAutoCommit || !dataSource.getConfigHolder().getExecutorContext().getStorageInfoManager().supportXA()) {
            if (modifyGsiTable) {
                // On mysql 5.6, if STORAGE_CHECK_ON_GSI=Off, skip
                if (!dataSource.getConfigHolder().getExecutorContext().getStorageInfoManager().supportXA()
                    && !ec.getParamManager().getBoolean(ConnectionParams.STORAGE_CHECK_ON_GSI)) {
                    return false;
                }
                // Only support XA/TSO trx
                if (!(ec.getTransaction().isStrongConsistent())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_ONLY_SUPPORT_XA);
                }
            }
            return false;
        }

        String policyName = ec.getParamManager().getString(ConnectionParams.TRANSACTION_POLICY);
        ITransactionPolicy policy = ITransactionPolicy.of(policyName);
        if (null != policy) {
            final boolean isDistributedTransaction =
                policy.getTransactionType(false, false, false, false)
                    .isA(TransactionClass.DISTRIBUTED_TRANSACTION);
            if (!isDistributedTransaction) {
                ec.getExtraCmds().put(ConnectionProperties.TRANSACTION_POLICY, null);
            }
        }

        // create new transaction object
        ITransaction trx = forceInitTransaction(ec, false);
        ec.setTransaction(trx);

        return true;
    }

    /**
     * update transaction and concurrent policy for columnar read
     *
     * @return is transaction policy updated
     */
    private boolean updateTransactionAndConcurrentPolicyForColumnar(ExecutionContext ec) {
        if (!ec.isUseColumnar()) {
            return false;
        }

        ITransaction trx = forceInitTransaction(ec, true);
        ec.setTransaction(trx);

        return true;
    }

    /**
     * update transaction and concurrent policy
     *
     * @return is transaction policy updated
     */
    private boolean updateTransactionAndConcurrentPolicy(ExecutionPlan plan, ExecutionContext ec) {
        if (updateTransactionAndConcurrentPolicyForColumnar(ec)) {
            return true;
        }

        return updateTransactionAndConcurrentPolicyForDml(plan, ec);
    }

    /**
     * acquire mdl for each table modified
     *
     * @return mdl wait nano time
     */
    private long acquireTransactionalMdl(final ByteString sql, final ExecutionPlan plan, final ExecutionContext ec) {
        final long startTime = System.nanoTime();
        final MdlContext mdlContext = getMdlContext();
        if (null == mdlContext) {
            // For sql statement mdl can never be null
            // But for internal use like DbLock.init(), mdlContext always null
            return 0;
        }

        if (!ec.getParamManager().getBoolean(ConnectionParams.ENABLE_MDL)) {
            return 0;
        }

        // disable mdl for no_transaction
        // no_transaction策略下，即使autocommit=false，每条语句也会生成一个新的AutoCommitTransaction对象
        // 每个AutoCommitTransaction拥有不同的id（MDL锁根据id来管理）
        // 这样最终TConnection.commit内进行MDL的释放的时候，只能释放最后一个AutoCommitTransaction对应的MDL，从而导致MDL泄露
        // 因此直接关掉NO_TRANSACTION的MDL，目前该事务策略主要被datax使用
        if (this.trxPolicy == ITransactionPolicy.NO_TRANSACTION) {
            return 0;
        }

        final Long trxId = ec.getTransaction().getId();

        Set<Pair<String, String>> tables = plan.getTableSet();
        if (tables != null) {
            for (Pair<String, String> table : tables) {
                String schemaName = table.getKey();
                if (schemaName == null) {
                    schemaName = executionContext.getSchemaName();
                }

                if (InformationSchema.NAME.equalsIgnoreCase(schemaName) ||
                    PerformanceSchema.NAME.equalsIgnoreCase(schemaName) ||
                    MysqlSchema.NAME.equalsIgnoreCase(schemaName)) {
                    return 0; // These schemas never change.
                }

                TableMeta meta = ec.getSchemaManager(schemaName).getTableWithNull(table.getValue());

                if (meta != null) {
                    boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
                    if (isNewPartDb && meta.getTableGroupDigestList() != null) {
                        mdlContext.acquireLock(MdlRequest.getTransactionalDmlMdlRequest(trxId,
                            schemaName, meta.getTableGroupDigest(trxId),
                            executionContext.getTraceId(), sql, frontendConnectionInfo));
                    }
                    if (!isNewPartDb && meta.getSchemaDigestList() != null) {
                        mdlContext.acquireLock(MdlRequest.getTransactionalDmlMdlRequest(trxId,
                            schemaName, meta.getSchemaDigest(trxId),
                            executionContext.getTraceId(), sql, frontendConnectionInfo));
                    }
                    if (executionContext.getLoadDataContext() != null) {
                        // only for ossLoadData, get writeLock first , then downgrade to readLock
                        mdlContext.acquireLock(MdlRequest.getTransactionalOssLoadDataMdlRequest(trxId,
                            schemaName, meta.getDigest(),
                            executionContext.getTraceId(), sql, frontendConnectionInfo));
                    }
                    mdlContext.acquireLock(MdlRequest.getTransactionalDmlMdlRequest(trxId,
                        schemaName, meta.getDigest(),
                        executionContext.getTraceId(), sql, frontendConnectionInfo));

                }
            }
            return System.nanoTime() - startTime;
        }
        return 0;
    }

    private void checkSqlLogBinXForbiddenAutoCommit() {
        Object sqlLoginBin = executionContext.getExtraServerVariables().get(ICdcManager.SQL_LOG_BIN);
        if (sqlLoginBin != null && !(Boolean) sqlLoginBin) {
            boolean isDML = isDML(executionContext.getSqlType());
            if (this.isAutoCommit && isDML) {
                throw new TddlRuntimeException(ErrorCode.ERR_SQL_LOG_BIN_NOT_SUPPORT_AUTO_COMMIT,
                    "sql_log_bin_x not support auto commit transaction!");
            }
            this.trxPolicy = TransactionAttribute.DEFAULT_IGNORE_BINLOG_TRANSACTION;
        }
    }

    private boolean instanceReadOnly() {
//        Object val = DynamicConfig.getInstance().isInstanceReadOnly();
//        if (val != null) {
//            return "1".equals(val) || "true".equalsIgnoreCase(val.toString()) || "on".equalsIgnoreCase(val.toString());
//        }
        return DynamicConfig.getInstance().isInstanceReadOnly();
    }

    private void instanceReadOnlyCheck() {
        boolean isDML = isDML(executionContext.getSqlType());
        boolean isDDL = isDDL(executionContext.getSqlType());
        if (!isDML && !isDDL) {
            return;
        }
        if (!instanceReadOnly()) {
            return;
        }

        // inner ddl
        if (!executionContext.isPrivilegeMode()) {
            return;
        }

        // root
        if (executionContext.isGod()) {
            return;
        }

        // carry super_write session variables
        String superWriteValue =
            StringUtils.upperCase(executionContext.getParamManager().getString(ConnectionParams.SUPER_WRITE));
        if ("1".equals(superWriteValue) || "ON".equals(superWriteValue) || "TRUE".equals(superWriteValue)) {
            return;
        }

        throw new TddlRuntimeException(ErrorCode.ERR_INSTANCE_READ_ONLY_OPTION_NOT_SUPPORT);
    }

    //when database status is readOnly, we forbid all dml sql
    public void databaseReadOnlyCheck(final ExecutionPlan plan) {
        final SqlNode sqlNode = plan.getAst();

        boolean isDML = sqlNode != null && SqlKind.DML.contains(sqlNode.getKind());
        boolean isDDL = sqlNode != null && SqlKind.DDL.contains(sqlNode.getKind());
        if (!isDML && !isDDL) {
            return;
        }

        if (ConfigDataMode.isReadOnlyMode()) {
            if (isDML || isDDL) {
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_CANNOT_EXECUTE_IN_RO_TRX);
            }
        }
        Set<Pair<String, String>> tables = plan.getTableSet();
        if (tables != null) {
            for (Pair<String, String> table : tables) {
                String schemaName = table.getKey();
                if (schemaName == null) {
                    schemaName = executionContext.getSchemaName();
                }
                if (DbInfoManager.getInstance().ifDatabaseIsReadOnly(schemaName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("dml/ddl error because database [%s] is read only now", schemaName));
                }

            }
        }
    }

    private void refreshTableMeta() {
        if (executionContext != null) {
            executionContext.refreshTableMeta();
        }
    }

    /**
     * release transactional mdl by transaction id
     */
    private void releaseTransactionalMdl(ExecutionContext ec) {
        final MdlContext mdlContext = getMdlContext();
        if (null == mdlContext) {
            // For sql statement mdl can never be null
            // But for internal use like DbLock.init(), mdlContext always null
            return;
        }

        if (null == ec) {
            // Defensive. When called in TConnection.close, it's possible that ec is null
            return;
        }

        if (ec.getTransaction() == null) {
            return;
        }

        if (!ec.getParamManager().getBoolean(ConnectionParams.ENABLE_MDL)) {
            return;
        }

        // disable mdl for no_transaction
        if (this.trxPolicy == ITransactionPolicy.NO_TRANSACTION) {
            return;
        }

        mdlContext.releaseTransactionalLocks(ec.getTransaction().getId());
    }

    /**
     * Load db privilege from connection properties if exists.
     */
    private void loadDbPriv(ExecutionContext executionContext) {
        PrivilegeContext pc = executionContext.getPrivilegeContext();
        if (pc != null) {
            DbPriv dbPriv = pc.getDatabasePrivilege();
            long dbPrivFromProperties = executionContext.getParamManager().getLong(ConnectionParams.DB_PRIV);
            if (dbPrivFromProperties > 0) {
                // 只要配了就覆盖其他渠道
                if (dbPriv == null) {
                    dbPriv = new DbPriv(dataSource.getSchemaName());
                    pc.setDatabasePrivilege(dbPriv);
                }
                dbPriv.loadPriv(dbPrivFromProperties);
            }
        }
    }

    private ExecutionContext prepareExecutionContext() throws SQLException {
        PrivilegeContext privilegeContext = null;

        long connId = 0;
        long txId = 0;
        String traceId = null;
        String clientIp = null;
        boolean testMode = false;
        Long phySqlId = 0L;
        boolean rescheduled = false;
        boolean isExecutingPreparedStmt = false;
        PreparedStmtCache preparedStmtCache = null;
        DdlContext ddlContext = null;
        boolean usingHint = false;
        String partitionHint = null;

        if (this.executionContext != null) {
            privilegeContext = this.executionContext.getPrivilegeContext();
            connId = this.executionContext.getConnId();
            txId = this.executionContext.getTxId();
            traceId = this.executionContext.getTraceId();
            clientIp = this.executionContext.getClientIp();
            testMode = this.executionContext.isTestMode();
            phySqlId = this.executionContext.getPhySqlId();
            rescheduled = this.executionContext.isRescheduled();
            isExecutingPreparedStmt = this.executionContext.isExecutingPreparedStmt();
            preparedStmtCache = this.executionContext.getPreparedStmtCache();
            ddlContext = this.executionContext.getDdlContext();
            usingHint = this.executionContext.isUseHint();
            partitionHint = this.executionContext.getPartitionHint();
        }
        if (privilegeContext == null) {
            privilegeContext = new PrivilegeContext();
            privilegeContext.setSchema(dataSource.getSchemaName());
        }
        if (isAutoCommit) {
            // 即使为autoCommit也需要记录
            // 因为在JDBC规范中，只要在statement.execute执行之前,设置autoCommit=false都是有效的
            this.executionContext = new ExecutionContext();

        } else {
            if (this.executionContext == null) {
                this.executionContext = new ExecutionContext();
            } else {
                this.executionContext.setMultiDdlContext(new MultiDdlContext());
            }

            if (this.executionContext.isAutoCommit()) {
                this.executionContext.setAutoCommit(false);
            }
        }

        this.executionContext.setSchemaName(dataSource.getSchemaName());
        this.executionContext.setPrivilegeContext(privilegeContext);
        this.executionContext.setPhysicalRecorder(this.dataSource.getPhysicalRecorder());
        this.executionContext.setRecorder(this.dataSource.getRecorder());
        this.executionContext.setConnection(this);
        this.executionContext.setStats(this.dataSource.getStatistics());
        this.executionContext.setExplain(null);
        this.executionContext.setSqlType(null);
        this.executionContext.setExecuteMode(ExecutorMode.NONE);
        this.executionContext.setModifyBroadcastTable(false);
        this.executionContext.setModifyGsiTable(false);
        this.executionContext.setModifySelect(false);
        this.executionContext.setModifySelectParallel(false);
        this.executionContext.setConnId(connId);
        this.executionContext.setClientIp(clientIp);
        this.executionContext.setTestMode(testMode);
        this.executionContext.setTxId(txId);
        this.executionContext.setTraceId(traceId);
        this.executionContext.setPhySqlId(phySqlId);
        this.executionContext.setCluster(
            CobarServer.getInstance().getConfig().getCluster());
        this.executionContext.setRescheduled(rescheduled);
        this.executionContext.setReturning(null);
        this.executionContext.setOptimizedWithReturning(false);
        this.executionContext.setClientFoundRows(isClientFoundRows());
        this.executionContext.setUseHint(usingHint);
        this.executionContext.setPartitionHint(partitionHint);

        this.executionContext.setIsExecutingPreparedStmt(isExecutingPreparedStmt);
        this.executionContext.setPreparedStmtCache(preparedStmtCache);
        this.executionContext.setDdlContext(ddlContext);
        this.executionContext.setCursorFetchMode(false);
        if (ddlContext != null && ddlContext.getParentDdlContext() != null) {
            DdlContext parentDdlContext = ddlContext.getParentDdlContext();
            executionContext.setServerVariables(parentDdlContext.getServerVariables());
            executionContext.setUserDefVariables(parentDdlContext.getUserDefVariables());
            executionContext.setExtraCmds(parentDdlContext.getExtraCmds());
            executionContext.setExtraServerVariables(parentDdlContext.getExtraServerVariables());
            if (StringUtils.isNotEmpty(parentDdlContext.getTimeZone())) {
                executionContext.setTimeZone(TimeZoneUtils.convertFromMySqlTZ(parentDdlContext.getTimeZone()));
            }
            executionContext.setEncoding(parentDdlContext.getEncoding());
        }

        this.executionContext.setNeedAutoSavepoint(false);
        return this.executionContext;
    }

    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return isAutoCommit;
    }

    /*
     * ========================================================================
     * JDBC事务相关的autoCommit设置、commit/rollback、TransactionIsolation等
     * ======================================================================
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        if (this.isAutoCommit == autoCommit) {
            // 先排除两种最常见的状态,true==true 和false == false: 什么也不做
            return;
        }
        this.isAutoCommit = autoCommit;

        if (this.trx != null) {
            try {
                this.trx.commit();
            } catch (TddlRuntimeException ex) {
                // Ignore ERR_TRANS_TERMINATED in case of connection pool error
                if (ex.getErrorCode() != ErrorCode.ERR_TRANS_TERMINATED.getCode()) {
                    throw ex;
                }
            } finally {
                try {
                    this.trx.close();
                } catch (Throwable t) {
                    logger.error("Trx closed failed in TConnection.setAutoCommit", t);
                }
                this.trx = null;
                refreshTableMeta();
                releaseTransactionalMdl(getExecutionContext());
            }
        }

        // HACK: Keep compatible with DataX
        // As far as we know nobody use NO_TRANSACTION transaction policy except
        // DataX
        if (this.trxPolicy != ITransactionPolicy.NO_TRANSACTION) {
            // 清理临时指定的事务策略
            this.trxPolicy = null;
        }
        this.shareReadView = ShareReadViewPolicy.DEFAULT;
        if (this.executionContext != null) {
            this.executionContext.setAutoCommit(autoCommit);
        }
    }

    public void commit() throws SQLException {
        checkClosed();

        if (this.trx != null) {
            try {
                // 事务结束,清理事务内容
                this.trx.commit();
                if (this.trx instanceof TsoTransaction) {
                    commitTso = ((TsoTransaction) this.trx).getCommitTso();
                }
            } catch (Throwable e) {
                // 增加打印事务异常日志
                logger.error(e);
                throw GeneralUtil.nestedException(e);
            } finally {
                if (isAutoCommit) {
                    if (trxPolicy == ITransactionPolicy.NO_TRANSACTION) {
                        // DTS relies on NO_TRANSACTION to process batch by batch.
                        // See DataXSpecialTest for details.
                    } else {
                        this.trxPolicy = null;
                    }
                }

                try {
                    this.trx.close();
                } catch (Throwable t) {
                    logger.error("Trx closed failed in TConnection.commit", t);
                }
                this.trx = null;
                // TODO: what if we are in async commit, and we release share lock before all branches are committed.
                refreshTableMeta();
                releaseTransactionalMdl(executionContext);
            }
        }
    }

    public void rollback() throws SQLException {
        checkClosed();

        if (this.trx != null) {
            try {
                this.trx.rollback();
            } catch (Throwable e) {
                // 增加打印事务异常日志
                logger.error(e);
                throw GeneralUtil.nestedException(e);
            } finally {
                if (isAutoCommit) {
                    this.trxPolicy = null;
                }
                try {
                    this.trx.close();
                } catch (Throwable t) {
                    logger.error("Trx closed failed in TConnection.rollback", t);
                }
                this.trx = null;
                refreshTableMeta();
                releaseTransactionalMdl(executionContext);
            }
        }
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("No operations allowed after connection closed.");
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        lock.lock();
        try {
            List<SQLException> exceptions = new LinkedList<SQLException>();

            List<TStatement> openedStatements = this.openedStatements;
            synchronized (openedStatements) {
                try {
                    // 关闭statement
                    for (int i = 0; i < openedStatements.size(); i++) {
                        TStatement stmt = openedStatements.get(i);
                        try {
                            stmt.close(false);
                        } catch (SQLException e) {
                            exceptions.add(e);
                        }
                    }
                } finally {
                    openedStatements.clear();
                }
            }

            if (executorService != null) {
                this.dataSource.releaseExecutorService(executorService);
            }

            ExceptionUtils.throwSQLException(exceptions, "close tconnection", Collections.EMPTY_LIST);
        } finally {
            try {
                cleanHints();
                if (this.trx != null) {
                    try {
                        this.trx.close();
                    } catch (Throwable t) {
                        logger.error("Trx closed failed in TConnection.close()", t);
                    }
                    releaseTransactionalMdl(executionContext);
                    refreshTableMeta();
                }
            } finally {
                this.trx = null;
                closed = true;
                if (executionContext != null) {
                    executionContext.clearAllMemoryPool();
                }
                lock.unlock();
            }

        }

    }

    private void buildExtraCommand(ByteString sql, Map<String, Object> extraCmd) {
        String andorExtra = "/* ANDOR ";
        String tddlExtra = "/* TDDL ";
        if (sql != null) {
            ByteString commet = ByteStringUtil.substringAfter(sql, tddlExtra);
            // 去掉注释
            if (ByteStringUtil.isNotEmpty(commet)) {
                commet = ByteStringUtil.substringBefore(commet, "*/");
            }

            if (ByteStringUtil.isEmpty(commet) && sql.startsWith(andorExtra)) {
                commet = ByteStringUtil.substringAfter(sql, andorExtra);
                commet = ByteStringUtil.substringBefore(commet, "*/");
            }

            if (ByteStringUtil.isNotEmpty(commet)) {
                String[] params = commet.toString().split(",");
                for (String param : params) {
                    String[] keyAndVal = param.split("=");
                    if (keyAndVal.length != 2) {
                        throw new IllegalArgumentException(param + " is wrong , only key = val supported");
                    }
                    String key = keyAndVal[0];
                    String val = keyAndVal[1];
                    extraCmd.put(key, val);
                }
            }
        }
    }

    protected ResultCursor buildSqlErrorInfoCursor(Throwable ex, String sql) {

        // 收集SQL在DRDS的新或旧引擎上的执行错误，包括完整堆栈、行号、方法名、文件名和IpPort
        ArrayResultCursor result = new ArrayResultCursor("SQL_ERROR_INFO");
        result.addColumn("node_address", DataTypes.StringType);
        result.addColumn("causedby_class", DataTypes.StringType);
        result.addColumn("causedby_msg", DataTypes.StringType);
        result.addColumn("error_sql", DataTypes.StringType);
        result.addColumn("error_stack", DataTypes.StringType);
        result.initMeta();

        if (ex == null) {
            return result;
        }

        Object[] objects = new Object[7];
        Throwable causedBy = ex.getCause();
        while (causedBy != null) {
            if (causedBy.getCause() != null) {
                causedBy = causedBy.getCause();
            } else {
                break;
            }
        }
        if (causedBy == null) {
            causedBy = ex;
        }

        String causedbyClass = causedBy.getClass().getName();
        String causedbyMsg = causedBy.getMessage();

        String serverHost = System.getProperty("tddlServerHost");
        if (serverHost == null) {
            serverHost = "";
        }
        String serverPort = System.getProperty("tddlServerPort");
        if (serverPort == null) {
            serverPort = "";
        }

        String nodeAddress = serverHost + ":" + serverPort;
        String errorSql = sql;

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        ex.printStackTrace(pw);
        String errorStack = sw.getBuffer().toString();

        objects[0] = String.valueOf(nodeAddress);
        objects[1] = String.valueOf(causedbyClass);
        objects[2] = String.valueOf(causedbyMsg);
        objects[3] = String.valueOf(errorSql);
        objects[4] = String.valueOf(errorStack);
        result.addRow(objects);
        return result;
    }

    public void setTransactionIsolation(int level) throws SQLException {
        if (this.transactionIsolation == level) {
            return;
        }
        this.transactionIsolation = level;
        if (!ShareReadViewPolicy.supportTxIsolation(transactionIsolation)) {
            this.shareReadView = ShareReadViewPolicy.OFF;
        }
        if (executionContext != null) {
            executionContext.setTxIsolation(level);
        }
    }

    public String getSqlMode() {
        return sqlMode;
    }

    public void setSqlMode(String sqlMode) {
        this.sqlMode = sqlMode;

        if (executionContext != null) {
            executionContext.setSqlMode(sqlMode);
        }
    }

    public boolean removeStatement(Object arg0) {
        synchronized (openedStatements) {
            return openedStatements.remove(arg0);
        }
    }

    public ExecutionContext getExecutionContext() {
        return this.executionContext;
    }

    public void newExecutionContext() {
        ExecutionContext ecNeedClear = this.executionContext;

        // TODO Remove special clear action for trans, context should be totally cleared when statement finished.
        if (this.executionContext != null && this.executionContext.getTransaction() != null && !this.executionContext
            .getTransaction().isClosed()) {
            this.executionContext.clearContextInsideTrans();
            return;
        }

        Object lastFailedMessage = this.executionContext.getExtraDatas().get(ExecutionContext.FAILED_MESSAGE);
        this.executionContext = new ExecutionContext();

        if (lastFailedMessage != null) {
            this.executionContext.getExtraDatas().put(ExecutionContext.LAST_FAILED_MESSAGE, lastFailedMessage);
        }

        if (ecNeedClear.getParamManager().getBoolean(ConnectionParams.OUTPUT_MYSQL_ERROR_CODE)) {
            ParamManager.setBooleanVal(this.executionContext.getParamManager().getProps(),
                ConnectionParams.OUTPUT_MYSQL_ERROR_CODE, true, false);
        }

        ecNeedClear.clearContextAfterTrans();
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }

    public IConnectionHolder getConnectionHolder() {
        if (this.trx == null) {
            return null;
        }

        return this.trx.getConnectionHolder();
    }

    public void kill() throws SQLException {
        lock.lock();

        try {
            if (closed) {
                return;
            }
            List<SQLException> exceptions = new LinkedList<SQLException>();

            try {
                if (this.trx != null) {
                    trx.kill();
                }

            } catch (SQLException e) {
                exceptions.add(e);
            }

            ExceptionUtils.throwSQLException(exceptions, "kill tconnection", Collections.EMPTY_LIST);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getLastInsertId() {
        return this.lastInsertId;
    }

    @Override
    public void setLastInsertId(long id) {
        this.lastInsertId = id;
    }

    @Override
    public long getReturnedLastInsertId() {
        // If NO_AUTO_VALUE_ON_ZERO is set, returnedLastInsertId is 0.
        return returnedLastInsertId;
    }

    @Override
    public void setReturnedLastInsertId(long id) {
        this.returnedLastInsertId = id;
    }

    @Override
    public List<Long> getGeneratedKeys() {
        return generatedKeys;
    }

    @Override
    public void setGeneratedKeys(List<Long> ids) {
        generatedKeys = ids;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
        if (executionContext != null) {
            executionContext.setEncoding(encoding);
        }
    }

    private void beginTransaction() {
        Object sqlLoginBin = executionContext.getExtraServerVariables().get(ICdcManager.SQL_LOG_BIN);
        if (sqlLoginBin != null && !(Boolean) sqlLoginBin) {
            // 这里先设置一下事务策略，checkSqlLogBinXForbiddenAutoCommit方法会判断是否需要限制autocommit
            this.trxPolicy = TransactionAttribute.DEFAULT_IGNORE_BINLOG_TRANSACTION;
        }
        beginTransaction(this.isAutoCommit);
    }

    private void beginTransaction(boolean autoCommit) {
        lock.lock();

        try {
            if (this.isClosed()) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONNECTION_CLOSED, "connection has been closed");
            }

            // SET 设置优先于 Hint 及全局配置
            if (trxPolicy == null) {
                trxPolicy = loadTrxPolicy(executionContext);
            }
            if (shareReadView == ShareReadViewPolicy.DEFAULT) {
                loadShareReadView(executionContext);
            }
            if (groupParallelism == null) {
                // When group parallelism is not set ,use default value of ConnectionParams.GROUP_PARALLELISM
                groupParallelism = this.executionContext.getParamManager().getLong(ConnectionParams.GROUP_PARALLELISM);
            }

            boolean readOnly =
                this.readOnly || (ConfigDataMode.isReadOnlyMode() && executionContext.getParamManager().getBoolean(
                    ConnectionParams.ENABLE_CONSISTENT_REPLICA_READ));
            TransactionClass trxConfig = trxPolicy.getTransactionType(autoCommit, readOnly, false, false);
            if (logicalTimeZone != null) {
                setTimeZoneVariable(serverVariables);
            }
            executionContext.setAppName(dataSource.getAppName());
            executionContext.setSchemaName(dataSource.getSchemaName());
            executionContext.setTxIsolation(transactionIsolation);
            executionContext.setServerVariables(serverVariables);
            executionContext.setUserDefVariables(userDefVariables);
            executionContext.setConnection(this);
            executionContext.setReadOnly(readOnly);
            executionContext.setShareReadView(shareReadView == ShareReadViewPolicy.ON);
            executionContext.setGroupParallelism(groupParallelism);

            ITransactionManager tm = this.dataSource.getConfigHolder().getExecutorContext().getTransactionManager();
            this.trx = tm.createTransaction(trxConfig, executionContext);
        } finally {
            lock.unlock();
        }
    }

    private ITransaction forceInitTransaction(ExecutionContext executionContext, boolean isColumnarRead) {
        return forceInitTransaction(executionContext, isColumnarRead, false);
    }

    private ITransaction forceInitTransaction(ExecutionContext executionContext, boolean isColumnarRead,
                                              boolean isForbidAutocommitTrx) {
        lock.lock();

        try {
            if (this.isClosed()) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONNECTION_CLOSED, "connection has been closed");
            }

            // force update transaction policy
            this.trxPolicy = loadTrxPolicy(executionContext);
            loadShareReadView(executionContext);
            if (groupParallelism == null) {
                groupParallelism = this.executionContext.getParamManager().getLong(ConnectionParams.GROUP_PARALLELISM);
            }

            TransactionClass trxConfig;
            if (!isColumnarRead) {
                boolean isSingleShard = false;
                if (executionContext.getFinalPlan() != null) {
                    RelNode plan = executionContext.getFinalPlan().getPlan();
                    isSingleShard = plan instanceof SingleTableOperation || plan instanceof DirectTableOperation
                        || plan instanceof DirectShardingKeyTableOperation;
                }

                trxConfig = trxPolicy.getTransactionType(false, executionContext.isReadOnly(),
                    isSingleShard, isForbidAutocommitTrx);
            } else {
                trxConfig = TransactionClass.COLUMNAR_READ_ONLY_TRANSACTION;
            }

            if (logicalTimeZone != null) {
                setTimeZoneVariable(serverVariables);
            }
            executionContext.setAppName(dataSource.getAppName());
            executionContext.setSchemaName(dataSource.getSchemaName());
            executionContext.setTxIsolation(transactionIsolation);
            executionContext.setServerVariables(serverVariables);
            executionContext.setUserDefVariables(userDefVariables);
            executionContext.setConnection(this);
            executionContext.setShareReadView(shareReadView == ShareReadViewPolicy.ON);
            executionContext.setGroupParallelism(this.groupParallelism);
            ITransactionManager tm = this.dataSource.getConfigHolder().getExecutorContext().getTransactionManager();
            this.trx = tm.createTransaction(trxConfig, executionContext);
            return this.trx;
        } finally {
            lock.unlock();
        }
    }

    public void initExecutionContextForPrepare(ByteString sql) {
        executionContext.setAppName(dataSource.getAppName());
        executionContext.setSchemaName(dataSource.getSchemaName());
        executionContext.setExecutorService(executorService);
        executionContext.setSql(sql);
        executionContext.setSqlMode(sqlMode);
        executionContext.setServerVariables(serverVariables);
        executionContext.setUserDefVariables(userDefVariables);
        executionContext.setEncoding(encoding);
        executionContext.setConnection(this);
        executionContext.setStressTestValid(dataSource.isStressTestValid());
        executionContext.setSocketTimeout(socketTimeout);
        executionContext.setModifySelect(false);
        executionContext.setModifySelectParallel(false);
        executionContext.setTimeZone(this.logicalTimeZone);
        executionContext.setTxIsolation(this.transactionIsolation);
        if (executionContext.isInternalSystemSql()) {
            /**
             * When the sql is labeled as internal system sql of drds, the
             * traceId that is get by executionContext.getContextId() has
             * been invalid （its val is null or the residual traceId of last
             * sql ), so the traceId must be reset by geting new traceId.
             */
            String internSqlTraceId = Long.toHexString(TrxIdGenerator.getInstance().nextId());
            executionContext.setTraceId(internSqlTraceId);
            executionContext.setPhySqlId(0L);
        }
        executionContext.setRuntimeStatistics(RuntimeStatHelper.buildRuntimeStat(executionContext));
        OptimizerContext.setContext(this.dataSource.getConfigHolder().getOptimizerContext());
    }

    @Override
    public ITransactionPolicy getTrxPolicy() {
        if (trxPolicy == null) {
            return loadTrxPolicy(executionContext);
        }
        return trxPolicy;
    }

    @Override
    public void setTrxPolicy(ITransactionPolicy trxPolicy, boolean check) {
        if (this.trxPolicy == trxPolicy) {
            return;
        }

        if (this.trx != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS,
                "Set transaction policy in the middle of transaction "
                    + "is not allowed. Please do this operation right after transaction begins.");
        }

        if (check) {
            if (dataSource.getConfigHolder().getExecutorContext().getStorageInfoManager().isReadOnly()
                && trxPolicy != ITransactionPolicy.TSO) {
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS,
                    "Distributed transaction is not supported in read-only DRDS instances");
            }
        }

        this.trxPolicy = trxPolicy;
    }

    public ITransactionPolicy getTrxPolicyForLogging() {
        return trxPolicy;
    }

    /**
     * First read connection property, then read `set batch_insert_policy`
     * variable.
     */
    @Override
    public BatchInsertPolicy getBatchInsertPolicy(Map<String, Object> extraCmds) {
        if (batchInsertPolicy != null) {
            return batchInsertPolicy;
        } else {
            return BatchInsertPolicy
                .getPolicyByName(executionContext.getParamManager().getString(ConnectionParams.BATCH_INSERT_POLICY));
        }
    }

    @Override
    public void setBatchInsertPolicy(BatchInsertPolicy policy) {
        this.batchInsertPolicy = policy;
    }

    /**
     * 最终清空缓存，无论是否在TStatement的时候清空了hint.
     */
    public void cleanHints() {
    }

    public TDataSource getDs() {
        return dataSource;
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        this.socketTimeout = milliseconds;
    }

    public String getSchema() throws SQLException {
        return this.dataSource.getSchemaName();
    }

    public void tryClose() throws SQLException {
        lock.lock();
        try {
            if (closed) {
                return;
            }

            if (this.trx != null) {
                this.trx.tryClose();
            }

            // Fix #33255974: Should close transaction after write into table with GSI
            if (isAutoCommit && trxPolicy != ITransactionPolicy.NO_TRANSACTION) {
                if (this.trx != null) {
                    try {
                        this.trx.close();
                    } catch (Throwable t) {
                        logger.error("Trx closed failed in TConnection.tryClose", t);
                    }
                    refreshTableMeta();
                }
            }
        } finally {
            if (isAutoCommit) {
                if (trxPolicy == ITransactionPolicy.NO_TRANSACTION) {
                    // DTS relies on NO_TRANSACTION to process batch by batch. Fuck it.
                    // See DataXSpecialTest for details.
                } else {
                    this.trx = null;
                    this.trxPolicy = null;
                }
            }
            lock.unlock();
        }
    }

    public ITransaction getTrx() {
        return trx;
    }

    public Map<String, Object> getServerVariables() {
        return serverVariables;
    }

    public void setServerVariables(Map<String, Object> serverVariables) {
        this.serverVariables = serverVariables;
    }

    public void setGlobalServerVariables(Map<String, Object> globalServerVariables) {
        this.globalServerVariables = globalServerVariables;
    }

    public Map<String, Object> getExtraServerVariables() {
        return extraServerVariables;
    }

    public void setExtraServerVariables(Map<String, Object> extraServerVariables) {
        this.extraServerVariables = extraServerVariables;
    }

    public void setConnectionVariables(Map<String, Object> connectionVariables) {
        this.connectionVariables = connectionVariables;
    }

    @Override
    public Map<String, Object> getConnectionVariables() {
        return connectionVariables;
    }

    public void setUserDefVariables(Map<String, Object> userDefVariables) {
        this.userDefVariables = userDefVariables;
    }

    public ServerThreadPool getExecutorService() {
        return executorService;
    }

    public int getWarningCount() {
        int warningCount = 0;
        if (executionContext.getExtraDatas().get(ExecutionContext.WARNING_MESSAGE) != null) {
            warningCount += ((List) executionContext.getExtraDatas()
                .get(ExecutionContext.WARNING_MESSAGE)).size();
        }
        if (executionContext.getExtraDatas().get(ExecutionContext.LAST_FAILED_MESSAGE) != null) {
            warningCount += ((List) executionContext.getExtraDatas()
                .get(ExecutionContext.LAST_FAILED_MESSAGE)).size();
        }
        return warningCount;
    }

    public String getWarningSimpleMessage() {
        Object warns = executionContext.getExtraDatas().get(ExecutionContext.FAILED_MESSAGE);
        if (warns == null) {
            return "";
        }
        if (((List<ExecutionContext.ErrorMessage>) warns).isEmpty()) {
            return "";
        }
        return ((List<ExecutionContext.ErrorMessage>) warns).get(0).getMessage()
            + ". you can use 'show warnings' for more detail message.";
    }

    @Override
    public long getFoundRows() {
        return foundRows;
    }

    @Override
    public void setFoundRows(long foundRows) {
        this.foundRows = foundRows;
    }

    @Override
    public long getAffectedRows() {
        return affectedRows;
    }

    @Override
    public void setAffectedRows(long affectedRows) {
        this.affectedRows = affectedRows;
    }

    @Override
    public String getUser() {
        return user;
    }

    public void setUser(String userName) {
        this.user = userName;
    }

    public MdlContext getMdlContext() {
        return mdlContext;
    }

    public void setMdlContext(MdlContext mdlContext) {
        this.mdlContext = mdlContext;
    }

    public String getFrontendConnectionInfo() {
        return frontendConnectionInfo;
    }

    public void setFrontendConnectionInfo(String frontendConnectionInfo) {
        this.frontendConnectionInfo = frontendConnectionInfo;
    }

    public long getLastExecutionBeginNano() {
        return lastExecutionBeginNano;
    }

    public long getLastExecutionBeginUnixTime() {
        return lastExecutionBeginUnixTime;
    }

    public void setAsyncDDLPureModeSession(Boolean asyncDDLPureModeSession) {
        this.asyncDDLPureModeSession = asyncDDLPureModeSession;
    }

    @Override
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public InternalTimeZone getTimeZone() {
        return logicalTimeZone;
    }

    public void setTimeZone(InternalTimeZone timeZone) {
        this.logicalTimeZone = timeZone;
    }

    public void resetTimeZone() {
        this.logicalTimeZone = dataSource.getLogicalDbTimeZone();
    }

    @Override
    public LockingFunctionHandle getLockHandle(Object ec) {
        // refuse other connection or thread to get lock handle.
        if (id != ((ExecutionContext) ec).getConnId()) {
            return null;
        }
        if (lockHandle == null) {
            lockHandle = LockingFunctionManager.getInstance().getHandle(id);
        }
        return lockHandle;
    }

    /**
     * If time zone on current connection is set, update the server variables.
     */
    private void setTimeZoneVariable(Map<String, Object> serverVariables) {
        String variableKey = "time_zone";
        if (serverVariables != null) {
            if (logicalTimeZone.getMySqlTimeZoneName() != null) {
                serverVariables.put(variableKey, logicalTimeZone.getMySqlTimeZoneName());
            }
        }
    }

    private boolean isCommitOnSuccess() {
        if (trx != null && trx.getInventoryMode() != null) {
            return trx.getInventoryMode().isCommitOnSuccess();
        }
        return false;
    }

    private boolean isRollbackOnFail() {
        if (trx != null && trx.getInventoryMode() != null) {
            return trx.getInventoryMode().isRollbackOnFail();
        }
        return false;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    @Override
    public ITStatement createStatement() throws SQLException {
        checkClosed();
        ExecutionContext context = prepareExecutionContext();
        TStatement stmt = new TStatement(dataSource, this, context);

        synchronized (openedStatements) {
            openedStatements.add(stmt);
        }
        return stmt;
    }

    @Override
    public ITPrepareStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(ByteString.from(sql));
    }

    public ITPrepareStatement prepareStatement(ByteString sql) throws SQLException {
        checkClosed();
        ExecutionContext context = prepareExecutionContext();
        TPreparedStatement stmt = new TPreparedStatement(dataSource, this, sql, context);
        synchronized (openedStatements) {
            openedStatements.add(stmt);
        }
        return stmt;
    }

    public void setGroupParallelism(Long groupParallelism) {
        if (this.groupParallelism != null && this.groupParallelism.equals(groupParallelism)) {
            return;
        }
        if (this.trx != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS,
                "Set group parallelism in the middle of transaction "
                    + "is not allowed. Please do this operation right after transaction begins.");
        }
        this.groupParallelism = groupParallelism;

        /**
         * Should clear plan cache of server here
         */
        try {
            if (dataSource != null) {
                /**
                 * <pre>
                 *     when groupParallelism is disable, some plan of dml will be change,
                 *     such as
                 *     (broacast_tbl: nation,
                 *     partition_tbl: customer)
                 *     sql: update nation, customer set n_name = 2, c_name = 2 where n_nationkey = c_nationkey;
                 *     when groupParallelism=1 (disable), the update sql CAN be pushdown directly;
                 *     when groupParallelism>1 (enable), the update sql CANNOT be pushdown because
                 *            dml need fetch shard lock on broadcast table of nation join partition table of customer
                 *            by using different group connections.
                 *     so this is the reason of d clearing plan cache
                 * </pre>
                 */
                PlanManager.getInstance().invalidateSchema(dataSource.getSchemaName());
            }
        } catch (Throwable ex) {
            logger.warn("clear plancache error for set group parallelism", ex);
        }

    }

    public boolean dnSupportShareReadView() {
        return dataSource.getConfigHolder().getExecutorContext()
            .getStorageInfoManager().supportSharedReadView();
    }

    public boolean isClientFoundRows() {
        return clientFoundRows;
    }

    public void setClientFoundRows(boolean clientFoundRows) {
        this.clientFoundRows = clientFoundRows;
    }

    @Override
    public boolean isMppConnection() {
        return false;
    }

    public long getCommitTso() {
        return commitTso;
    }
}
