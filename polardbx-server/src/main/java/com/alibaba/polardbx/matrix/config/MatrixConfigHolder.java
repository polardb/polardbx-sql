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

package com.alibaba.polardbx.matrix.config;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.logical.ITPrepareStatement;
import com.alibaba.polardbx.common.logical.ITStatement;
import com.alibaba.polardbx.common.model.App;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.model.RepoInst;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.AbstractSequenceManager;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.SequenceLoadFromDBManager;
import com.alibaba.polardbx.executor.common.SequenceManager;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.PurgeOssFileScheduleTask;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRemoteTaskExecutor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineScheduler;
import com.alibaba.polardbx.executor.ddl.newengine.DdlPlanScheduler;
import com.alibaba.polardbx.executor.ddl.newengine.cross.AsyncPhyObjectRecorder;
import com.alibaba.polardbx.executor.ddl.sync.JobRequest;
import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.executor.gms.TableListListener;
import com.alibaba.polardbx.executor.gsi.GsiManager;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.ScalarSubqueryExecHelper;
import com.alibaba.polardbx.executor.utils.SchemaMetaUtil;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.utils.VariableProxy;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.RepoSchemaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.AsyncDDLContext;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext.ErrorMessage;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.util.TableMetaFetcher;
import com.alibaba.polardbx.optimizer.rule.MockSchemaManager;
import com.alibaba.polardbx.optimizer.rule.Partitioner;
import com.alibaba.polardbx.optimizer.rule.RuleSchemaManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.statis.SQLRecord;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.SubQueryDynamicParamUtils;
import com.alibaba.polardbx.optimizer.variable.VariableManager;
import com.alibaba.polardbx.optimizer.view.PolarDbXSystemTableView;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import com.alibaba.polardbx.repo.mysql.spi.MyDataSourceGetter;
import com.alibaba.polardbx.rule.TddlRule;
import com.alibaba.polardbx.server.conn.InnerConnectionManager;
import com.alibaba.polardbx.server.handler.SyncPointExecutor;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.trx.AutoCommitTransaction;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.alibaba.polardbx.common.cdc.ICdcManager.DEFAULT_DDL_VERSION_ID;

/**
 * 依赖的组件
 *
 * @since 5.0.0
 */
public class MatrixConfigHolder extends AbstractLifecycle {

    private final static Logger logger = LoggerFactory.getLogger(MatrixConfigHolder.class);

    private TDataSource dataSource;
    private String schemaName;
    private String appName;
    private String unitName;
    private boolean sharding = true;
    private TddlRuleManager tddlRuleManager;
    private Partitioner partitioner;
    private PartitionInfoManager partitionInfoManager;
    private TopologyHandler topologyHandler;
    private ITopologyExecutor topologyExecutor;
    private ITransactionManager transactionManager = null;
    private AbstractSequenceManager sequenceManager = null;
    private TableListListener tableListListener;
    private OptimizerContext optimizerContext;
    private ExecutorContext executorContext;
    private Matrix matrix;
    private List<App> subApps;
    private TddlRule tddlRule;
    private MatrixStatistics statistics;
    private IServerConfigManager serverConfigManager;
    private StorageInfoManager storageInfoManager;
    private GsiManager gsiManager;
    private ViewManager viewManager;
    private VariableManager variableManager;
    private InternalTimeZone shardRouterDefaultTimeZone;
    private DdlEngineScheduler ddlEngineScheduler;
    private TableGroupInfoManager tableGroupInfoManager;

    @Override
    public void doInit() {
        ExecutorContext executorContext = new ExecutorContext();

        this.executorContext = executorContext;
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = appName;
        }

        OptimizerContext oc = new OptimizerContext(schemaName);
        this.optimizerContext = oc;
        // 将自己做为config holder
        topologyInit();

        initGroups();

        this.storageInfoManager = new StorageInfoManager(topologyHandler);
        this.storageInfoManager.init();

        ruleInit();
        schemaInit();

        executorContext.setTopologyHandler(topologyHandler);
        executorContext.setTopologyExecutor(topologyExecutor);

        Objects.requireNonNull(partitioner);
        Objects.requireNonNull(tddlRuleManager);
        Objects.requireNonNull(partitionInfoManager);
        Objects.requireNonNull(tableGroupInfoManager);

        oc.setMatrix(topologyHandler.getMatrix());
        oc.setRuleManager(tddlRuleManager);
        oc.setPartitioner(partitioner);
        oc.setStatistics(this.statistics);
        oc.setPartitionInfoManager(partitionInfoManager);
        oc.setTableGroupInfoManager(tableGroupInfoManager);

        SubQueryDynamicParamUtils.initScalarSubQueryExecHelper(ScalarSubqueryExecHelper.getInstance());

        //由于load app存在并发加载的问题, 所以这里需要最终确认加载初始化成功的app上下文
        loadContext();

        sequenceInit();

        matrix.setMasterRepoInstMap(topologyHandler.getRepoInstMaps(RepoInst.REPO_INST_TYPE_MASTER));

        executorContext.setStorageInfoManager(storageInfoManager);

        // 允许事务管理器扩展
        transactionManager = ExtensionLoader.load(ITransactionManager.class);
        transactionManager.prepare(schemaName, this.getDataSource().getConnectionProperties(), storageInfoManager);
        transactionManager.init();
        executorContext.setTransactionManager(transactionManager);

        // init global secondary index manager
        gsiInit();

        // Register current schema for Async DDL.
        // Create necessary system tables for transaction executor
        if (topologyExecutor instanceof TransactionExecutor && ConfigDataMode.isMasterMode() &&
            !InformationSchema.NAME.equalsIgnoreCase(schemaName)) {
            ((TransactionExecutor) topologyExecutor).initSystemTables();
            ((TransactionExecutor) this.topologyExecutor).checkTsoTransaction();
        }

        // Initialize table meta related.
        tableMetaInit();

        DataSource defaultDataSource = null;

        // Initialize DDL engines before plan manager for dependency.
        if (!ConfigDataMode.isFastMock()) {
            // Initialize the DDL engine before plan manager because of dependency.
            ddlEngineInit();
            defaultDataSource = MetaDbDataSource.getInstance().getDataSource();
        }
        schedulerInit();

        /* init StatisticManager */
        MyDataSourceGetter myDataSourceGetter = new MyDataSourceGetter(this.schemaName);

        SystemTableView systemTableView = new PolarDbXSystemTableView(defaultDataSource, schemaName);
        oc.setParamManager(new ParamManager(dataSource.getConnectionProperties()));

        // init ViewManager
        ViewManager viewManager = new ViewManager(schemaName, systemTableView, dataSource.getConnectionProperties());
        this.viewManager = viewManager;
        this.viewManager.init();
        oc.setViewManager(viewManager);

        // init VariableManager
        DataSource variableDs =
            myDataSourceGetter.getDataSource(tddlRuleManager.getDefaultDbIndex());
        VariableProxy variableProxy = new VariableProxy((TGroupDataSource) variableDs);
        VariableManager variableManager =
            new VariableManager(schemaName, variableProxy, dataSource.getConnectionProperties());
        this.variableManager = variableManager;
        this.variableManager.init();
        oc.setVariableManager(variableManager);

        oc.setParamManager(new ParamManager(dataSource.getConnectionProperties()));

        // Start XA recover task. In case of leader of CN without requests(in other AZ)
        // and that makes all pending trx unfinished forever.
        if (storageInfoManager.supportXA() || storageInfoManager.supportTso()) {
            transactionManager.scheduleTimerTask();
        }
        if (ConfigDataMode.isPolarDbX()) {
            PurgeOssFileScheduleTask.getInstance().init(new ParamManager(dataSource.getConnectionProperties()));
        }

        executorContext.setInnerConnectionManager(InnerConnectionManager.getInstance());

        this.executorContext.setSyncPointExecutor(SyncPointExecutor.getInstance());
        DbGroupInfoManager.getInstance().reloadGroupsOfDb(schemaName);
        oc.setFinishInit(true);//Label oc of the db finish init
    }

    private void loadContext() {
        ExecutorContext.setContext(schemaName, executorContext);
        OptimizerContext.loadContext(optimizerContext);
    }

    private void unLoadContext() {
        OptimizerContext.clearContext(this.schemaName);
        ExecutorContext.clearContext(this.schemaName);
    }

    private void sequenceInit() {
        SequenceLoadFromDBManager manager =
            new SequenceLoadFromDBManager(schemaName, this.dataSource.getConnectionProperties());

        this.sequenceManager = new SequenceManager(manager);

        if (ConfigDataMode.getMode() == ConfigDataMode.Mode.MOCK) {
            this.sequenceManager.init();
        }
        executorContext.setSeqeunceManager(sequenceManager);
    }

    private void gsiInit() {
        GsiManager manager = new GsiManager(this.topologyHandler, this.storageInfoManager);
        manager.init();

        this.gsiManager = manager;
        this.executorContext.setGsiManager(this.gsiManager);
    }

    @Override
    protected void doDestroy() {
        try {
            if (optimizerContext != null && optimizerContext.getLatestSchemaManager() != null) {
                optimizerContext.getLatestSchemaManager().destroy();
            }
        } catch (Exception ex) {
            logger.warn("schemaManager destroy error", ex);
        }

        try {
            if (tddlRuleManager != null) {
                tddlRuleManager.destroy();
            }
        } catch (Exception ex) {
            logger.warn("optimizerRule destroy error", ex);
        }

        try {
            if (topologyHandler != null) {
                topologyHandler.destroy();
            }
        } catch (Exception ex) {
            logger.warn("topologyHandler destroy error", ex);
        }

        try {
            if (topologyExecutor != null) {
                dataSource.releaseExecutorService(topologyExecutor.getExecutorService());
                topologyExecutor.destroy();
            }
        } catch (Exception ex) {
            logger.warn("topologyExecutor destroy error", ex);
        }

        try {
            if (transactionManager != null) {
                transactionManager.destroy();
            }
        } catch (Exception ex) {
            logger.warn("transactionManager destroy error", ex);
        }

        try {
            if (sequenceManager != null) {
                sequenceManager.destroy();
            }
        } catch (Exception ex) {
            logger.warn("sequenceManager destroy error", ex);
        }

        try {
            if (gsiManager != null) {
                gsiManager.destroy();
            }
        } catch (Exception ex) {
            logger.warn("GSI manager destroy error", ex);
        }

        try {
            if (storageInfoManager != null) {
                storageInfoManager.destroy();
            }
        } catch (Exception ex) {
            logger.warn("storageInfoManager destroy error", ex);
        }

        try {
            if (viewManager != null) {
                viewManager.destroy();
            }
        } catch (Exception ex) {
            logger.warn("viewManager destroy error", ex);
        }

        try {
            if (variableManager != null) {
                variableManager.destroy();
            }
        } catch (Exception ex) {
            logger.warn("VariableManager destroy error", ex);
        }

        try {
            if (tableGroupInfoManager != null) {
                tableGroupInfoManager.destroy();
            }
        } catch (Exception ex) {
            logger.warn("TableGroupInfoManager destroy error", ex);
        }

        try {
            CommonMetaChanger.invalidateBufferPool(schemaName);
        } catch (Exception ex) {
            logger.warn("Invalidate BufferPool error", ex);
        }

        ddlEngineDestroy();

        tableMetaDestroy();

        unLoadContext();
        serverConfigManager = null;
    }

    public void topologyInit() {
        if (schemaName == null) {
            schemaName = appName;
        }
        topologyHandler = new TopologyHandler(appName,
            schemaName,
            unitName,
            this.dataSource.getConnectionProperties(), executorContext);
        topologyHandler.init();
        executorContext.setRepositoryHolder(topologyHandler.getRepositoryHolder());

        // 允许执行器扩展
        topologyExecutor = ExtensionLoader.load(ITopologyExecutor.class);
        topologyExecutor.setExecutorService(dataSource.borrowExecutorService());
        topologyExecutor.setTopology(topologyHandler);
        topologyExecutor.init();

        matrix = topologyHandler.getMatrix();
    }

    public void ruleInit() {

        boolean isInitDefaultDb = dataSource.isDefaultDb();
        TddlRule rule = tddlRule;
        if (rule == null) {
            if (!isInitDefaultDb) {
                rule = new TddlRule();
                rule.setAppName(this.appName);
                rule.setSchemaName(this.schemaName);
                rule.setUnitName(this.unitName);
                rule.setAllowEmptyRule(!sharding || ConfigDataMode.isFastMock());

                String defaultDbIndexGroup = TableInfoManager.getSchemaDefaultDbIndex(this.schemaName);
                rule.setDefaultDbIndex(defaultDbIndexGroup);

            } else {
                if (InformationSchema.NAME.equalsIgnoreCase(schemaName)) {
                    rule = ruleInitForInformationSchemaDb();
                } else { // default polardbx db
                    rule = ruleInitForDefaultDb();
                }
            }
        }

        String enableShardConstExprStr = GeneralUtil.getPropertyString(dataSource.getConnectionProperties(),
            ConnectionProperties.ENABLE_SHARD_CONST_EXPR, Boolean.FALSE.toString());
        boolean enableShardConstExpr = Boolean.parseBoolean(enableShardConstExprStr);

        if (ConfigDataMode.isFastMock()) {
            rule.init();

            partitionInfoManager = new PartitionInfoManager(schemaName, appName);
            tableGroupInfoManager = new TableGroupInfoManager(schemaName);

            tddlRuleManager = new TddlRuleManager(rule, partitionInfoManager, tableGroupInfoManager, schemaName);
            tddlRuleManager.init();

            partitioner = new Partitioner(rule, optimizerContext);
            partitioner.setShardRouterTimeZone(this.shardRouterDefaultTimeZone);
            partitioner.setEnableConstExpr(enableShardConstExpr);

            return;
        }

        logger.info("ConnectionProperties=" + dataSource.getConnectionProperties());

        // Init for partition info manager
        try {
            partitionInfoManager = new PartitionInfoManager(schemaName, appName);
            // init for table group info manager
            tableGroupInfoManager = new TableGroupInfoManager(schemaName);
            partitionInfoManager.setTableMetaFetcher(new TableMetaFetcher() {
                @Override
                public TableMeta getTableMeta(String schemaName, String appName, String tableName) {
                    return GmsTableMetaManager.fetchTableMeta(null, schemaName, tableName, null, null, true,
                        true);
                }
            });
            rule.init();

            tddlRuleManager = new TddlRuleManager(rule, partitionInfoManager, tableGroupInfoManager, schemaName);
            partitioner = new Partitioner(rule, optimizerContext);
            partitioner.setShardRouterTimeZone(this.shardRouterDefaultTimeZone);
            partitioner.setEnableConstExpr(enableShardConstExpr);
        } catch (Throwable e) {
            logger.error("initialize PartitionInfoManager failed: e");
            throw e;
        }

        tddlRuleManager.init();
        partitionInfoManager.setRule(tddlRuleManager);

        logger.info(String.format("initialize rule success: appName=%s schema=%s",
            this.appName, this.schemaName));
    }

    public void tableMetaInit() {
        // Register table list dataId.
        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);
        MetaDbConfigManager.getInstance().register(tableListDataId, null);

        // Initialize table list listener that initializes all table listeners.
        tableListListener = new TableListListener(schemaName);
        tableListListener.init();
        // Bind a table list listener.
        MetaDbConfigManager.getInstance().bindListener(tableListDataId, tableListListener);

    }

    public void tableMetaDestroy() {
        if (!DbTopologyManager.checkDbExists(schemaName)) {
            try {
                try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                    metaDbConn.setAutoCommit(false);
                    SchemaMetaUtil.cleanupSchemaMeta(schemaName, metaDbConn, DEFAULT_DDL_VERSION_ID);
                    metaDbConn.commit();
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
                }
            } catch (Exception ex) {
                logger.warn("Table meta remove error: " + ex.getMessage(), ex);
            }
        }

        try {
            if (tableListListener != null) {
                tableListListener.destroy();
            }
            // Unregister table list dataId and unbind table list listener.
            String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);
            MetaDbConfigManager.getInstance().unbindListener(tableListDataId);
        } catch (Exception ex) {
            logger.warn("Table meta unbind error: " + ex.getMessage(), ex);
        }
    }

    private DataSource buildDataSource(String groupKey) {
        return MetaDbDataSource.getInstance().getDataSource();
    }

    public void ddlEngineInit() {
        // Register new DDL Engine Scheduler.
        ddlEngineScheduler = DdlEngineScheduler.getInstance();
        ddlEngineScheduler.register(schemaName, dataSource.borrowExecutorService());

        AsyncPhyObjectRecorder.register(schemaName.toLowerCase());

        DdlPlanScheduler.getINSTANCE();
    }

    public void ddlEngineDestroy() {

        try {
            if (ddlEngineScheduler != null) {
                ddlEngineScheduler.deregister(schemaName);
            }
        } catch (Exception ex) {
            logger.warn("DdlEngineScheduler destroy error", ex);
        }

        AsyncPhyObjectRecorder.deregister(schemaName.toLowerCase());
    }

    public void prepareExecutionContext(ExecutionContext context, String sql, String schema) {
        context.setTraceId("balancer");
        context.setSchemaName(schema);
    }

    public void prepareExecutionContext(ExecutionContext context, Map<String, Object> dataPassed) {
        Object connId = dataPassed.get(SQLRecord.CONN_ID);
        if (connId != null) {
            context.setConnId((Long) connId);
            context.getAsyncDDLContext().setConnId((Long) connId);
        }

        Object testMode = dataPassed.get(JobRequest.TEST_MODE);
        if (testMode != null) {
            context.setTestMode((Boolean) testMode);
            context.getAsyncDDLContext().setTestMode((Boolean) testMode);
        }

        Object clientIP = dataPassed.get(SQLRecord.CLIENT_IP);
        if (clientIP != null) {
            context.setClientIp((String) clientIP);
            context.getAsyncDDLContext().setClientIp((String) clientIP);
        }

        Object traceId = dataPassed.get(SQLRecord.TRACE_ID);
        if (traceId != null) {
            context.setTraceId((String) traceId);
            context.getAsyncDDLContext().setTraceId((String) traceId);
        }

        Object txId = dataPassed.get(SQLRecord.TX_ID);
        if (txId != null) {
            context.setTxId((Long) txId);
            context.getAsyncDDLContext().setTxId((Long) txId);
        }

        if (schemaName != null) {
            context.setSchemaName(schemaName);
        }
    }

    public List<ErrorMessage> performAsyncDDLJob(Job job, String schemaName, JobRequest jobRequest) {
        try (TConnection conn = (TConnection) dataSource.getConnection()) {

            prepareExecutionContext(conn.getExecutionContext(), jobRequest.getDataPassed());

            if (AsyncDDLContext.isSeparateJob(job) || AsyncDDLContext.isParentJob(job)) {
                try (ITPrepareStatement ps = conn.prepareStatement(job.getDdlStmt())) {
                    ExecutionContext ec = conn.getExecutionContext();
                    AsyncDDLContext asyncDDLContext = conn.getExecutionContext().getAsyncDDLContext();
                    // Perform a job newly created or recovered.
                    asyncDDLContext.setJob(job);
                    ec.getExtraCmds().put(ConnectionParams.FORCE_DDL_ON_LEGACY_ENGINE.getName(), "true");
                    ps.execute();
                    // Return warnings if exist.
                    if (asyncDDLContext.getExtraData() != null) {
                        return (List<ErrorMessage>) asyncDDLContext.getExtraData().get(ExecutionContext.FAILED_MESSAGE);
                    }
                    return new ArrayList<>();
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, e, e.getMessage());
                }
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNSUPPORTED, "cannot perform a sub-job '"
                    + job.getId() + "' (parent job '"
                    + job.getParentId()
                    + "') separately in " + schemaName);
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, e, e.getMessage());
        }
    }

    public DdlContext restoreDDL(String schemaName, Long jobId) {
        ITransaction autoCommitTrans = null;
        try (TConnection conn = (TConnection) dataSource.getConnection()) {
            ExecutionContext executionContext = conn.getExecutionContext();
            executionContext.setExecutorService(conn.getExecutorService());

            TransactionManager transactionManager = (TransactionManager) this.executorContext.getTransactionManager();
            autoCommitTrans = new AutoCommitTransaction(executionContext, transactionManager);
            executionContext.setTransaction(autoCommitTrans);

            executionContext.setStats(dataSource.getStatistics());
            executionContext.setPhysicalRecorder(dataSource.getPhysicalRecorder());
            executionContext.setConnection(conn);
            // fastchecker 中需要共享readview
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            executionContext.setShareReadView(conn.getShareReadView());

            DdlEngineDagExecutor.restoreAndRun(schemaName, jobId, executionContext);
            return executionContext.getDdlContext();
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "Failed to get a TConnection to perform the DDL. Caused by: " + e.getMessage(), e);
        } finally {
            if (autoCommitTrans != null) {
                autoCommitTrans.close();
            }
        }
    }

    public void remoteExecuteDdlTask(String schemaName, Long jobId, Long taskId) {
        ITransaction autoCommitTrans = null;
        try (TConnection conn = (TConnection) dataSource.getConnection()) {
            ExecutionContext executionContext = conn.getExecutionContext();
            executionContext.setExecutorService(conn.getExecutorService());

            TransactionManager transactionManager = (TransactionManager) this.executorContext.getTransactionManager();
            autoCommitTrans = new AutoCommitTransaction(executionContext, transactionManager);
            executionContext.setTransaction(autoCommitTrans);

            executionContext.setStats(dataSource.getStatistics());
            executionContext.setPhysicalRecorder(dataSource.getPhysicalRecorder());
            executionContext.setConnection(conn);
            // fastchecker 中需要共享readview
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            executionContext.setShareReadView(conn.getShareReadView());

            DdlEngineRemoteTaskExecutor.executeRemoteTask(schemaName, jobId, taskId, executionContext);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        } finally {
            if (autoCommitTrans != null) {
                autoCommitTrans.close();
            }
        }
    }

    public void executeBackgroundSql(String sql, String schema, InternalTimeZone timeZone) {
        try (TConnection conn = (TConnection) dataSource.getConnection()) {
            if (timeZone != null) {
                conn.setTimeZone(timeZone);
            }
            ExecutionContext executionContext = conn.getExecutionContext();
            executionContext.setSchemaName(schema);
            executionContext.setPrivilegeMode(false);
            try (ITStatement stmt = conn.createStatement()) {
                stmt.executeUpdate(sql);
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int executeBackgroundDmlWithTConnection(String sql,
                                                   String schema,
                                                   InternalTimeZone timeZone,
                                                   TConnection conn) {
        try {
            if (timeZone != null) {
                conn.setTimeZone(timeZone);
            }
            ExecutionContext executionContext = conn.getExecutionContext();
            executionContext.setSchemaName(schema);
            executionContext.setPrivilegeMode(false);
            int affectRows = 0;
            try (ITStatement stmt = conn.createStatement()) {
                affectRows = stmt.executeUpdate(sql);
            }
            return affectRows;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public long submitRebalanceDDL(String schema, String ddlSql) {
        try (TConnection conn = (TConnection) dataSource.getConnection()) {
            ExecutionContext executionContext = conn.getExecutionContext();
            executionContext.setSchemaName(schema);
            executionContext.setPrivilegeMode(false);

            SQLRecorderLogger.ddlEngineLogger.info(
                String.format("submit job, schemaName:[%s], ddlSql:[%s]", schema, ddlSql));

            try (ITStatement stmt = conn.createStatement()) {
                ResultSet resultSet = stmt.executeQuery(ddlSql);
                if (resultSet.next()) {
                    return resultSet.getLong(DdlConstants.JOB_ID);
                } else {
                    throw new TddlNestableRuntimeException("Submit Rebalance error");
                }
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<Map<String, Object>> executeQuerySql(String sql, String schema, InternalTimeZone timeZone) {
        try (TConnection conn = (TConnection) dataSource.getConnection()) {
            if (timeZone != null) {
                conn.setTimeZone(timeZone);
            }
            ExecutionContext executionContext = conn.getExecutionContext();
            executionContext.setSchemaName(schema);
            executionContext.setPrivilegeMode(false);
            List<Map<String, Object>> result = null;
            ResultSet rs = null;
            try (ITStatement stmt = conn.createStatement()) {
                rs = stmt.executeQuery(sql);
                result = ExecUtils.resultSetToList(rs);
            }
            return result;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public long submitSubDDL(String schema, DdlContext parentDdlContext, long parentJobId, long parentTaskId,
                             boolean forRollback, String ddlSql) {
        try (TConnection conn = (TConnection) dataSource.getConnection()) {
            ExecutionContext executionContext = conn.getExecutionContext();
            executionContext.setSchemaName(schema);
            executionContext.setPrivilegeMode(false);
            DdlContext ddlContext = new DdlContext();
            ddlContext.setIsSubJob(true);
            ddlContext.setParentJobId(parentJobId);
            ddlContext.setParentTaskId(parentTaskId);
            ddlContext.setForRollback(forRollback);

            ddlContext.setParentDdlContext(parentDdlContext);
            boolean withDdlParentContext = parentDdlContext != null;
            if (withDdlParentContext) {
                executionContext.setServerVariables(parentDdlContext.getServerVariables());
                executionContext.setUserDefVariables(parentDdlContext.getUserDefVariables());
                executionContext.setExtraCmds(parentDdlContext.getExtraCmds());
                executionContext.setExtraServerVariables(parentDdlContext.getExtraServerVariables());
                if (StringUtils.isNotEmpty(parentDdlContext.getTimeZone())) {
                    executionContext.setTimeZone(TimeZoneUtils.convertFromMySqlTZ(parentDdlContext.getTimeZone()));
                }
                executionContext.setEncoding(parentDdlContext.getEncoding());
            }

            executionContext.setDdlContext(ddlContext);

            SQLRecorderLogger.ddlEngineLogger.info(String.format(
                "submit sub job, schemaName:[%s], parentJobId:[%s], parentTaskId:[%s], forRollback:[%s], ddlSql:[%s]",
                schema, parentJobId, parentTaskId, forRollback, ddlSql
            ));

            try (ITStatement stmt = conn.createStatement()) {
                ResultSet resultSet = stmt.executeQuery(ddlSql);
                if (resultSet.next()) {
                    return resultSet.getLong(DdlConstants.JOB_ID);
                } else {
                    throw new TddlNestableRuntimeException("Submit SubJob error");
                }
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void schemaInit() {

        SchemaManager schemaManager;
        if (!ConfigDataMode.isFastMock()) {
            RepoSchemaManager tableMetaManager = null;
            if (!InformationSchema.NAME.equalsIgnoreCase(schemaName)) {
                schemaManager =
                    new GmsTableMetaManager(schemaName, appName, this.tddlRuleManager, this.storageInfoManager);
            } else {

                RuleSchemaManager ruleSchemaManager = new RuleSchemaManager(tddlRuleManager, tableMetaManager,
                    topologyHandler.getMatrix(),
                    GeneralUtil.getPropertyLong(this.dataSource.getConnectionProperties(),
                        ConnectionProperties.TABLE_META_CACHE_EXPIRE_TIME,
                        TddlConstants.DEFAULT_TABLE_META_EXPIRE_TIME));
                schemaManager = ruleSchemaManager;
            }
            schemaManager.init();

        } else {
            schemaManager = new MockSchemaManager();
            ((MockSchemaManager) schemaManager).setTddlRule(tddlRuleManager);
        }

        optimizerContext.setSchemaManager(schemaManager);
    }

    public void schedulerInit() {
        if (ConfigDataMode.isPolarDbX() && !ConfigDataMode.isFastMock()) {
            ScheduledJobsManager.getInstance();
        }
    }

    protected void initGroups() {
        for (Group group : matrix.getGroups()) {
            topologyHandler.createOne(group);
        }
    }

    protected TddlRule ruleInitForDefaultDb() {

        TddlRule rule = tddlRule;
        if (rule == null) {
            rule = new TddlRule();
            rule.setAppName(SystemDbHelper.DEFAULT_DB_APP_NAME);
            rule.setSchemaName(SystemDbHelper.DEFAULT_DB_NAME);
            rule.setUnitName(this.unitName);
            rule.setAllowEmptyRule(false);
            rule.setDefaultDbIndex(SystemDbHelper.DEFAULT_DB_GROUP_NAME);
        }
        return rule;
    }

    protected TddlRule ruleInitForInformationSchemaDb() {

        TddlRule rule = tddlRule;
        if (rule == null) {
            rule = new TddlRule();
            rule.setAppName(SystemDbHelper.INFO_SCHEMA_DB_APP_NAME);
            rule.setSchemaName(SystemDbHelper.INFO_SCHEMA_DB_NAME);
            rule.setUnitName(this.unitName);
            rule.setAllowEmptyRule(false);
            rule.setDefaultDbIndex(SystemDbHelper.INFO_SCHEMA_DB_GROUP_NAME);
        }
        return rule;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public ExecutorContext getExecutorContext() {
        return this.executorContext;
    }

    public OptimizerContext getOptimizerContext() {
        return this.optimizerContext;
    }

    public void setSharding(boolean sharding) {
        this.sharding = sharding;
    }

    public void setSubApps(List<App> subApps) {
        this.subApps = subApps;
    }

    public Matrix getMatrix() {
        return matrix;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public void setTddlRule(TddlRule tddlRule) {
        this.tddlRule = tddlRule;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public MatrixStatistics getStatistics() {
        return statistics;
    }

    public void setStatistics(MatrixStatistics statistics) {
        this.statistics = statistics;
    }

    public void setDataSource(TDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public TDataSource getDataSource() {
        return this.dataSource;
    }

    public IServerConfigManager getServerConfigManager() {
        return serverConfigManager;
    }

    public void setServerConfigManager(IServerConfigManager serverConfigManager) {
        this.serverConfigManager = serverConfigManager;
    }

    public void setShardRouterDefaultTimeZone(InternalTimeZone shardRouterDefaultTimeZone) {
        this.shardRouterDefaultTimeZone = shardRouterDefaultTimeZone;
    }

    public boolean setShardRouterTimeZoneSuccess(String shardRulerTimeZoneStr) {
        LoggerInit.TDDL_DYNAMIC_CONFIG.info(appName + " Shard Router Time Zone init: " + shardRulerTimeZoneStr);
        InternalTimeZone tz = TimeZoneUtils.convertFromMySqlTZ(shardRulerTimeZoneStr);
        if (tz != null) {
            this.shardRouterDefaultTimeZone = tz;
            return true;
        } else {
            return false;
        }
    }

    public StorageInfoManager getStorageInfoManager() {
        return storageInfoManager;
    }
}
