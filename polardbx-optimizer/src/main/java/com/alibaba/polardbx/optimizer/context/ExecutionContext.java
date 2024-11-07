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

package com.alibaba.polardbx.optimizer.context;

import com.alibaba.polardbx.Capabilities;
import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.MergedStorageInfo;
import com.alibaba.polardbx.common.SQLMode;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.ShareReadViewPolicy;
import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.oss.IDeltaReadOption;
import com.alibaba.polardbx.common.privilege.PrivilegeVerifyItem;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.MergeHashMap;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.metadb.columnar.FlashbackColumnarManager;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.ccl.common.CclContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.profiler.RuntimeStat;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.QueryMemoryPoolHolder;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.PreparedStmtCache;
import com.alibaba.polardbx.optimizer.planmanager.parametric.Point;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.alibaba.polardbx.optimizer.statis.SQLRecorder;
import com.alibaba.polardbx.optimizer.statis.SQLTracer;
import com.alibaba.polardbx.optimizer.statis.XplanStat;
import com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.alibaba.polardbx.util.ValueHolder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
import org.apache.commons.lang.StringUtils;

import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.TSO_TRANSACTION;

/**
 * 一次执行过程中的上下文 All queries in the same transaction will only share single
 * ExecutionContext object, so any query related property in an ExecutionContext
 * (e.g. explain, sqlType) should be ALWAYS cleared at the beginning of the
 * execution. The clear operation could be done in
 *
 * @author whisper
 */
public class ExecutionContext {
    private static final Logger logger = LoggerFactory.getLogger(ExecutionContext.class);

    public static final String SUCCESS_MESSAGE = "SUCCESS_MESSAGE";
    public static final String FAILED_MESSAGE = "FAILED_MESSAGE";
    public static final String WARNING_MESSAGE = "WARNING_MESSAGE";
    public static final String LAST_FAILED_MESSAGE = "LAST_FAILED_MESSAGE";

    /**
     * 当前事务
     */
    private ITransaction transaction;

    private Map<String, Object> extraCmds = new HashMap<>();

    private Map<String, Object> defaultExtraCmds;

    /**
     * 需要传输到mpp worker端的hint参数列表, extraCmds不包含在hintCmds中
     */
    private Map<String, Object> hintCmds = new HashMap<>();

    /**
     * schema manager used in this query
     */
    private Map<String, SchemaManager> schemaManagers = new ConcurrentHashMap<>();

    /**
     * schema manager of this schema used in this query
     */
    private SchemaManager currentSchemaManager = null;

    private ParamManager paramManager = new ParamManager(extraCmds);
    private Parameters params = null;

    private Map<Integer, NlsString> parameterNlsStrings;

    private ServerThreadPool concurrentService;

    private boolean autoCommit = true;

    private int txIsolation = Connection.TRANSACTION_READ_COMMITTED;

    private String groupHint = null;

    private int autoGeneratedKeys = -1;

    private int[] columnIndexes = null;

    private String[] columnNames = null;

    private int resultSetType = -1;

    private int resultSetConcurrency = -1;

    private int resultSetHoldability = -1;

    private ITConnection connection = null;

    private InputStream localInFileInputStream = null;

    private String sqlMode = null;

    private long sqlModeFlags = 0L;

    private ByteString sql = null;

    private String encoding = null;

    private CharsetName sessionCharset = null;

    private String appName;

    private String schemaName;

    private SQLRecorder physicalRecorder;

    private SQLRecorder recorder;

    private SQLTracer tracer;

    private ColumnarTracer columnarTracer;

    private boolean enableTrace;

    private boolean enableDdlTrace;

    private boolean enableFeedBackWorkload;

    private boolean stressTestValid = false;

    private int socketTimeout = -1;

    private boolean fkModifyCascade = false;

    /**
     * 放置一些额外的数据， Alter table用来放置发生过的错误信息, List<ErrorMessage> dbPrivs DbPriv
     */
    private Map<String, Object> extraDatas = new ConcurrentHashMap<>(8);

    private MatrixStatistics stats = null;
    // 下推到下层的系统变量，全部小写
    private Map<String, Object> serverVariables = null;
    // 用户定义的变量，全部小写
    private Map<String, Object> userDefVariables = null;
    // 特殊处理的系统变量以及自定义的系统变量，全部小写
    private Map<String, Object> extraServerVariables = null;

    private String originSql;

    private boolean isPrivilegeMode;

    // INSERT SELECT or UPDATE / DELETE that cannot be pushed down
    private boolean modifySelect;

    /**
     * INSERT/UPDATE/DELETE select 时读写并行，即Select过程和 Insert/Update/Delete 同时进行
     */
    private boolean modifySelectParallel = false;

    private Map<CorrelationId, Row> correlateRowMap = Maps.newHashMap();
    private Map<RexFieldAccess, RexNode> correlateFieldInViewMap = Maps.newHashMap();
    private Map<Integer, ScalarSubQueryExecContext> scalarSubqueryCtxMap = Maps.newHashMap();

    private ExplainResult explain;
    private SqlType sqlType;
    private BitSet planProperties = new BitSet(ExecutionPlanProperties.getMaxPropertyValue());

    /**
     * 用于sql.log日志，方便sql审计 hasScanWholeTable 是否存在全表扫描 :
     * 一个logicalview里面访问的分片数目大于1 hasUnpushedJoin 是否存在跨库join : 存在没下推的Join
     * hasTempTable 是否存在临时表 : 存在TempSort
     */
    private boolean hasScanWholeTable = false;

    private boolean hasUnpushedJoin = false;

    private boolean hasTempTable = false;

    // Save privilegeVerifyItems for running logicalPlans
    private List<PrivilegeVerifyItem> privilegeVerifyItems = new ArrayList<>();

    // Memory Pool
    private boolean onlyUseTmpTblPool = true;
    private boolean internalSystemSql = true;
    private String sqlTemplateId = null;
    private RuntimeStat runtimeStatistics = null;

    /**
     * Only use physical sql cache when it is a query from external user.
     * When it is an internal sql, caching physical sql may cause expansion of params in ExecutionContext
     *
     * @see com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder
     * @see com.alibaba.polardbx.optimizer.core.rel.LogicalView
     */
    private boolean usingPhySqlCache = false;

    private QueryMemoryPoolHolder memoryPoolHolder = new QueryMemoryPoolHolder();
    private boolean doingBatchInsertBySpliter = false;

    /**
     * 当前的的SQL执行是否是正在在apply子查询的操作，默认是false
     */
    private boolean isApplyingSubquery = false;

    private String subqueryId;

    private Map<Integer, Object> cacheRefs = Maps.newConcurrentMap();

    private Set<Integer> cacheRelNodeIds = Sets.newConcurrentHashSet();

    private ExecutionPlan finalPlan;

    private RelNode unOptimizedPlan = null;

    protected InternalTimeZone timeZone;

    private String traceId;

    private Long phySqlId;

    private Long sqlId;

    private String cluster;

    private long startTime;

    private long logicalSqlStartTimeInMs = -1;

    private long logicalSqlStartTime = -1;

    private ExecutorMode executeMode = ExecutorMode.NONE;

    private WorkloadType workloadType;

    private String mdcConnString;

    private Map<Integer, Integer> recordRowCnt = Maps.newConcurrentMap();

    private Map<Integer, Integer> distinctKeyCnt = Maps.newConcurrentMap();

    // DDL Related Parameters
    private AsyncDDLContext asyncDDLContext = new AsyncDDLContext();
    private DdlContext ddlContext = null;
    private PhyDdlExecutionRecord phyDdlExecutionRecord = null;

    public boolean isEnableTwoPhaseDdl() {
        return enableTwoPhaseDdl;
    }

    private boolean enableTwoPhaseDdl = false;
    private MultiDdlContext multiDdlContext = new MultiDdlContext();
    private boolean randomPhyTableEnabled = true;
    private boolean phyTableRenamed = true;
    private boolean runOnNewDdlEngine = false;
    // End of DDL Related Parameters

    private TableInfoManager tableInfoManager = null;

    /**
     * Key: Logical Dml relNodeId
     * Val: flag that label if the Logical Dml relNode can be directed do mirror write in scale out
     */
    private Map<Integer, Boolean> dmlRelScaleOutWriteFlagMap = new HashMap<>();
    private boolean hasScaleOutWrite = false;
    private boolean isOriginSqlPushdownOrRoute = false;

    private PrivilegeContext privilegeContext;

    private boolean flashbackArea = false;

    private long txId = 0L;
    private long connId;
    private String clientIp;
    private boolean testMode = false;
    private boolean useHint;
    private boolean readOnly;
    private PlanManager.PLAN_SOURCE planSource;
    private static final int MAX_ERROR_COUNT = 1024;

    private LoadDataContext loadDataContext;

    private volatile CclContext cclContext;

    private boolean rescheduled;

    private QuerySpillSpaceMonitor querySpillSpaceMonitor;

    private boolean shareReadView = false;

    private Long groupParallelism = 1L;

    private Point point;

    private Map<String, Object> constantValues = Maps.newHashMap();

    private String returning = null;

    private String backfillReturning = null;

    private boolean optimizedWithReturning = false;
    /**
     * For DirectShardingKeyTableOperation
     */
    private Long backfillId;
    private Long taskId;

    private long estimatedBackfillBatchRows;
    private boolean clientFoundRows = true;

    private volatile XplanStat xplanStat = null;
    private volatile Integer blockBuilderCapacity = null;
    private volatile Boolean enableOssCompatible = null;
    private volatile Boolean enableOssDelayMaterializationOnExchange = null;

    private int columnarMaxShard = -1;
    private boolean useColumnar = false;
    private boolean columnarPlanCache = false;

    private boolean executingPreparedStmt = false;
    private PreparedStmtCache preparedStmtCache = null;

    private Map<Pair<String, List<String>>, Parameters> pruneRawStringMap = null;

    /**
     * True means in cursor-fetch mode.
     */
    private boolean cursorFetchMode = false;

    public void setCursorFetchMode(boolean cursorFetchMode) {
        this.cursorFetchMode = cursorFetchMode;
    }

    public boolean isCursorFetchMode() {
        return cursorFetchMode;
    }

    private CalcitePlanOptimizerTrace calcitePlanOptimizerTrace;

    private String partitionHint;

    private boolean visitDBBuildIn;

    private boolean needAutoSavepoint = false;

    private Set<String> ignoredGsiSet = null;

    private Map<String, List<Object[]>> driverStatistics;

    private boolean checkingCci = false;

    private Map<String, Set<String>> readOrcFiles = null;

    private Map<String, IDeltaReadOption> readDeltaFiles = null;

    private Map<List<Object>, FlashbackColumnarManager> fcManager = new HashMap<>();

    @Getter
    @Setter
    private Function<String, MergedStorageInfo> storageInfoSupplier;

    private long pruningTime = 0L;

    public boolean isOverrideDdlParams() {
        return overrideDdlParams;
    }

    public void setOverrideDdlParams(boolean overrideDdlParams) {
        this.overrideDdlParams = overrideDdlParams;
    }

    private boolean overrideDdlParams = false;

    /**
     * Set this flag to true when executing user sql.
     */
    private boolean userSql = false;

    public ExecutionContext() {
    }

    public ExecutionContext(String schemaName) {
        this.schemaName = schemaName;
    }

    public Map<String, Object> getServerVariables() {
        return serverVariables;
    }

    public void setServerVariables(Map<String, Object> serverVariables) {
        this.serverVariables = serverVariables;
    }

    public Map<String, Object> getExtraServerVariables() {
        return extraServerVariables;
    }

    public void setExtraServerVariables(Map<String, Object> extraServerVariables) {
        this.extraServerVariables = extraServerVariables;
    }

    public Map<String, Object> getUserDefVariables() {
        return userDefVariables;
    }

    public void setUserDefVariables(Map<String, Object> userDefVariables) {
        this.userDefVariables = userDefVariables;
    }

    public ITransaction getTransaction() {
        return transaction;
    }

    public void setTransaction(ITransaction transaction) {
        this.transaction = transaction;
    }

    public Map<String, Object> getExtraCmds() {
        return extraCmds;
    }

    public void setExtraCmds(Map<String, Object> extraCmds) {
        this.extraCmds = extraCmds;
        this.paramManager = new ParamManager(extraCmds);
        asyncDDLContext.setParamManager(this.paramManager);
    }

    public Map<Integer, ParameterContext> getParamMap() {
        if (params == null) {
            return null;
        }

        if (isExecutingPreparedStmt()) {
            return params.getBatchPreparedParameters();
        }
        return params.getCurrentParameter();
    }

    public Parameters getParams() {
        return params;
    }

    public void setParams(Parameters params) {
        this.params = params;
    }

    public Parameters cloneParamsOrNull() {
        return Optional.ofNullable(params).map(Parameters::clone).orElse(null);
    }

    public ServerThreadPool getExecutorService() {
        return this.concurrentService;
    }

    public void setExecutorService(ServerThreadPool concurrentService) {
        this.concurrentService = concurrentService;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public String getGroupHint() {
        return groupHint;
    }

    public void setGroupHint(String groupHint) {
        this.groupHint = groupHint;
    }

    public InternalTimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(InternalTimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public int getAutoGeneratedKeys() {
        return autoGeneratedKeys;
    }

    public void setAutoGeneratedKeys(int autoGeneratedKeys) {
        this.autoGeneratedKeys = autoGeneratedKeys;
    }

    public int[] getColumnIndexes() {
        return columnIndexes;
    }

    public void setColumnIndexes(int[] columnIndexes) {
        this.columnIndexes = columnIndexes;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public int getResultSetType() {
        return resultSetType;
    }

    public void setResultSetType(int resultSetType) {
        this.resultSetType = resultSetType;
    }

    public int getResultSetConcurrency() {
        return resultSetConcurrency;
    }

    public void setResultSetConcurrency(int resultSetConcurrency) {
        this.resultSetConcurrency = resultSetConcurrency;
    }

    public int getResultSetHoldability() {
        return resultSetHoldability;
    }

    public void setResultSetHoldability(int resultSetHoldability) {
        this.resultSetHoldability = resultSetHoldability;
    }

    public ParamManager getParamManager() {
        return this.paramManager;
    }

    public void setParamManager(ParamManager pm) {
        this.paramManager = pm;
    }

    public ITConnection getConnection() {
        return connection;
    }

    public void setConnection(ITConnection connection) {
        this.connection = connection;
    }

    public int getTxIsolation() {
        return txIsolation;
    }

    public void setTxIsolation(int txIsolation) {
        if (txIsolation == this.txIsolation) {
            return;
        }
        if (!ShareReadViewPolicy.supportTxIsolation(txIsolation)) {
            this.shareReadView = false;
        }
        this.txIsolation = txIsolation;
    }

    public InputStream getLocalInfileInputStream() {
        return this.localInFileInputStream;
    }

    public void setLocalInfileInputStream(InputStream stream) {
        this.localInFileInputStream = stream;
    }

    public String getSqlMode() {
        return sqlMode;
    }

    public void setSqlMode(String sqlMode) {
        this.sqlMode = sqlMode;
        this.sqlModeFlags = SQLMode.getCachedFlag(sqlMode);
    }

    public long getSqlModeFlags() {
        return sqlModeFlags;
    }

    public ByteString getSql() {
        return this.sql;
    }

    public void setSql(ByteString sql) {
        this.sql = sql;
    }

    public String getEncoding() {
        return encoding;
    }

    public CharsetName getSessionCharset() {
        return sessionCharset;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
        this.sessionCharset = CharsetName.of(encoding);
    }

    public String getAppName() {
        return this.appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getSchemaName() {
        if (StringUtils.isEmpty(schemaName)) {
            this.schemaName = appName;
        }
        return this.schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
        asyncDDLContext.setSchemaName(schemaName);
    }

    public SQLRecorder getPhysicalRecorder() {
        return this.physicalRecorder;
    }

    public void setPhysicalRecorder(SQLRecorder recorder) {
        this.physicalRecorder = recorder;
    }

    public SQLTracer getTracer() {
        return this.tracer;
    }

    public void setTracer(SQLTracer tracer) {
        this.tracer = tracer;
    }

    public boolean isEnableTrace() {
        return this.enableTrace;
    }

    public void setEnableTrace(boolean enableTrace) {
        this.enableTrace = enableTrace;
    }

    public boolean isEnableDdlTrace() {
        return this.enableDdlTrace;
    }

    public void setEnableDdlTrace(final boolean enableDdlTrace) {
        this.enableDdlTrace = enableDdlTrace;
    }

    public boolean isEnableFeedBackWorkload() {
        return this.enableFeedBackWorkload;
    }

    public void setEnableFeedBackWorkload(boolean enableFeedBackWorkload) {
        this.enableFeedBackWorkload = enableFeedBackWorkload;
    }

    public boolean isStressTestValid() {
        return stressTestValid;
    }

    public void setStressTestValid(boolean stressTestValid) {
        this.stressTestValid = stressTestValid;
    }

    public void setFkModifyCascade(boolean fkModifyCascade) {
        this.fkModifyCascade = fkModifyCascade;
    }

    public boolean getFkModifyCascade() {
        return fkModifyCascade;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public Map<String, Object> getExtraDatas() {
        return extraDatas;
    }

    public void setExtraDatas(Map<String, Object> extraDatas) {
        this.extraDatas = extraDatas;
    }

    public MatrixStatistics getStats() {
        return stats;
    }

    public void setStats(MatrixStatistics stats) {
        this.stats = stats;
    }

    public Map<CorrelationId, Row> getCorrelateRowMap() {
        return correlateRowMap;
    }

    public void registCorrelateRow(CorrelationId correlationId, Row row) {
        if (correlationId != null && row != null) {
            correlateRowMap.put(correlationId, row);
        }
    }

    public List<PrivilegeVerifyItem> getPrivilegeVerifyItems() {
        return privilegeVerifyItems;
    }

    public MemoryPool getMemoryPool() {
        return memoryPoolHolder.getQueryMemoryPool();
    }

    public void setMemoryPool(MemoryPool memoryPool) {
        memoryPoolHolder.initQueryMemoryPool(memoryPool);
    }

    public boolean isFlashbackArea() {
        return flashbackArea;
    }

    public void setFlashbackArea(boolean flashbackArea) {
        this.flashbackArea = flashbackArea;
    }

    public void renewMemoryPoolHolder() {
        memoryPoolHolder.destroy();
        this.memoryPoolHolder = new QueryMemoryPoolHolder();
    }

    public void clearAllMemoryPool() {
        memoryPoolHolder.destroy();
    }

    public Map<RexFieldAccess, RexNode> getCorrelateFieldInViewMap() {
        return correlateFieldInViewMap;
    }

    /**
     * cache
     */
    public Map<Integer, Object> getCacheRefs() {
        return cacheRefs;
    }

    public Map<Integer, Integer> getRecordRowCnt() {
        return recordRowCnt;
    }

    public Map<Integer, Integer> getDistinctKeyCnt() {
        return distinctKeyCnt;
    }

    public Set<Integer> getCacheRelNodeIds() {
        return cacheRelNodeIds;
    }

    public Point getPoint() {
        return point;
    }

    public void setPoint(Point point) {
        this.point = point;
    }

    public Object getConstantValue(String name, AbstractScalarFunction function, Object[] args) {
        if (constantValues.get(name) == null) {
            constantValues.put(name, buildConstantFunction(function, args));
        }
        return constantValues.get(name);
    }

    private Object buildConstantFunction(AbstractScalarFunction function, Object[] args) {
        return function.compute(args, this);
    }

    public String getSqlTemplateId() {
        return sqlTemplateId;
    }

    public void setSqlTemplateId(String sqlTemplateId) {
        this.sqlTemplateId = sqlTemplateId;
    }

    public PlanManager.PLAN_SOURCE getPlanSource() {
        return planSource;
    }

    public void setPlanSource(PlanManager.PLAN_SOURCE planSource) {
        this.planSource = planSource;
    }

    public Pair<String, String> getDbIndexAndTableName() {
        return finalPlan.getDbIndexAndTableName();
    }

    /**
     * subquery paramKey equals RelNode RelatedId,
     * which was built by org.apache.calcite.rel.AbstractRelNode#NEXT_ID
     * it might be negatived
     */
    public Object getScalarSubqueryVal(int paramKey) {
        ScalarSubQueryExecContext ctx = scalarSubqueryCtxMap.get(paramKey);
        Object sbRs = ctx.getSubQueryResult();
        if (sbRs == RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY) {
            return null;
        }
        return sbRs;
    }

    public Map<Integer, ScalarSubQueryExecContext> getScalarSubqueryCtxMap() {
        return scalarSubqueryCtxMap;
    }

    public void clearPreparedStmt() {
        this.executingPreparedStmt = false;
        this.preparedStmtCache = null;
    }

    public String getPartitionHint() {
        return partitionHint;
    }

    public void setPartitionHint(String partitionHint) {
        this.partitionHint = partitionHint;
    }

    public boolean isVisitDBBuildIn() {
        return visitDBBuildIn;
    }

    public void setVisitDBBuildIn(boolean visitDBBuildIn) {
        this.visitDBBuildIn = visitDBBuildIn;
    }

    public ColumnarTracer getColumnarTracer() {
        return columnarTracer;
    }

    public boolean getUseColumnarTracer() {
        return columnarTracer != null;
    }

    public void setColumnarTracer(ColumnarTracer columnarTracer) {
        this.columnarTracer = columnarTracer;
    }

    public long getPruningTime() {
        return pruningTime;
    }

    public void addPruningTime(long pruningTime) {
        this.pruningTime += pruningTime;
    }

    public static class ErrorMessage {

        final int code;
        final String groupName;
        final String message;

        public ErrorMessage(int code, String groupName, String message) {
            this.code = code;
            this.message = message;
            this.groupName = groupName;
        }

        public int getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }

        public String getGroupName() {
            return groupName;
        }

    }

    public SQLRecorder getRecorder() {
        return this.recorder;
    }

    public void setRecorder(SQLRecorder recorder) {
        this.recorder = recorder;
    }

    /**
     * @return the originSql
     */
    public String getOriginSql() {
        return originSql;
    }

    /**
     * @param originSql the originSql to set
     */
    public void setOriginSql(String originSql) {
        this.originSql = originSql;
    }

    public boolean isPrivilegeMode() {
        return isPrivilegeMode;
    }

    public void setPrivilegeMode(boolean privilegeMode) {
        isPrivilegeMode = privilegeMode;
    }

    public boolean isModifySelect() {
        return modifySelect;
    }

    public void setModifySelect(boolean modifySelect) {
        this.modifySelect = modifySelect;
    }

    public boolean isModifySelectParallel() {
        return modifySelectParallel;
    }

    public void setModifySelectParallel(boolean modifySelectParallel) {
        this.modifySelectParallel = modifySelectParallel;
    }

    public ExplainResult getExplain() {
        return explain;
    }

    public void setExplain(ExplainResult explain) {
        this.explain = explain;
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    public void setSqlType(SqlType sqlType) {
        this.sqlType = sqlType;
        if (runtimeStatistics != null) {
            runtimeStatistics.setSqlType(sqlType);
        }
    }

    public void setIgnoredGsi(Set<String> gsiSet) {
        if (gsiSet == null) {
            ignoredGsiSet = null;
            return;
        }
        ignoredGsiSet = ImmutableSet.<String>builder().addAll(gsiSet).build();
    }

    public Set<String> getIgnoredGsi() {
        return ignoredGsiSet;
    }

    public boolean isModifyBroadcastTable() {
        return getPlanProperties().get(ExecutionPlanProperties.MODIFY_BROADCAST_TABLE);
    }

    public void setModifyBroadcastTable(boolean modifyBroadcastTable) {
        getPlanProperties().set(ExecutionPlanProperties.MODIFY_BROADCAST_TABLE, modifyBroadcastTable);
    }

    public boolean isModifyGsiTable() {
        return getPlanProperties().get(ExecutionPlanProperties.MODIFY_GSI_TABLE);
    }

    public void setModifyGsiTable(boolean modifyGsiTable) {
        getPlanProperties().set(ExecutionPlanProperties.MODIFY_GSI_TABLE, modifyGsiTable);
    }

    public boolean isScaleoutWritableTable() {
        return getPlanProperties().get(ExecutionPlanProperties.SCALE_OUT_WRITABLE_TABLE);
    }

    public void setScaleoutWritableTable(boolean modifyScaleoutTable) {
        getPlanProperties().set(ExecutionPlanProperties.SCALE_OUT_WRITABLE_TABLE, modifyScaleoutTable);
    }

    public boolean isModifyOnlineColumnTable() {
        return getPlanProperties().get(ExecutionPlanProperties.MODIFY_ONLINE_COLUMN_TABLE);
    }

    public void setModifyOnlineColumnTable(boolean modifyOnlineColumnTable) {
        getPlanProperties().set(ExecutionPlanProperties.MODIFY_ONLINE_COLUMN_TABLE, modifyOnlineColumnTable);
    }

    public boolean isModifyReplicateTable() {
        return getPlanProperties().get(ExecutionPlanProperties.REPLICATE_TABLE);
    }

    public void setModifyReplicateTable(boolean modifyReplicateTable) {
        getPlanProperties().set(ExecutionPlanProperties.REPLICATE_TABLE, modifyReplicateTable);
    }

    public boolean isModifyShardingColumn() {
        return getPlanProperties().get(ExecutionPlanProperties.MODIFY_SHARDING_COLUMN);
    }

    public boolean isModifyForeignKey() {
        return getPlanProperties().get(ExecutionPlanProperties.MODIFY_FOREIGN_KEY);
    }

    public void setModifyShardingColumn(boolean modifyShardingColumn) {
        getPlanProperties().set(ExecutionPlanProperties.MODIFY_SHARDING_COLUMN, modifyShardingColumn);
    }

    public void setModifyForeignKey(boolean modifyForeignKey) {
        getPlanProperties().set(ExecutionPlanProperties.MODIFY_FOREIGN_KEY, modifyForeignKey);
    }

    public void setModifyScaleOutGroup(boolean isModifyScaleOutGroup) {
        getPlanProperties().set(ExecutionPlanProperties.MODIFY_SCALE_OUT_GROUP, isModifyScaleOutGroup);
    }

    public void isModifyScaleOutGroup() {
        getPlanProperties().get(ExecutionPlanProperties.MODIFY_SCALE_OUT_GROUP);
    }

    public boolean isModifyCrossDb() {
        return getPlanProperties().get(ExecutionPlanProperties.MODIFY_CROSS_DB);
    }

    public void setModifyCrossDb(boolean modifyShardingColumn) {
        getPlanProperties().set(ExecutionPlanProperties.MODIFY_CROSS_DB, modifyShardingColumn);
    }

    public void setPlanProperties(BitSet planProperties) {
        this.planProperties = planProperties;
    }

    public BitSet getPlanProperties() {
        return planProperties;
    }

    public boolean is(BitSet propertySet) {
        return getPlanProperties().intersects(propertySet);
    }

    public boolean hasUnpushedJoin() {
        return hasUnpushedJoin;
    }

    public void setHasUnpushedJoin(boolean hasUnpushedJoin) {
        this.hasUnpushedJoin = hasUnpushedJoin;
    }

    public boolean hasTempTable() {
        return hasTempTable;
    }

    public void setHasTempTable(boolean hasTempTable) {
        this.hasTempTable = hasTempTable;
    }

    public boolean hasScanWholeTable() {
        return hasScanWholeTable;
    }

    public void setHasScanWholeTable(boolean hasScanWholeTable) {
        this.hasScanWholeTable = hasScanWholeTable;
    }

    public void setRuntimeStatistics(RuntimeStat runtimeStatistics) {
        this.runtimeStatistics = runtimeStatistics;
    }

    public RuntimeStat getRuntimeStatistics() {
        return runtimeStatistics;
    }

    public AsyncDDLContext getAsyncDDLContext() {
        return asyncDDLContext;
    }

    public void setAsyncDDLContext(AsyncDDLContext asyncDDLContext) {
        this.asyncDDLContext = asyncDDLContext;
    }

    public DdlContext getDdlContext() {
        return ddlContext;
    }

    public void setDdlContext(DdlContext ddlContext) {
        this.ddlContext = ddlContext;
    }

    public PhyDdlExecutionRecord getPhyDdlExecutionRecord() {
        return this.phyDdlExecutionRecord;
    }

    public void setPhyDdlExecutionRecord(final PhyDdlExecutionRecord phyDdlExecutionRecord) {
        this.phyDdlExecutionRecord = phyDdlExecutionRecord;
    }

    public void setEnableTwoPhaseDdl(final Boolean enableTwoPhaseDdl) {
        this.enableTwoPhaseDdl = enableTwoPhaseDdl;
    }

    public Long getDdlJobId() {
        return getDdlContext() == null ? null : getDdlContext().getJobId();
    }

    public MultiDdlContext getMultiDdlContext() {
        return multiDdlContext;
    }

    public void setMultiDdlContext(MultiDdlContext multiDdlContext) {
        this.multiDdlContext = multiDdlContext;
    }

    public boolean isRandomPhyTableEnabled() {
        return paramManager.getBoolean(ConnectionParams.ENABLE_RANDOM_PHY_TABLE_NAME);
    }

    public boolean needToRenamePhyTables() {
        return !isRandomPhyTableEnabled();
    }

    public TableInfoManager getTableInfoManager() {
        if (tableInfoManager == null) {
            tableInfoManager = new TableInfoManager();
        }
        return tableInfoManager;
    }

    public void setTableInfoManager(TableInfoManager tableInfoManager) {
        this.tableInfoManager = tableInfoManager;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public ExecutionContext copy(Parameters params) {
        return this.copy(new CopyOption().setParameters(params));
    }

    public ExecutionContext copy(CopyOption option) {
        ExecutionContext ec = new ExecutionContext();
        ec.transaction = getTransaction();
        ec.extraCmds = deepCopyExtraCmds(this.extraCmds);
        ec.paramManager = new ParamManager(ec.extraCmds);
        ec.params = option.getParams().getOrElse(() -> this.params);
        ec.concurrentService = getExecutorService();
        ec.autoCommit = isAutoCommit();
        ec.txIsolation = getTxIsolation();
        ec.groupHint = getGroupHint();
        ec.autoGeneratedKeys = getAutoGeneratedKeys();
        ec.columnIndexes = getColumnIndexes();
        ec.columnNames = getColumnNames();
        ec.resultSetType = getResultSetType();
        ec.resultSetConcurrency = getResultSetConcurrency();
        ec.resultSetHoldability = getResultSetHoldability();
        ec.connection = getConnection();
        ec.localInFileInputStream = getLocalInfileInputStream();
        ec.sqlMode = getSqlMode();
        ec.sqlModeFlags = getSqlModeFlags();
        ec.sql = getSql();
        ec.encoding = getEncoding();
        ec.sessionCharset = getSessionCharset();
        ec.appName = getAppName();
        ec.schemaName = getSchemaName();
        ec.physicalRecorder = getPhysicalRecorder();
        ec.recorder = getRecorder();
        ec.tracer = getTracer();
        ec.enableTrace = isEnableTrace();
        ec.enableDdlTrace = isEnableDdlTrace();
        ec.enableFeedBackWorkload = isEnableFeedBackWorkload();
        ec.stressTestValid = isStressTestValid();
        ec.socketTimeout = getSocketTimeout();
        ec.extraDatas = getExtraDatas();
        ec.stats = getStats();
        ec.serverVariables = getServerVariables();
        ec.extraServerVariables = getExtraServerVariables();
        ec.userDefVariables = getUserDefVariables();
        ec.originSql = getOriginSql();
        ec.isPrivilegeMode = isPrivilegeMode();
        ec.modifySelect = isModifySelect();
        ec.modifySelectParallel = isModifySelectParallel();
        ec.correlateRowMap = Maps.newHashMap(getCorrelateRowMap());
        ec.correlateFieldInViewMap = Maps.newConcurrentMap();
        ec.correlateFieldInViewMap.putAll(correlateFieldInViewMap);
        ec.scalarSubqueryCtxMap = Maps.newHashMap(getScalarSubqueryCtxMap());
        ec.explain = getExplain();
        ec.sqlType = getSqlType();
        ec.planProperties = (BitSet) getPlanProperties().clone();
        ec.runtimeStatistics = getRuntimeStatistics();
        ec.sqlTemplateId = getSqlTemplateId();
        ec.asyncDDLContext = getAsyncDDLContext();
        ec.ddlContext = getDdlContext();
        ec.phyDdlExecutionRecord = getPhyDdlExecutionRecord();
        ec.multiDdlContext = getMultiDdlContext();
        ec.phyTableRenamed = isPhyTableRenamed();
        ec.runOnNewDdlEngine = isRunOnNewDdlEngine();
        ec.tableInfoManager = getTableInfoManager();
        ec.cluster = getCluster();
        ec.timeZone = getTimeZone();
        ec.cacheRefs = getCacheRefs();
        ec.cacheRelNodeIds = getCacheRelNodeIds();
        ec.traceId = getTraceId();
        ec.privilegeVerifyItems = getPrivilegeVerifyItems();
        ec.startTime = getStartTime();
        ec.mdcConnString = getMdcConnString();
        ec.executeMode = getExecuteMode();
        ec.hintCmds = getHintCmds();
        ec.recordRowCnt = getRecordRowCnt();
        ec.distinctKeyCnt = getDistinctKeyCnt();
        ec.onlyUseTmpTblPool = isOnlyUseTmpTblPool();
        ec.doingBatchInsertBySpliter = isDoingBatchInsertBySpliter();
        ec.internalSystemSql = isInternalSystemSql();
        ec.usingPhySqlCache = isUsingPhySqlCache();
        ec.runtimeStatistics = getRuntimeStatistics();
        ec.isApplyingSubquery = isApplyingSubquery();
        ec.subqueryId = getSubqueryId();
        ec.xplanStat = getXplanStat();
        ec.memoryPoolHolder = option.getMemoryPoolHolder().getOrElse(() -> this.memoryPoolHolder);
        ec.dmlRelScaleOutWriteFlagMap = getDmlRelScaleOutWriteFlagMap();
        ec.hasScaleOutWrite = isHasScaleOutWrite();
        ec.isOriginSqlPushdownOrRoute = isOriginSqlPushdownOrRoute();
        ec.privilegeContext = getPrivilegeContext();
        ec.txId = getTxId();
        ec.ignoredGsiSet = getIgnoredGsi();
        ec.clientIp = getClientIp();
        ec.flashbackArea = isFlashbackArea();
        ec.connId = getConnId();
        ec.rescheduled = isRescheduled();
        ec.testMode = isTestMode();
        ec.useHint = isUseHint();
        ec.loadDataContext = getLoadDataContext();
        ec.schemaManagers = new ConcurrentHashMap<>(this.schemaManagers);
        ec.currentSchemaManager = this.currentSchemaManager;
        ec.finalPlan = getFinalPlan();
        ec.parameterNlsStrings = getParameterNlsStrings();
        ec.unOptimizedPlan = getUnOptimizedPlan();
        ec.querySpillSpaceMonitor = getQuerySpillSpaceMonitor();
        ec.shareReadView = isShareReadView();
        ec.groupParallelism = getGroupParallelism();
        ec.point = getPoint();
        ec.workloadType = getWorkloadType();
        ec.phySqlId = getPhySqlId();
        ec.sqlId = getSqlId();
        ec.planSource = getPlanSource();
        ec.returning = getReturning();
        ec.backfillReturning = getBackfillReturning();
        ec.optimizedWithReturning = isOptimizedWithReturning();
        ec.readOnly = isReadOnly();
        ec.backfillId = getBackfillId();
        ec.taskId = getTaskId();
        ec.clientFoundRows = isClientFoundRows();
        ec.blockBuilderCapacity = getBlockBuilderCapacity();
        ec.enableOssCompatible = isEnableOssCompatible();
        ec.enableOssDelayMaterializationOnExchange = isEnableOssDelayMaterializationOnExchange();
        ec.executingPreparedStmt = isExecutingPreparedStmt();
        ec.preparedStmtCache = getPreparedStmtCache();
        ec.logicalSqlStartTimeInMs = getLogicalSqlStartTimeInMs();
        ec.logicalSqlStartTime = getLogicalSqlStartTime();
        ec.needAutoSavepoint = isNeedAutoSavepoint();
        ec.setColumnarTracer(getColumnarTracer());
        ec.columnarMaxShard = getColumnarMaxShard();
        ec.useColumnar = isUseColumnar();
        ec.columnarPlanCache = isColumnarPlanCache();
        ec.storageInfoSupplier = getStorageInfoSupplier();
        ec.pruningTime = getPruningTime();
        ec.overrideDdlParams = isOverrideDdlParams();
        ec.userSql = isUserSql();
        return ec;
    }

    public void refreshTableMeta() {
        this.schemaManagers = new ConcurrentHashMap<>();
        this.currentSchemaManager = null;
    }

    public ExecutionContext copy() {
        return copy(this.params == null ? null : getParams().clone());
    }

    public ExecutionPlan getFinalPlan() {
        return finalPlan;
    }

    public void setFinalPlan(ExecutionPlan finalPlan) {
        this.finalPlan = finalPlan;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String getMdcConnString() {
        return mdcConnString;
    }

    public void setMdcConnString(String mdcConnString) {
        this.mdcConnString = mdcConnString;
    }

    public ExecutorMode getExecuteMode() {
        return executeMode;
    }

    public void setExecuteMode(ExecutorMode executeMode) {
        this.executeMode = executeMode;
    }

    public Map<String, Object> getHintCmds() {
        return hintCmds;
    }

    public Map<String, Object> getDefaultExtraCmds() {
        return defaultExtraCmds;
    }

    public void putAllHintCmdsWithDefault(Map<String, Object> hintCmds) {
        this.hintCmds.putAll(hintCmds);
        if (this.extraCmds != null && hintCmds != null) {
            this.defaultExtraCmds = new HashMap<>();
            for (Map.Entry<String, Object> entry : hintCmds.entrySet()) {
                String key = entry.getKey();
                // prepare default extra cmd for 'explain statistics' and 'select into outfile statistics'
                this.defaultExtraCmds.put(key, this.extraCmds.getOrDefault(key, null));
                this.extraCmds.put(key, entry.getValue());
            }
        }
    }

    public void putAllHintCmds(Map<String, Object> hintCmds) {
        this.hintCmds.putAll(hintCmds);
        if (this.extraCmds != null && hintCmds != null) {
            this.extraCmds.putAll(hintCmds);
        }
    }

    public void putIntoHintCmds(String key, Object value) {
        this.hintCmds.put(key, value);
        if (this.extraCmds != null) {
            this.extraCmds.put(key, value);
        }
    }

    public boolean isOnlyUseTmpTblPool() {
        return onlyUseTmpTblPool;
    }

    public void setOnlyUseTmpTblPool(boolean onlyUseTmpTblPool) {
        this.onlyUseTmpTblPool = onlyUseTmpTblPool;
    }

    public boolean isDoingBatchInsertBySpliter() {
        return doingBatchInsertBySpliter;
    }

    public void setDoingBatchInsertBySpliter(boolean doingBatchInsertBySpliter) {
        this.doingBatchInsertBySpliter = doingBatchInsertBySpliter;
    }

    public boolean isApplyingSubquery() {
        return isApplyingSubquery;
    }

    public void setApplyingSubquery(boolean applyingSubquery) {
        isApplyingSubquery = applyingSubquery;
    }

    public boolean isInternalSystemSql() {
        return internalSystemSql;
    }

    public void setInternalSystemSql(boolean internalSystemSql) {
        this.internalSystemSql = internalSystemSql;
    }

    private boolean isUsingPhySqlCache() {
        return usingPhySqlCache;
    }

    public void setUsingPhySqlCache(boolean usingPhySqlCache) {
        this.usingPhySqlCache = usingPhySqlCache;
    }

    public boolean enablePhySqlCache() {
        return usingPhySqlCache && getParamManager()
            .getBoolean(ConnectionParams.PHY_SQL_TEMPLATE_CACHE);
    }

    public String getSubqueryId() {
        return subqueryId;
    }

    public void setSubqueryId(String subqueryId) {
        this.subqueryId = subqueryId;
    }

    public Map<Integer, Boolean> getDmlRelScaleOutWriteFlagMap() {
        return dmlRelScaleOutWriteFlagMap;
    }

    public void setDmlRelScaleOutWriteFlagMap(Map<Integer, Boolean> dmlRelScaleOutWriteFlagMap) {
        this.dmlRelScaleOutWriteFlagMap = dmlRelScaleOutWriteFlagMap;
    }

    public boolean isHasScaleOutWrite() {
        return hasScaleOutWrite;
    }

    public void setHasScaleOutWrite(boolean hasScaleOutWrite) {
        this.hasScaleOutWrite = hasScaleOutWrite;
    }

    public boolean isOriginSqlPushdownOrRoute() {
        return isOriginSqlPushdownOrRoute;
    }

    public void setOriginSqlPushdownOrRoute(boolean originSqlPushdownOrRoute) {
        isOriginSqlPushdownOrRoute = originSqlPushdownOrRoute;
    }

    public PrivilegeContext getPrivilegeContext() {
        return privilegeContext;
    }

    public void setPrivilegeContext(PrivilegeContext privilegeContext) {
        this.privilegeContext = privilegeContext;
    }

    public synchronized void addMessage(String type, ErrorMessage message) {
        @SuppressWarnings("unchecked")
        List<ErrorMessage> messages = (List<ErrorMessage>) extraDatas.computeIfAbsent(type, t -> new ArrayList<>());

        if (type == ExecutionContext.FAILED_MESSAGE) {
            if (messages.size() > MAX_ERROR_COUNT) {
                messages.remove(0);
            }
        }
        messages.add(message);
    }

    public synchronized void clearMessage(String type) {
        @SuppressWarnings("unchecked")
        List<ErrorMessage> messages = (List<ErrorMessage>) extraDatas.get(type);

        if (messages != null) {
            messages.clear();
        }
    }

    public long getTxId() {
        return txId;
    }

    public void setTxId(long txId) {
        this.txId = txId;
    }

    public long getConnId() {
        return connId;
    }

    public void setConnId(long connId) {
        this.connId = connId;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public int getExecutorChunkLimit() {
        return getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
    }

    public boolean isTestMode() {
        return testMode;
    }

    public void setTestMode(boolean testMode) {
        this.testMode = testMode;
    }

    public boolean isUseHint() {
        return useHint;
    }

    public void setUseHint(boolean useHint) {
        this.useHint = useHint;
    }

    public LoadDataContext getLoadDataContext() {
        return loadDataContext;
    }

    public void setLoadDataContext(LoadDataContext loadDataContext) {
        this.loadDataContext = loadDataContext;
    }

    public void newStatement() {
        this.testMode = false;
        this.executingPreparedStmt = false;
        this.preparedStmtCache = null;
    }

    public MergedStorageInfo getStorageInfo(String schemaName) {
        return storageInfoSupplier.apply(schemaName);
    }

    public SchemaManager getSchemaManager(String schemaName) {
        if (schemaName == null) {
            schemaName = DefaultSchema.getSchemaName();
        }
        if (schemaName == null) {
            throw new IllegalArgumentException("Default schema cannot be null");
        }
        schemaName = schemaName.toLowerCase();

        SchemaManager m = schemaManagers.get(schemaName);

        if (m != null) {
            return m;
        }
        OptimizerContext oc = OptimizerContext.getContext(schemaName);
        if (oc == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_DATABASE, schemaName);
        }
        m = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        schemaManagers.put(schemaName, m);

        if (schemaName.equalsIgnoreCase(this.schemaName)) {
            currentSchemaManager = m;
        }
        return m;
    }

    public SchemaManager getSchemaManager() {
        if (this.currentSchemaManager != null) {
            return this.currentSchemaManager;
        }
        return this.getSchemaManager(this.schemaName);
    }

    public Map<String, SchemaManager> getSchemaManagers() {
        return this.schemaManagers;
    }

    public ExecutionContext setSchemaManagers(Map<String, SchemaManager> schemaManagers) {
        this.schemaManagers = schemaManagers;
        return this;
    }

    public ExecutionContext setSchemaManager(String schemaName, SchemaManager sm) {
        if (schemaName == null || sm == null) {
            return this;
        }
        schemaManagers.put(schemaName, sm);
        return this;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public Map<Integer, NlsString> getParameterNlsStrings() {
        return parameterNlsStrings;
    }

    public void setParameterNlsStrings(Map<Integer, NlsString> parameterNlsStrings) {
        this.parameterNlsStrings = parameterNlsStrings;
    }

    public static final class CopyOption {
        private final ValueHolder<Parameters> params = new ValueHolder<>();
        private final ValueHolder<QueryMemoryPoolHolder> memoryPoolHolder = new ValueHolder<>();
        //private final ValueHolder<Map<String, Object>> extraCmds = new ValueHolder<>();

        public CopyOption setParameters(Parameters params) {
            this.params.set(params);
            return this;
        }

        public ValueHolder<Parameters> getParams() {
            return params;
        }

        public ValueHolder<QueryMemoryPoolHolder> getMemoryPoolHolder() {
            return memoryPoolHolder;
        }

        // Do be careful when setting this because it may cause memory leak.
        public CopyOption setMemoryPoolHolder(QueryMemoryPoolHolder memoryPoolHolder) {
            this.memoryPoolHolder.set(memoryPoolHolder);
            return this;
        }

    }

    public static Map<String, Object> deepCopyExtraCmds(Map<String, Object> extraCmds) {
        if (extraCmds == null) {
            return null;
        } else if (extraCmds instanceof MergeHashMap) {
            return ((MergeHashMap) extraCmds).deepCopy();
        } else if (extraCmds instanceof TreeMap) {
            TreeMap newTreeMap = Maps.newTreeMap(((TreeMap) extraCmds).comparator());
            newTreeMap.putAll(extraCmds);
            return newTreeMap;
        } else {
            return Maps.newHashMap(extraCmds);
        }
    }

    public RelNode getUnOptimizedPlan() {
        return unOptimizedPlan;
    }

    public void setUnOptimizedPlan(RelNode unOptimizedPlan) {
        this.unOptimizedPlan = unOptimizedPlan;
    }

    public XplanStat getXplanStat() {
        return xplanStat;
    }

    public void setXplanIndex(String xplanIndex) {
        if (xplanStat != null) {
            this.xplanStat.setXplanIndex(xplanIndex);
        }
    }

    public void setXplanStat(boolean forbidXplan) {
        this.xplanStat = new XplanStat(forbidXplan);
    }

    public CclContext getCclContext() {
        return this.cclContext;
    }

    public void setCclContext(CclContext cclContext) {
        this.cclContext = cclContext;
    }

    public boolean isRescheduled() {
        return rescheduled;
    }

    public void setRescheduled(boolean rescheduled) {
        this.rescheduled = rescheduled;
    }

    public QuerySpillSpaceMonitor getQuerySpillSpaceMonitor() {
        return querySpillSpaceMonitor;
    }

    public void setQuerySpillSpaceMonitor(QuerySpillSpaceMonitor querySpillSpaceMonitor) {
        if (this.querySpillSpaceMonitor == null) {
            this.querySpillSpaceMonitor = querySpillSpaceMonitor;
        }
    }

    public boolean isShareReadView() {
        return shareReadView;
    }

    public void setShareReadView(boolean shareReadView) {
        if (shareReadView == this.shareReadView) {
            return;
        }
        if (shareReadView) {
            // 如果是在serverConnection之外设置shareReadView 需重新检查隔离级别条件
            ShareReadViewPolicy.checkTxIsolation(this.txIsolation);
        }
        this.shareReadView = shareReadView;
    }

    public Long getGroupParallelism() {
        return groupParallelism;
    }

    public void setGroupParallelism(Long groupParallelism) {
        this.groupParallelism = groupParallelism;
    }

    public boolean isAllowGroupMultiWriteConns() {
        return groupParallelism != null && groupParallelism > 1;
    }

    public WorkloadType getWorkloadType() {
        return workloadType;
    }

    public void setWorkloadType(WorkloadType workloadType) {
        this.workloadType = workloadType;
    }

    public Long getPhySqlId() {
        return phySqlId;
    }

    public void setPhySqlId(Long phySqlId) {
        this.phySqlId = phySqlId;
    }

    public Long getSqlId() {
        return sqlId;
    }

    public void setSqlId(Long sqlId) {
        this.sqlId = sqlId;
    }

    public void clearContextInsideTrans() {
        // Make sure memory pool is released after query
        try {
            if (getRuntimeStatistics() != null) {
                getRuntimeStatistics().holdMemoryPool();
            }
            clearAllMemoryPool();
        } catch (Throwable e) {
            logger.warn("Failed to release memory of current request", e);
        }

        scalarSubqueryCtxMap.clear();
        this.cclContext = null;

        try {
            if (getQuerySpillSpaceMonitor() != null) {
                getQuerySpillSpaceMonitor().close();
            }
        } catch (Exception e) {
            logger.error("close querySpillSpaceMonitor: ", e);
        }

        constantValues.clear();
        cacheRefs.clear();
        cacheRelNodeIds.clear();

        // clear params to release memory
        params = null;

        calcitePlanOptimizerTrace = null;

        flashbackArea = false;

        // reset use hint flag
        useHint = false;
        xplanStat = null;
        pruningTime = 0L;
        ignoredGsiSet = null;

        // clear tid
        sqlTemplateId = null;
        columnarMaxShard = -1;
        useColumnar = false;
        columnarPlanCache = false;

        planProperties = new BitSet();
    }

    /**
     * clear context after the execution of every statement
     */
    public void clearContextAfterTrans() {
        clearContextInsideTrans();

        // clear fieldsConnectionParams.
        Object lastFailedMessage = getExtraDatas().get(ExecutionContext.FAILED_MESSAGE);
        if (lastFailedMessage != null) {
            getExtraDatas().put(ExecutionContext.LAST_FAILED_MESSAGE, lastFailedMessage);
        }
        defaultExtraCmds = null;
        hintCmds = null;
        schemaManagers = new ConcurrentHashMap<>();
        currentSchemaManager = null;
        parameterNlsStrings = null;
        concurrentService = null;
        autoCommit = true;
        txIsolation = Connection.TRANSACTION_READ_COMMITTED;
        groupHint = null;
        autoGeneratedKeys = -1;
        columnIndexes = null;
        columnNames = null;
        resultSetType = -1;
        resultSetConcurrency = -1;
        resultSetHoldability = -1;
        connection = null;
        localInFileInputStream = null;
        sqlMode = null;
        sqlModeFlags = 0L;
        sql = null;
        encoding = null;
        sessionCharset = null;
        physicalRecorder = null;
        recorder = null;
        tracer = null;
        enableTrace = false;
        enableDdlTrace = false;
        enableFeedBackWorkload = false;
        stressTestValid = false;
        socketTimeout = -1;
        stats = null;
        originSql = null;
        isPrivilegeMode = false;
        modifySelect = false;
        correlateRowMap = Maps.newHashMap();
        correlateFieldInViewMap = Maps.newHashMap();
        explain = null;
        xplanStat = null;
        sqlType = null;
        hasScanWholeTable = false;
        hasUnpushedJoin = false;
        hasTempTable = false;
        privilegeVerifyItems = new ArrayList<>();
        onlyUseTmpTblPool = true;
        internalSystemSql = true;
        runtimeStatistics = null;
        usingPhySqlCache = false;
        doingBatchInsertBySpliter = false;
        isApplyingSubquery = false;
        subqueryId = null;
        blockBuilderCapacity = null;
        enableOssCompatible = null;
        enableOssDelayMaterializationOnExchange = null;
        finalPlan = null;
        unOptimizedPlan = null;
        timeZone = null;
        traceId = null;
        phySqlId = null;
        sqlId = null;
        cluster = null;
        startTime = 0L;
        executeMode = ExecutorMode.NONE;
        workloadType = null;
        mdcConnString = null;
        recordRowCnt = Maps.newConcurrentMap();
        distinctKeyCnt = Maps.newConcurrentMap();
        asyncDDLContext = new AsyncDDLContext();
        ddlContext = null;
        phyDdlExecutionRecord = null;
        multiDdlContext = new MultiDdlContext();
        randomPhyTableEnabled = true;
        phyTableRenamed = true;
        runOnNewDdlEngine = false;
        tableInfoManager = null;
        dmlRelScaleOutWriteFlagMap = new HashMap<>();
        hasScaleOutWrite = false;
        isOriginSqlPushdownOrRoute = false;
        privilegeContext = null;
        txId = 0L;
        connId = 0L;
        clientIp = null;
        testMode = false;
        useHint = false;
        readOnly = false;
        planSource = null;
        loadDataContext = null;
        rescheduled = false;
        cclContext = null;
        querySpillSpaceMonitor = null;
        shareReadView = false;
        point = null;
        optimizedWithReturning = false;
        backfillId = 0L;
        clientFoundRows = true;

        executingPreparedStmt = false;

        blockBuilderCapacity = null;
        enableOssCompatible = null;
        enableOssDelayMaterializationOnExchange = null;

        fcManager = null;
    }

    public boolean useReturning() {
        return null != returning || null != backfillReturning;
    }

    public String getReturning() {
        return returning;
    }

    public void setReturning(String returning) {
        this.returning = returning;
    }

    public String getBackfillReturning() {
        return backfillReturning;
    }

    public void setBackfillReturning(String backfillReturning) {
        this.backfillReturning = backfillReturning;
    }

    public boolean isOptimizedWithReturning() {
        return optimizedWithReturning;
    }

    public void setOptimizedWithReturning(boolean optimizedWithReturning) {
        this.optimizedWithReturning = optimizedWithReturning;
    }

    public boolean isBatchPrepare() {
        return executingPreparedStmt && params != null && params.isBatch();
    }

    public boolean isExecutingPreparedStmt() {
        return executingPreparedStmt;
    }

    public void setIsExecutingPreparedStmt(boolean preparedStmt) {
        this.executingPreparedStmt = preparedStmt;
    }

    public PreparedStmtCache getPreparedStmtCache() {
        return preparedStmtCache;
    }

    public void setPreparedStmtCache(PreparedStmtCache preparedStmtCache) {
        this.preparedStmtCache = preparedStmtCache;
    }

    public Long getBackfillId() {
        return backfillId;
    }

    public void setBackfillId(Long backfillId) {
        this.backfillId = backfillId;
    }

    public long getEstimatedBackfillBatchRows() {
        return estimatedBackfillBatchRows;
    }

    public void setEstimatedBackfillBatchRows(long estimatedBackfillBatchRows) {
        this.estimatedBackfillBatchRows = estimatedBackfillBatchRows;
    }

    public Long getTaskId() {
        return taskId;
    }

    public void setTaskId(Long taskId) {
        this.taskId = taskId;
    }

    public boolean isClientFoundRows() {
        return clientFoundRows;
    }

    public boolean isPhyTableRenamed() {
        return phyTableRenamed;
    }

    public boolean isRunOnNewDdlEngine() {
        return runOnNewDdlEngine;
    }

    public void setClientFoundRows(boolean clientFoundRows) {
        this.clientFoundRows = clientFoundRows;
    }

    public long getCapabilityFlags() {
        // 默认的capabilities为:
        // (Capabilities.CLIENT_FOUND_ROWS |
        //  Capabilities.CLIENT_MULTI_RESULTS |
        //  Capabilities.CLIENT_DEPRECATE_EOF |
        //  Capabilities.CLIENT_PS_MULTI_RESULTS)
        // 这里使用0来表示这4个默认flags的组合，如果对应其他flags的组合，则需要传递对应的flags值
        // 使用0作为默认值能够在构建protobuf时候减少数据传输量（1个tag+varint，约几个字节），减少网络带宽使用
        // 由于不存在所有flags都不开的情况，所以0可以作为默认值来代表着4个flags的情况
        return clientFoundRows ? 0 : Capabilities.CLIENT_MULTI_RESULTS | Capabilities.CLIENT_DEPRECATE_EOF |
            Capabilities.CLIENT_PS_MULTI_RESULTS;
    }

    public int getBlockBuilderCapacity() {
        if (blockBuilderCapacity == null) {
            blockBuilderCapacity = paramManager.getInt(ConnectionParams.BLOCK_BUILDER_CAPACITY);
        }
        return blockBuilderCapacity;
    }

    public boolean isEnableOssCompatible() {
        if (enableOssCompatible == null) {
            enableOssCompatible = paramManager.getBoolean(ConnectionParams.ENABLE_OSS_COMPATIBLE);
        }
        return enableOssCompatible;
    }

    @VisibleForTesting
    public void setEnableOssCompatible(Boolean enableOssCompatible) {
        this.enableOssCompatible = enableOssCompatible;
    }

    public boolean isEnableOssDelayMaterializationOnExchange() {
        if (enableOssDelayMaterializationOnExchange == null) {
            enableOssDelayMaterializationOnExchange =
                paramManager.getBoolean(ConnectionParams.ENABLE_OSS_DELAY_MATERIALIZATION_ON_EXCHANGE);
        }
        return enableOssDelayMaterializationOnExchange;
    }

    public int getColumnarMaxShard() {
        return columnarMaxShard;
    }

    public void setColumnarMaxShard(int columnarMaxShard) {
        this.columnarMaxShard = columnarMaxShard;
    }

    public boolean isUseColumnar() {
        return useColumnar;
    }

    public void setUseColumnar(boolean useColumnar) {
        this.useColumnar = useColumnar;
    }

    public boolean isColumnarPlanCache() {
        return columnarPlanCache;
    }

    public void setColumnarPlanCache(boolean columnarPlanCache) {
        this.columnarPlanCache = columnarPlanCache;
    }

    /**
     * copy context for optimizer
     */
    public ExecutionContext copyContextForOptimizer() {
        ExecutionContext executionContext = new ExecutionContext(schemaName);

        executionContext.setExtraCmds(deepCopyExtraCmds(this.extraCmds));
        executionContext.setParams(params.clone());
        executionContext.setExplain(explain);
        executionContext.setSqlType(sqlType);
        executionContext.setAutoCommit(autoCommit);
        executionContext.setTraceId(traceId);
        return executionContext;
    }

    public boolean isSupportAutoSavepoint() {
        // Return False by default.
        return null != this.getParamManager() && this.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_AUTO_SAVEPOINT);
    }

    public boolean enableForeignKey() {
        // Return false by default.
        return null != this.getParamManager()
            && this.getParamManager().getBoolean(ConnectionParams.ENABLE_FOREIGN_KEY);
    }

    public boolean foreignKeyChecks() {
        if (this.getParamManager().getInt(ConnectionParams.CN_FOREIGN_KEY_CHECKS) != 2) {
            return this.getParamManager().getInt(ConnectionParams.CN_FOREIGN_KEY_CHECKS) == 1;
        } else {
            // First, try to return session config value.
            if (null != serverVariables
                && null != serverVariables.get(ConnectionProperties.FOREIGN_KEY_CHECKS)) {
                return (boolean) serverVariables.get(ConnectionProperties.FOREIGN_KEY_CHECKS);
            }
            // Return global config value.
            return this.getParamManager().getBoolean(ConnectionParams.FOREIGN_KEY_CHECKS);
        }
    }

    public boolean isSuperUser() {
        return this.getPrivilegeContext().getPolarUserInfo().getAccountType().isSuperUser();
    }

    public boolean isGod() {
        return this.getPrivilegeContext().getPolarUserInfo().getAccountType().isGod();
    }

    public boolean isSuperUserOrAllPrivileges() {
        try {
            final AccountType accountType = this.getPrivilegeContext().getPolarUserInfo().getAccountType();
            if (accountType.isGod() || accountType.isDBA()) {
                return true;
            }

            final List<String> grants = PolarPrivManager.getInstance().showGrants(
                this.getPrivilegeContext().getPolarUserInfo(),
                this.getPrivilegeContext().getActiveRoles(),
                this.getPrivilegeContext().getPolarUserInfo().getAccount(),
                Collections.emptyList());
            for (String grant : grants) {
                if (StringUtils.startsWithIgnoreCase(grant, "GRANT ALL PRIVILEGES ON *.*")) {
                    return true;
                }
            }
        } catch (Throwable t) {
            logger.error("Check super privilege error: ", t);
        }
        return false;
    }

    public Optional<CalcitePlanOptimizerTrace> getCalcitePlanOptimizerTrace() {
        return Optional.ofNullable(calcitePlanOptimizerTrace);
    }

    public SqlExplainLevel getSqlExplainLevel() {
        return calcitePlanOptimizerTrace == null ? CalcitePlanOptimizerTrace.DEFAULT_LEVEL :
            calcitePlanOptimizerTrace.getSqlExplainLevel();
    }

    public void setCalcitePlanOptimizerTrace(CalcitePlanOptimizerTrace calcitePlanOptimizerTrace) {
        this.calcitePlanOptimizerTrace = calcitePlanOptimizerTrace;
    }

    public boolean isUserSql() {
        return userSql;
    }

    public void setUserSql(boolean userSql) {
        this.userSql = userSql;
    }

    public boolean isTsoTransaction() {
        return null != transaction && transaction.getTransactionClass().isA(TSO_TRANSACTION);
    }

    public boolean enableForcePrimaryForTso() {
        // Return false by default.
        return null != this.getParamManager() && this.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_FORCE_PRIMARY_FOR_TSO);
    }

    public boolean enableForcePrimaryForFilter() {
        // Return false by default.
        return null != this.getParamManager() && this.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_FORCE_PRIMARY_FOR_FILTER);
    }

    public boolean enableForcePrimaryForGroupBy() {
        // Return false by default.
        return null != this.getParamManager() && this.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_FORCE_PRIMARY_FOR_GROUP_BY);
    }

    public boolean enableAsyncCommit() {
        // Return false by default.
        return null != this.getParamManager()
            && this.getParamManager().getBoolean(ConnectionParams.ENABLE_ASYNC_COMMIT);
    }

    public boolean omitPrepareTs() {
        return null != this.getParamManager()
            && this.getParamManager().getBoolean(ConnectionParams.ASYNC_COMMIT_OMIT_PREPARE_TS);
    }

    public void setLogicalSqlStartTimeInMs(long logicalSqlStartTimeInMs) {
        this.logicalSqlStartTimeInMs = logicalSqlStartTimeInMs;
    }

    public void setLogicalSqlStartTime(long logicalSqlStartTime) {
        this.logicalSqlStartTime = logicalSqlStartTime;
    }

    public long getLogicalSqlStartTimeInMs() {
        return logicalSqlStartTimeInMs;
    }

    public long getLogicalSqlStartTime() {
        return logicalSqlStartTime;
    }

    public long getIdleTrxTimeout() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getLong(ConnectionParams.IDLE_TRANSACTION_TIMEOUT);
        }
        return 0L;
    }

    public long getIdleROTrxTimeout() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getLong(ConnectionParams.IDLE_READONLY_TRANSACTION_TIMEOUT);
        }
        return 0L;
    }

    public long getIdleRWTrxTimeout() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getLong(ConnectionParams.IDLE_WRITE_TRANSACTION_TIMEOUT);
        }
        return 0L;
    }

    public void setNeedAutoSavepoint(boolean needAutoSavepoint) {
        this.needAutoSavepoint = needAutoSavepoint;
    }

    public boolean isNeedAutoSavepoint() {
        return needAutoSavepoint;
    }

    public boolean isIgnoreSettingNoTransaction() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getBoolean(ConnectionParams.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION);
        }
        return false;
    }

    public Set<String> skipDdlTasks() {
        final Set<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        result.addAll(
            Arrays.asList(
                TStringUtil.split(
                    this.getParamManager().getString(ConnectionParams.SKIP_DDL_TASKS),
                    ",")));
        return result;
    }

    public Map<String, List<Object[]>> getDriverStatistics() {
        return driverStatistics;
    }

    public void setDriverStatistics(Map<String, List<Object[]>> driverStatistics) {
        this.driverStatistics = driverStatistics;
    }

    public long getSnapshotTs() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getLong(ConnectionParams.SNAPSHOT_TS);
        }
        return -1L;
    }

    public boolean isCheckingCci() {
        if (checkingCci) {
            return true;
        }
        if (null != this.getParamManager()) {
            return this.getParamManager().getBoolean(ConnectionParams.FORCE_CCI_VISIBLE);
        }
        return false;
    }

    public void setCheckingCci(boolean checkingCci) {
        this.checkingCci = checkingCci;
    }

    public boolean isEnableOrcDeletedScan() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_DELETED_SCAN);
        }
        return false;
    }

    public boolean isEnableOrcRawTypeBlock() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getBoolean(ConnectionParams.ENABLE_ORC_RAW_TYPE_BLOCK);
        }
        return false;
    }

    public boolean isReadCsvOnly() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getBoolean(ConnectionParams.READ_CSV_ONLY);
        }
        return false;
    }

    public Map<String, Set<String>> getReadOrcFiles() {
        return readOrcFiles;
    }

    public void setReadOrcFiles(Map<String, Set<String>> readOrcFiles) {
        this.readOrcFiles = readOrcFiles;
    }

    public Map<String, IDeltaReadOption> getReadDeltaFiles() {
        return readDeltaFiles;
    }

    public void setReadDeltaFiles(Map<String, IDeltaReadOption> readDeltaFiles) {
        this.readDeltaFiles = readDeltaFiles;
    }

    public boolean isReadOrcOnly() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getBoolean(ConnectionParams.READ_ORC_ONLY);
        }
        return false;
    }

    public boolean isReadSpecifiedColumnarFiles() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getBoolean(ConnectionParams.READ_SPECIFIED_COLUMNAR_FILES);
        }
        return false;
    }

    public boolean isCciIncrementalCheck() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getBoolean(ConnectionParams.CCI_INCREMENTAL_CHECK);
        }
        return false;
    }

    public boolean isEnableCciFastChecker() {
        if (null != this.getParamManager()) {
            return getParamManager().getBoolean(ConnectionParams.ENABLE_CCI_FAST_CHECKER);
        }
        return false;
    }

    public boolean isEnableFastParseOrcRawType() {
        if (null != this.getParamManager()) {
            return getParamManager().getBoolean(ConnectionParams.ENABLE_FAST_PARSE_ORC_RAW_TYPE);
        }
        // Default true.
        return true;
    }

    public boolean isEnableAccurateRelTypeToDataType() {
        if (null != this.getParamManager()) {
            return getParamManager().getBoolean(ConnectionParams.ENABLE_ACCURATE_REL_TYPE_TO_DATA_TYPE);
        }
        // Default false.
        return false;
    }

    public boolean isForce2pcDuringCciCheck() {
        if (null != this.getParamManager()) {
            return getParamManager().getBoolean(ConnectionParams.FORCE_2PC_DURING_CCI_CHECK);
        }
        return false;
    }

    public boolean isEnableXaTso() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getBoolean(ConnectionParams.ENABLE_XA_TSO);
        }
        return false;
    }

    public boolean isEnableAutoCommitTso() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getBoolean(ConnectionParams.ENABLE_AUTO_COMMIT_TSO);
        }
        return false;
    }

    public boolean isMarkSyncPoint() {
        // default false
        return null != this.getExtraServerVariables() &&
            null != this.getExtraServerVariables().get(ConnectionProperties.MARK_SYNC_POINT) &&
            Boolean.parseBoolean((String) this.getExtraServerVariables().get(ConnectionProperties.MARK_SYNC_POINT));
    }

    public boolean isEnable1PCOpt() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getBoolean(ConnectionParams.ENABLE_1PC_OPT);
        }
        return true;
    }

    public boolean isForbidAutoCommitTrx() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getBoolean(ConnectionParams.FORBID_AUTO_COMMIT_TRX);
        }
        return false;
    }

    public boolean isEnableExternalConsistencyForWriteTrx() {
        if (null != this.getParamManager()) {
            return this.getParamManager().getBoolean(ConnectionParams.ENABLE_EXTERNAL_CONSISTENCY_FOR_WRITE_TRX);
        }
        return false;
    }

    public FlashbackColumnarManager getFlashbackColumnarManager(Long tso, String logicalSchema, String logicalTable) {
        List<Object> fcKey = Arrays.asList(tso, logicalSchema, logicalTable);
        return fcManager.computeIfAbsent(fcKey, k -> new FlashbackColumnarManager(tso, logicalSchema, logicalTable));
    }

}
