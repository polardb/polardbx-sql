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

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.SQLMode;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.SqlType;
import com.alibaba.polardbx.common.privilege.PrivilegeVerifyItem;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
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
import com.alibaba.polardbx.optimizer.planmanager.feedback.PhyFeedBack;
import com.alibaba.polardbx.optimizer.planmanager.parametric.Point;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import com.alibaba.polardbx.optimizer.statis.SQLRecorder;
import com.alibaba.polardbx.optimizer.statis.SQLTracer;
import com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.alibaba.polardbx.util.ValueHolder;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang.StringUtils;

import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

    public static final String SuccessMessage = "SUCCESS_MESSAGE";
    public static final String FailedMessage = "FAILED_MESSAGE";
    public static final String WARNING_MESSAGE = "WARNING_MESSAGE";
    public static final String LastFailedMessage = "Last_FAILED_MESSAGE";

    /**
     * 当前事务
     */
    private ITransaction transaction;

    private Map<String, Object> extraCmds = new HashMap<>();

    /**
     * 需要传输到mpp worker端的hint参数列表, extraCmds不包含在hintCmds中
     */
    private Map<String, Object> hintCmds = null;

    /**
     * schema manager used in this query
     */
    private Map<String, SchemaManager> schemaManagers = new HashMap<>();

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

    private IConnection connection = null;

    private InputStream localInFileInputStream = null;

    private String sqlMode = null;

    private long sqlModeFlags = 0L;

    private ByteString sql = null;

    private String encoding = null;

    private String appName;

    private String schemaName;

    private SQLRecorder physicalRecorder;

    private SQLRecorder recorder;

    private SQLTracer tracer;

    private boolean enableTrace;

    private boolean enableDdlTrace;

    private boolean enableFeedBackWorkload;

    private boolean stressTestValid = false;

    private int socketTimeout = -1;

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

    private boolean isInFilter;

    // INSERT SELECT or UPDATE / DELETE that cannot be pushed down
    private boolean modifySelect;

    private Map<CorrelationId, Row> correlateRowMap = Maps.newHashMap();
    private Map<RexFieldAccess, RexNode> correlateFieldInViewMap = Maps.newHashMap();

    private ExplainResult explain;
    private SqlType sqlType;
    private BitSet planProperties = new BitSet();

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

    private String cluster;

    private long startTime;

    private ExecutorMode executeMode = ExecutorMode.NONE;

    private WorkloadType workloadType;

    private String mdcConnString;

    private Map<Integer, Integer> recordRowCnt = Maps.newConcurrentMap();

    // DDL Related Parameters
    private DdlContext ddlContext = null;
    private PhyDdlExecutionRecord phyDdlExecutionRecord = null;
    private MultiDdlContext multiDdlContext = new MultiDdlContext();
    private boolean randomPhyTableEnabled = true;
    private boolean phyTableRenamed = true;
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

    private boolean shareReadView;

    private Point point;

    private Map<String, Object> constantValues = Maps.newHashMap();

    private Map<String, PhyFeedBack> xFeedBackMap = Maps.newHashMap();

    private String returning = null;

    private boolean optimizedWithReturning = false;
    private long backfillId;

    private boolean executingPreparedStmt = false;
    private PreparedStmtCache preparedStmtCache = null;

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

    public IConnection getConnection() {
        return connection;
    }

    public void setConnection(IConnection connection) {
        this.connection = connection;
    }

    public int getTxIsolation() {
        return txIsolation;
    }

    public void setTxIsolation(int txIsolation) {
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
        this.sqlModeFlags = SQLMode.convertToFlag(sqlMode);
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

    public void setEncoding(String encoding) {
        this.encoding = encoding;
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

    /**
     * feedback infos from xResult
     */
    public PhyFeedBack getXFeedBack() {
        long examinedRowCount =
            xFeedBackMap.values().stream().mapToLong(xFeedBack -> xFeedBack.getExaminedRowCount()).sum();
        return new PhyFeedBack(examinedRowCount, null);
    }

    public Map<String, PhyFeedBack> getxFeedBackMap() {
        return xFeedBackMap;
    }

    public void clearPreparedStmt() {
        this.executingPreparedStmt = false;
        this.preparedStmtCache = null;
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

    public boolean isInFilter() {
        return isInFilter;
    }

    public void setIsInFilter(boolean isInFilter) {
        this.isInFilter = isInFilter;
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

    public boolean isModifyReplicateTable() {
        return getPlanProperties().get(ExecutionPlanProperties.REPLICATE_TABLE);
    }

    public void setModifyReplicateTable(boolean modifyReplicateTable) {
        getPlanProperties().set(ExecutionPlanProperties.REPLICATE_TABLE, modifyReplicateTable);
    }

    public boolean isModifyShardingColumn() {
        return getPlanProperties().get(ExecutionPlanProperties.MODIFY_SHARDING_COLUMN);
    }

    public void setModifyShardingColumn(boolean modifyShardingColumn) {
        getPlanProperties().set(ExecutionPlanProperties.MODIFY_SHARDING_COLUMN, modifyShardingColumn);
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
        return randomPhyTableEnabled && paramManager.getBoolean(ConnectionParams.ENABLE_RANDOM_PHY_TABLE_NAME);
    }

    public void setRandomPhyTableEnabled(boolean randomPhyTableEnabled) {
        this.randomPhyTableEnabled = randomPhyTableEnabled;
    }

    public boolean isPhyTableRenamed() {
        return phyTableRenamed;
    }

    public void setPhyTableRenamed(boolean phyTableRenamed) {
        this.phyTableRenamed = phyTableRenamed;
    }

    public boolean needToRenamePhyTables() {
        return !isRandomPhyTableEnabled() || isPhyTableRenamed();
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
        ec.extraCmds = getExtraCmds();
        ec.paramManager = getParamManager();
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
        ec.correlateRowMap = Maps.newHashMap(getCorrelateRowMap());
        ec.correlateFieldInViewMap = Maps.newConcurrentMap();
        ec.correlateFieldInViewMap.putAll(correlateFieldInViewMap);
        ec.explain = getExplain();
        ec.sqlType = getSqlType();
        ec.planProperties = (BitSet) getPlanProperties().clone();
        ec.runtimeStatistics = getRuntimeStatistics();
        ec.sqlTemplateId = getSqlTemplateId();
        ec.ddlContext = getDdlContext();
        ec.phyDdlExecutionRecord = getPhyDdlExecutionRecord();
        ec.multiDdlContext = getMultiDdlContext();
        ec.phyTableRenamed = isPhyTableRenamed();
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
        ec.onlyUseTmpTblPool = isOnlyUseTmpTblPool();
        ec.doingBatchInsertBySpliter = isDoingBatchInsertBySpliter();
        ec.internalSystemSql = isInternalSystemSql();
        ec.runtimeStatistics = getRuntimeStatistics();
        ec.isApplyingSubquery = isApplyingSubquery();
        ec.subqueryId = getSubqueryId();
        ec.memoryPoolHolder = option.getMemoryPoolHolder().getOrElse(() -> this.memoryPoolHolder);
        ec.dmlRelScaleOutWriteFlagMap = getDmlRelScaleOutWriteFlagMap();
        ec.hasScaleOutWrite = isHasScaleOutWrite();
        ec.isOriginSqlPushdownOrRoute = isOriginSqlPushdownOrRoute();
        ec.privilegeContext = getPrivilegeContext();
        ec.txId = getTxId();
        ec.clientIp = getClientIp();
        ec.connId = getConnId();
        ec.rescheduled = isRescheduled();
        ec.testMode = isTestMode();
        ec.useHint = isUseHint();
        ec.loadDataContext = getLoadDataContext();
        ec.schemaManagers = new HashMap<>(this.schemaManagers);
        ec.currentSchemaManager = this.currentSchemaManager;
        ec.finalPlan = getFinalPlan();
        ec.parameterNlsStrings = getParameterNlsStrings();
        ec.unOptimizedPlan = getUnOptimizedPlan();
        ec.querySpillSpaceMonitor = getQuerySpillSpaceMonitor();
        ec.shareReadView = isShareReadView();
        ec.point = getPoint();
        ec.workloadType = getWorkloadType();
        ec.phySqlId = getPhySqlId();
        ec.planSource = getPlanSource();
        ec.returning = getReturning();
        ec.optimizedWithReturning = isOptimizedWithReturning();
        ec.readOnly = isReadOnly();
        ec.backfillId = getBackfillId();
        ec.executingPreparedStmt = false;
        ec.preparedStmtCache = null;
        return ec;
    }

    public void refreshTableMeta() {
        this.schemaManagers = new HashMap<>();
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

    public void putAllHintCmds(Map<String, Object> hintCmds) {
        this.hintCmds = hintCmds;
        if (this.extraCmds != null && hintCmds != null) {
            this.extraCmds.putAll(hintCmds);
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

        if (type == ExecutionContext.FailedMessage) {
            if (messages.size() > MAX_ERROR_COUNT) {
                messages.remove(0);
            }
        }
        messages.add(message);
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

    public void setSchemaManagers(Map<String, SchemaManager> schemaManagers) {
        this.schemaManagers = schemaManagers;
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

    public RelNode getUnOptimizedPlan() {
        return unOptimizedPlan;
    }

    public void setUnOptimizedPlan(RelNode unOptimizedPlan) {
        this.unOptimizedPlan = unOptimizedPlan;
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
        this.shareReadView = shareReadView;
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

    /**
     * clear context after the execution of every statement
     */
    public void clearContextForStatement() {
        // Make sure memory pool is released after query
        try {
            if (getRuntimeStatistics() != null) {
                getRuntimeStatistics().holdMemoryPool();
            }
            clearAllMemoryPool();
        } catch (Throwable e) {
            logger.warn("Failed to release memory of current request", e);
        }

        this.cclContext = null;

        try {
            if (getQuerySpillSpaceMonitor() != null) {
                getQuerySpillSpaceMonitor().close();
            }
        } catch (Exception e) {
            logger.error("close querySpillSpaceMonitor: ", e);
        }

        constantValues.clear();
        getxFeedBackMap().clear();
        // clear params to release memory
        if (params != null) {
            params.clear();
        }
    }

    public boolean useReturning() {
        return null != returning;
    }

    public String getReturning() {
        return returning;
    }

    public void setReturning(String returning) {
        this.returning = returning;
    }

    public boolean isOptimizedWithReturning() {
        return optimizedWithReturning;
    }

    public void setOptimizedWithReturning(boolean optimizedWithReturning) {
        this.optimizedWithReturning = optimizedWithReturning;
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

    public long getBackfillId() {
        return backfillId;
    }

    public void setBackfillId(long backfillId) {
        this.backfillId = backfillId;
    }
}
