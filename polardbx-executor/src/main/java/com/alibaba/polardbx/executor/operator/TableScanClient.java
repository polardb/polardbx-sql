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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalTypeBase;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.optimizer.planmanager.feedback.PhyFeedBack;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.rpc.compatible.XPreparedStatement;
import com.alibaba.polardbx.rpc.jdbc.CharsetMapping;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.rpc.result.XResultObject;
import com.alibaba.polardbx.rpc.result.chunk.BlockDecoder;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ConnectionStats;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.ZeroDate;
import com.alibaba.polardbx.common.jdbc.ZeroTimestamp;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.logger.support.LogFormat;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.Xprotocol.XRowSet;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.operator.RangeScanMode;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.transaction.PhyOpTrxConnUtils;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.row.ResultSetRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.statis.SQLRecord;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.repo.mysql.spi.MyDataSourceGetter;
import com.alibaba.polardbx.rpc.compatible.XPreparedStatement;
import com.alibaba.polardbx.rpc.jdbc.CharsetMapping;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.rpc.result.XResultObject;
import com.alibaba.polardbx.rpc.result.chunk.BlockDecoder;
import com.alibaba.polardbx.statistics.ExecuteSQLOperation;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import io.airlift.slice.Slice;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.ANONAMOUS_DBKEY;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTE_MPP;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_X_PROTOCOL_RESULT;
import static com.alibaba.polardbx.common.properties.ConnectionParams.WAIT_BLOOM_FILTER_TIMEOUT_MS;
import static com.alibaba.polardbx.executor.operator.ProducerExecutor.NOT_BLOCKED;
import static com.alibaba.polardbx.optimizer.core.rel.util.RuntimeFilterDynamicParamInfo.ARG_IDX_BLOOM_FILTER_INFO;
import static com.alibaba.polardbx.optimizer.core.rel.util.RuntimeFilterDynamicParamInfo.ARG_IDX_RUNTIME_FILTER_ID;

public class TableScanClient {
    public static final Logger logger = LoggerFactory.getLogger(TableScanClient.class);

    public final static ScheduledExecutorService timeoutService = ExecutorUtil.createScheduler(4,
        new NamedThreadFactory("TableScanClient-XProtocol-Timeout-Scheduler"),
        new ThreadPoolExecutor.CallerRunsPolicy());

    protected final ExecutionContext context;
    protected final boolean useTransaction;
    protected final boolean enableTaskCpu;
    protected final CursorMeta meta;
    protected volatile int prefetchNum;
    protected final AtomicInteger noMoreSplitNum = new AtomicInteger(0);
    protected final AtomicInteger sourceExecNum = new AtomicInteger(0);
    protected final List<Split> splitList = Collections.synchronizedList(new ArrayList<>());
    protected final AtomicInteger completeExecuteNum = new AtomicInteger(0);
    protected final AtomicInteger pushdownSplitIndex = new AtomicInteger(0);
    protected final AtomicInteger completePrefetchNum = new AtomicInteger(0);
    private final int socketTimeout;
    private final long slowTimeThreshold;
    private final Set<SourceExec> sourceExecHashSet = Collections.synchronizedSet(new HashSet<SourceExec>());
    //reset
    @GuardedBy("this")
    private final List<SettableFuture<?>> blockedCallers = new ArrayList<>();
    @Nullable
    protected RuntimeStatistics.OperatorStatisticsGroup targetPlanStatGroup;
    protected RuntimeStatistics runtimeStat;
    protected List<PrefetchThread> prefetchThreads = new ArrayList<>();
    protected Queue<SplitResultSet> readyResultSet = new ConcurrentLinkedQueue<>();

    protected volatile TddlRuntimeException exception = null;
    protected volatile boolean isClosed;

    private volatile SettableFuture<?> waitBloomFilterFuture = null;
    private volatile ScheduledFuture<?> monitorWaitBloomFilterFuture = null;
    private boolean needWaitBloomFilter;
    private volatile Map<Integer, BloomFilterInfo> bloomFilterInfos = null;
    private boolean killStreaming;
    private boolean lessMy56Version = false;

    private RangeScanMode rangeScanMode;

    public TableScanClient(ExecutionContext context, CursorMeta meta,
                           boolean useTransaction, int prefetchNum) {
        this.context = context;
        this.meta = meta;
        this.useTransaction = useTransaction;
        this.socketTimeout = (int) context.getParamManager().getLong(ConnectionParams.SOCKET_TIMEOUT);
        this.prefetchNum = prefetchNum;
        this.slowTimeThreshold = context.getParamManager().getLong(ConnectionParams.SLOW_SQL_TIME);
        this.enableTaskCpu = ExecUtils.isSQLMetricEnabled(context);
        if (context.getRuntimeStatistics() != null) {
            this.runtimeStat = (RuntimeStatistics) context.getRuntimeStatistics();
        }
        this.needWaitBloomFilter = false;
        this.killStreaming = context.getParamManager().getBoolean(
            ConnectionParams.KILL_CLOSE_STREAM);
        try {
            lessMy56Version = ExecutorContext.getContext(
                context.getSchemaName()).getStorageInfoManager().isLessMy56Version();
        } catch (Throwable t) {
            //ignore
        }
    }

    public TableScanClient(ExecutionContext context, CursorMeta meta,
                           boolean useTransaction, int prefetchNum, RangeScanMode rangeScanMode) {
        this(context, meta, useTransaction, prefetchNum);
        this.rangeScanMode = rangeScanMode;
    }

    // Must be called before start
    public synchronized void initWaitFuture(ListenableFuture<List<BloomFilterInfo>> listListenableFuture) {
        if (this.waitBloomFilterFuture == null) {
            this.waitBloomFilterFuture = SettableFuture.create();

            int waitTimeout = context.getParamManager().getInt(WAIT_BLOOM_FILTER_TIMEOUT_MS);
            monitorWaitBloomFilterFuture = ServiceProvider.getInstance().getTimerTaskExecutor().schedule(() -> {
                synchronized (TableScanClient.this) {
                    if (!waitBloomFilterFuture.isDone()) {
                        setException(new TddlRuntimeException(ERR_EXECUTE_MPP,
                            "Table scant client wait bloom filter timeout after " + waitTimeout + " milliseconds"));
                    }
                }
            }, waitTimeout, TimeUnit.MILLISECONDS);

            listListenableFuture.addListener(
                () -> {
                    synchronized (TableScanClient.this) {
                        try {
                            registerBloomFilter(listListenableFuture.get());
                            waitBloomFilterFuture.set(null);
                            monitorWaitBloomFilterFuture.cancel(false);
                        } catch (Throwable t) {
                            logger.error("Failed to register bloom filter!", t);
                            setException(new TddlRuntimeException(ERR_EXECUTE_MPP,
                                "Failed to register bloom filter in table scan client!", t));
                        }
                    }
                }, context.getExecutorService());

            this.needWaitBloomFilter = true;
        }
    }

    public void registerSouceExec(SourceExec sourceExec) {
        this.sourceExecHashSet.add(sourceExec);
    }

    public void addSplit(Split split) {
        this.splitList.add(split);
    }

    private void registerBloomFilter(List<BloomFilterInfo> bloomFilterInfos) {
        try {
            logger.info("Start registering bloom filters.");
            this.bloomFilterInfos = bloomFilterInfos.stream()
                .collect(Collectors.toMap(BloomFilterInfo::getId, Function.identity(), (info1, info2) -> info1));
            for (BloomFilterInfo bloomFilterInfo : bloomFilterInfos) {
                logger.info(String.format("Bloom filter id: %d, first value %x", bloomFilterInfo.getId(),
                    bloomFilterInfo.getData()[0]));
            }
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        }
    }

    public synchronized void executePrefetchThread(boolean force) {
        if (isClosed) {
            return;
        }
        throwIfFailed();
        if (needWaitBloomFilter && waitBloomFilterFuture != null && !waitBloomFilterFuture.isDone()) {
            return;
        }
        int needFetchNum = needFetch();

        if (!useTransaction) {
            if (needFetchNum <= 0 && force && splitList.size() - pushdownSplitIndex.get() > 0) {
                if (beingConnectionCount() < 1) {
                    //不存在正在建连的连接
                    needFetchNum = 1;
                }
            }
        }

        if (needFetchNum > 0) {
            for (int i = 0; i < needFetchNum; i++) {
                int splitIndex = pushdownSplitIndex.get();
                PrefetchThread thread =
                    new PrefetchThread(splitList.get(splitIndex), rangeScanMode != null, splitIndex);
                prefetchThreads.add(thread);
                // Use async X-protocol or original jdbc.
                if (thread.isPureAsyncMode()) {
                    try {
                        thread.call(); // Invoke it directly.
                    } catch (Exception e) {
                        throw GeneralUtil.nestedException(e);
                    }
                } else {
                    //use DRDS server thread pool
                    ServiceProvider.getInstance().getServerExecutor().submit(context.getSchemaName(),
                        context.getTraceId(), -1, thread, null, context.getRuntimeStatistics());
                }
                pushdownSplitIndex.incrementAndGet();
            }
        }
    }

    protected int needFetch() {
        return Math.min(splitList.size() - pushdownSplitIndex.get(), prefetchNum - connectionCount());
    }

    public int getSplitNum() {
        return splitList.size();
    }

    public TableScanClient incrementSourceExec() {
        sourceExecNum.incrementAndGet();
        return this;
    }

    public void incrementNoMoreSplit() {
        noMoreSplitNum.incrementAndGet();
    }

    public boolean noMoreSplit() {
        return noMoreSplitNum.get() >= sourceExecNum.get();
    }

    public boolean noMorePushDownSplit() {
        return noMoreSplit() && splitList.size() == pushdownSplitIndex.get();
    }

    public boolean compeleteAllExecuteSplit() {
        return completeExecuteNum.get() == splitList.size();
    }

    public boolean noMorePrefetchSplit() {
        return completePrefetchNum.get() == splitList.size();
    }

    public boolean noMoreExecuteSplit() {
        return completePrefetchNum.get() == splitList.size() && readyResultSet.peek() == null;
    }

    public int connectionCount() {
        return pushdownSplitIndex.get() - completeExecuteNum.get();
    }

    public int beingConnectionCount() {
        return pushdownSplitIndex.get() - completePrefetchNum.get();
    }

    public List<Split> getSplitList() {
        return splitList;
    }

    public synchronized void addSplitResultSet(SplitResultSet splitResultSet) {
        readyResultSet.add(splitResultSet);
        notifyBlockedCallers();
        if (exception != null) {
            for (PrefetchThread prefetchThread : prefetchThreads) {
                prefetchThread.cancel(false);
            }
        }
    }

    public synchronized SplitResultSet popResultSet() {
        return readyResultSet.poll();
    }

    protected boolean isReady() {
        return readyResultSet.peek() != null || noMoreExecuteSplit();
    }

    public synchronized ListenableFuture<?> isBlocked() {
        throwIfFailed();
        if (isFinished() || isFailed() || isReady()) {
            notifyBlockedCallers();
            return NOT_BLOCKED;
        } else {
            if (needWaitBloomFilter && waitBloomFilterFuture != null && !waitBloomFilterFuture.isDone()) {
                blockedCallers.add(waitBloomFilterFuture);
                return waitBloomFilterFuture;
            } else {
                SettableFuture<?> future = SettableFuture.create();
                blockedCallers.add(future);
                return future;
            }
        }
    }

    public void reset() {
        this.isClosed = true;
        cancelAllThreads(true);
        notifyBlockedCallers();
        prefetchThreads.clear();
        clearResultSet();
        this.completePrefetchNum.set(0);
        this.pushdownSplitIndex.set(0);
        this.completeExecuteNum.set(0);
        this.isClosed = false;
    }

    public void setTargetPlanStatGroup(RuntimeStatistics.OperatorStatisticsGroup targetPlanStatGroup) {
        this.targetPlanStatGroup = targetPlanStatGroup;
    }

    public RangeScanMode getRangeScanMode() {
        return rangeScanMode;
    }

    public void setPrefetchNum(int prefetchNum) {
        this.prefetchNum = prefetchNum;
    }

    public synchronized void setException(TddlRuntimeException exception) {
        if (this.exception == null) {
            this.exception = exception;
        }
        cancelAllThreads(false);
        isClosed = true;
        notifyBlockedCallers();
    }

    public void throwIfFailed() {
        if (exception != null) {
            throw Throwables.propagate(exception);
        }
    }

    protected synchronized void notifyBlockedCallers() {
        for (int i = 0; i < blockedCallers.size(); i++) {
            SettableFuture<?> blockedCaller = blockedCallers.get(i);
            blockedCaller.set(null);
        }
        blockedCallers.clear();
    }

    public boolean isFinished() {
        throwIfFailed();
        return isClosed || compeleteAllExecuteSplit();
    }

    private boolean isFailed() {
        return exception != null;
    }

    public synchronized void close(SourceExec sourceExec) {
        sourceExecHashSet.remove(sourceExec);
        if (sourceExecHashSet.isEmpty()) {
            cancelAllThreads(false);
            isClosed = true;
            prefetchThreads.clear();
            clearResultSet();
        }
        notifyBlockedCallers();
    }

    /**
     * Clears the current ResultSet object.
     * This method should be synchronized, otherwise the async execute of addSplitResultSet may cause the status incorrect.
     */
    synchronized void clearResultSet() {
        readyResultSet.clear();
    }

    public void cancelAllThreads(boolean ignoreCnt) {
        if (!prefetchThreads.isEmpty()) {
            for (PrefetchThread prefetchThread : prefetchThreads) {
                prefetchThread.cancel(ignoreCnt);
            }
        }

    }

    public SplitResultSet newSplitResultSet(JdbcSplit jdbcSplit, boolean rangeScan, int splitIndex) {
        if (!rangeScan) {
            return new SplitResultSet(jdbcSplit);
        } else {
            return new SplitResultSet(jdbcSplit, splitIndex);
        }
    }

    public class SplitResultSet {

        protected JdbcSplit jdbcSplit;
        protected IConnection conn;
        protected PreparedStatement stmt;
        protected ResultSet rs; // Null if pure async.
        protected ResultSetRow rowSet = null; // Null if pure async.
        protected final boolean pureAsync; // Indicate whether use async mode of X-protocol.
        protected XResult xResult = null; // Use this to fetch data when use async mode.
        protected long startTime; // Move time here for log.
        protected long nanoStartTime; // Move time here for log.
        protected ExecuteSQLOperation op;
        protected ConnectionStats connectionStats;
        protected long count;
        protected int phyConnLastSocketTimeout = -1;
        protected boolean closeConnection = false;
        protected AtomicBoolean closed = new AtomicBoolean(false);
        protected SettableFuture blockedFuture;
        protected SettableFuture<String> connectionFuture;

        protected int splitIndex;

        SplitResultSet(JdbcSplit jdbcSplit) {
            this.jdbcSplit = jdbcSplit;
            // Check whether use pure async mode.
            final IDataSource dataSource = ExecutorContext.getContext(this.jdbcSplit.getSchemaName())
                .getTopologyHandler().get(jdbcSplit.getDbIndex()).getDataSource();
            final boolean isXDatasource = ((TGroupDataSource) dataSource).isXDataSource();
            this.pureAsync = isXDatasource && XConnectionManager.getInstance().isEnablePureAsyncMpp();
        }

        SplitResultSet(JdbcSplit jdbcSplit, int splitIndex) {
            this(jdbcSplit);
            this.splitIndex = splitIndex;
        }

        public boolean isPureAsyncMode() {
            return pureAsync;
        }

        public boolean isOnlyXResult() {
            return null == rowSet;
        }

        private void initConnection() throws SQLException {
            if (jdbcSplit.getDbIndex() != null) {
                Object o = ExecutorContext.getContext(jdbcSplit.getSchemaName())
                    .getTopologyHandler().get(jdbcSplit.getDbIndex()).getDataSource();

                if (o instanceof TGroupDataSource) {
//                    conn = context.getTransaction()
//                        .getConnection(jdbcSplit.getSchemaName(), jdbcSplit.getDbIndex(), (TGroupDataSource) o,
//                            jdbcSplit.getTransactionRw());
                    conn = (IConnection) PhyOpTrxConnUtils.getConnection(context.getTransaction(),
                        jdbcSplit.getSchemaName(), jdbcSplit.getDbIndex(), (TGroupDataSource) o,
                        jdbcSplit.getTransactionRw(), context, jdbcSplit.getGrpConnId(context));
                    if (conn == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL,
                            "can't getConnection:" + jdbcSplit.getDbIndex());
                    }

                    if (isPureAsyncMode() && !conn.isWrapperFor(XConnection.class)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_CONNECTION,
                            "Connection protocol changed:" + jdbcSplit.getDbIndex());
                    }

                    if (conn.isWrapperFor(XConnection.class)) {
                        conn.unwrap(XConnection.class).setTraceId(context.getTraceId());
                    }

                    if (conn.getRealConnection() instanceof TGroupDirectConnection) {
                        this.connectionStats = conn.getConnectionStats();
                        collectConnectionStats();
                    }

                    long startInitStmtEnvNano = System.nanoTime();

                    if (socketTimeout >= 0) {
                        setNetworkTimeout(conn, socketTimeout);
                    } else if (context.getSocketTimeout() >= 0) {
                        setNetworkTimeout(conn, context.getSocketTimeout());
                    }
                    //TimeZone 已经设置到serverVariables了
                    conn.setServerVariables(context.getServerVariables());
                    // 只处理设定过的txIsolation
                    if (context.getTxIsolation() >= 0) {
                        conn.setTransactionIsolation(context.getTxIsolation());
                    }
                    // 只处理设置过的编码
                    if (context.getEncoding() != null) {
                        conn.setEncoding(context.getEncoding());
                    }
                    // 只处理设置过的sqlMode
                    if (context.getSqlMode() != null) {
                        conn.setSqlMode(context.getSqlMode());
                    }

                    // FIXME 是否需要把prepareStmt包含进来
                    if (targetPlanStatGroup != null) {
                        targetPlanStatGroup.createAndInitJdbcStmtDuration
                            .addAndGet(System.nanoTime() - startInitStmtEnvNano);
                    }

                    // Add feedback flag if XConnection.
                    if (conn.isWrapperFor(XConnection.class)) {
                        conn.unwrap(XConnection.class).setWithFeedback(true);
                        // set client JDBC capability
                        conn.unwrap(XConnection.class).setCapabilities(context.getCapabilityFlags());
                    }
                } else {
                    throw new SQLException("TGroupDataSource is null:" + jdbcSplit);
                }
            }
        }

        private void setNetworkTimeout(IConnection conn, int socketTimeout) throws SQLException {
            int phyConnLastSocketTimeout = conn.getNetworkTimeout();
            if (socketTimeout != phyConnLastSocketTimeout) {
                conn.setNetworkTimeout(TGroupDirectConnection.socketTimeoutExecutor, socketTimeout);
                this.phyConnLastSocketTimeout = phyConnLastSocketTimeout;
            }
        }

        protected void collectConnectionStats() {
            if (connectionStats != null) {
                if (enableTaskCpu && targetPlanStatGroup != null) {
                    targetPlanStatGroup.createConnDuration.addAndGet(connectionStats.getCreateConnectionNano());
                    targetPlanStatGroup.waitConnDuration.addAndGet(connectionStats.getWaitConnectionNano());
                    targetPlanStatGroup.initConnDuration.addAndGet(connectionStats.getInitConnectionNano());
                }
                if (runtimeStat != null) {
                    runtimeStat.addPhyConnTimecost(
                        connectionStats.getCreateConnectionNano() + connectionStats.getWaitConnectionNano()
                            + connectionStats.getInitConnectionNano());
                }
            }
        }

        protected String getCurrentDbkey() {
            if (ConfigDataMode.isColumnarMode()) {
                return ANONAMOUS_DBKEY;
            }
            IDataSource o = ExecutorContext.getContext(jdbcSplit.getSchemaName())
                .getTopologyHandler().get(jdbcSplit.getDbIndex()).getDataSource();
            String currentDbKey = ANONAMOUS_DBKEY;
            if (o != null && o instanceof TGroupDataSource) {
                if (conn != null) {
                    IConnection realConneciton = conn.getRealConnection();
                    if (realConneciton instanceof TGroupDirectConnection) {
                        currentDbKey = ((TGroupDirectConnection) realConneciton).getDbKey();
                    }
                }
                if (ANONAMOUS_DBKEY == currentDbKey) {
                    MasterSlave masterSlave =
                        ExecUtils.getMasterSlave(useTransaction, jdbcSplit.getTransactionRw().equals(
                            ITransaction.RW.WRITE), context);
                    currentDbKey =
                        ((TGroupDataSource) o).getConfigManager().getDataSource(masterSlave).getDsConfHandle()
                            .getDbKey();
                }
                if (StringUtils.isEmpty(currentDbKey)) {
                    currentDbKey = ANONAMOUS_DBKEY;
                }
            }
            return currentDbKey;
        }

        private PreparedStatement preparedSplit(
            IConnection conn, byte[] hint, BytesSql sql, List<ParameterContext> params, boolean supportGalaxyPrepare,
            byte[] galaxyDigest) throws SQLException {
            final PreparedStatement stmt;
            if (conn.isBytesSqlSupported()) {
                stmt = conn.prepareStatement(sql, hint);
            } else {
                stmt = conn.prepareStatement(new String(hint) + sql.toString(params), ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            }
            if (stmt.isWrapperFor(XPreparedStatement.class)) {
                final XPreparedStatement xPreparedStatement = stmt.unwrap(XPreparedStatement.class);
                final boolean noDigest = jdbcSplit.getTableNames().size() != 1 || // select with union all
                    sql.containRawString(params); // compact n-D param array
                if (noDigest || !supportGalaxyPrepare) {
                    xPreparedStatement.setUseGalaxyPrepare(false);
                } else if (galaxyDigest != null) {
                    xPreparedStatement.setGalaxyDigest(ByteString.copyFrom(galaxyDigest));
                }
            }
            stmt.setFetchSize(Integer.MIN_VALUE);
            if (params != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("preparedSplit:" + context.getTraceId() + ",sql=" + sql.display()
                        + ",params:" + params);
                }
                if (!conn.isBytesSqlSupported()) {
                    params = GeneralUtil.prepareParam(params);
                }
                ParameterMethod.setParameters(stmt, params);
            }
            return stmt;
        }

        protected void executeQuery() throws SQLException {
            startTime = System.currentTimeMillis();
            nanoStartTime = System.nanoTime();
            long sqlCostTime = -1, createCostTime = -1, waitCostTime = -1, startTimeNano = -1;
            long startPrepStmtEnvNano = 0, timeCostNano = 0;
            if (enableTaskCpu) {
                startPrepStmtEnvNano = ThreadCpuStatUtil.getThreadCpuTimeNano();
            }
            try {
                if (bloomFilterInfos != null) {
                    for (ParameterContext paramContext : jdbcSplit.getFlattedParams()) {
                        if (paramContext.getParameterMethod().isBloomFilterParameterMethod()) {
                            Object[] args = paramContext.getArgs();
                            int runtimeFilterId = (Integer) args[ARG_IDX_RUNTIME_FILTER_ID];
                            if (bloomFilterInfos.containsKey(runtimeFilterId)) {
                                args[ARG_IDX_BLOOM_FILTER_INFO] = bloomFilterInfos.get(runtimeFilterId);
                            } else {
                                throw new IllegalStateException("Runtime filter id not found: " + runtimeFilterId);
                            }
                        }
                    }
                }
                stmt = preparedSplit(conn, jdbcSplit.getHint(), jdbcSplit.getUnionBytesSql(false),
                    jdbcSplit.getFlattedParams(), jdbcSplit.isSupportGalaxyPrepare(), jdbcSplit.getGalaxyDigest());
                if (enableTaskCpu && targetPlanStatGroup != null) {
                    targetPlanStatGroup.prepareStmtEnvDuration
                        .addAndGet(ThreadCpuStatUtil.getThreadCpuTimeNano() - startPrepStmtEnvNano);
                }
                context.getStats().physicalRequest.incrementAndGet();
                if (pureAsync) {
                    final XPreparedStatement xPreparedStatement = stmt.unwrap(XPreparedStatement.class);
                    assert xPreparedStatement.getConnection().unwrap(XConnection.class).isStreamMode();
                    boolean chunkResult = true;
                    if (TableScanClient.this.context.isEnableOrcRawTypeBlock()) {
                        // For raw type block, do not enable chunk result.
                        // Normal query should not get here.
                        chunkResult = false;
                    }
                    xPreparedStatement.getConnection().unwrap(XConnection.class).getSession()
                        .setChunkResult(chunkResult);
                    xResult = xPreparedStatement.executeQueryX(); // Return immediately when stream mode.
                } else {
                    startTimeNano = System.nanoTime();
                    rs = stmt.executeQuery();
                    timeCostNano = System.nanoTime() - startTimeNano;
                    // 这里统计的是mysql的执行时间
                    sqlCostTime = (timeCostNano / 1000);
                    rowSet = new ResultSetRow(meta, rs);
                }
            } finally {
                if (pureAsync) {
                    asyncWriteTrace();
                } else {
                    finishQuery(timeCostNano, sqlCostTime, createCostTime, waitCostTime);
                }
            }
        }

        void asyncWriteTrace() {
            if (context.isEnableTrace()) {
                if (null == op) {
                    HashMap<Integer, ParameterContext> logParams = new HashMap<>();
                    for (int i = 0; i < jdbcSplit.getFlattedParams().size(); i++) {
                        logParams.put(i, jdbcSplit.getFlattedParams().get(i));
                    }
                    String currentDbKey = getCurrentDbkey();
                    this.op = new ExecuteSQLOperation(jdbcSplit.getDbIndex(), currentDbKey, jdbcSplit.getSqlString(
                        context.getParamManager().getBoolean(ConnectionParams.ENABLE_SIMPLIFY_TRACE_SQL)),
                        startTime);
                    op.setThreadName(Thread.currentThread().getName());
                    op.setParams(new Parameters(logParams, false));
                }
            }
        }

        void finishAsyncQuery() {
            assert xResult != null && pureAsync;
            final long nowNanos = System.nanoTime();
            final long timeCostNano = nowNanos - xResult.getStartNanos();
            final long sqlCostTime = (timeCostNano / 1000);
            finishQuery(timeCostNano, sqlCostTime, -1, -1);
        }

        void finishQuery(long timeCostNano, long sqlCostTime, long createCostTime, long waitCostTime) {
            // Record info for normal JDBC.
            if (enableTaskCpu && targetPlanStatGroup != null) {
                targetPlanStatGroup.execJdbcStmtDuration.addAndGet(timeCostNano);
            }
            context.getStats().recordPhysicalTimeCost(sqlCostTime);

            if (context.isEnableTrace()) {
                if (null == op) {
                    HashMap<Integer, ParameterContext> logParams = new HashMap<>();
                    for (int i = 0; i < jdbcSplit.getFlattedParams().size(); i++) {
                        logParams.put(i, jdbcSplit.getFlattedParams().get(i));
                    }
                    String currentDbKey = getCurrentDbkey();
                    this.op = new ExecuteSQLOperation(jdbcSplit.getDbIndex(), currentDbKey, jdbcSplit.getSqlString(
                        context.getParamManager().getBoolean(ConnectionParams.ENABLE_SIMPLIFY_TRACE_SQL)),
                        startTime);
                    op.setThreadName(Thread.currentThread().getName());
                    op.setParams(new Parameters(logParams, false));
                }
                // Update trace time.
                op.setTimeCost(System.currentTimeMillis() - startTime);
                long getConnectionCostNano = 0;
                if (conn != null) {
                    getConnectionCostNano = conn.getConnectionStats().getGetConnectionTotalNano();
                }
                op.setGetConnectionTimeCost(getConnectionCostNano / (float) (1000 * 1000));
            }

            if (SQLRecorderLogger.physicalSlowLogger.isInfoEnabled()) {

                recordSqlLog(jdbcSplit,
                    startTime,
                    nanoStartTime,
                    sqlCostTime,
                    createCostTime,
                    waitCostTime
                );
            }

            if (runtimeStat != null) {
                runtimeStat.addPhySqlTimecost(timeCostNano);
                runtimeStat.addPhySqlCount(1);
            }

            jdbcSplit.reset();
        }

        SQLRecord recordSqlLog(JdbcSplit jdbcSplit, long startTime, long nanoStartTime, long sqlCostTime,
                               long getConnectionCreateCostNano, long getConnectionWaitCostNano) {
            SQLRecord record = null;
            if (SQLRecorderLogger.physicalSlowLogger.isInfoEnabled()) {
                long time = System.currentTimeMillis() - startTime;
                try {
                    if (slowTimeThreshold > 0L && time > slowTimeThreshold) {
                        context.getStats().physicalSlowRequest++;

                        HashMap<Integer, ParameterContext> param = new HashMap<>();
                        for (int i = 0; i < jdbcSplit.getFlattedParams().size(); i++) {
                            param.put(i, jdbcSplit.getFlattedParams().get(i));
                        }
                        String sql = jdbcSplit.getSqlString();
                        String currentDbKey = getCurrentDbkey();
                        long length = context.getPhysicalRecorder().getMaxSizeThreshold();
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

                        record = new SQLRecord();
                        record.schema = context.getSchemaName();
                        record.statement = formatSql;
                        record.startTime = startTime;
                        record.executeTime = time;
                        record.sqlTime = sqlCostTime;
                        record.getLockConnectionTime = getConnectionWaitCostNano;
                        record.createConnectionTime = getConnectionCreateCostNano;
                        record.dataNode = jdbcSplit.getDbIndex();
                        record.dbKey = currentDbKey;
                        //record.affectRow = rowsAffect;
                        record.physical = true;
                        record.traceId = context.getTraceId();

                        if (context.getPhysicalRecorder().check(time)) {
                            context.getPhysicalRecorder().recordSql(record);
                        }

                        //现在只记录physical_slow.log
                        SQLRecorderLogger.physicalSlowLogger
                            .info(SQLRecorderLogger.physicalLogFormat.format(new Object[] {
                                formatSql, jdbcSplit.getDbIndex(), currentDbKey, time, sqlCostTime,
                                getConnectionWaitCostNano,
                                getConnectionCreateCostNano, param, record.traceId}));
                    }
                } catch (Throwable e) {
                    logger.error("Error occurs when record SQL", e);
                }
            }
            return record;
        }

        private void block2block(DataType type, PolarxResultset.ColumnMetaData metaData, BlockDecoder src,
                                 BlockBuilder dst, int rowCount) throws Exception {
            final Class clazz = type.getDataClass();
            if (clazz == Integer.class) {
                for (int i = 0; i < rowCount; ++i) {
                    src.next();
                    if (src.isNull()) {
                        dst.appendNull();
                    } else {
                        dst.writeInt((int) src.getLong());
                    }
                }
            } else if (clazz == Long.class) {
                for (int i = 0; i < rowCount; ++i) {
                    src.next();
                    if (src.isNull()) {
                        dst.appendNull();
                    } else {
                        dst.writeLong(src.getLong());
                    }
                }
            } else if (clazz == Short.class) {
                for (int i = 0; i < rowCount; ++i) {
                    src.next();
                    if (src.isNull()) {
                        dst.appendNull();
                    } else {
                        dst.writeShort((short) src.getLong());
                    }
                }
            } else if (clazz == Byte.class) {
                for (int i = 0; i < rowCount; ++i) {
                    src.next();
                    if (src.isNull()) {
                        dst.appendNull();
                    } else {
                        dst.writeByte((byte) src.getLong());
                    }
                }
            } else if (clazz == Float.class) {
                for (int i = 0; i < rowCount; ++i) {
                    src.next();
                    if (src.isNull()) {
                        dst.appendNull();
                    } else {
                        dst.writeFloat(src.getFloat());
                    }
                }
            } else if (clazz == Double.class) {
                for (int i = 0; i < rowCount; ++i) {
                    src.next();
                    if (src.isNull()) {
                        dst.appendNull();
                    } else {
                        dst.writeDouble(src.getDouble());
                    }
                }
            } else if (clazz == Slice.class) {
                final boolean utf8 = metaData.hasCollation() && CharsetMapping.isUtf8((int) metaData.getCollation());
                final boolean latin1 =
                    !utf8 && metaData.hasCollation() && CharsetMapping.isLatin1((int) metaData.getCollation());
                if (utf8) {
                    for (int i = 0; i < rowCount; ++i) {
                        src.next();
                        if (src.isNull()) {
                            dst.appendNull();
                        } else {
                            final com.alibaba.polardbx.rpc.result.chunk.Slice slice = src.getString();
                            dst.writeByteArray(slice.getData(), slice.getOffset(), slice.getLength());
                        }
                    }
                } else if (latin1) {
                    for (int i = 0; i < rowCount; ++i) {
                        src.next();
                        if (src.isNull()) {
                            dst.appendNull();
                        } else {
                            final com.alibaba.polardbx.rpc.result.chunk.Slice slice = src.getString();
                            ((SliceBlockBuilder) dst)
                                .writeBytesInLatin1(slice.getData(), slice.getOffset(), slice.getLength());
                        }
                    }
                } else {
                    final String encoding;
                    if (metaData.hasCollation()) {
                        encoding = CharsetMapping.getJavaEncodingForCollationIndex((int) metaData.getCollation());
                    } else {
                        encoding = null;
                    }
                    for (int i = 0; i < rowCount; ++i) {
                        src.next();
                        if (src.isNull()) {
                            dst.appendNull();
                        } else {
                            final com.alibaba.polardbx.rpc.result.chunk.Slice slice = src.getString();
                            final String str;
                            if (null == encoding) {
                                str = new String(slice.getData(), slice.getOffset(), slice.getLength());
                            } else {
                                str = new String(slice.getData(), slice.getOffset(), slice.getLength(), encoding);
                            }
                            dst.writeString(str);
                        }
                    }
                }
            } else if (clazz == String.class || clazz == Enum.class) {
                final String encoding;
                if (metaData.hasCollation()) {
                    encoding = CharsetMapping.getJavaEncodingForCollationIndex((int) metaData.getCollation());
                } else {
                    encoding = null;
                }
                for (int i = 0; i < rowCount; ++i) {
                    src.next();
                    if (src.isNull()) {
                        dst.appendNull();
                    } else {
                        final com.alibaba.polardbx.rpc.result.chunk.Slice slice = src.getString();
                        final String str;
                        if (null == encoding) {
                            str = new String(slice.getData(), slice.getOffset(), slice.getLength());
                        } else {
                            str = new String(slice.getData(), slice.getOffset(), slice.getLength(), encoding);
                        }
                        dst.writeString(str);
                    }
                }
            } else if (clazz == BigInteger.class || clazz == UInt64.class) {
                final boolean unsigned = PolarxResultset.ColumnMetaData.FieldType.UINT == metaData.getType();
                for (int i = 0; i < rowCount; ++i) {
                    src.next();
                    if (src.isNull()) {
                        dst.appendNull();
                    } else {
                        final long val = src.getLong();
                        if (unsigned && val < 0) {
                            dst.writeBigInteger(
                                new BigInteger(ByteBuffer.allocate(9).put((byte) 0).putLong(val).array()));
                        } else {
                            dst.writeBigInteger(BigInteger.valueOf(val));
                        }
                    }
                }
            } else if (clazz == Decimal.class) {
                DecimalBlockBuilder decBuilder = (DecimalBlockBuilder) dst;
                boolean useDecimal64 = DynamicConfig.getInstance().enableXResultDecimal64()
                    && decBuilder.canWriteDecimal64()
                    && type.getScale() != DecimalTypeBase.DEFAULT_SCALE;
                for (int i = 0; i < rowCount; ++i) {
                    src.next();
                    if (src.isNull()) {
                        dst.appendNull();
                    } else {
                        final com.alibaba.polardbx.rpc.result.chunk.Decimal decimal = src.getDecimal();
                        if (null == decimal.getBigUnscaled()) {
                            if (useDecimal64) {
                                dst.writeLong(decimal.getUnscaled());
                            } else {
                                dst.writeDecimal(new Decimal(decimal.getUnscaled(), decimal.getScale()));
                            }
                        } else {
                            dst.writeDecimal(
                                Decimal.fromBigDecimal(new BigDecimal(decimal.getBigUnscaled(), decimal.getScale())));
                        }
                    }
                }
            } else if (clazz == Timestamp.class) {
                for (int i = 0; i < rowCount; ++i) {
                    src.next();
                    if (src.isNull()) {
                        dst.appendNull();
                    } else {
                        final long datetime = src.getDatetime();
                        if (0 == datetime) {
                            dst.writeTimestamp(ZeroTimestamp.instance);
                        } else {
                            dst.writeDatetimeRawLong(datetime);
                        }
                    }
                }
            } else if (clazz == Date.class) {
                for (int i = 0; i < rowCount; ++i) {
                    src.next();
                    if (src.isNull()) {
                        dst.appendNull();
                    } else {
                        final long date = src.getDate();
                        if (0 == date) {
                            dst.writeDate(ZeroDate.instance);
                        } else {
                            dst.writeDatetimeRawLong(date);
                        }
                    }
                }
            } else if (clazz == Time.class) {
                for (int i = 0; i < rowCount; ++i) {
                    src.next();
                    if (src.isNull()) {
                        dst.appendNull();
                    } else {
                        dst.writeDatetimeRawLong(src.getTime());
                    }
                }
            } else if (clazz == byte[].class) {
                for (int i = 0; i < rowCount; ++i) {
                    src.next();
                    if (src.isNull()) {
                        dst.appendNull();
                    } else {
                        final com.alibaba.polardbx.rpc.result.chunk.Slice slice = src.getString();
                        dst.writeByteArray(slice.getData(), slice.getOffset(), slice.getLength());
                    }
                }
            } else if (clazz == Blob.class) {
                for (int i = 0; i < rowCount; ++i) {
                    src.next();
                    if (src.isNull()) {
                        dst.appendNull();
                    } else {
                        final com.alibaba.polardbx.rpc.result.chunk.Slice slice = src.getString();
                        dst.writeBlob(new com.alibaba.polardbx.optimizer.core.datatype.Blob(Arrays
                            .copyOfRange(slice.getData(), slice.getOffset(), slice.getOffset() + slice.getLength())));
                    }
                }
            } else {
                // Ignore Clob.
                throw new AssertionError("Data type " + clazz.getName() + " not supported");
            }
        }

        /**
         * Basic routine:
         * 1. invoke chunkNext() to load new XResultObject to XResult::current()
         * 1.1. chunkNext() check current, load new if empty or row layout or all chunk has consumed
         * 2. invoke fillChunk to transmit X-Protocol chunk to local chunk
         */
        // Pure async(X-Protocol only)
        protected int fillChunk(DataType[] dataTypes, BlockBuilder[] blockBuilders, int maxFill) throws Exception {
            assert xResult != null;
            XResultObject resultObject = xResult.current();
            if (null == resultObject) {
                return 0;
            }
            if (resultObject.getRow() != null) {
                // Original row transmit.
                XRowSet.buildChunkRow(xResult, xResult.getMetaData(), resultObject.getRow(), dataTypes, blockBuilders);
                ++count;
                return 1;
            } else {
                // Chunk to chunk.
                if (null == resultObject.getDecoders()) {
                    // Dealing second chunks.
                    final List<PolarxResultset.ColumnMetaData> metaData = xResult.getMetaData();
                    while (resultObject.getChunkColumnCount() < metaData.size()) {
                        resultObject = xResult.mergeNext(resultObject);
                    }
                    resultObject.intiForChunkDecode(metaData);
                }
                final List<BlockDecoder> decoders = resultObject.getDecoders();
                assert decoders != null;
                int fetchNumber = decoders.get(0).restCount();
                if (maxFill > 0 && fetchNumber > maxFill) {
                    fetchNumber = maxFill;
                }
                // Now decode fetchNumber rows to chunk.
                assert dataTypes.length == blockBuilders.length;
                for (int i = 0; i < dataTypes.length; ++i) {
                    blockBuilders[i].ensureCapacity(blockBuilders[i].getPositionCount() + fetchNumber);
                    block2block(dataTypes[i], xResult.getMetaData().get(i), decoders.get(i), blockBuilders[i],
                        fetchNumber);
                }
                count += fetchNumber;
                return fetchNumber;
            }
        }

        protected void fillRawOrcTypeRow(DataType[] dataTypes, BlockBuilder[] blockBuilders,
                                         ExecutionContext context) throws Exception {
            assert xResult != null;
            XResultObject resultObject = xResult.current();
            if (resultObject.getRow() != null) {
                XRowSet.appendRawOrcTypeRowForX(xResult, dataTypes, blockBuilders, context);
                ++count;
            }
        }

        private boolean chunkNext() throws SQLException {
            final XResultObject current = xResult.current();
            if (null == current || current.getRow() != null) {
                // Not even fetch result or result in row layout.
                return xResult.next() != null;
            } else if (null == current.getDecoders() || current.getDecoders().get(0).restCount() > 0) {
                // Chunk layout and not initialized or has more data.
                return true;
            } else {
                // Fetch next whatever.
                return xResult.next() != null;
            }
        }

        protected Row current() {
            if (isOnlyXResult()) {
                throw new TddlRuntimeException(ERR_X_PROTOCOL_RESULT, "Should use chunk2chunk to fetch data.");
            }
            return rowSet;
        }

        public ResultSet getResultSet() {
            if (isOnlyXResult()) {
                throw new TddlRuntimeException(ERR_X_PROTOCOL_RESULT, "Should use chunk2chunk to fetch data.");
            }
            return rs;
        }

        protected boolean next() {
            try {
                if (isOnlyXResult() && xResult != null) {
                    return chunkNext();
                } else {
                    if (rowSet != null) {
                        rowSet.clearCache();
                    }
                    if (rs != null && rs.next()) {
                        count++;
                        return true;
                    }
                }
            } catch (SQLException e) {
                if (isClosed) {
                    logger.warn(context.getTraceId() + " here occur error, but current scan is closed!", e);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL, e, jdbcSplit.getDbIndex(),
                        getCurrentDbkey(), context.getTraceId() + "," + e.getMessage());
                }
            }
            return false;
        }

        void close() {
            close(false);
        }

        void close(boolean ignoreCnt) {
            if (closed.compareAndSet(false, true)) {
                long startCloseJdbcNano = System.nanoTime();
                closeConnection();
                notifyBlockedCallers();
                if (enableTaskCpu && targetPlanStatGroup != null) {
                    targetPlanStatGroup.closeAndClearJdbcEnv.addAndGet(System.nanoTime() - startCloseJdbcNano);
                }

                if (runtimeStat != null) {
                    runtimeStat.addPhyFetchRows(count);
                }

                if (op != null) {
                    op.setRowsCount(count);
                    op.setTotalTimeCost(System.currentTimeMillis() - op.getTimestamp());
                    op.setPhysicalCloseCost((System.nanoTime() - startCloseJdbcNano) / 1000 / 1000);
                    op.setGrpConnId(jdbcSplit.getGrpConnId(context));
                    op.setTraceId(context.getTraceId());
                    context.getTracer().trace(op);
                    op = null;
                }
                if (!ignoreCnt) {
                    completeExecuteNum.incrementAndGet();
                } else {
                    logger.warn("ignore calculate the complete num!");
                }
            }
        }

        synchronized void closeConnection() {

            if (isOnlyXResult() && xResult != null) {

                if (!killStreaming) {
                    try {
                        while (xResult.next() != null) {
                        }
                    } catch (Throwable ignore) {
                    }
                }

                try {
                    xResult.close();
                    // Just close is ok, not set to null.
                } catch (Throwable ignore) {
                    logger.warn("close X-Result!", ignore);
                }
            } else if (rs != null) {
                throw new AssertionError("unreachable");
            }

            if (stmt != null) {
                try {
                    //Don't use stmt.cancel() because this method is executed in async mode.
                    stmt.close();
                } catch (Throwable e) {
                    //ignore
                } finally {
                    stmt = null;
                }
            }

            if (conn != null) {
                //修改了NetworkTimeout才需要重置
                if (phyConnLastSocketTimeout >= 0) {
                    try {
                        this.conn.setNetworkTimeout(TGroupDirectConnection.socketTimeoutExecutor,
                            this.phyConnLastSocketTimeout);
                    } catch (Throwable ex) {
                        logger.warn("reset conn socketTimeout failed, lastSocketTimeout is "
                            + this.phyConnLastSocketTimeout, ex);
                    }
                }
                try {
                    //分布式事务中的tryClose
                    context.getTransaction().tryClose(conn, jdbcSplit.getDbIndex());
                } catch (Throwable e) {
                    logger.warn(context.getTraceId() + " close conn failed", e);
                } finally {
                    conn = null;
                }
            }
            this.closeConnection = true;
        }

        public SettableFuture getFuture() {
            return blockedFuture;
        }

        public void setFuture(SettableFuture future) {
            Preconditions.checkState(future != null, "SplitResultSet future is null");
            this.blockedFuture = future;
        }
    }

    public class PrefetchThread implements Callable {
        final SplitResultSet resultSet;
        JdbcSplit split;
        AtomicReference<ScheduledFuture> timeoutNotify = new AtomicReference<>(null);
        boolean ignoreCnt;

        public PrefetchThread(Split split, boolean rangeScan, int splitIndex) {
            this.split = (JdbcSplit) split.getConnectorSplit();
            this.resultSet = newSplitResultSet(this.split, rangeScan, splitIndex);
        }

        public boolean isPureAsyncMode() {
            return this.resultSet.isPureAsyncMode();
        }

        private void finishFetch() {
            if (isClosed) {
                resultSet.close(ignoreCnt);
            }

            //先关闭ResultSet我们再记录compeletePrefetchNum, 否则可能存在连接未关闭，而查询
            //结束，这样连接未及时关闭导致私有协议的连接状态不符合预期的问题
            if (!ignoreCnt) {
                completePrefetchNum.incrementAndGet();
            } else {
                logger.warn("ignore calculate the prefetch Num!");
            }
            if (isReady()) {
                notifyBlockedCallers();
            }
        }

        class AsyncCallback implements Runnable {
            // For pure async complete routine.
            @Override
            public void run() {
                assert resultSet.xResult != null;
                final ScheduledFuture future = timeoutNotify.getAndSet(null); // Clear ref.
                if (null == future) {
                    return; // Only run once.
                } else {
                    future.cancel(false);
                }

                // Complete the query routine.
                final Runnable notifyTask = () -> {
                    try {
                        MDC.put(MDC.MDC_KEY_APP, context.getSchemaName().toLowerCase());
                        MDC.put(MDC.MDC_KEY_CON, context.getMdcConnString());

                        // Finish async query first. Write log & trace.
                        resultSet.finishAsyncQuery();

                        if (!isClosed) {
                            addSplitResultSet(resultSet);
                        }
                    } catch (Throwable t) {
                        setException(
                            new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL, t,
                                resultSet.jdbcSplit.getDbIndex(),
                                resultSet.getCurrentDbkey(), t.getMessage()));
                    } finally {
                        // Complete async finish routine.
                        finishFetch();

                        MDC.remove(MDC.MDC_KEY_APP);
                        MDC.remove(MDC.MDC_KEY_CON);
                    }
                };

                // Caution: Never do anything blocking. So use server thread.
                ServiceProvider.getInstance().getServerExecutor().submit(context.getSchemaName(),
                    context.getTraceId(), -1, notifyTask, null, context.getRuntimeStatistics());
            }
        }

        @Override
        public Object call() throws Exception {
            boolean asyncFinish = false;
            final String previousApp = MDC.get(MDC.MDC_KEY_APP);
            final String previousCon = MDC.get(MDC.MDC_KEY_CON);
            try {
                MDC.put(MDC.MDC_KEY_APP, context.getSchemaName().toLowerCase());
                MDC.put(MDC.MDC_KEY_CON, context.getMdcConnString());
                if (!isClosed) {
                    synchronized (resultSet) {
                        //close && executeQuery 互斥
                        //否则存在ResultSet 已经被close，存在异步线程去建连导致私有协议的连接状态不符合预期的问题
                        if (!resultSet.closeConnection) {
                            resultSet.initConnection();
                            resultSet.executeQuery();
                        }
                    }
                    if (resultSet.xResult != null) {
                        // Pure async mode.
                        assert isPureAsyncMode();

                        final Runnable callback = new AsyncCallback();
                        // Add timeout notify to guarantee this result set finish.
                        timeoutNotify.set(timeoutService
                            .schedule(callback, resultSet.xResult.getQueryTimeoutNanos() - System.nanoTime(),
                                TimeUnit.NANOSECONDS));
                        asyncFinish = true; // Successfully added in scheduler and we don't finish this query here.

                        // Use synchronized to prevent the concurrency of data fetch and data ready probe.
                        // Data fetch after addSplitResultSet invoked which hold the lock of TableScanClient.
                        final boolean dataReady;
                        synchronized (TableScanClient.this) {
                            resultSet.xResult.setDataNotify(callback);
                            dataReady = resultSet.xResult.isDataReady();
                        }
                        if (dataReady) {
                            // Already done? Just run routine within this thread.
                            resultSet.xResult.setDataNotify(null);
                            callback.run();
                        }
                    } else {
                        addSplitResultSet(resultSet);
                    }
                }
            } catch (Throwable e) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder
                    .append("The splitList ")
                    .append(splitList.size())
                    .append(" prefetch ")
                    .append(prefetchNum)
                    .append(" pushdownSplitIndex ")
                    .append(pushdownSplitIndex.get())
                    .append(" compeletePrefetchNum ")
                    .append(completePrefetchNum.get())
                    .append(" compeleteExecuteNum ")
                    .append(completeExecuteNum.get())
                    .append("!")
                    .append(" Current Physical JdbcSplit: ")
                    .append(split);
                logger.error(stringBuilder.toString(), e);
                setException(
                    new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL, e, resultSet.jdbcSplit.getDbIndex(),
                        resultSet.getCurrentDbkey(), e.getMessage()));
            } finally {
                if (!asyncFinish) {
                    // Complete normal finish routine.
                    finishFetch();
                }
                if (null == previousApp) {
                    MDC.remove(MDC.MDC_KEY_APP);
                } else {
                    MDC.put(MDC.MDC_KEY_APP, previousApp);
                }
                if (null == previousCon) {
                    MDC.remove(MDC.MDC_KEY_CON);
                } else {
                    MDC.put(MDC.MDC_KEY_CON, previousCon);
                }
            }
            return split;
        }

        public void cancel(boolean ignoreCnt) {
            if (resultSet != null) {
                resultSet.close();
            }
            this.ignoreCnt = ignoreCnt;
        }

        public SplitResultSet getResultSet() {
            return resultSet;
        }
    }
}
