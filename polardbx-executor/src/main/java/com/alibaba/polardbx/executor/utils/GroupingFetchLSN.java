package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.google.common.util.concurrent.SettableFuture;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fetch lsn for per DN by grouping way.
 */
public class GroupingFetchLSN extends AbstractLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupingFetchLSN.class);

    private static GroupingFetchLSN instance = new GroupingFetchLSN();

    public static GroupingFetchLSN getInstance() {
        if (instance == null) {
            return null;
        }
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    public ConcurrentHashMap<String, DNLsnInfo> masterDNLsnMap = new ConcurrentHashMap<>();

    private ExecutorService fetcherPool;
    private long keepAliveTime = 3600000;
    private long timeout;
    private int poolSize;

    @Override
    protected void doInit() {
        this.timeout = DynamicConfig.getInstance().getGroupingTimeout();
        this.poolSize = DynamicConfig.getInstance().getGroupingThread();
        this.fetcherPool = DdlHelper.createThreadPool(poolSize, keepAliveTime,
            "GroupingFetchLSN");
    }

    public class DNLsnInfo {

        private String dnId;
        private AtomicLong sendLsnCount = new AtomicLong(0);
        private AtomicLong getLsnCount = new AtomicLong(0);

        public DNLsnInfo(String dnId) {
            this.dnId = dnId;
        }

        private volatile LsnFuture currentLsnFuture = new LsnFuture(this);

        public synchronized void reset() {
            /**
             *  the lsnFuture is already notify, so here use the new LsnFuture().
             */
            if (currentLsnFuture.notify.get()) {
                this.currentLsnFuture = new LsnFuture(this);
            }
        }

        public synchronized LsnFuture getLsnFuture(long tso) {
            currentLsnFuture.updateTsoHeartbeat(tso);
            return currentLsnFuture;
        }
    }

    public class LsnFuture {
        private SettableFuture<Long> settableFuture = SettableFuture.create();
        AtomicBoolean notify = new AtomicBoolean(false);
        AtomicLong tsoHeartbeat = new AtomicLong(-1);
        DNLsnInfo dnLsnInfo;
        long sendHeartbeat = -1L;
        String hint;

        public LsnFuture(DNLsnInfo dnLsnInfo) {
            this.dnLsnInfo = dnLsnInfo;
            this.hint = dnLsnInfo.dnId;
        }

        public synchronized void updateTsoHeartbeat(long tso) {
            long expect = tsoHeartbeat.get();
            if (expect < tso) {
                tsoHeartbeat.set(tso);
            }
        }

        public synchronized long getTsoHeartbeat() {
            return tsoHeartbeat.get();
        }

        public void notifyFetch(String dnMasterKey) {
            dnLsnInfo.getLsnCount.incrementAndGet();
            if (notify.compareAndSet(false, true)) {
                fetcherPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        long masterLSN = -1;
                        try {
                            dnLsnInfo.reset();
                            sendHeartbeat = getTsoHeartbeat();
                            hint = String.format("/* %s%s*/", dnLsnInfo.dnId, dnLsnInfo.sendLsnCount.incrementAndGet());
                            long start = System.currentTimeMillis();
                            masterLSN = getLsnBasedCDC(dnMasterKey, sendHeartbeat, hint);
                            long cost = System.currentTimeMillis() - start;
                            if (cost > timeout) {
                                LOGGER.error("LSN take cost " + cost + " milliseconds!");
                            }
                            settableFuture.set(masterLSN);
                        } catch (Exception e) {
                            settableFuture.setException(e);
                        }
                    }
                });
            }
        }

        public long waitLsn() throws Exception {
            try {
                /**
                 * The MAX RT for this method is (dn * 2 / poolSize * (rt of {@link ExecUtils.getLsn})
                 */
                final long value = settableFuture.get(timeout, TimeUnit.MILLISECONDS);
                if (sendHeartbeat < tsoHeartbeat.get()) {
                    LOGGER.error(
                        String.format("sendHeartbeat: %s, tsoHeartbeat: %s for %s", sendHeartbeat,
                            tsoHeartbeat, dnLsnInfo.dnId));
                    throw new RuntimeException("use the invalid tso HeartBeat!");
                }
                return value;
            } catch (TimeoutException e) {
                LOGGER.error(String.format("the getLsnCount: %s, the sendLsnCount: %s, the using sql hint: %s",
                    dnLsnInfo.getLsnCount.get(), dnLsnInfo.sendLsnCount.get(), hint));
                throw new TimeoutException("Fetch LSN timeout.");
            }
        }
    }

    public long groupingLsn(String masterDNId, long tso) throws Exception {
        long masterLsn = -1L;
        if (masterDNId != null && !IDataSource.EMPTY.equalsIgnoreCase(masterDNId)) {
            DNLsnInfo lsnInfo = masterDNLsnMap.computeIfAbsent(masterDNId, key -> new DNLsnInfo(masterDNId));
            LsnFuture currentFuture = lsnInfo.getLsnFuture(tso);
            currentFuture.notifyFetch(masterDNId);
            masterLsn = currentFuture.waitLsn();
        }
        return masterLsn;
    }

    public long getLsnBasedCDC(String dnMasterKey, long tso, String hint) throws SQLException {
        long masterLSN = -1;
        if (ExecutorContext.getContext(SystemDbHelper.CDC_DB_NAME) != null) {
            TopologyHandler topologyHandler =
                ExecutorContext.getContext(SystemDbHelper.CDC_DB_NAME).getTopologyHandler();

            for (Group group : topologyHandler.getMatrix().getGroups()) {
                if (!DbGroupInfoManager.isVisibleGroup(group)) {
                    continue;
                }
                if (GroupInfoUtil.isSingleGroup(group.getName())) {
                    continue;
                }
                String groupName = group.getName();
                IGroupExecutor groupExecutor = topologyHandler.get(groupName);

                TGroupDataSource dataSource = (TGroupDataSource) groupExecutor.getDataSource();
                if (dataSource.getMasterDNId().equalsIgnoreCase(dnMasterKey)) {
                    masterLSN = ExecUtils.getLsn(dataSource, tso, hint);
                    break;
                }
            }
        }
        return masterLSN;
    }

    private long groupingLsn(TGroupDataSource groupDataSource, long tso) {
        try {
            long masterLsn = -1L;
            if (DynamicConfig.getInstance().isBasedCDC()
                && ExecutorContext.getContext(SystemDbHelper.CDC_DB_NAME) != null) {
                String masterDNId = groupDataSource.getMasterDNId();
                masterLsn = groupingLsn(masterDNId, tso);
            }
            if (masterLsn == -1L) {
                /**
                 *  The cdc database maybe not exist!
                 */
                masterLsn = ExecUtils.getLsn(groupDataSource, tso, "");
            }
            return masterLsn;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * fetch the log sequence number from the master DN.
     *
     * @param group mark the dn from group
     * @param dnLsnMap The param dnLsnMap usually is null. The method is only invoke  by MPP if the param is not null.
     * @param tso Task to send a timestamp to storage nodes in order to keep their latest timestamp up-to-date.
     * @return the log sequence number.
     */
    public long fetchLSN(
        TopologyHandler topology, String group, ConcurrentHashMap<String, Long> dnLsnMap, long tso) {
        TGroupDataSource groupDataSource = (TGroupDataSource) topology.get(group).getDataSource();
        String masterId = groupDataSource.getMasterDNId();
        long retLsn = -1;
        if (dnLsnMap != null && masterId != null && !masterId.equalsIgnoreCase(IDataSource.EMPTY)) {
            if (!dnLsnMap.containsKey(masterId)) {
                retLsn = dnLsnMap.computeIfAbsent(
                    masterId, key -> GroupingFetchLSN.getInstance().groupingLsn(groupDataSource, tso));
            } else {
                retLsn = dnLsnMap.get(masterId);
            }
        } else {
            retLsn = GroupingFetchLSN.getInstance().groupingLsn(groupDataSource, tso);
        }
        return retLsn;
    }

    public void resetGroupingPool(long timeout, int poolSize) {
        this.timeout = timeout;
        ExecutorService oldFetcherPool = this.fetcherPool;
        ConcurrentHashMap<String, DNLsnInfo> oldMasterDNLsnMap = this.masterDNLsnMap;
        this.fetcherPool = DdlHelper.createThreadPool(poolSize, keepAliveTime, "GroupingFetchLSN");
        this.masterDNLsnMap = new ConcurrentHashMap<>();
        this.poolSize = poolSize;
        oldMasterDNLsnMap.clear();
        oldFetcherPool.shutdown();
    }

    public ExecutorService getFetcherPool() {
        return fetcherPool;
    }
}
