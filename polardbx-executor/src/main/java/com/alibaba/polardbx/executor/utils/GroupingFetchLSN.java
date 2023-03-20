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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

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

    public final ConcurrentHashMap<String, DNLsnInfo> masterDNLsnMap = new ConcurrentHashMap<>();

    private ExecutorService fetcherPool;
    private long keepAliveTime = 3600000;
    private long timeout = 1000;
    private int poolSize = 4;

    @Override
    protected void doInit() {
        this.fetcherPool = DdlHelper.createThreadPool(poolSize, keepAliveTime,
            "GroupingFetchLSN");
    }

    public class DNLsnInfo {

        private volatile LsnFuture currentLsnFuture = new LsnFuture(this);

        public synchronized void reset() {
            /**
             *  the lsnFuture is already notify, so here use the new LsnFuture().
             */
            if (currentLsnFuture.notify.get()) {
                this.currentLsnFuture = new LsnFuture(this);
            }
        }

        public LsnFuture getLsnFuture() {
            return currentLsnFuture;
        }
    }

    public class LsnFuture {
        private SettableFuture<Long> settableFuture = SettableFuture.create();
        AtomicBoolean notify = new AtomicBoolean(false);
        DNLsnInfo dnLsnInfo;

        public LsnFuture(DNLsnInfo dnLsnInfo) {
            this.dnLsnInfo = dnLsnInfo;
        }

        public void notifyFetch(String dnMasterKey) {
            if (notify.compareAndSet(false, true)) {
                fetcherPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        long masterLSN = -1;
                        try {
                            dnLsnInfo.reset();
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
                                        masterLSN = ExecUtils.getLsn(dataSource);
                                        break;
                                    }
                                }
                            }
                        } catch (Exception e) {
                            settableFuture.setException(e);
                        } finally {
                            settableFuture.set(masterLSN);
                        }
                    }
                });
            }
        }

        public long waitLsn() throws Exception {
            try {
                final long value = settableFuture.get(timeout, TimeUnit.MILLISECONDS);
                return value;
            } catch (TimeoutException e) {
                throw new TimeoutException("Fetch LSN timeout.");
            }
        }
    }

    public long getLsn(TGroupDataSource groupDataSource) {
        try {
            long masterLsn = -1L;
            if (DynamicConfig.getInstance().isBasedCDC()
                && ExecutorContext.getContext(SystemDbHelper.CDC_DB_NAME) != null) {
                String masterDNId = groupDataSource.getMasterDNId();
                if (masterDNId != null && !IDataSource.EMPTY.equalsIgnoreCase(masterDNId)) {
                    DNLsnInfo lsnInfo = masterDNLsnMap.computeIfAbsent(masterDNId, key -> new DNLsnInfo());
                    LsnFuture currentFuture = lsnInfo.getLsnFuture();
                    currentFuture.notifyFetch(masterDNId);
                    masterLsn = currentFuture.waitLsn();
                }
            }
            if (masterLsn == -1L) {
                /**
                 *  The cdc database maybe not exist!
                 */
                masterLsn = ExecUtils.getLsn(groupDataSource);
            }
            return masterLsn;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public long getLsn(TopologyHandler topology, String group, ConcurrentHashMap<String, Long> dnLsnMap) {
        TGroupDataSource groupDataSource = (TGroupDataSource) topology.get(group).getDataSource();
        String masterId = groupDataSource.getMasterDNId();
        long retLsn = -1;
        if (masterId != null && !masterId.equalsIgnoreCase(IDataSource.EMPTY)) {
            if (!dnLsnMap.containsKey(masterId)) {
                retLsn = dnLsnMap.computeIfAbsent(
                    masterId, key -> GroupingFetchLSN.getInstance().getLsn(groupDataSource));
            } else {
                retLsn = dnLsnMap.get(masterId);
            }
        } else {
            retLsn = GroupingFetchLSN.getInstance().getLsn(groupDataSource);
        }
        return retLsn;
    }
}
