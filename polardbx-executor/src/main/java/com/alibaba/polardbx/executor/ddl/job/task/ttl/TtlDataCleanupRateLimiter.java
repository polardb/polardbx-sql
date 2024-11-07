package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.google.common.util.concurrent.RateLimiter;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author chenghui.lch
 */
public class TtlDataCleanupRateLimiter extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(TtlDataCleanupRateLimiter.class);

    /**
     * The cleanup rows speed rate limiter for all dn, unit: rows/s
     */
    private TtlAutoReloadRowsSpeedRateLimiterSubListener subListener =
        new TtlAutoReloadRowsSpeedRateLimiterSubListener();
    private volatile Map<String, RateLimiter> allDnRowsSpeedLimiter = new ConcurrentHashMap<>();

    private static TtlDataCleanupRateLimiter instance = new TtlDataCleanupRateLimiter();

    public static TtlDataCleanupRateLimiter getInstance() {
        return instance;
    }

    private TtlDataCleanupRateLimiter() {
        initAllRateLimiters();
        StorageHaManager.getInstance().registerStorageInfoSubListener(subListener);
    }

    @Override
    protected void doInit() {
    }

    private void initAllRateLimiters() {
        long initialRate = TtlConfigUtil.getCleanupRowsSpeedLimitEachDn();
        List<StorageInstHaContext> rwDnList = StorageHaManager.getInstance().getMasterStorageList();
        Map<String, RateLimiter> newAllDnRowsSpeedLimiter = new ConcurrentHashMap<>();
        for (int i = 0; i < rwDnList.size(); i++) {
            String dnId = rwDnList.get(i).getStorageInstId().toLowerCase();
            RateLimiter limiterOfOneDn = RateLimiter.create(initialRate);
            newAllDnRowsSpeedLimiter.put(dnId, limiterOfOneDn);
        }
        this.allDnRowsSpeedLimiter = newAllDnRowsSpeedLimiter;
    }

    public boolean tryAcquire(String dbId, int permits, long timeout, TimeUnit unit) {
        try {
            RateLimiter rateLimiter = this.allDnRowsSpeedLimiter.get(dbId);
            if (rateLimiter == null) {
                return true;
            }
            return rateLimiter.tryAcquire(permits, timeout, unit);
        } catch (Throwable ex) {
            throw new TtlJobRuntimeException(ex);
        }
    }

    protected class TtlAutoReloadRowsSpeedRateLimiterSubListener
        implements StorageHaManager.StorageInfoConfigSubListener {

        @Override
        public String getSubListenerName() {
            return "TtlAutoReloadRowsSpeedRateLimiterSubListener";
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            TtlDataCleanupRateLimiter.getInstance().reload();
        }
    }

    /**
     * Add or remove rows speed limiter by the rw-dn count
     */
    public void reload() {
        long permitsOfEachDn = TtlConfigUtil.getCleanupRowsSpeedLimitEachDn();
        Set<String> tmpRwDnSetOfLimiters = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        tmpRwDnSetOfLimiters.addAll(this.allDnRowsSpeedLimiter.keySet());

        Set<String> tmpRwDnSetOfHaMgr = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        List<StorageInstHaContext> dnCtxList = StorageHaManager.getInstance().getMasterStorageList();
        for (int i = 0; i < dnCtxList.size(); i++) {
            tmpRwDnSetOfHaMgr.add(dnCtxList.get(i).getStorageInstId());
        }

        /**
         * Add new RateLimiter for the new-added rw-dn
         */
        for (String dnIdOfHaMgr : tmpRwDnSetOfHaMgr) {
            if (this.allDnRowsSpeedLimiter.containsKey(dnIdOfHaMgr)) {
                continue;
            }
            RateLimiter limiterOfOneDn = RateLimiter.create(permitsOfEachDn);
            this.allDnRowsSpeedLimiter.put(dnIdOfHaMgr, limiterOfOneDn);
        }

        /**
         * Remove old RateLimiter for the removed rw-dn
         */
        for (String dnIdOfLimiter : tmpRwDnSetOfLimiters) {
            if (tmpRwDnSetOfHaMgr.contains(dnIdOfLimiter)) {
                continue;
            }
            this.allDnRowsSpeedLimiter.remove(dnIdOfLimiter);
        }
    }

    /**
     * Dynamic adjust permits of limiter
     */
    public void adjustRate(long newPermitsVal) {
        try {
            for (Map.Entry<String, RateLimiter> limiterItem : allDnRowsSpeedLimiter.entrySet()) {
                RateLimiter limiter = limiterItem.getValue();
                limiter.setRate(newPermitsVal);
            }
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
            logger.warn(ex.getMessage(), ex);
        }
    }
}
