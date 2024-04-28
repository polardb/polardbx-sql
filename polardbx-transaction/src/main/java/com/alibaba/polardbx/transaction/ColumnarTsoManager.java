package com.alibaba.polardbx.transaction;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.transaction.async.PurgeColumnarTsoTimerTask;
import com.alibaba.polardbx.transaction.async.UpdateColumnarTsoTimerTask;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ColumnarTsoManager extends AbstractLifecycle {

    private ScheduledExecutorService columnarTsoPurgeTaskExecutor;
    private ScheduledExecutorService columnarTsoUpdateTaskExecutor;
    private volatile int tsoPurgeInterval;
    private volatile int tsoUpdateInterval;
    private ScheduledFuture<?> tsoPurgeFuture;
    private ScheduledFuture<?> tsoUpdateFuture;

    private PurgeColumnarTsoTimerTask columnarTsoPurgeTask;
    private UpdateColumnarTsoTimerTask columnarTsoUpdateTask;

    final protected static ColumnarTsoManager INSTANCE = new ColumnarTsoManager();

    public static ColumnarTsoManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }

        return INSTANCE;
    }

    @Override
    protected void doInit() {
        // TDOD(siyun): purge TSO on readonly node?
        if (ConfigDataMode.isPolarDbX() && ConfigDataMode.isMasterMode()) {
            enableColumnarTsoPurge();
        }

        // Master instance and readonly node can provide different SLA.
        // So RO node can also maintain own columnar tso
        if (ConfigDataMode.isPolarDbX()) {
            enableColumnarTsoUpdate();
        }
    }

    private void enableColumnarTsoPurge() {
        if (ConfigDataMode.isFastMock()) {
            return;
        }

        tsoPurgeInterval = InstConfUtil.getInt(ConnectionParams.COLUMNAR_TSO_PURGE_INTERVAL);
        columnarTsoPurgeTaskExecutor =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("ColumnarTsoPurgeTaskExecutor"));
        columnarTsoPurgeTask = new PurgeColumnarTsoTimerTask();

        tsoPurgeFuture = columnarTsoPurgeTaskExecutor.scheduleAtFixedRate(
            columnarTsoPurgeTask,
            tsoPurgeInterval,
            tsoPurgeInterval,
            TimeUnit.MILLISECONDS
        );
    }

    private void enableColumnarTsoUpdate() {
        if (ConfigDataMode.isFastMock()) {
            return;
        }

        tsoUpdateInterval = InstConfUtil.getInt(ConnectionParams.COLUMNAR_TSO_UPDATE_INTERVAL);
        columnarTsoUpdateTaskExecutor =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("ColumnarTsoUpdateTaskExecutor"));
        columnarTsoUpdateTask = new UpdateColumnarTsoTimerTask();

        tsoUpdateFuture = columnarTsoUpdateTaskExecutor.scheduleAtFixedRate(
            columnarTsoUpdateTask,
            tsoUpdateInterval,
            tsoUpdateInterval,
            TimeUnit.MILLISECONDS
        );
    }

    public synchronized void resetColumnarTsoPurgeInterval(int intervalMs) {
        if (ConfigDataMode.isFastMock()) {
            return;
        }

        if (tsoPurgeInterval != intervalMs) {
            tsoPurgeInterval = intervalMs;

            tsoPurgeFuture.cancel(false);

            tsoPurgeFuture = columnarTsoPurgeTaskExecutor.scheduleAtFixedRate(
                columnarTsoPurgeTask,
                tsoPurgeInterval,
                tsoPurgeInterval,
                TimeUnit.MILLISECONDS
            );
        }
    }

    public synchronized void resetColumnarTsoUpdateInterval(int intervalMs) {
        if (ConfigDataMode.isFastMock()) {
            return;
        }

        if (tsoUpdateInterval != intervalMs) {
            tsoUpdateInterval = intervalMs;

            tsoUpdateFuture.cancel(false);

            tsoUpdateFuture = columnarTsoUpdateTaskExecutor.scheduleAtFixedRate(
                columnarTsoUpdateTask,
                tsoUpdateInterval,
                tsoUpdateInterval,
                TimeUnit.MILLISECONDS
            );
        }

    }
}
