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
        if (ConfigDataMode.isPolarDbX()) {
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

    @Override
    protected void doDestroy() {
        if (columnarTsoUpdateTaskExecutor != null) {
            columnarTsoUpdateTaskExecutor.shutdown();
        }

        if (columnarTsoPurgeTaskExecutor != null) {
            columnarTsoPurgeTaskExecutor.shutdown();
        }
    }
}
