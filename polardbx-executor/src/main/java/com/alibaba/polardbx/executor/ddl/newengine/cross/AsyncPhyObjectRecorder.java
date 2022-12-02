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

package com.alibaba.polardbx.executor.ddl.newengine.cross;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlJobManagerUtils;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.config.schema.PerformanceSchema;
import com.alibaba.polardbx.optimizer.context.PhyDdlExecutionRecord;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class AsyncPhyObjectRecorder {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncPhyObjectRecorder.class);

    private static final Map<String, AsyncPhyObjectRecorder> ASYNC_PHY_OBJECT_RECORDER_MAP = new ConcurrentHashMap<>();

    private static final int MAX_RETRY_TIMES = 3;

    private volatile boolean active = true;

    private final ExecutorService asyncPhyObjectRecorderThread;
    private final List<PhyObjectTask> phyObjectTaskQueue = Lists.newArrayList();

    private AsyncPhyObjectRecorder(String schemaName) {
        String threadName = String.format(DdlConstants.DDL_PHY_OBJ_RECORDER_NAME, schemaName);
        this.asyncPhyObjectRecorderThread = DdlHelper.createSingleThreadPool(threadName);
        this.asyncPhyObjectRecorderThread.submit(AsyncTask.build(new PhyObjectRecorderTask()));
    }

    public static void register(final String schemaName) {
        if (DdlHelper.isRunnable() &&
            !SystemDbHelper.isDBBuildInExceptCdc(schemaName) &&
            !TStringUtil.containsIgnoreCase(schemaName, PerformanceSchema.NAME)) {
            ASYNC_PHY_OBJECT_RECORDER_MAP.put(schemaName, new AsyncPhyObjectRecorder(schemaName));
            LOGGER.warn("Physical DDL Object Recorder has been initialized for " + schemaName);
        }
    }

    public static void deregister(final String schemaName) {
        AsyncPhyObjectRecorder asyncPhyObjectRecorder = ASYNC_PHY_OBJECT_RECORDER_MAP.remove(schemaName);
        if (asyncPhyObjectRecorder != null) {
            try {
                asyncPhyObjectRecorder.destroy();
                LOGGER.warn("Physical DDL Object Recorder has been destroyed for " + schemaName);
            } catch (Exception e) {
                LOGGER.warn("Failed to destroy Physical DDL Object Recorder for " + schemaName);
            }
        }
    }

    public static List<PhyObjectTask> getPhyObjectTaskQueue(String schemaName) {
        AsyncPhyObjectRecorder asyncPhyObjectRecorder = ASYNC_PHY_OBJECT_RECORDER_MAP.get(schemaName.toLowerCase());
        return asyncPhyObjectRecorder != null ? asyncPhyObjectRecorder.getPhyObjectTaskQueue() : null;
    }

    public class PhyObjectRecorderTask implements Runnable {

        @Override
        public void run() {
            while (active) {
                try {
                    record();
                } catch (Throwable t) {
                    LOGGER.error(t);
                }
            }
        }

        private void record() {
            Map<String, Pair<PhyDdlExecutionRecord, List<PhyObjectTask>>> phyDdlInfoMap = Maps.newHashMap();

            synchronized (phyObjectTaskQueue) {
                while (active && phyObjectTaskQueue.isEmpty()) {
                    try {
                        phyObjectTaskQueue.wait();
                    } catch (InterruptedException ignored) {
                    }
                }

                for (PhyObjectTask phyObjectTask : phyObjectTaskQueue) {
                    Pair<PhyDdlExecutionRecord, List<PhyObjectTask>> phyDdlInfo =
                        phyDdlInfoMap.get(phyObjectTask.getKey());
                    if (phyDdlInfo == null) {
                        phyDdlInfo = Pair.of(phyObjectTask.getPhyDdlExecutionRecord().copy(), Lists.newArrayList());
                        phyDdlInfoMap.put(phyObjectTask.getKey(), phyDdlInfo);
                    }
                    phyDdlInfo.getValue().add(phyObjectTask);
                }

                phyObjectTaskQueue.clear();
            }

            for (Pair<PhyDdlExecutionRecord, List<PhyObjectTask>> phyDdlInfo : phyDdlInfoMap.values()) {
                Exception exception = null;

                int retryTimes = 0;
                while (true) {
                    try {
                        DdlJobManagerUtils.resetPhyTablesDone(phyDdlInfo.getKey());
                        break;
                    } catch (Exception e) {
                        boolean ignorable = TStringUtil.containsIgnoreCase(e.getMessage(), "Deadlock")
                            || TStringUtil.containsIgnoreCase(e.getMessage(), "Lock wait timeout");
                        if (ignorable && retryTimes < MAX_RETRY_TIMES) {
                            // Try again in case we hit such errors.
                            retryTimes++;
                            continue;
                        }
                        exception = e;
                        break;
                    }
                }

                if (exception != null) {
                    for (PhyObjectTask phyObjectTask : phyDdlInfo.getValue()) {
                        phyObjectTask.setException(exception);
                    }
                } else {
                    for (PhyObjectTask phyObjectTask : phyDdlInfo.getValue()) {
                        phyObjectTask.run();
                    }
                }
            }
        }
    }

    static class PhyObjectTask {

        private final String key;
        private final PhyDdlExecutionRecord phyDdlExecutionRecord;

        private final AtomicReference<Exception> exception = new AtomicReference<>(null);
        private final FutureTask<String> futureTask = new FutureTask<>(this::getKey);

        public PhyObjectTask(PhyDdlExecutionRecord phyDdlExecutionRecord) {
            this.key = String.format("%s:%s", phyDdlExecutionRecord.getJobId(), phyDdlExecutionRecord.getTaskId());
            this.phyDdlExecutionRecord = phyDdlExecutionRecord;
        }

        public void run() {
            futureTask.run();
        }

        public void setException(Exception exception) {
            this.exception.set(exception);
            futureTask.run();
        }

        public void waitForDone() throws Exception {
            try {
                futureTask.get(DdlConstants.MAX_WAITING_TIME, TimeUnit.MILLISECONDS);
                if (exception.get() != null) {
                    throw exception.get();
                }
            } catch (TimeoutException e) {
                throw new TimeoutException("Waiting for physical object recording timed out.");
            }
        }

        public String getKey() {
            return key;
        }

        public PhyDdlExecutionRecord getPhyDdlExecutionRecord() {
            return phyDdlExecutionRecord;
        }
    }

    public List<PhyObjectTask> getPhyObjectTaskQueue() {
        return phyObjectTaskQueue;
    }

    public void destroy() {
        this.active = false;
        synchronized (phyObjectTaskQueue) {
            phyObjectTaskQueue.notify();
        }
        if (asyncPhyObjectRecorderThread != null) {
            asyncPhyObjectRecorderThread.shutdown();
        }
    }
}
