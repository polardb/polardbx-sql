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

package com.alibaba.polardbx.executor.backfill;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.workqueue.PriorityWorkQueue;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.executor.gsi.GsiBackfillManager.BackfillStatus.SUCCESS;

public class Reporter {
    private final GsiBackfillManager backfillManager;

    /**
     * Extractor position mark
     */
    private GsiBackfillManager.BackfillBean backfillBean;

    /**
     * Calcualte backfill speed.
     */
    private long startTime = System.currentTimeMillis();
    private AtomicInteger taskCount = new AtomicInteger(0);
    private AtomicLong backfillCount = new AtomicLong(0);

    public Reporter(GsiBackfillManager backfillManager) {
        this.backfillManager = backfillManager;
    }

    public void loadBackfillMeta(long ddlJobId) {
        backfillBean = backfillManager.loadBackfillMeta(ddlJobId);
    }

    public GsiBackfillManager.BackfillBean getBackfillBean() {
        return backfillBean;
    }

    public void addBackfillCount(long count) {
        taskCount.incrementAndGet();
        backfillCount.addAndGet(count);
    }

    public void updateBackfillStatus(ExecutionContext ec, GsiBackfillManager.BackfillStatus status) {
        if (status == SUCCESS) {
            backfillBean.setProgress(100);

            // Log the backfill speed.
            long timeMillis = System.currentTimeMillis() - startTime;
            double time = (timeMillis / 1000.0);
            double speed = backfillCount.get() / time;
            DdlEngineStats.METRIC_BACKFILL_TIME_MILLIS.update(timeMillis);

            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] Backfill job: {1} total: {2} time: {3}s speed: {4}row/s with {5} task(s) PWQ total {6} thread(s).",
                ec.getTraceId(), backfillBean.jobId, backfillCount.get(), time, speed, taskCount,
                PriorityWorkQueue.getInstance().getPoolSize()));
        }
        backfillManager.updateLogicalBackfillObject(backfillBean, status);
    }

    public void splitBackfillRange(ExecutionContext ec, List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                   List<GsiBackfillManager.BackfillObjectRecord> newBackfillRecord) {
        backfillManager.splitBackfillObject(ec, backfillObjects, newBackfillRecord);
    }

    public void updatePositionMark(ExecutionContext ec, List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                   long successRowCount, List<ParameterContext> lastPk,
                                   List<ParameterContext> beforeLastPk, boolean finished,
                                   Map<Long, Long> primaryKeysIdMap) {

        final GsiBackfillManager.BackfillStatus
            status =
            finished ? GsiBackfillManager.BackfillStatus.SUCCESS : GsiBackfillManager.BackfillStatus.RUNNING;
        final List<ParameterContext> pk = (finished && null == lastPk) ? beforeLastPk : lastPk;

        // Update position mark and status
        final Integer partial =
            backfillManager.updateBackfillObject(backfillObjects, pk, successRowCount, status, primaryKeysIdMap);

        assert backfillBean.backfillObjects != null;
        final GsiBackfillManager.BackfillObjectKey key = backfillObjects.get(0).key();
        final int objectsCount = backfillBean.backfillObjects.size();
        final AtomicInteger total = new AtomicInteger(partial);
        backfillBean.backfillObjects.forEach((k, v) -> {
            if (!k.equals(key)) {
                total.addAndGet(v.get(0).progress);
            } else {
                v.forEach(bfo -> bfo.setProgress(partial));
            }
        });

        final Integer progress = total.get() / objectsCount;

        // Update progress
        final int totalProgress;
        if (ec.getParamManager().getBoolean(ConnectionParams.GSI_CHECK_AFTER_CREATION)) {
            totalProgress = progress / 2;
        } else {
            totalProgress = progress;
        }
        backfillManager.updateLogicalBackfillProcess(String.valueOf(totalProgress), ec.getBackfillId());
    }

}
