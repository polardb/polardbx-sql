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

package com.alibaba.polardbx.executor.mpp.execution;

import com.google.common.base.Ticker;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.units.Duration.succinctNanos;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class QueryStateTimer {

    private final boolean needStats;
    private final Ticker ticker;
    private final long createMillis;
    private final long executeCreateNanos;
    private final long queryStartNanos;

    private final AtomicReference<Long> executionStartNanos = new AtomicReference<>();
    private final AtomicReference<Long> endNanos = new AtomicReference<>();

    private final AtomicReference<Long> queuedTime = new AtomicReference<>();
    private final AtomicReference<Duration> distributedPlanningTime = new AtomicReference<>();

    private final AtomicReference<Long> finishingStartNanos = new AtomicReference<>();
    private final AtomicReference<Duration> finishingTime = new AtomicReference<>();

    private final AtomicReference<Long> totalPlanningStartNanos = new AtomicReference<>();
    private final AtomicReference<Long> totalPlanningTime = new AtomicReference<>();

    private final AtomicReference<Long> lastHeartbeat;

    public QueryStateTimer(Ticker ticker, long queryStartNanos, boolean needStats) {
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.queryStartNanos = queryStartNanos;
        this.needStats = needStats;
        this.executeCreateNanos = ticker.read();
        this.createMillis = System.currentTimeMillis();
        this.lastHeartbeat = new AtomicReference<Long>(executeCreateNanos);
    }

    public void beginPlanning() {
        if (needStats) {
            queuedTime.compareAndSet(null, ticker.read() - executeCreateNanos);
            //FXIME 这里暂时就用query提交时间表示优化开始时间吧？
            totalPlanningStartNanos.compareAndSet(null, queryStartNanos);
        }
    }

    public void beginStarting() {
        if (needStats) {
            long now = ticker.read();
            queuedTime.compareAndSet(null, now - executeCreateNanos);
            totalPlanningStartNanos.compareAndSet(null, queryStartNanos);
            totalPlanningTime.compareAndSet(null, now - totalPlanningStartNanos.get());
        }
    }

    public void beginRunning() {
        if (needStats) {
            long now = ticker.read();
            queuedTime.compareAndSet(null, now - executeCreateNanos);
            totalPlanningStartNanos.compareAndSet(null, queryStartNanos);
            totalPlanningTime.compareAndSet(null, now - totalPlanningStartNanos.get());
            executionStartNanos.compareAndSet(null, now);
        }
    }

    public void beginFinishing() {
        if (needStats) {
            long now = ticker.read();
            queuedTime.compareAndSet(null, now - executeCreateNanos);
            totalPlanningStartNanos.compareAndSet(null, queryStartNanos);
            totalPlanningTime.compareAndSet(null, now - totalPlanningStartNanos.get());
            executionStartNanos.compareAndSet(null, now);
            finishingStartNanos.compareAndSet(null, now);
        }
    }

    public void endQuery() {
        long nanos = ticker.read();
        if (needStats) {
            queuedTime.compareAndSet(null, nanos - executeCreateNanos);
            totalPlanningStartNanos.compareAndSet(null, queryStartNanos);
            totalPlanningTime.compareAndSet(null, nanos - totalPlanningStartNanos.get());
            executionStartNanos.compareAndSet(null, nanos);
            finishingStartNanos.compareAndSet(null, nanos);
            finishingTime.compareAndSet(null, Duration.succinctNanos(nanos - finishingStartNanos.get()));
        }
        endNanos.compareAndSet(null, nanos);
    }

    public long getCreateMillis() {
        return createMillis;
    }

    public DateTime getCreateTime() {
        return new DateTime(createMillis);
    }

    public Duration getElapsedTime() {
        Long end = endNanos.get();
        if (end != null) {
            return succinctNanos(end - queryStartNanos);
        }
        return nanosSince(queryStartNanos, ticker.read());
    }

    public DateTime getExecutionStartNanos() {
        return toDateTime(this.executionStartNanos);
    }

    private static Duration nanosSince(long start, long now) {
        return succinctNanos(max(0, now - start));
    }

    private long tickerNanos() {
        return ticker.read();
    }

    private DateTime toDateTime(AtomicReference<Long> instantNanos) {
        Long nanos = instantNanos.get();
        if (nanos == null) {
            return DateTime.now();
        }
        return toDateTime(nanos);
    }

    private DateTime toDateTime(long instantNanos) {
        long millisSinceCreate = NANOSECONDS.toMillis(instantNanos - executeCreateNanos);
        return new DateTime(createMillis + millisSinceCreate);
    }

    public void recordHeartbeat() {
        this.lastHeartbeat.set(tickerNanos());
    }

    public void recordDistributedPlanningTime(long distributedPlanningStart) {
        distributedPlanningTime
            .compareAndSet(null, new Duration(System.currentTimeMillis() - distributedPlanningStart, MILLISECONDS));
    }

    public DateTime getLastHeartbeat() {
        return toDateTime(this.lastHeartbeat.get());
    }

    public DateTime getEndTime() {
        Long nanos = endNanos.get();
        if (nanos == null) {
            return null;
        }
        return toDateTime(nanos);
    }

    public Duration getQueuedTime() {
        Long queuedTime = this.queuedTime.get();
        if (queuedTime != null) {
            return Duration.succinctNanos(queuedTime);
        }
        return null;
    }

    public Duration getDistributedPlanningTime() {
        Duration distributedPlanningTime = this.distributedPlanningTime.get();
        if (distributedPlanningTime != null) {
            return distributedPlanningTime;
        }
        return new Duration(0, MILLISECONDS);
    }

    public Duration getTotalPlanningTime() {
        Long totalPlanningTime = this.totalPlanningTime.get();
        if (totalPlanningTime != null) {
            return Duration.succinctNanos(totalPlanningTime);
        }
        return new Duration(0, MILLISECONDS);
    }

    public Duration getFinishingTime() {
        return getDuration(finishingTime, finishingStartNanos);
    }

    private Duration getDuration(AtomicReference<Duration> finalDuration, AtomicReference<Long> start) {
        Duration duration = finalDuration.get();
        if (duration != null) {
            return duration;
        }
        Long startNanos = start.get();
        if (startNanos != null) {
            return nanosSince(startNanos, tickerNanos());
        }
        return new Duration(0, MILLISECONDS);
    }
}
