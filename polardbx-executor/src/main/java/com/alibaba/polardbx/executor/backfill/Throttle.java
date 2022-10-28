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

import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.commons.collections.set.SynchronizedSet;

import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mengshi
 */
public class Throttle {
    private long minRate;
    private long maxRate;
    private double rate;
    private Queue<FeedbackStats> statsQueue;
    ReentrantLock lock = new ReentrantLock();
    private volatile Long backFillId;

    private ScheduledThreadPoolExecutor timerTaskExecutor = new
        ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "Throttle-Timer");
            return thread;
        }
    });

    private final static Set<Throttle> THROTTLE_INSTANCES = SynchronizedSet.decorate(new HashSet<>());

    public long getMaxRate() {
        return this.maxRate;
    }

    public static final class FeedbackStats {
        public final long timeCost;
        public final long startTime;
        public final long rows;

        public FeedbackStats(long timeCost, long startTime, long rows) {
            this.timeCost = timeCost;
            this.startTime = startTime;
            this.rows = rows;
        }

        @Override
        public String toString() {
            return "FeedbackStats{" +
                "timeCost=" + timeCost +
                ", startTime=" + startTime +
                ", rows=" + rows +
                '}';
        }
    }

    private enum State {
        INIT, RUNNING
    }

    private State state = State.INIT;
    private long rowsLastCycle = 0;
    private long totalRows;
    private long startTime;
    private long startTimeLastCycle;
    private double actualRateLastCycle;
    private double baseTimeCost = Long.MAX_VALUE;
    private int growthFactor = 0;
    private int degrowthFactor = 0;
    private int cyclePeriod = 3;

    public Throttle(long minRate, long maxRate, String schema) {
        synchronized (THROTTLE_INSTANCES){
            THROTTLE_INSTANCES.add(this);
            this.minRate = minRate;
            this.maxRate = maxRate;
            reset();
            this.statsQueue = new ArrayDeque<>();
            this.timerTaskExecutor
                    .scheduleWithFixedDelay(new Runnable() {
                        @Override
                        public void run() {
                            LoggerUtil.buildMDC(schema);
                            long totalTimeCost = 0;
                            lock.lock();
                            try {
                                if (state == State.INIT) {
                                    return;
                                }

                                if (statsQueue.isEmpty()) {
                                    reset();
                                    return;
                                }

                                if (statsQueue.size() < 3) {
                                    return;
                                }

                                long period = System.currentTimeMillis() - startTimeLastCycle;

                                if (period < cyclePeriod * 1000) {
                                    return;
                                }

                                actualRateLastCycle = rowsLastCycle / (period / 1000);
                                startTimeLastCycle = System.currentTimeMillis();
                                rowsLastCycle = 0;

                                for (FeedbackStats stats : statsQueue) {
                                    totalTimeCost += stats.timeCost;
                                }

                                double avgTimeCost = totalTimeCost / statsQueue.size();

                                statsQueue = new ArrayDeque<>();

                                if (baseTimeCost > avgTimeCost) {
                                    baseTimeCost = avgTimeCost;
                                }

                                double aimTimeCost = baseTimeCost * 2;

                                if (avgTimeCost <= (aimTimeCost * 0.75) && actualRateLastCycle >= rate * 0.9) {
                                    growthFactor++;
                                    rate = rate + rate * 0.05 * (1 - avgTimeCost / aimTimeCost) * (Math.log(growthFactor) / Math
                                            .log(2));
                                    degrowthFactor = 0;
                                } else if (avgTimeCost > aimTimeCost) {
                                    degrowthFactor++;
                                    rate =
                                            rate - rate * 0.1 * (1 - aimTimeCost / avgTimeCost) * (Math.log(degrowthFactor) / Math
                                                    .log(2));
                                    growthFactor = 0;
                                } else if (actualRateLastCycle <= rate * 0.7) {
                                    degrowthFactor++;
                                    rate =
                                            rate - rate * 0.1 * (Math.log(degrowthFactor) / Math
                                                    .log(2));
                                    growthFactor = 0;
                                } else {
                                    growthFactor = 0;
                                    degrowthFactor = 0;
                                }

                                if (rate > Throttle.this.maxRate) {
                                    rate = Throttle.this.maxRate;
                                } else if (rate < Throttle.this.minRate) {
                                    rate = Throttle.this.minRate;
                                    baseTimeCost = Long.MAX_VALUE;
                                }

                                SQLRecorderLogger.ddlLogger.info(
                                        "speed: " + (long) actualRateLastCycle + " rows/s, avg cost: " + (long) avgTimeCost
                                                + ", baseTimeCost: "
                                                + (long) baseTimeCost + ", rate: " + (long) rate
                                                + " rows/s");

                            } catch (Throwable t) {
                                SQLRecorderLogger.ddlLogger.error(t);
                            } finally {
                                lock.unlock();
                            }

                        }
                    }, 0, cyclePeriod, TimeUnit.SECONDS);
        }
    }

    /**
     * Get total throttle rate of all instances
     */
    public static long getTotalThrottleRate() {
        long res = 0;
        for (Throttle t : THROTTLE_INSTANCES) {
            res += t.getNewRate();
        }
        return res;
    }

    public void stop() {
        timerTaskExecutor.shutdown();
        reset();
        synchronized (THROTTLE_INSTANCES){
            THROTTLE_INSTANCES.remove(this);
        }
    }

    public double getActualRateLastCycle() {
        return this.actualRateLastCycle;
    }

    public void resetMaxRate(long maxRate) {
        this.maxRate = maxRate;
    }

    public void resetMinRate(long minRate) {
        this.minRate = this.minRate;
    }

    private void reset() {
        if (state != State.INIT) {
            long elapsedMillis = Math.max(1, System.currentTimeMillis() - startTime);
            double speed = 1000.0 * totalRows / elapsedMillis;
            SQLRecorderLogger.ddlLogger.info(String.format("Throttle reset, rows: %d, avg speed: %f/s",
                totalRows, speed));
        }

        baseTimeCost = Long.MAX_VALUE;
        rate = Math.min(Math.max(minRate * 3, rate * 0.8), maxRate);
        growthFactor = 0;
        degrowthFactor = 0;
        state = State.INIT;
        totalRows = 0;
        startTime = -1;
    }

    /**
     * @param stats for last batch
     */
    public void feedback(FeedbackStats stats) {
        lock.lock();
        try {
            totalRows += stats.rows;
            if (startTime == -1) {
                startTime = stats.startTime;
            }
            switch (state) {
            case INIT:
                statsQueue.offer(stats);
                if (statsQueue.size() == 15) {
                    state = State.RUNNING;
                    startTimeLastCycle = System.currentTimeMillis();
                }
                break;
            default:
                statsQueue.offer(stats);
                rowsLastCycle += stats.rows;
                break;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return rate for next cycle
     */
    public double getNewRate() {
        return rate;
    }

    public Long getBackFillId() {
        return backFillId;
    }

    public void setBackFillId(Long backFillId) {
        this.backFillId = backFillId;
    }

    public static List<ThrottleInfo> getThrottleInfoList(){
        synchronized (THROTTLE_INSTANCES){
            List<ThrottleInfo> result = new ArrayList<>(THROTTLE_INSTANCES.size());
            for(Throttle throttle: THROTTLE_INSTANCES){
                if(throttle.backFillId == null){
                    //checkers do not have backFillId
                    continue;
                }
                ThrottleInfo row = new ThrottleInfo(throttle.backFillId, throttle.actualRateLastCycle, throttle.totalRows);
                result.add(row);
            }
            return result;
        }
    }
}

