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

package com.alibaba.polardbx.optimizer.optimizeralert;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

public class OptimizerAlertLoggerBaseImpl implements OptimizerAlertLogger {
    protected final static Logger logger = LoggerFactory.getLogger("STATISTICS", true);

    protected ReentrantLock lock;

    /**
     * previous exception count reported to schedule jobs
     */
    protected AtomicLong prevCount;

    /**
     * total exception count
     */
    protected LongAdder totalCount;

    protected AtomicLong lastAccessTime;

    protected OptimizerAlertType optimizerAlertType;

    public OptimizerAlertLoggerBaseImpl() {
        this.lock = new ReentrantLock();
        this.prevCount = new AtomicLong(0);
        this.totalCount = new LongAdder();
        this.lastAccessTime = new AtomicLong(0);
    }

    @Override
    public void inc() {
        totalCount.increment();
    }

    @Override
    public boolean logDetail(ExecutionContext ec, Object object) {
        if (lock.tryLock()) {
            try {
                long lastTime = lastAccessTime.get();
                long currentTime = System.currentTimeMillis();
                if (currentTime >= lastTime + DynamicConfig.getInstance().getOptimizerAlertLogInterval()) {
                    lastAccessTime.set(currentTime);
                    if (ec == null) {
                        logger.info(optimizerAlertType.name());
                    } else {
                        logger.info(String.format("alert_type{ %s }: schema{ %s } trace_id { %s }",
                            optimizerAlertType.name(),
                            ec.getSchemaName(),
                            ec.getTraceId()));
                    }
                    return true;
                }
            } finally {
                lock.unlock();
            }
        }
        return false;
    }

    @Override
    public Pair<OptimizerAlertType, Long> collectByScheduleJob() {
        long total = totalCount.sum();
        long diff = total - prevCount.get();
        // exceeded, back to zero
        if (total < 0) {
            totalCount.reset();
            prevCount.set(0);
            return null;
        }
        if (diff > 0) {
            Pair<OptimizerAlertType, Long> result = new Pair<>(optimizerAlertType, diff);
            prevCount.set(total);
            return result;
        }
        return null;
    }

    @Override
    public Pair<OptimizerAlertType, Long> collectByView() {
        return new Pair<>(optimizerAlertType, totalCount.sum());
    }
}
