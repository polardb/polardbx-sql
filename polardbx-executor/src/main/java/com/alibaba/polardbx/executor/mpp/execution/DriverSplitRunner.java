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

import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.mpp.operator.Driver;

import javax.annotation.concurrent.GuardedBy;

import static java.util.Objects.requireNonNull;

public class DriverSplitRunner implements SplitRunner {

    @GuardedBy("this")
    private boolean closed;

    @GuardedBy("this")
    private Driver driver;

    public DriverSplitRunner(Driver driver) {
        this.driver = requireNonNull(driver, "Driver is null");
    }

    public Driver getDriver() {
        return driver;
    }

    @Override
    public synchronized boolean isFinished() {
        if (closed) {
            return true;
        }
        return driver.isFinished();
    }

    @Override
    public ListenableFuture<?> processFor(long duration, long start) {
        String schema = driver.getDriverContext().getPipelineContext().getTaskContext().getContext().getSchemaName();
        DefaultSchema.setSchemaName(schema);
        return driver.processFor(duration, start);
    }

    @Override
    public String getInfo() {
        return driver.getDriverContext().getUniqueId();
    }

    @Override
    public void close() {
        Driver driver;
        synchronized (this) {
            closed = true;
            driver = this.driver;
        }

        if (driver != null) {
            driver.close();
        }
    }

    @Override
    public void recordBlocked() {
        if (this.driver.getDriverContext() != null) {
            this.driver.getDriverContext().recordBlocked();
        }
    }

    @Override
    public void recordBlockedFinished() {
        if (this.driver.getDriverContext() != null) {
            this.driver.getDriverContext().recordBlockedFinished();
        }
    }

    @Override
    public void buildMDC() {
        if (this.driver.getDriverContext() != null) {
            TaskContext taskContext = driver.getDriverContext().getPipelineContext().getTaskContext();
            MDC.put(MDC.MDC_KEY_APP, taskContext.getContext().getSchemaName().toLowerCase());
            MDC.put(MDC.MDC_KEY_CON, taskContext.getContext().getMdcConnString());
        }
    }

    @Override
    public boolean moveLowPrioritizedQuery(long executeTime) {
        if (this.driver.getDriverContext() != null) {
            TaskContext taskContext = driver.getDriverContext().getPipelineContext().getTaskContext();
            if (taskContext.getExecuteMillisLong(executeTime) > MppConfig.getInstance().getQueryMaxDelayTime()) {
                return true;
            }
        }
        return false;
    }
}
