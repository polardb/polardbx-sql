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

package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.utils.ParamValidationUtils;

import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_DEADLOCK_DETECTION_PARAM;

/**
 * This timer task wrapper supports modifying task parameters dynamically.
 *
 * @author wuzhe
 */
public abstract class BaseTimerTaskWrapper {
    /**
     * In PolarDB-X, some timer tasks only do one thing:
     * schedule an async task periodically.
     * And the async task only do one thing:
     * ensure only one raw task is scheduled and run at a time.
     * Here is the timer task.
     */
    private TimerTask timerTask;

    /**
     * Current parameters with which this task is running.
     */
    private Map<String, String> currentParam;

    /**
     * This lock is used to protect the {@link #timerTask}
     * and the raw task from being reset while it is running.
     * Acquire this lock before the timer task runs or resets.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * If a reset fails because the task is running, set this flag to cause a delayed reset.
     * After the task finishes running, it calls {@link #resetTask} to start a new reset routine.
     */
    private final AtomicBoolean needReset = new AtomicBoolean(false);

    protected final Map<String, Object> properties;
    protected final AsyncTaskQueue asyncTaskQueue;

    public BaseTimerTaskWrapper(Map<String, Object> properties, AsyncTaskQueue asyncTaskQueue) {
        this.properties = properties;
        this.asyncTaskQueue = asyncTaskQueue;
    }

    /**
     * Reset the current timer task with new parameters.
     */
    public void resetTask() {
        if (ConfigDataMode.isFastMock()) {
            cancel();
            return;
        }

        // 1. Get new parameters.
        final Map<String, String> newParams = getNewParams();

        // 2. Validate parameters.
        validateParams(newParams);

        // 3. If new parameters are identical to the current running ones, ignore the reset.
        final Map<String, String> currentParam = getCurrentParam();
        if (null != currentParam &&
            ParamValidationUtils.isIdentical(newParams, currentParam, getParamsDef())) {
            return;
        }

        // 4. Reset the timer task.
        innerReset(newParams);
    }

    abstract Set<String> getParamsDef();

    abstract void validateParams(Map<String, String> newParams);

    abstract Map<String, String> getNewParams();

    /**
     * Create a new timer task.
     *
     * @param newParam new parameters.
     * @return A newly created timer task.
     */
    abstract TimerTask createTask(Map<String, String> newParam);

    /**
     * @return Whether the timer task is null.
     */
    public Boolean isTimerTaskNull() {
        return null == timerTask;
    }

    /**
     * @return The current parameters the current timer task uses.
     * Return null if the current timer task is null.
     */
    public Map<String, String> getCurrentParam() {
        return isTimerTaskNull() ? null : this.currentParam;
    }

    /**
     * A brute-force cancellation, it does not check whether the raw task is running.
     * Use the {@link #lock} to protected {@link #timerTask} if needed.
     */
    public void cancel() {
        if (null != this.timerTask) {
            this.timerTask.cancel();
            this.timerTask = null;
        }
        this.currentParam = null;
        this.needReset.set(false);
    }

    /**
     * @param task the raw task. It can be the deadlock detection task, log cleaning task...
     * @return A task which will prevent the timer task being reset
     * and try to reset it after it finishes running.
     */
    protected Runnable getTask(Runnable task) {
        return () -> {
            if (!this.lock.tryLock()) {
                // If this lock is held by another thread,
                // the timer task is being reset or another thread is running this task, so just return.
                return;
            }

            try {
                task.run();
            } finally {
                try {
                    // If necessary, reset the timer task in this thread
                    // since it already holds the lock.
                    if (this.needReset.compareAndSet(true, false)) {
                        TransactionLogger.warn("Start a delay reset.");
                        resetTask();
                    }
                } finally {
                    this.lock.unlock();
                }
            }
        };
    }

    /**
     * Try to reset the timer task with new parameters.
     * If the task is running, is causes a delayed reset.
     */
    protected void innerReset(Map<String, String> newParams) {
        try {
            // The lock may be held by other thread because:
            // 1. the timer task is running now;
            // 2. or the timer task is resetting now (by other threads).
            // For the second case, try to wait some time until the reset is completed.
            if (this.lock.tryLock(5L, TimeUnit.SECONDS)) {
                try {
                    TransactionLogger.warn("Start resetting timer task with param: " + newParams);

                    // 1. Cancel the current task.
                    this.cancel();
                    // 2. Now, the timer task should be null, we first set the current parameters.
                    this.currentParam = newParams;
                    // 3. Then, set the timer task to a newly created timer task.
                    this.timerTask = createTask(newParams);

                    TransactionLogger.warn("Finish resetting timer task with param: " + newParams);

                    // Reset the timer task successfully, return.
                    return;
                } finally {
                    this.lock.unlock();
                }
            }
        } catch (InterruptedException e) {
            // Ignore.
        }

        // Fail to reset the timer task, so set the {needReset} flag to delay the reset.
        TransactionLogger.info("Delay resetting timer task with param: " + newParams);
        this.needReset.set(true);
    }
}
