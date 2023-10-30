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

package com.alibaba.polardbx.sequence.impl;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import com.alibaba.polardbx.sequence.impl.NewSequenceDao.MergingResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INCREMENT_BY;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NEW_SEQ_GROUPING_TIMEOUT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NEW_SEQ_TASK_QUEUE_NUM_PER_DB;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NEW_SEQ_VALUE_HANDLER_KEEP_ALIVE_TIME;

public class NewSequenceScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(NewSequenceScheduler.class);

    private static long groupingTimeout = NEW_SEQ_GROUPING_TIMEOUT;

    private final String schemaName;
    private final NewSequenceDao newSequenceDao;

    private volatile boolean valid = true;
    private volatile boolean active = true;

    private volatile int taskQueueNum = NEW_SEQ_TASK_QUEUE_NUM_PER_DB;
    private volatile boolean requestMergingEnabled = true;
    private volatile long valueHandlerKeepAliveTime = NEW_SEQ_VALUE_HANDLER_KEEP_ALIVE_TIME;

    private final List<Pair<Integer, Boolean>> activeQueueIndexes = Lists.newArrayList();
    private final List<List<NewSeqValueTask>> valueTaskQueues = Lists.newArrayList();
    private final List<List<NewSeqSkipTask>> skipTaskQueues = Lists.newArrayList();

    private final ReentrantReadWriteLock queuesLock = new ReentrantReadWriteLock();

    private final Future monitorFuture;
    private final Map<Integer, Future> valueFetcherFutures = Maps.newConcurrentMap();
    private final Map<Integer, Future> valueUpdaterFutures = Maps.newConcurrentMap();

    private final ExecutorService handlerMonitor;
    private ExecutorService valueFetchers;
    private ExecutorService valueUpdaters;

    public NewSequenceScheduler(NewSequenceDao newSequenceDao) {
        this.schemaName = newSequenceDao.getSchemaName();
        this.newSequenceDao = newSequenceDao;
        this.handlerMonitor = DdlHelper.createSingleThreadPool(String.format("NewSeqHandlerMonitor-%s", schemaName));
        monitorFuture = this.handlerMonitor.submit(AsyncTask.build(new MonitorHandlerTask()));
    }

    public void init() {
        this.valueFetchers = DdlHelper.createThreadPool(taskQueueNum, valueHandlerKeepAliveTime,
            String.format("NewSeqValueFetcher-%s", schemaName));
        this.valueUpdaters = DdlHelper.createThreadPool(taskQueueNum, valueHandlerKeepAliveTime,
            String.format("NewSeqValueUpdater-%s", schemaName));
        queuesLock.writeLock().lock();
        try {
            initQueuesAndHandlers(true);
        } finally {
            queuesLock.writeLock().unlock();
        }
    }

    private void initQueuesAndHandlers(boolean forceClearQueues) {
        this.active = true;
        for (int i = 0; i < taskQueueNum; i++) {
            if (forceClearQueues) {
                valueTaskQueues.add(new ArrayList<>());
                skipTaskQueues.add(new ArrayList<>());
            }
        }
        for (int i = 0; i < taskQueueNum; i++) {
            valueFetcherFutures.put(i, valueFetchers.submit(AsyncTask.build(new FetchNewSeqValueTask(i))));
            valueUpdaterFutures.put(i, valueUpdaters.submit(AsyncTask.build(new SkipNewSeqValueTask(i))));
        }
    }

    class MonitorHandlerTask implements Runnable {

        @Override
        public void run() {
            while (true) {
                if (!valid) {
                    return;
                }
                try {
                    monitor();
                } catch (Throwable t) {
                    LOGGER.error(t);
                }
            }
        }

        private void monitor() {
            List<Pair<Integer, Boolean>> queueIndexesRequested = new ArrayList<>();

            synchronized (activeQueueIndexes) {
                while (valid && activeQueueIndexes.isEmpty()) {
                    try {
                        activeQueueIndexes.wait();
                    } catch (InterruptedException ignored) {
                    }
                }
                queueIndexesRequested.addAll(activeQueueIndexes);
                activeQueueIndexes.clear();
            }

            for (Pair<Integer, Boolean> queueInfo : queueIndexesRequested) {
                // If any value handler has exited due to idle for more than keep alive time,
                // and the monitor receives new request, then submit a new task to rebind to
                // the corresponding queue to handle requests.
                Integer queueIndex = queueInfo.getKey();
                boolean isNextval = queueInfo.getValue();

                String warnMsg = "Rebound a %s handler to %s queue " + queueIndex;

                if (isNextval && !valueFetcherFutures.keySet().contains(queueIndex)) {
                    valueFetcherFutures.put(queueIndex,
                        valueFetchers.submit(AsyncTask.build(new FetchNewSeqValueTask(queueIndex))));
                    LOGGER.warn(String.format(warnMsg, "fetcher", "value"));
                }

                if (!isNextval && !valueUpdaterFutures.keySet().contains(queueIndex)) {
                    valueUpdaterFutures.put(queueIndex,
                        valueUpdaters.submit(AsyncTask.build(new SkipNewSeqValueTask(queueIndex))));
                    LOGGER.warn(String.format(warnMsg, "updater", "skip"));
                }
            }
        }
    }

    class FetchNewSeqValueTask implements Runnable {

        private volatile boolean idleTimeout = false;

        private final int queueIndex;
        private final List<NewSeqValueTask> valueTaskQueue;

        public FetchNewSeqValueTask(int queueIndex) {
            this.queueIndex = queueIndex;
            queuesLock.readLock().lock();
            try {
                this.valueTaskQueue = valueTaskQueues.get(queueIndex);
            } finally {
                queuesLock.readLock().unlock();
            }
        }

        @Override
        public void run() {
            while (true) {
                if (!active || idleTimeout) {
                    String warnMsg = "The fetcher handler bound to queue " + queueIndex + " exited due to %s";
                    if (!active) {
                        LOGGER.warn(String.format(warnMsg, "reset or destroyed"));
                    }
                    if (idleTimeout) {
                        LOGGER.warn(String.format(warnMsg, "idle timeout"));
                    }
                    valueFetcherFutures.remove(queueIndex);
                    return;
                }
                try {
                    fetchNewSeqValue();
                } catch (Throwable t) {
                    LOGGER.error(t);
                }
            }
        }

        private void fetchNewSeqValue() {
            Map<String, List<NewSeqValueTask>> valueTasksBySeq = new HashMap<>();

            synchronized (valueTaskQueue) {
                while (active && !idleTimeout && valueTaskQueue.isEmpty()) {
                    try {
                        long startWaitingTime = System.currentTimeMillis();
                        valueTaskQueue.wait(valueHandlerKeepAliveTime);
                        long actualWaitingTime = System.currentTimeMillis() - startWaitingTime;
                        if (actualWaitingTime > valueHandlerKeepAliveTime && valueTaskQueue.isEmpty()) {
                            idleTimeout = true;
                            return;
                        }
                    } catch (InterruptedException ignored) {
                    }
                }
                for (NewSeqValueTask valueTask : valueTaskQueue) {
                    List<NewSeqValueTask> valueTasksForSeq = valueTasksBySeq.get(valueTask.getSeqName());
                    if (valueTasksForSeq == null) {
                        valueTasksForSeq = new ArrayList<>();
                        valueTasksBySeq.put(valueTask.getSeqName(), valueTasksForSeq);
                    }
                    valueTasksForSeq.add(valueTask);
                }
                valueTaskQueue.clear();
            }

            List<Pair<String, Integer>> seqBatchSizes = new ArrayList<>();

            for (Map.Entry<String, List<NewSeqValueTask>> entry : valueTasksBySeq.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    seqBatchSizes.add(Pair.of(entry.getKey(), entry.getValue().size()));
                }
            }

            Exception exception;

            if (requestMergingEnabled) {
                MergingResult mergingResult = newSequenceDao.nextValueMerging(seqBatchSizes,
                    v -> respondToTasks(v.getKey(), valueTasksBySeq.get(v.getKey()), v.getValue(), 0, null));
                if (mergingResult != null) {
                    if (mergingResult.failedBeforeAnyResponse) {
                        // Failed before doing any next value merging process,
                        // so we should respond to all tasks in this situation.
                        exception = new SequenceException(mergingResult.origEx, mergingResult.errMsg);
                        for (Pair<String, Integer> pair : seqBatchSizes) {
                            String seqName = pair.getKey();
                            respondToTasks(seqName, valueTasksBySeq.get(seqName), 0, 0, exception);
                        }
                    } else if (mergingResult.indexProcessed < (seqBatchSizes.size() - 1)) {
                        // Failed from some sequence's next value merging process, so
                        // we should respond to the rest of the tasks here.
                        exception = new SequenceException(mergingResult.origEx, mergingResult.errMsg);
                        for (int index = mergingResult.indexProcessed + 1; index < seqBatchSizes.size(); index++) {
                            String seqName = seqBatchSizes.get(index).getKey();
                            respondToTasks(seqName, valueTasksBySeq.get(seqName), 0, 0, exception);
                        }
                    }
                }
            } else {
                for (Pair<String, Integer> seqBatchSize : seqBatchSizes) {
                    exception = null;

                    String seqName = seqBatchSize.getKey();
                    int batchSize = seqBatchSize.getValue();

                    int increment = DEFAULT_INCREMENT_BY;
                    if (batchSize > 1) {
                        increment = SequenceManagerProxy.getInstance().getIncrement(schemaName, seqName);
                    }

                    long valueBase = 0L;
                    try {
                        valueBase = newSequenceDao.nextValue(seqName, batchSize) - batchSize * increment + increment;
                    } catch (Exception e) {
                        exception = e;
                    }

                    respondToTasks(seqName, valueTasksBySeq.get(seqName), valueBase, increment, exception);
                }
            }
        }

        private void respondToTasks(String seqName, List<NewSeqValueTask> valueTasks, long valueBase, int increment,
                                    Exception exception) {
            if (exception == null && valueBase <= 0L) {
                exception = new SequenceException("Unexpected value base: " + valueBase);
            }
            if (exception != null) {
                for (NewSeqValueTask valueTask : valueTasks) {
                    valueTask.setException(exception);
                }
            } else {
                if (increment < 1) {
                    increment = SequenceManagerProxy.getInstance().getIncrement(schemaName, seqName);
                }
                for (NewSeqValueTask valueTask : valueTasks) {
                    valueTask.setValue(valueBase);
                    valueBase += increment;
                }
            }
        }
    }

    class SkipNewSeqValueTask implements Runnable {

        private volatile boolean idleTimeout = false;

        private final int queueIndex;
        private final List<NewSeqSkipTask> skipTaskQueue;

        public SkipNewSeqValueTask(int queueIndex) {
            this.queueIndex = queueIndex;
            queuesLock.readLock().lock();
            try {
                this.skipTaskQueue = skipTaskQueues.get(queueIndex);
            } finally {
                queuesLock.readLock().unlock();
            }
        }

        @Override
        public void run() {
            while (true) {
                if (!active || idleTimeout) {
                    String warnMsg = "The updater handler bound to queue " + queueIndex + " exited due to %s";
                    if (!active) {
                        LOGGER.warn(String.format(warnMsg, "reset or destroyed"));
                    }
                    if (idleTimeout) {
                        LOGGER.warn(String.format(warnMsg, "idle timeout"));
                    }
                    valueUpdaterFutures.remove(queueIndex);
                    return;
                }
                try {
                    updateNewSeqValue();
                } catch (Throwable t) {
                    LOGGER.error(t);
                }
            }
        }

        private void updateNewSeqValue() {
            Map<String, List<NewSeqSkipTask>> skipTasksBySeq = new HashMap<>();

            synchronized (skipTaskQueue) {
                while (active && !idleTimeout && skipTaskQueue.isEmpty()) {
                    try {
                        long startWaitingTime = System.currentTimeMillis();
                        skipTaskQueue.wait(valueHandlerKeepAliveTime);
                        long actualWaitingTime = System.currentTimeMillis() - startWaitingTime;
                        if (actualWaitingTime > valueHandlerKeepAliveTime && skipTaskQueue.isEmpty()) {
                            idleTimeout = true;
                            return;
                        }
                    } catch (InterruptedException ignored) {
                    }
                }
                for (NewSeqSkipTask skipTask : skipTaskQueue) {
                    List<NewSeqSkipTask> skipTasksForSeq = skipTasksBySeq.get(skipTask.getSeqName());
                    if (skipTasksForSeq == null) {
                        skipTasksForSeq = new ArrayList<>();
                        skipTasksBySeq.put(skipTask.getSeqName(), skipTasksForSeq);
                    }
                    skipTasksForSeq.add(skipTask);
                }
                skipTaskQueue.clear();
            }

            Map<String, Long> seqMaxValues = new HashMap<>();

            for (Map.Entry<String, List<NewSeqSkipTask>> entry : skipTasksBySeq.entrySet()) {
                Optional<NewSeqSkipTask> max = entry.getValue().stream().max(NewSeqSkipTask::compare);
                if (max.isPresent() && max.get().value > 0L) {
                    seqMaxValues.put(entry.getKey(), max.get().value);
                }
            }

            Exception exception = null;

            if (requestMergingEnabled) {
                try {
                    newSequenceDao.updateValueMerging(seqMaxValues);
                } catch (Exception e) {
                    exception = e;
                }

                for (String seqName : seqMaxValues.keySet()) {
                    respondToTasks(skipTasksBySeq.get(seqName), exception);
                }
            } else {
                for (Map.Entry<String, Long> seqMaxValue : seqMaxValues.entrySet()) {
                    exception = null;

                    String seqName = seqMaxValue.getKey();
                    long maxValue = seqMaxValue.getValue();

                    try {
                        newSequenceDao.updateValue(seqName, maxValue);
                    } catch (Exception e) {
                        exception = e;
                    }

                    respondToTasks(skipTasksBySeq.get(seqName), exception);
                }
            }
        }

        private void respondToTasks(List<NewSeqSkipTask> skipTasks, Exception exception) {
            if (exception != null) {
                for (NewSeqSkipTask skipTask : skipTasks) {
                    skipTask.setException(exception);
                }
            } else {
                for (NewSeqSkipTask skipTask : skipTasks) {
                    skipTask.run();
                }
            }
        }
    }

    static class NewSeqValueTask {

        private final String seqName;

        private final AtomicLong value = new AtomicLong(0);
        private final AtomicReference<Exception> exception = new AtomicReference<>(null);
        private final FutureTask<Long> futureTask = new FutureTask<>(value::get);

        public NewSeqValueTask(String seqName) {
            this.seqName = seqName;
        }

        public void setValue(long value) {
            this.value.set(value);
            futureTask.run();
        }

        public void setException(Exception exception) {
            this.exception.set(exception);
            futureTask.run();
        }

        public long waitForValue() throws Exception {
            try {
                final long value = futureTask.get(groupingTimeout, TimeUnit.MILLISECONDS);
                if (exception.get() != null) {
                    throw exception.get();
                }
                return value;
            } catch (TimeoutException e) {
                throw new TimeoutException("Fetching New Sequence value timed out.");
            }
        }

        public String getSeqName() {
            return seqName;
        }
    }

    static class NewSeqSkipTask {

        private final String seqName;
        private final long value;

        private final AtomicReference<Exception> exception = new AtomicReference<>(null);
        private final FutureTask<Long> futureTask = new FutureTask<>(this::getValue);

        public NewSeqSkipTask(String seqName, long value) {
            this.seqName = seqName;
            this.value = value;
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
                futureTask.get(groupingTimeout, TimeUnit.MILLISECONDS);
                if (exception.get() != null) {
                    throw exception.get();
                }
            } catch (TimeoutException e) {
                throw new TimeoutException("Skipping New Sequence value timed out.");
            }
        }

        public String getSeqName() {
            return seqName;
        }

        public long getValue() {
            return value;
        }

        public static int compare(NewSeqSkipTask o1, NewSeqSkipTask o2) {
            if (o1.value > o2.value) {
                return 1;
            } else if (o1.value < o2.value) {
                return -1;
            }
            return 0;
        }
    }

    public List<Pair<Integer, Boolean>> getActiveQueueIndexes() {
        return activeQueueIndexes;
    }

    public List<NewSeqValueTask> getValueTaskQueue(int index) {
        queuesLock.readLock().lock();
        try {
            if (index >= valueTaskQueues.size()) {
                return null;
            }
            return valueTaskQueues.get(index);
        } finally {
            queuesLock.readLock().unlock();
        }
    }

    public List<NewSeqSkipTask> getSkipTaskQueue(int index) {
        queuesLock.readLock().lock();
        try {
            if (index >= skipTaskQueues.size()) {
                return null;
            }
            return skipTaskQueues.get(index);
        } finally {
            queuesLock.readLock().unlock();
        }
    }

    public static void setGroupingTimeout(long groupingTimeout) {
        NewSequenceScheduler.groupingTimeout = groupingTimeout;
    }

    public static void resetGroupingTimeout(long groupingTimeout) {
        if (groupingTimeout > 0L && groupingTimeout != NewSequenceScheduler.groupingTimeout) {
            LOGGER.warn(String.format("Reset grouping timeout from %s to %s", NewSequenceScheduler.groupingTimeout,
                groupingTimeout));
            NewSequenceScheduler.groupingTimeout = groupingTimeout;
        }
    }

    public int getTaskQueueNum() {
        return taskQueueNum;
    }

    public void setTaskQueueNum(int taskQueueNum) {
        this.taskQueueNum = taskQueueNum;
    }

    public void resetTaskQueueNum(int taskQueueNum) {
        if (taskQueueNum > 0 && taskQueueNum != this.taskQueueNum) {
            LOGGER.warn(String.format("Reset queues and handlers from %s to %s", this.taskQueueNum, taskQueueNum));
            this.taskQueueNum = taskQueueNum;
            ((ThreadPoolExecutor) valueFetchers).setCorePoolSize(taskQueueNum);
            ((ThreadPoolExecutor) valueFetchers).setMaximumPoolSize(taskQueueNum);
            ((ThreadPoolExecutor) valueUpdaters).setCorePoolSize(taskQueueNum);
            ((ThreadPoolExecutor) valueUpdaters).setMaximumPoolSize(taskQueueNum);
            resetQueuesAndHandlers(true);
        }
    }

    public void resetQueuesAndHandlers(boolean forceClearQueues) {
        this.active = false;
        queuesLock.writeLock().lock();
        try {
            notifyAndClearHandlerQueues(forceClearQueues);
            cancelAllHandlerTasks();
            waitUntilNoActiveHandlers();
            initQueuesAndHandlers(forceClearQueues);
        } finally {
            queuesLock.writeLock().unlock();
        }
    }

    private static final int MAX_SLEEP_TIMES = 300;

    private void waitUntilNoActiveHandlers() {
        int sleepTimes = 0;
        while ((((ThreadPoolExecutor) valueFetchers).getActiveCount() != 0
            || ((ThreadPoolExecutor) valueUpdaters).getActiveCount() != 0)
            && sleepTimes < MAX_SLEEP_TIMES) {
            try {
                Thread.sleep(1000);
                sleepTimes++;
            } catch (InterruptedException ignored) {
            }
        }
    }

    public void setRequestMergingEnabled(boolean requestMergingEnabled) {
        this.requestMergingEnabled = requestMergingEnabled;
    }

    public void resetRequestMergingEnabled(boolean requestMergingEnabled) {
        if (this.requestMergingEnabled != requestMergingEnabled) {
            this.requestMergingEnabled = requestMergingEnabled;
            LOGGER.warn(String.format("Reset to %s request merging", requestMergingEnabled ? "Enable" : "Disable"));
        }
    }

    public void setValueHandlerKeepAliveTime(long valueHandlerKeepAliveTime) {
        this.valueHandlerKeepAliveTime = valueHandlerKeepAliveTime;
    }

    public void resetValueHandlerKeepAliveTime(long valueHandlerKeepAliveTime) {
        if (valueHandlerKeepAliveTime > 0L && this.valueHandlerKeepAliveTime != valueHandlerKeepAliveTime) {
            LOGGER.warn(String.format("Reset value handlers' keep alive time from %s to %s",
                this.valueHandlerKeepAliveTime, valueHandlerKeepAliveTime));
            this.valueHandlerKeepAliveTime = valueHandlerKeepAliveTime;
            ((ThreadPoolExecutor) valueFetchers).setKeepAliveTime(valueHandlerKeepAliveTime, TimeUnit.MILLISECONDS);
            ((ThreadPoolExecutor) valueUpdaters).setKeepAliveTime(valueHandlerKeepAliveTime, TimeUnit.MILLISECONDS);
            queuesLock.readLock().lock();
            try {
                notifyHandlerQueuesOnly();
            } finally {
                queuesLock.readLock().unlock();
            }
        }
    }

    public void destroy() {
        this.valid = false;
        this.active = false;
        notifyAndClearAllQueues();
        cancelAllHandlerTasks();
        shutdownThreadPools();
    }

    private void notifyAndClearAllQueues() {
        queuesLock.writeLock().lock();
        try {
            notifyAndClearHandlerQueues(true);
        } finally {
            queuesLock.writeLock().unlock();
        }
        notifyAndClearMonitorQueue();
    }

    private void notifyAndClearHandlerQueues(boolean forceClearQueues) {
        notifyHandlerQueuesOnly();
        if (forceClearQueues) {
            valueTaskQueues.clear();
            skipTaskQueues.clear();
        }
    }

    private void notifyAndClearMonitorQueue() {
        if (activeQueueIndexes != null) {
            synchronized (activeQueueIndexes) {
                activeQueueIndexes.notify();
            }
        }
        activeQueueIndexes.clear();
    }

    private void notifyHandlerQueuesOnly() {
        for (List<NewSeqValueTask> valueTaskQueue : valueTaskQueues) {
            if (valueTaskQueue != null) {
                synchronized (valueTaskQueue) {
                    valueTaskQueue.notify();
                }
            }
        }
        for (List<NewSeqSkipTask> skipTaskQueue : skipTaskQueues) {
            if (skipTaskQueue != null) {
                synchronized (skipTaskQueue) {
                    skipTaskQueue.notify();
                }
            }
        }
    }

    private void cancelAllHandlerTasks() {
        if (valueFetcherFutures != null) {
            for (Future future : valueFetcherFutures.values()) {
                future.cancel(true);
            }
            valueFetcherFutures.clear();
        }
        if (valueUpdaterFutures != null) {
            for (Future future : valueUpdaterFutures.values()) {
                future.cancel(true);
            }
            valueUpdaterFutures.clear();
        }
    }

    private void shutdownThreadPools() {
        if (valueFetchers != null) {
            try {
                valueFetchers.shutdown();
            } catch (Throwable t) {
                LOGGER.error(t);
            }
        }

        if (valueUpdaters != null) {
            try {
                valueUpdaters.shutdown();
            } catch (Throwable t) {
                LOGGER.error(t);
            }
        }

        monitorFuture.cancel(true);
        if (handlerMonitor != null) {
            try {
                handlerMonitor.shutdown();
            } catch (Throwable t) {
                LOGGER.error(t);
            }
        }
    }

    public void printF(String msg) {
        print("HANDLER-F", msg);
    }

    public void printU(String msg) {
        print("HANDLER-U", msg);
    }

    public static void print(String role, String msg) {
        StringBuilder buf = new StringBuilder();
        buf.append(role).append(" => Task-").append(role.charAt(0)).append("-").append(msg);
        LOGGER.error(buf.toString());
    }
}
