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

import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import com.alibaba.polardbx.sequence.impl.NewSequenceScheduler.NewSeqSkipTask;
import com.alibaba.polardbx.sequence.impl.NewSequenceScheduler.NewSeqValueTask;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class NewSequence extends FunctionalSequence {

    private final NewSequenceDao newSequenceDao;
    private final NewSequenceScheduler newSequenceScheduler;
    private final List<Pair<Integer, Boolean>> activeQueueIndexes;

    private boolean groupingEnabled = true;

    private final static int MAX_RETRY_TIMES_ON_SAME_QUEUE = 3;
    private final static int REQUEST_COUNTER_THRESHOLD = Integer.MAX_VALUE;
    private final AtomicInteger requestCounter = new AtomicInteger(0);

    public NewSequence(String name, NewSequenceDao newSequenceDao, NewSequenceScheduler newSequenceScheduler) {
        this.type = Type.NEW;
        this.name = name;
        this.newSequenceDao = newSequenceDao;
        this.newSequenceScheduler = newSequenceScheduler;
        this.activeQueueIndexes = newSequenceScheduler != null ? newSequenceScheduler.getActiveQueueIndexes() :
            Lists.newArrayList();
    }

    public void init() throws TddlRuntimeException {
        if (!newSequenceDao.isInited()) {
            newSequenceDao.init();
        }
        newSequenceDao.validate(this);
    }

    @Override
    public long nextValue() throws SequenceException {
        long value = groupingEnabled ? nextValueGrouping() : newSequenceDao.nextValue(name);
        currentValue = value;
        return value;
    }

    @Override
    public long nextValue(int size) throws SequenceException {
        long value = size <= 1 ? nextValue() : newSequenceDao.nextValue(name, size);
        currentValue = value;
        return value;
    }

    private long nextValueGrouping() {
        final NewSeqValueTask valueTask = new NewSeqValueTask(name);

        Pair<Integer, List<NewSeqValueTask>> queueInfo = getTaskQueue(true);

        synchronized (activeQueueIndexes) {
            activeQueueIndexes.add(Pair.of(queueInfo.getKey(), Boolean.TRUE));
            activeQueueIndexes.notify();
        }

        List<NewSeqValueTask> valueTaskQueue = queueInfo.getValue();
        synchronized (valueTaskQueue) {
            valueTaskQueue.add(valueTask);
            valueTaskQueue.notify();
        }

        try {
            return valueTask.waitForValue();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public void updateValue(long value) throws SequenceException {
        if (value < 1L) {
            return;
        }
        if (groupingEnabled) {
            updateValueGrouping(value);
        } else {
            newSequenceDao.updateValue(name, value);
        }
    }

    private void updateValueGrouping(long value) {
        final NewSeqSkipTask skipTask = new NewSeqSkipTask(name, value);

        Pair<Integer, List<NewSeqSkipTask>> queueInfo = getTaskQueue(false);

        synchronized (activeQueueIndexes) {
            activeQueueIndexes.add(Pair.of(queueInfo.getKey(), Boolean.FALSE));
            activeQueueIndexes.notify();
        }

        List<NewSeqSkipTask> skipTaskQueue = queueInfo.getValue();
        synchronized (skipTaskQueue) {
            skipTaskQueue.add(skipTask);
            skipTaskQueue.notify();
        }

        try {
            skipTask.waitForDone();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private <T> Pair<Integer, List<T>> getTaskQueue(boolean isNextval) {
        List<T> taskQueue = null;

        final int NUM_ALL_QUEUES = newSequenceScheduler.getTaskQueueNum();

        int queueIndex = assignQueueIndex(NUM_ALL_QUEUES);

        int numQueuesRetried = 0;
        while (numQueuesRetried < NUM_ALL_QUEUES) {

            int retryTimesOnSameQueue = 0;
            while (retryTimesOnSameQueue < MAX_RETRY_TIMES_ON_SAME_QUEUE) {

                if (isNextval) {
                    taskQueue = (List<T>) newSequenceScheduler.getValueTaskQueue(queueIndex);
                } else {
                    taskQueue = (List<T>) newSequenceScheduler.getSkipTaskQueue(queueIndex);
                }

                if (taskQueue != null) {
                    break;
                }

                retryTimesOnSameQueue++;
            }

            if (taskQueue != null) {
                break;
            }

            numQueuesRetried++;
            queueIndex = assignQueueIndex(NUM_ALL_QUEUES);
        }

        if (taskQueue != null) {
            return Pair.of(queueIndex, taskQueue);
        } else {
            throw new SequenceException("Unexpected: still get a null value task queue after all retries");
        }
    }

    private int assignQueueIndex(final int numTaskQueue) {
        int queueIndex = 0;
        if (numTaskQueue > 1) {
            requestCounter.compareAndSet(REQUEST_COUNTER_THRESHOLD, 0);
            queueIndex = requestCounter.getAndIncrement() % numTaskQueue;
        }
        return queueIndex;
    }

    @Override
    public boolean exhaustValue() throws SequenceException {
        return true;
    }

    public void setGroupingEnabled(boolean groupingEnabled) {
        this.groupingEnabled = groupingEnabled;
    }

    public void printN(String msg) {
        NewSequenceScheduler.print("NEW_SEQ", msg);
    }
}
