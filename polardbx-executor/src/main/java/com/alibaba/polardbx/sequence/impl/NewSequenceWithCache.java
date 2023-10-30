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

import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.sequence.SequenceRangePlus;
import com.alibaba.polardbx.sequence.exception.SequenceException;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NewSequenceWithCache extends NewSequence {

    private final Lock lock = new ReentrantLock();

    private volatile SequenceRangePlus currentRange;

    private int cacheSize;

    public NewSequenceWithCache(String name, int cacheSize, NewSequenceDao newSequenceDao,
                                NewSequenceScheduler newSequenceScheduler) {
        super(name, newSequenceDao, newSequenceScheduler);
        this.cacheSize = cacheSize;
    }

    @Override
    public long nextValue() throws SequenceException {
        checkSequenceRange();
        long value = currentRange.getAndIncrement();
        if (value == -1) {
            lock.lock();
            try {
                while (true) {
                    if (currentRange.isOver()) {
                        setSequenceRange();
                    }

                    value = currentRange.getAndIncrement();
                    if (value == -1) {
                        continue;
                    }

                    break;
                }
            } finally {
                lock.unlock();
            }
        }

        if (value < 0) {
            throw new SequenceException(String.format("New Sequence value %s overflows", value));
        }

        currentValue = value;
        return value;
    }

    @Override
    public long nextValue(int size) throws SequenceException {
        checkBatchSize(size);

        checkSequenceRange();

        long value = currentRange.getBatch(size);

        if (value == -1) {
            lock.lock();
            try {
                for (; ; ) {
                    if (currentRange.isOver()) {
                        setSequenceRange();
                    }

                    value = currentRange.getBatch(size);
                    if (value == -1) {
                        continue;
                    }

                    break;
                }
            } finally {
                lock.unlock();
            }
        }

        if (value < 0) {
            throw new SequenceException(String.format("New Sequence value %s overflows", value));
        }

        currentValue = value;
        return value;
    }

    @Override
    public boolean exhaustValue() throws SequenceException {
        lock.lock();
        try {
            if (currentRange != null) {
                currentRange.setOver(true);
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    public long[] getCurrentAndMax() throws SequenceException {

        checkSequenceRange();

        long[] currentAndMax = currentRange.getCurrentAndMax();
        if (currentAndMax == null) {
            lock.lock();
            try {
                for (; ; ) {
                    if (currentRange.isOver()) {
                        setSequenceRange();
                    }

                    currentAndMax = currentRange.getCurrentAndMax();
                    if (currentAndMax == null) {
                        continue;
                    }

                    break;
                }
            } finally {
                lock.unlock();
            }
        }

        if (currentAndMax[0] < 0) {
            throw new SequenceException(String.format("New Sequence value %s overflows", currentAndMax[0]));
        }

        return currentAndMax;
    }

    protected void checkSequenceRange() {
        if (currentRange == null) {
            lock.lock();
            try {
                if (currentRange == null) {
                    setSequenceRange();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    protected void setSequenceRange() {
        long maxValueInRange = super.nextValue(cacheSize);
        long minValueInRange = maxValueInRange - cacheSize * incrementBy + incrementBy;
        currentRange = new SequenceRangePlus(minValueInRange, maxValueInRange, incrementBy);
        String infoMsg = String.format("Got a new range for New Sequence %s. Range: %s", name, currentRange);
        LoggerInit.TDDL_SEQUENCE_LOG.info(infoMsg);
    }

    protected void checkBatchSize(int size) {
        if (size > cacheSize) {
            throw new SequenceException(
                String.format("Batch size %s > New Sequence cache size %s on CN. Please reduce batch size", size,
                    cacheSize));
        }
    }

}
