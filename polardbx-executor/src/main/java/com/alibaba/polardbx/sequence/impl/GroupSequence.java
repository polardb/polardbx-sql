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
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.sequence.SequenceRange;
import com.alibaba.polardbx.sequence.exception.SequenceException;

import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INNER_STEP;

public class GroupSequence extends BaseSequence {

    protected static final Logger logger = LoggerFactory.getLogger(GroupSequence.class);

    private final Lock lock = new ReentrantLock();
    private final Lock updateLock = new ReentrantLock();

    protected GroupSequenceDao groupSequenceDao;
    protected volatile SequenceRange currentRange;

    /**
     * The value that is candidate and will probably be updated
     */
    private volatile long valueToUpdate = DEFAULT_INNER_STEP;

    public void init() throws SequenceException, SQLException {
        this.type = Type.GROUP;

        Exception ex = null;
        synchronized (this) {
            for (int i = 0; i < this.groupSequenceDao.getRetryTimes(); i++) {
                try {
                    groupSequenceDao.adjust(name);
                    ex = null;
                    break;
                } catch (Exception e) {
                    ex = e;
                    String errMsg =
                        String.format("Failed to initialize Group Sequence %s after %sst retry. Caused by: %s", name,
                            i + 1, e.getMessage());
                    logger.error(errMsg, e);
                }
            }
        }

        if (ex != null) {
            String errMsg =
                String.format("Failed initialize Group Sequence %s after all %s retries. Caused by: %s", name,
                    this.groupSequenceDao.getRetryTimes(), ex.getMessage());
            logger.error(errMsg, ex);
            throw new SequenceException(ex, errMsg);
        }
    }

    @Override
    public long nextValue() throws SequenceException {
        checkSequenceRange();
        long value = getSequenceRange().getAndIncrement();
        if (value == -1) {
            lock.lock();
            try {
                for (; ; ) {
                    if (getSequenceRange().isOver()) {
                        setSequenceRange();
                    }

                    value = getSequenceRange().getAndIncrement();
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
            throw new SequenceException(String.format("Sequence value %s overflows", value));
        }

        currentValue = value;
        return value;
    }

    @Override
    public long nextValue(int size) throws SequenceException {
        checkBatchSize(size);

        checkSequenceRange();

        long value = getSequenceRange().getBatch(size);

        if (value == -1) {
            lock.lock();
            try {
                for (; ; ) {
                    if (getSequenceRange().isOver()) {
                        setSequenceRange();
                    }

                    value = getSequenceRange().getBatch(size);
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
            throw new SequenceException(String.format("Sequence value %s overflows", value));
        }

        currentValue = value;
        return value;
    }

    @Override
    public boolean exhaustValue() throws SequenceException {
        lock.lock();
        try {
            SequenceRange range = getSequenceRange();
            if (range != null) {
                range.setOver(true);
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    @Override
    public void updateValue(long value) {
        updateLock.lock();
        try {
            if (value > valueToUpdate) {
                // We just record the value for regular task to check and update.
                valueToUpdate = value;
            }
        } finally {
            updateLock.unlock();
        }
    }

    public boolean needToUpdateValue() {
        updateLock.lock();
        try {
            return valueToUpdate > DEFAULT_INNER_STEP;
        } finally {
            updateLock.unlock();
        }
    }

    public boolean updateValueRegularly(long minValueInAllRanges) {
        updateLock.lock();
        try {
            if (valueToUpdate > DEFAULT_INNER_STEP) {
                return updateValueInternal(minValueInAllRanges);
            }
            return false;
        } finally {
            updateLock.unlock();
        }
    }

    private boolean updateValueInternal(long minValueInAllRanges) {
        boolean needSync = false;
        String schemaName = groupSequenceDao.getSchemaName();

        // currentAndMax[0]: the current value of the current range
        // currentAndMax[1]: the maximum value of the current range
        long[] currentAndMax = getCurrentAndMax();

        StringBuilder infoMsg = new StringBuilder();
        infoMsg.append("Update explicit insert value ").append(valueToUpdate);
        infoMsg.append(" for ").append(schemaName).append(".").append(name).append(" [");
        infoMsg.append(currentAndMax[0]).append(",").append(currentAndMax[1]).append("]");

        // If valueToUpdate is less than minimum value in all ranges, then do nothing.
        if (valueToUpdate > currentAndMax[1]) {
            // The user value exceeds the max value of current range. Let's get the range
            // containing the value to catch up with.
            boolean updated = groupSequenceDao.updateValue(name, valueToUpdate);
            // Invalidate local range
            if (getSequenceRange() != null) {
                getSequenceRange().setOver(true);
            }
            // Need sync to reload cached range on each node.
            needSync = true;
            infoMsg.append(" in database (updated=").append(updated).append("). Then SYNC");
            LoggerInit.TDDL_SEQUENCE_LOG.info(infoMsg.toString());
        } else if (valueToUpdate >= currentAndMax[0] && valueToUpdate <= currentAndMax[1]) {
            // The user value is within current range. Let's reset current value.
            boolean successful = getSequenceRange().updateValue(currentAndMax[0], valueToUpdate + 1);
            while (!successful) {
                currentAndMax = getCurrentAndMax();
                if (valueToUpdate > currentAndMax[0] && valueToUpdate <= currentAndMax[1]) {
                    successful = getSequenceRange().updateValue(currentAndMax[0], valueToUpdate + 1);
                } else {
                    successful = true;
                }
            }
            infoMsg.append(" in range [").append(currentAndMax[0]).append(",").append(currentAndMax[1]).append("]");
            LoggerInit.TDDL_SEQUENCE_LOG.info(infoMsg.toString());
        } else if (valueToUpdate >= minValueInAllRanges) {
            // The user value may be in less range on other nodes, so we still need sync to
            // avoid conflict, but we don't have to update anything.
            infoMsg.append(" without change. Then SYNC (minValueInAllRanges=").append(minValueInAllRanges).append(")");
            LoggerInit.TDDL_SEQUENCE_LOG.info(infoMsg.toString());
            needSync = true;
        }

        // Reset for next round.
        valueToUpdate = DEFAULT_INNER_STEP;

        return needSync;
    }

    public long[] getCurrentAndMax() throws SequenceException {

        checkSequenceRange();

        long[] currentAndMax = getSequenceRange().getCurrentAndMax();
        if (currentAndMax == null) {
            lock.lock();
            try {
                for (; ; ) {
                    if (getSequenceRange().isOver()) {
                        setSequenceRange();
                    }

                    currentAndMax = getSequenceRange().getCurrentAndMax();
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
            throw new SequenceException(String.format("Sequence value %s overflows", currentAndMax[0]));
        }

        return currentAndMax;
    }

    protected void checkSequenceRange() {
        if (getSequenceRange() == null) {
            lock.lock();
            try {
                if (getSequenceRange() == null) {
                    setSequenceRange();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private SequenceRange getSequenceRange() {
        return this.currentRange;
    }

    private void setSequenceRange(SequenceRange range) {
        this.currentRange = range;
    }

    protected void setSequenceRange() {
        setSequenceRange(groupSequenceDao.nextRange(name));
    }

    protected void checkBatchSize(int size) {
        if (size > this.groupSequenceDao.getStep()) {
            throw new SequenceException(
                String.format("Batch size %s > sequence step %s. Please change batch size", size,
                    this.groupSequenceDao.getStep()));
        }
    }

    public void setGroupSequenceDao(GroupSequenceDao groupSequenceDao) {
        this.groupSequenceDao = groupSequenceDao;
    }

}
