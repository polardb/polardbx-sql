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
import com.alibaba.polardbx.sequence.SequenceDao;
import com.alibaba.polardbx.sequence.SequenceRange;
import com.alibaba.polardbx.sequence.exception.SequenceException;

import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INNER_STEP;

public class GroupSequence extends BaseSequence {

    private final Lock lock = new ReentrantLock();
    protected SequenceDao sequenceDao;

    protected volatile SequenceRange currentRange;

    private final Lock updateLock = new ReentrantLock();

    /**
     * The value that is candidate and will probably be updated
     */
    private volatile long valueToUpdate = DEFAULT_INNER_STEP;

    protected static final Logger logger = LoggerFactory.getLogger(GroupSequence.class);

    /**
     * 初始化一下，如果name不存在，则给其初始值<br>
     */
    public void init() throws SequenceException, SQLException {
        this.type = Type.GROUP;
        if (!(sequenceDao instanceof GroupSequenceDao)) {
            throw new SequenceException("please use  GroupSequenceDao!");
        }
        GroupSequenceDao groupSequenceDao = (GroupSequenceDao) sequenceDao;
        Exception ex = null;
        synchronized (this) {
            for (int i = 0; i < sequenceDao.getRetryTimes(); i++) {
                try {
                    groupSequenceDao.adjust(name);
                    ex = null;
                    break;
                } catch (Exception e) {
                    ex = e;
                    logger.error("Sequence第" + (i + 1) + "次初始化失败, name:" + name, e);

                }
            }
        }

        if (ex != null) {
            logger.error("Sequence初始化失败，切重试" + sequenceDao.getRetryTimes() + "次后，仍然失败! name:" + name, ex);
            throw new SequenceException(ex, ex.getMessage());
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
            throw new SequenceException("Sequence value overflow, value = " + value);
        }

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
            throw new SequenceException("Sequence value overflow, value = " + value);
        }

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
        String schemaName = ((GroupSequenceDao) sequenceDao).getSchemaName();

        // currentAndMax[0]: the current value of the current range
        // currentAndMax[1]: the maximum value of the current range
        long[] currentAndMax = getCurrentAndMax();

        StringBuilder infoMsg = new StringBuilder();
        infoMsg.append("Update explicit insert value ").append(valueToUpdate).append(" for sequence '").append(name);

        // If valueToUpdate is less than minimum value in all ranges, then do nothing.
        if (valueToUpdate > currentAndMax[1]) {
            // The user value exceeds the max value of current range. Let's get the range
            // containing the value to catch up with.
            boolean updated = ((GroupSequenceDao) sequenceDao).updateExplicitValue(name, valueToUpdate);
            // Invalidate local range
            if (getSequenceRange() != null) {
                getSequenceRange().setOver(true);
            }
            if (updated) {
                infoMsg.append("[").append(currentAndMax[0]).append(",").append(currentAndMax[1]);
                infoMsg.append("]' in database in ").append(schemaName);
                LoggerInit.TDDL_SEQUENCE_LOG.info(infoMsg.toString());
            }
            // Need sync to reload cached range on each node.
            needSync = true;
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
            if (successful) {
                infoMsg.append("[").append(currentAndMax[0]).append(",").append(currentAndMax[1]);
                infoMsg.append("]' in current range in ").append(schemaName);
                LoggerInit.TDDL_SEQUENCE_LOG.info(infoMsg.toString());
            }
        } else if (valueToUpdate >= minValueInAllRanges) {
            // The user value may be in less range on other nodes, so we still need sync to
            // avoid conflict, but we don't have to update anything.
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
            throw new SequenceException("Sequence value overflow, value = " + currentAndMax[0]);
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
        setSequenceRange(sequenceDao.nextRange(name));
    }

    protected void checkBatchSize(int size) {
        if (size > this.getSequenceDao().getStep()) {
            throw new SequenceException("batch size " + size + " > sequence step " + this.getSequenceDao().getStep()
                + ", please change batch size");
        }
    }

    public SequenceDao getSequenceDao() {
        return sequenceDao;
    }

    public void setSequenceDao(SequenceDao sequenceDao) {
        this.sequenceDao = sequenceDao;
    }

}
