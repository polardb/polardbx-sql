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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.sequence.exception.SequenceException;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INCREMENT_BY;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_MAX_VALUE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_START_WITH;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NOCYCLE;

/**
 * A simple implementation for global sequence with no cache.
 *
 * @author chensr 2016/04/12 11:14:55
 * @since 5.0.0
 */
public class SimpleSequence extends BaseSequence {

    private static final Logger logger = LoggerFactory.getLogger(SimpleSequence.class);

    private SimpleSequenceDao simpleSequenceDao;

    private volatile boolean initialized = false;

    private int incrementBy = DEFAULT_INCREMENT_BY;
    private long startWith = DEFAULT_START_WITH;
    private long maxValue = DEFAULT_MAX_VALUE;
    private int cycle = NOCYCLE;

    public void init() throws TddlRuntimeException {
        if (initialized) {
            return;
        }

        this.type = Type.SIMPLE;

        if (!simpleSequenceDao.isInited()) {
            simpleSequenceDao.init();
        }

        TddlRuntimeException ex = null;
        int retryTimes = simpleSequenceDao.getRetryTimes();
        for (int i = 1; i <= retryTimes; i++) {
            try {
                simpleSequenceDao.validate(this);
                ex = null;
                initialized = true;
                break;
            } catch (TddlRuntimeException e) {
                ex = e;
                logger.warn("Warning: already failed to initialize sequence '" + name + "' " + i + " time(s)!");
            }
        }
        if (ex != null) {
            logger.error("Still failed to initialize sequence '" + name + "' after total " + retryTimes + " attempts!",
                ex);
            throw ex;
        }
    }

    @Override
    public long nextValue() throws SequenceException {
        return nextValue(1);
    }

    @Override
    public long nextValue(int size) throws SequenceException {
        if (name == null) {
            throw new SequenceException("The sequence name cannot be null!");
        }
        long value = simpleSequenceDao.nextValue(name, size);
        currentValue = value;
        return value;
    }

    @Override
    public boolean exhaustValue() throws SequenceException {
        // Always return true since there is no cache with the solution.
        return true;
    }

    @Override
    public void updateValue(long value) throws SequenceException {
        if (name == null) {
            throw new SequenceException("The sequence name cannot be null!");
        }
        // Update sequence value directly.
        simpleSequenceDao.updateValue(name, value, maxValue);
    }

    public SimpleSequenceDao getSimpleSequenceDao() {
        return simpleSequenceDao;
    }

    public void setSimpleSequenceDao(SimpleSequenceDao simpleSequenceDao) {
        this.simpleSequenceDao = simpleSequenceDao;
    }

    public int getIncrementBy() {
        return incrementBy;
    }

    public void setIncrementBy(int incrementBy) {
        if (incrementBy <= 0 || incrementBy > Integer.MAX_VALUE) {
            incrementBy = 1;
        }
        this.incrementBy = incrementBy;
    }

    public long getStartWith() {
        return startWith;
    }

    public void setStartWith(long startWith) {
        if (startWith <= 0 || startWith > Long.MAX_VALUE) {
            startWith = 1;
        }
        this.startWith = startWith;
    }

    public long getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(long maxValue) {
        if (maxValue <= 0 || maxValue > Long.MAX_VALUE) {
            maxValue = Long.MAX_VALUE;
        }
        this.maxValue = maxValue;
    }

    public int getCycle() {
        return cycle;
    }

    public void setCycle(int cycle) {
        this.cycle = cycle;
    }

}
