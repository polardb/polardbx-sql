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

import com.alibaba.druid.util.StringUtils;
import com.alibaba.polardbx.sequence.SequenceDao;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

/**
 * A simple implementation for global sequence with no cache.
 *
 * @author chensr 2016年4月12日 上午11:14:55
 * @since 5.0.0
 */
public class SimpleSequence extends BaseSequence {

    private static final Logger logger = LoggerFactory.getLogger(SimpleSequence.class);

    private SimpleSequenceDao sequenceDao;

    private volatile boolean isInited = false;

    public void init() throws TddlRuntimeException {
        if (isInited) {
            return;
        }
        this.type = Type.SIMPLE;
        if (StringUtils.isEmpty(name)) {
            throw new SequenceException("Please correctly set the sequence name!");
        }
        if (sequenceDao == null || !(sequenceDao instanceof SimpleSequenceDao)) {
            throw new SequenceException("Please correctly set up the sequenceDao!");
        }
        if (!sequenceDao.isInited()) {
            sequenceDao.init();
        }
        TddlRuntimeException ex = null;
        int retryTimes = sequenceDao.getRetryTimes();
        for (int i = 1; i <= retryTimes; i++) {
            try {
                sequenceDao.validate(name);
                ex = null;
                isInited = true;
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
        return sequenceDao.nextValue(name, size);
    }

    @Override
    public boolean exhaustValue() throws SequenceException {
        // Always return true since there is no cache with the solution.
        return true;
    }

    public void updateValue(long value) throws SequenceException {
        if (name == null) {
            throw new SequenceException("The sequence name cannot be null!");
        }
        // Update sequence value directly.
        sequenceDao.updateValue(name, value);
    }

    public SequenceDao getSequenceDao() {
        return sequenceDao;
    }

    public void setSequenceDao(SequenceDao sequenceDao) {
        this.sequenceDao = (SimpleSequenceDao) sequenceDao;
    }

    public boolean isInited() {
        return isInited;
    }

    public String toString() {
        return "Sequence Name: " + name + ", " + sequenceDao.getConfigStr();
    }

}
