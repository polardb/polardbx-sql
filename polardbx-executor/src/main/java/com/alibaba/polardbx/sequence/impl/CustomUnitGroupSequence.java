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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.sequence.SequenceRange;
import com.alibaba.polardbx.sequence.exception.SequenceException;

import java.sql.SQLException;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.Type.GROUP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UNDEFINED_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UNDEFINED_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UNDEFINED_UNIT_INDEX;

public class CustomUnitGroupSequence extends GroupSequence {

    protected static final Logger logger = LoggerFactory.getLogger(CustomUnitGroupSequence.class);

    private int unitCount = UNDEFINED_UNIT_COUNT;
    private int unitIndex = UNDEFINED_UNIT_INDEX;
    private int innerStep = UNDEFINED_INNER_STEP;

    @Override
    public void init() throws SequenceException, SQLException {
        this.type = GROUP;

        CustomUnitGroupSequenceDao groupSequenceDao = (CustomUnitGroupSequenceDao) this.groupSequenceDao;

        Exception ex = null;
        synchronized (this) {
            for (int i = 0; i < this.groupSequenceDao.getRetryTimes(); i++) {
                try {
                    int[] unitArgs = groupSequenceDao.adjustPlus(name);

                    if (unitArgs.length != 3) {
                        throw new SequenceException("Unexpected unit arguments [" + unitArgs + "].");
                    }

                    unitCount = unitArgs[0];
                    unitIndex = unitArgs[1];
                    innerStep = unitArgs[2];

                    ex = null;
                    break;
                } catch (Exception e) {
                    ex = e;
                    logger.error("The " + (i + 1) + (i == 0 ? "st" : i == 1 ? "nd" : "th")
                            + " initialization failed for sequence '" + name + "'.",
                        e);
                }
            }
        }

        if (ex != null) {
            logger.error("Failed to initialize sequence '" + name + "' after retrying all "
                + this.groupSequenceDao.getRetryTimes() + " times.");
            throw new SequenceException(ex, ex.getMessage());
        }
    }

    @Override
    protected void setSequenceRange() {
        CustomUnitGroupSequenceDao groupSequenceDao = (CustomUnitGroupSequenceDao) this.groupSequenceDao;

        long rangeStart = groupSequenceDao.nextRangeStart(name);

        this.currentRange = new SequenceRange(rangeStart + 1, rangeStart + innerStep);

        String infoMsg =
            String.format("Got a new range for custom unit group sequence %s. Range Info: %s", name, currentRange);
        LoggerInit.TDDL_SEQUENCE_LOG.info(infoMsg);
    }

    @Override
    protected void checkBatchSize(int size) {
        if (size > innerStep) {
            throw new SequenceException(
                String.format("Batch size %s > sequence step %s. Please change batch size", size, innerStep));
        }
    }

}
