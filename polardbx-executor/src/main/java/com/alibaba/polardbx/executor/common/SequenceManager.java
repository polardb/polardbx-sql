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

package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.sequence.Sequence;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import com.alibaba.polardbx.sequence.impl.SimpleSequence;

import java.util.Map;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.STR_NA;

public class SequenceManager extends AbstractSequenceManager {

    public final static Logger logger = LoggerFactory.getLogger(SequenceManager.class);

    private SequenceLoadFromDBManager subManager;

    public SequenceManager(SequenceLoadFromDBManager subManager) {
        this.subManager = subManager;
    }

    @Override
    public void doInit() {
    }

    @Override
    public Long nextValue(String schemaName, String seqName) {
        return nextValue(schemaName, seqName, 1);
    }

    @Override
    public Long nextValue(String schemaName, String seqName, int batchSize) {
        Sequence seq = getSequence(schemaName, seqName);
        try {
            return seq.nextValue(batchSize);
        } catch (SequenceException e) {
            if (!e.getMessage().contains("exceeds maximum value allowed")) {
                // We should invalidate cached sequence with the same name and
                // try again because cached group sequence may affect simple
                // sequence and cause exception in some rare scenario. Note
                // that we don't differentiate specific exceptions here.
                invalidate(schemaName, seqName);

                // Try again.
                seq = getSequence(schemaName, seqName);

                try {
                    return seq.nextValue(batchSize);
                } catch (SequenceException ex) {
                    // If still failed, then means a real failure occurred.
                    throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE_NEXT_VALUE, ex, seqName, ex.getMessage());
                }
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE_NEXT_VALUE, e, seqName, e.getMessage());
            }
        }
    }

    @Override
    public Long currValue(String schemaName, String seqName) {
        Sequence seq = getSequence(schemaName, seqName);
        try {
            return seq.currValue();
        } catch (SequenceException e) {
            // We should invalidate cached sequence with the same name and
            // try again because cached group sequence may affect simple
            // sequence and cause exception in some rare scenario. Note
            // that we don't differentiate specific exceptions here.
            invalidate(schemaName, seqName);

            // Try again.
            seq = getSequence(schemaName, seqName);

            try {
                return seq.currValue();
            } catch (SequenceException ex) {
                // If still failed, then means a real failure occurred.
                throw new SequenceException(ex, seqName);
            }
        }
    }

    @Override
    public void updateValue(String schemaName, String seqName, long value) {
        Sequence seq = getSequence(schemaName, seqName);
        try {
            seq.updateValue(value);
        } catch (SequenceException e) {
            // Check if the exception can be ignored for New Sequence's skip operation.
            // TODO: should remove the logic after AliSQL Sequence return a warning instead of error.
            if (TStringUtil.equalsIgnoreCase(e.getSQLState(), "HY000") && e.getErrorCode() == 7543) {
                // Updated explicit value is equal or less than currently allocated id,
                // so ignore the exception for now.
                return;
            }

            // We should invalidate cached sequence with the same name and
            // try again because cached group sequence may affect simple
            // sequence and cause exception in some rare scenario. Note
            // that we don't differentiate specific exceptions here.
            invalidate(schemaName, seqName);

            // Try again.
            seq = getSequence(schemaName, seqName);

            try {
                seq.updateValue(value);
            } catch (SequenceException ex) {
                // If still failed, then means a real failure occurred.
                throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE_NEXT_VALUE, ex, seqName, ex.getMessage());
            }
        }
    }

    @Override
    public boolean exhaustValue(String schemaName, String seqName) {
        Sequence seq = getSequence(schemaName, seqName);
        try {
            return seq.exhaustValue();
        } catch (SequenceException e) {
            // We should invalidate cached sequence with the same name and
            // try again because cached group sequence may affect simple
            // sequence and cause exception in some rare scenario. Note
            // that we don't differentiate specific exceptions here.
            invalidate(schemaName, seqName);

            // Try again.
            seq = getSequence(schemaName, seqName);

            try {
                return seq.exhaustValue();
            } catch (SequenceException ex) {
                // If still failed, then means a real failure occurred.
                throw new SequenceException(ex, seqName);
            }
        }
    }

    @Override
    public Integer getIncrement(String schemaName, String seqName) {
        Sequence seq = getSequence(schemaName, seqName);
        if (seq instanceof SimpleSequence) {
            return ((SimpleSequence) seq).getIncrementBy();
        } else {
            // For non-simple sequence, the increment is always 1.
            return SequenceAttribute.DEFAULT_INCREMENT_BY;
        }
    }

    @Override
    public Sequence getSequence(String schemaName, String seqName) {
        if (!ConfigDataMode.isMasterMode()) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPERATION_NOT_ALLOWED,
                "Sequence operations are not allowed on a Read-Only Instance");
        }

        checkSubManager();

        Sequence seq = subManager.getSequence(schemaName, seqName);

        if (seq == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_SEQUENCE, seqName);
        }

        return seq;
    }

    @Override
    public void invalidate(String schemaName, String seqName) {
        checkSubManager();
        subManager.invalidate(schemaName, seqName);
    }

    @Override
    public int invalidateAll(String schemaName) {
        checkSubManager();
        return subManager.invalidateAll(schemaName);
    }

    @Override
    public Type checkIfExists(String schemaName, String seqName) {
        checkSubManager();
        return subManager.checkIfExists(schemaName, seqName);
    }

    @Override
    public boolean isUsingSequence(String schemaName, String tableName) {
        checkSubManager();
        return subManager.isUsingSequence(schemaName, tableName);
    }

    @Override
    public String getCurrentSeqRange(String schemaName, String seqName) {
        if (!ConfigDataMode.isMasterMode()) {
            return STR_NA;
        }
        checkSubManager();
        return subManager.getCurrentSeqRange(schemaName, seqName);
    }

    @Override
    public long getMinValueFromCurrentSeqRange(String schemaName, String seqName) {
        if (!ConfigDataMode.isMasterMode()) {
            return 0L;
        }
        checkSubManager();
        return subManager.getMinValueFromCurrentSeqRange(schemaName, seqName);
    }

    @Override
    public int[] getCustomUnitArgsForGroupSeq(String schemaName) {
        checkSubManager();
        return subManager.getCustomUnitArgsForGroupSeq(schemaName);
    }

    @Override
    public void reloadConnProps(String schemaName, Map<String, Object> connProps) {
        checkSubManager();
        subManager.reloadConnProps(schemaName, connProps);
    }

    @Override
    public void resetNewSeqResources(String schemaName) {
        checkSubManager();
        subManager.resetNewSeqResources(schemaName);
    }

    @Override
    public boolean areAllSequencesSameType(String schemaName, Type seqType) {
        checkSubManager();
        return subManager.areAllSequencesSameType(schemaName, seqType);
    }

    private void checkSubManager() {
        if (this.subManager != null) {
            if (!subManager.isInited()) {
                try {
                    subManager.init();
                } catch (Exception ex) {
                    throw GeneralUtil.nestedException(ex);
                }
            }
        } else {
            // This should never occur.
            throw new SequenceException("Unexpected: Sequence DB Manager is not set.");
        }
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
        if (subManager != null) {
            subManager.destroy();
        }
    }

}
