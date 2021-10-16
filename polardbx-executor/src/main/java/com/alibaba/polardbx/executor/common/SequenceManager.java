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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.sequence.Sequence;
import com.alibaba.polardbx.sequence.SequenceDao;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import com.alibaba.polardbx.sequence.impl.GroupSequence;
import com.alibaba.polardbx.sequence.impl.SimpleSequence;
import com.alibaba.polardbx.sequence.impl.SimpleSequenceDao;

import java.util.Map;
import java.util.TreeMap;

public class SequenceManager extends AbstractSequenceManager {

    public final static Logger logger = LoggerFactory.getLogger(SequenceManager.class);
    private Map<String, Sequence> sequences = new TreeMap<String, Sequence>(
        String.CASE_INSENSITIVE_ORDER);
    private SequenceLoadFromDBManager subManager = null;

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
        if (seq == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_SEQUENCE, seqName);
        }
        try {
            if (batchSize > 1) {
                return seq.nextValue(batchSize);
            } else {
                return seq.nextValue();
            }
        } catch (SequenceException e) {
            if (!e.getMessage().contains("exceeds maximum value allowed")) {
                // We should invalidate cached sequence with the same name and
                // try again because cached group sequence may affect simple
                // sequence and cause exception in some rare scenario. Note
                // that we don't differentiate specific exceptions here.
                invalidate(schemaName, seqName);
                // Try again.
                seq = getSequence(schemaName, seqName);
                if (seq == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_MISS_SEQUENCE, seqName);
                }
                try {
                    if (batchSize > 1) {
                        return seq.nextValue(batchSize);
                    } else {
                        return seq.nextValue();
                    }
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
    public boolean exhaustValue(String schemaName, String seqName) {
        Sequence seq = getSequence(schemaName, seqName);
        if (seq == null) {
            return false;
        }
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
            if (seq == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_MISS_SEQUENCE, seqName);
            }
            try {
                return seq.exhaustValue();
            } catch (SequenceException ex) {
                // If still failed, then means a real failure occurred.
                throw new SequenceException(ex, seqName);
            }
        }
    }

    @Override
    public void updateValue(String schemaName, String seqName, long value) {
        Sequence seq = getSequence(schemaName, seqName);
        if (seq == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_SEQUENCE, seqName);
        }
        try {
            if (seq instanceof SimpleSequence) {
                ((SimpleSequence) seq).updateValue(value);
            } else if (seq instanceof GroupSequence) {
                ((GroupSequence) seq).updateValue(value);
            }
        } catch (SequenceException e) {
            // We should invalidate cached sequence with the same name and
            // try again because cached group sequence may affect simple
            // sequence and cause exception in some rare scenario. Note
            // that we don't differentiate specific exceptions here.
            invalidate(schemaName, seqName);
            // Try again.
            seq = getSequence(schemaName, seqName);
            if (seq == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_MISS_SEQUENCE, seqName);
            }
            try {
                if (seq instanceof SimpleSequence) {
                    ((SimpleSequence) seq).updateValue(value);
                } else if (seq instanceof GroupSequence) {
                    ((GroupSequence) seq).updateValue(value);
                }
            } catch (SequenceException ex) {
                // If still failed, then means a real failure occurred.
                throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE_NEXT_VALUE, ex, seqName, ex.getMessage());
            }
        }
    }

    @Override
    public Integer getIncrement(String schemaName, String seqName) {
        Sequence seq = getSequence(schemaName, seqName);
        if (seq == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_SEQUENCE, seqName);
        }
        if (seq instanceof SimpleSequence) {
            SequenceDao seqDao = ((SimpleSequence) seq).getSequenceDao();
            return ((SimpleSequenceDao) seqDao).getIncrementBy(seqName);
        } else {
            // For non-simple sequence, the increment is always 1.
            return SequenceAttribute.DEFAULT_INCREMENT_BY;
        }
    }

    @Override
    public void invalidate(String schemaName, String seqName) {
        Sequence seq = this.sequences.get(seqName);
        if (seq != null) {
            // Exhaust cached values if the sequence comes from sequence file.
            seq.exhaustValue();
        } else if (this.subManager != null) {
            // Invalidate cached sequence.
            if (!subManager.isInited()) {
                try {
                    subManager.init();
                } catch (Exception ex) {
                    throw GeneralUtil.nestedException(ex);
                }
            }
            subManager.invalidate(schemaName, seqName);
        }
    }

    @Override
    public int invalidateAll(String schemaName) {
        int size = this.sequences.size();

        // Exhaust all cached sequence values
        for (Sequence seq : sequences.values()) {
            seq.exhaustValue();
        }

        if (subManager != null) {
            // Invalidate all cached sequence objects
            if (!subManager.isInited()) {
                try {
                    subManager.init();
                } catch (Exception ex) {
                    throw GeneralUtil.nestedException(ex);
                }
            }
            size += subManager.invalidateAll(schemaName);
        }

        return size;
    }

    @Override
    public void validateDependence(String schemaName) {
        if (this.subManager != null) {
            if (!subManager.isInited()) {
                try {
                    subManager.init();
                } catch (Exception ex) {
                    throw GeneralUtil.nestedException(ex);
                }
            }
            subManager.validateDependence(schemaName);
        }
    }

    @Override
    public Type checkIfExists(String schemaName, String seqName) {
        // Check sequences cached from sequence file on diamond
        Sequence seq = this.sequences.get(seqName);
        if (seq != null) {
            // We should not attempt to fetch sequence info from database any more if the
            // sequence is defined in sequence file.
            // Sequence file is a legacy use and only support Group Sequence.
            return Type.GROUP;
        } else {
            // Check sequence existence and type against database
            checkSubManager();
            return subManager.checkIfExists(schemaName, seqName);
        }
    }

    @Override
    public boolean isUsingSequence(String schemaName, String tableName) {
        String seqName = SequenceAttribute.AUTO_SEQ_PREFIX + tableName;
        Sequence seq = this.sequences.get(seqName);
        if (seq != null) {
            return true;
        }

        if (this.subManager != null) {
            if (!subManager.isInited()) {
                try {
                    subManager.init();
                } catch (Exception ex) {
                    throw GeneralUtil.nestedException(ex);
                }
            }
            return subManager.isUsingSequence(schemaName, tableName);
        }

        // This should never occur.
        throw new SequenceException("Sequence DB Manager is not set.");
    }

    @Override
    public String getCurrentSeqRange(String schemaName, String seqName) {
        checkSubManager();
        return subManager.getCurrentSeqRange(schemaName, seqName);
    }

    @Override
    public long getMinValueFromCurrentSeqRange(String schemaName, String seqName) {
        checkSubManager();
        return subManager.getMinValueFromCurrentSeqRange(schemaName, seqName);
    }

    @Override
    public boolean isCustomUnitGroupSeqSupported(String schemaName) {
        checkSubManager();
        return subManager.isCustomUnitGroupSeqSupported(schemaName);
    }

    @Override
    public int[] getCustomUnitArgsForGroupSeq(String schemaName) {
        checkSubManager();
        return subManager.getCustomUnitArgsForGroupSeq(schemaName);
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
            throw new SequenceException("Sequence DB Manager is not set.");
        }
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
        destroySequence();
        if (subManager != null) {
            subManager.destroy();
        }
    }

    private void destroySequence() {
        for (Sequence sequence : sequences.values()) {
            if (sequence instanceof SimpleSequence) {
                SequenceDao sequenceDao = ((SimpleSequence) sequence).getSequenceDao();
                sequenceDao.destroy();
            }
            if (sequence instanceof GroupSequence) {
                SequenceDao sequenceDao = ((GroupSequence) sequence).getSequenceDao();
                sequenceDao.destroy();
            }
        }
        sequences.clear();
    }

    @Override
    public Sequence getSequence(String schemaName, String seqName) {
        if (!ConfigDataMode.isMasterMode()) {
            throw new SequenceException("Sequence operations are not allowed in non-master node");
        }

        Sequence seq = this.sequences.get(seqName);
        if (seq == null) {
            if (this.subManager != null) {
                // 如果老的sequence没有,才会走到sub sequence
                if (!subManager.isInited()) {
                    try {
                        subManager.init();
                    } catch (Exception ex) {
                        throw GeneralUtil.nestedException(ex);
                    }
                }
                seq = subManager.getSequence(schemaName, seqName);
            }
        }
        return seq;
    }

    @Override
    public boolean areAllSequencesSameType(String schemaName, Type seqType) {
        if (this.subManager != null) {
            if (!subManager.isInited()) {
                try {
                    subManager.init();
                } catch (Exception ex) {
                    throw GeneralUtil.nestedException(ex);
                }
            }
            return subManager.areAllSequencesSameType(schemaName, seqType);
        }
        return false;
    }
}
