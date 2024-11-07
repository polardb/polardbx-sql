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

package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.metadb.seq.SequenceBaseRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequenceOptRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequenceRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.function.Supplier;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INCREMENT_BY;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_START_WITH;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_INDEX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UPPER_LIMIT_UNIT_COUNT;

public class SequenceUtil {

    public static SequenceBaseRecord convert(SequenceBean sequence, String tableSchema,
                                             ExecutionContext executionContext) {
        setSchemaName(sequence, tableSchema, executionContext);
        switch (sequence.getKind()) {
        case CREATE_SEQUENCE:
            return buildCreateRecord(sequence);
        case ALTER_SEQUENCE:
            return buildAlterRecord(sequence);
        case RENAME_SEQUENCE:
            return buildRenameRecord(sequence);
        case DROP_SEQUENCE:
            return buildDropRecord(sequence);
        default:
            throw new SequenceException("Unexpected operation: " + sequence.getKind());
        }
    }

    public static void resetSequence4TruncateTable(String schema, String table, Connection metaDbConn,
                                                   ExecutionContext executionContext) {
        SequencesAccessor sequencesAccessor = new SequencesAccessor();
        sequencesAccessor.setConnection(metaDbConn);
        final String sequenceName = AUTO_SEQ_PREFIX + table;
        final SequenceBean sequenceBean = new SequenceBean();
        sequenceBean.setStart(1L);
        sequenceBean.setToType(Type.NA);
        sequenceBean.setKind(SqlKind.ALTER_SEQUENCE);
        sequenceBean.setSchemaName(schema);
        sequenceBean.setName(sequenceName);

        long newSeqCacheSize = executionContext.getParamManager().getLong(ConnectionParams.NEW_SEQ_CACHE_SIZE);
        newSeqCacheSize = newSeqCacheSize < 1 ? 0 : newSeqCacheSize;

        SequenceBaseRecord record = SequenceUtil.convert(sequenceBean, null, executionContext);

        Type existingType = SequenceManagerProxy.getInstance().checkIfExists(schema, sequenceName);
        if (existingType != Type.TIME) {
            sequencesAccessor.update(record, newSeqCacheSize);
        }
    }

    public static Pair<SequenceBaseRecord, SequenceBaseRecord> change(SequenceBean sequence, String tableSchema,
                                                                      ExecutionContext executionContext) {
        setSchemaName(sequence, tableSchema, executionContext);

        Type existingType =
            SequenceManagerProxy.getInstance().checkIfExists(sequence.getSchemaName(), sequence.getName());

        Type targetType = sequence.getToType();

        TddlRuntimeException badSwitch = new TddlRuntimeException(ErrorCode.ERR_SEQUENCE,
            "the sequence type change from " + existingType + " to " + targetType + " isn't supported");

        if (existingType == targetType) {
            return null;
        }

        switch (existingType) {
        case NEW:
            switch (targetType) {
            case GROUP:
                return buildRecordPair(sequence, Type.NEW, Type.GROUP);
            case SIMPLE:
                return buildRecordPair(sequence, Type.NEW, Type.SIMPLE);
            case TIME:
                return buildRecordPair(sequence, Type.NEW, Type.TIME);
            default:
                throw badSwitch;
            }
        case GROUP:
            switch (targetType) {
            case NEW:
                return buildRecordPair(sequence, Type.GROUP, Type.NEW);
            case SIMPLE:
                return buildRecordPair(sequence, Type.GROUP, Type.SIMPLE);
            case TIME:
                return buildRecordPair(sequence, Type.GROUP, Type.TIME);
            default:
                throw badSwitch;
            }
        case SIMPLE:
            switch (targetType) {
            case NEW:
                return buildRecordPair(sequence, Type.SIMPLE, Type.NEW);
            case GROUP:
                return buildRecordPair(sequence, Type.SIMPLE, Type.GROUP);
            case TIME:
                return buildRecordPair(sequence, Type.SIMPLE, Type.TIME);
            default:
                throw badSwitch;
            }
        case TIME:
            switch (targetType) {
            case NEW:
                return buildRecordPair(sequence, Type.TIME, Type.NEW);
            case GROUP:
                return buildRecordPair(sequence, Type.TIME, Type.GROUP);
            case SIMPLE:
                return buildRecordPair(sequence, Type.TIME, Type.SIMPLE);
            default:
                throw badSwitch;
            }
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE,
                "Sequence Type Change for " + existingType + " isn't supported yet");
        }
    }

    public static Supplier<?> buildFailPointInjector(ExecutionContext executionContext) {
        return () -> {
            FailPoint.injectFromHint(FailPointKey.FP_NEW_SEQ_EXCEPTION_RIGHT_AFTER_PHY_CREATION, executionContext,
                () -> FailPoint.injectException(FailPointKey.FP_NEW_SEQ_EXCEPTION_RIGHT_AFTER_PHY_CREATION)
            );
            return null;
        };
    }

    private static Pair<SequenceBaseRecord, SequenceBaseRecord> buildRecordPair(SequenceBean sequence,
                                                                                Type deletedType, Type insertedType) {
        sequence.setType(deletedType);
        SequenceBaseRecord deletedRecord = buildDropRecord(sequence);
        sequence.setType(insertedType);
        SequenceBaseRecord insertedRecord = buildCreateRecord(sequence);
        return new Pair<>(deletedRecord, insertedRecord);
    }

    private static SequenceBaseRecord buildCreateRecord(SequenceBean sequence) {
        return buildFullRecord(sequence, true);
    }

    private static SequenceBaseRecord buildAlterRecord(SequenceBean sequence) {
        return buildFullRecord(sequence, false);
    }

    private static SequenceBaseRecord buildRenameRecord(SequenceBean sequence) {
        return buildBasicRecord(sequence);
    }

    private static SequenceBaseRecord buildDropRecord(SequenceBean sequence) {
        return buildBasicRecord(sequence);
    }

    private static SequenceBaseRecord buildBasicRecord(SequenceBean sequence) {
        Type type = sequence.getType();
        if (type == null) {
            type = SequenceManagerProxy.getInstance().checkIfExists(sequence.getSchemaName(), sequence.getName());
        }
        switch (type) {
        case NEW:
            SequenceOptRecord newRecord = buildSequenceOptRecord(sequence);
            newRecord.cycle = SequenceAttribute.NEW_SEQ;
            return newRecord;
        case SIMPLE:
        case TIME:
            return buildSequenceOptRecord(sequence);
        case GROUP:
        default:
            return buildSequenceRecord(sequence);
        }
    }

    private static SequenceBaseRecord buildFullRecord(SequenceBean sequence, boolean isNew) {
        Type type = sequence.getType();
        if (type == null) {
            type = SequenceManagerProxy.getInstance().checkIfExists(sequence.getSchemaName(), sequence.getName());
        }
        switch (type) {
        case NEW:
        case SIMPLE:
            SequenceOptRecord newRecord = buildSequenceOptRecord(sequence);
            if (type == Type.NEW) {
                newRecord.cycle |= SequenceAttribute.NEW_SEQ;
            }
            if (!isNew) {
                if (sequence.getStart() == null) {
                    newRecord.value = 0;
                    newRecord.startWith = 0;
                }
                if (sequence.getIncrement() == null) {
                    newRecord.incrementBy = 0;
                }
                if (sequence.getMaxValue() == null) {
                    newRecord.maxValue = 0;
                }
                if (sequence.getCycle() == null) {
                    newRecord.cycleReset = false;
                }
            }
            return newRecord;
        case TIME:
            SequenceOptRecord timeRecord = buildSequenceOptRecord(sequence);
            timeRecord.value = 0;
            timeRecord.incrementBy = 0;
            timeRecord.startWith = 0;
            timeRecord.maxValue = 0;
            timeRecord.cycle = SequenceAttribute.TIME_BASED;
            return timeRecord;
        case GROUP:
        default:
            return buildSequenceRecord(sequence);
        }
    }

    private static SequenceRecord buildSequenceRecord(SequenceBean sequence) {
        SequenceRecord seqRecord = new SequenceRecord();

        buildCommonInfo(seqRecord, sequence);

        if (sequence.getUnitCount() != null) {
            if (sequence.getUnitCount() < DEFAULT_UNIT_COUNT
                || sequence.getUnitCount() > UPPER_LIMIT_UNIT_COUNT) {
                seqRecord.unitCount = DEFAULT_UNIT_COUNT;
            } else {
                seqRecord.unitCount = sequence.getUnitCount();
            }
        }

        if (sequence.getUnitIndex() != null) {
            if (sequence.getUnitIndex() < DEFAULT_UNIT_INDEX
                || sequence.getUnitIndex() >= seqRecord.unitCount) {
                seqRecord.unitIndex = DEFAULT_UNIT_INDEX;
            } else {
                seqRecord.unitIndex = sequence.getUnitIndex();
            }
        }

        if (sequence.getInnerStep() != null) {
            if (sequence.getInnerStep() < 1) {
                seqRecord.innerStep = DEFAULT_INNER_STEP;
            } else {
                seqRecord.innerStep = sequence.getInnerStep();
            }
        }

        if (sequence.getStart() != null) {
            if (sequence.getStart() < 1 || sequence.getStart() > Long.MAX_VALUE) {
                seqRecord.value = DEFAULT_START_WITH;
            } else {
                seqRecord.value = sequence.getStart();
            }
        }

        // Recalculate start value for group sequence.
        if (seqRecord.value <= 1) {
            seqRecord.value = seqRecord.unitIndex > 0 ? seqRecord.unitIndex * seqRecord.innerStep : 0;
        }

        seqRecord.status = TableStatus.ABSENT.getValue();

        return seqRecord;
    }

    private static SequenceOptRecord buildSequenceOptRecord(SequenceBean sequence) {
        SequenceOptRecord seqOptRecord = new SequenceOptRecord();

        buildCommonInfo(seqOptRecord, sequence);

        if (sequence.getIncrement() != null) {
            if (sequence.getIncrement() < DEFAULT_INCREMENT_BY || sequence.getIncrement() > Integer.MAX_VALUE) {
                seqOptRecord.incrementBy = DEFAULT_INCREMENT_BY;
            } else {
                seqOptRecord.incrementBy = sequence.getIncrement();
            }
        }

        if (sequence.getMaxValue() != null) {
            if (sequence.getMaxValue() < 1 || sequence.getMaxValue() > Long.MAX_VALUE) {
                seqOptRecord.maxValue = Long.MAX_VALUE;
            } else {
                seqOptRecord.maxValue = sequence.getMaxValue();
            }
        }

        if (sequence.getCycle() != null) {
            seqOptRecord.cycle = sequence.getCycle() ? SequenceAttribute.TRUE : SequenceAttribute.FALSE;
        }

        if (sequence.getStart() != null) {
            if (sequence.getStart() < 1 || sequence.getStart() > Long.MAX_VALUE) {
                seqOptRecord.value = DEFAULT_START_WITH;
                seqOptRecord.startWith = DEFAULT_START_WITH;
            } else {
                seqOptRecord.value = sequence.getStart();
                seqOptRecord.startWith = sequence.getStart();
            }
        }

        seqOptRecord.status = TableStatus.ABSENT.getValue();

        return seqOptRecord;
    }

    private static void buildCommonInfo(SequenceBaseRecord record, SequenceBean sequence) {
        if (sequence.getSchemaName() != null) {
            record.schemaName = sequence.getSchemaName();
        }

        if (sequence.getName() != null) {
            record.name = sequence.getName();
        }

        if (sequence.getNewName() != null) {
            record.newName = sequence.getNewName();
        }
    }

    private static void setSchemaName(SequenceBean sequence, String tableSchema,
                                      ExecutionContext executionContext) {
        if (TStringUtil.isBlank(sequence.getSchemaName())) {
            sequence.setSchemaName(
                TStringUtil.isBlank(tableSchema) ? executionContext.getSchemaName() : tableSchema);
        }
    }

}
