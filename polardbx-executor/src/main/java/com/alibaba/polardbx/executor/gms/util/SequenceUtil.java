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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequenceOptRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequenceRecord;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import org.apache.calcite.sql.SequenceBean;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INCREMENT_BY;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_START_WITH;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_INDEX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NA;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UPPER_LIMIT_UNIT_COUNT;

public class SequenceUtil {

    public static SystemTableRecord convert(SequenceBean sequenceBean, String tableSchema,
                                            ExecutionContext executionContext) {
        setSchemaName(sequenceBean, tableSchema, executionContext);
        switch (sequenceBean.getKind()) {
        case CREATE_SEQUENCE:
            return buildCreateRecord(sequenceBean);
        case ALTER_SEQUENCE:
            return buildAlterRecord(sequenceBean);
        case DROP_SEQUENCE:
            return buildDropRecord(sequenceBean);
        case RENAME_SEQUENCE:
            return buildRenameRecord(sequenceBean);
        default:
            throw new SequenceException("Unexpected Sequence operation: " + sequenceBean.getType());
        }
    }

    public static Pair<SystemTableRecord, SystemTableRecord> change(SequenceBean sequenceBean, String tableSchema,
                                                                    ExecutionContext executionContext) {
        setSchemaName(sequenceBean, tableSchema, executionContext);

        Type existingType =
            SequenceManagerProxy.getInstance()
                .checkIfExists(sequenceBean.getSchemaName(), sequenceBean.getSequenceName());

        Type targetType = sequenceBean.getToType();

        TddlRuntimeException badSwitch = new TddlRuntimeException(ErrorCode.ERR_GMS_UNSUPPORTED,
            "the group type switch from " + existingType.getKeyword() + " to " + targetType.getKeyword()
                + " isn't supported");

        if (existingType == targetType) {
            return null;
        }

        switch (existingType) {
        case GROUP:
            switch (targetType) {
            case SIMPLE:
                return buildRecordPair(sequenceBean, Type.GROUP, Type.SIMPLE);
            case TIME:
                return buildRecordPair(sequenceBean, Type.GROUP, Type.TIME);
            default:
                throw badSwitch;
            }
        case SIMPLE:
            switch (targetType) {
            case GROUP:
                return buildRecordPair(sequenceBean, Type.SIMPLE, Type.GROUP);
            case TIME:
                return buildRecordPair(sequenceBean, Type.SIMPLE, Type.TIME);
            default:
                throw badSwitch;
            }
        case TIME:
            switch (targetType) {
            case GROUP:
                return buildRecordPair(sequenceBean, Type.TIME, Type.GROUP);
            case SIMPLE:
                return buildRecordPair(sequenceBean, Type.TIME, Type.SIMPLE);
            default:
                throw badSwitch;
            }
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNSUPPORTED,
                "Group Type " + existingType.getKeyword() + " isn't supported yet");
        }
    }

    private static Pair<SystemTableRecord, SystemTableRecord> buildRecordPair(SequenceBean sequenceBean,
                                                                              Type deletedType, Type insertedType) {
        sequenceBean.setType(deletedType);
        SystemTableRecord deletedRecord = buildDropRecord(sequenceBean);
        sequenceBean.setType(insertedType);
        SystemTableRecord insertedRecord = buildCreateRecord(sequenceBean);
        return new Pair<>(deletedRecord, insertedRecord);
    }

    private static void setSchemaName(SequenceBean sequenceBean, String tableSchema,
                                      ExecutionContext executionContext) {
        if (TStringUtil.isBlank(sequenceBean.getSchemaName())) {
            if (TStringUtil.isNotBlank(tableSchema)) {
                sequenceBean.setSchemaName(tableSchema);
            } else {
                sequenceBean.setSchemaName(executionContext.getSchemaName());
            }
        }
    }

    private static SystemTableRecord buildCreateRecord(SequenceBean sequenceBean) {
        return buildFullRecord(sequenceBean, true);
    }

    private static SystemTableRecord buildAlterRecord(SequenceBean sequenceBean) {
        return buildFullRecord(sequenceBean, false);
    }

    private static SystemTableRecord buildDropRecord(SequenceBean sequenceBean) {
        return buildBaseRecord(sequenceBean);
    }

    private static SystemTableRecord buildRenameRecord(SequenceBean sequenceBean) {
        return buildBaseRecord(sequenceBean);
    }

    private static SystemTableRecord buildBaseRecord(SequenceBean sequenceBean) {
        Pair<SequenceRecord, SequenceOptRecord> sequence = buildRecord(sequenceBean);

        Type type = sequenceBean.getType();
        if (type == null) {
            type = SequenceManagerProxy.getInstance()
                .checkIfExists(sequenceBean.getSchemaName(), sequenceBean.getSequenceName());
        }

        switch (type) {
        case SIMPLE:
        case TIME:
            return sequence.getValue();
        case GROUP:
        default:
            return sequence.getKey();
        }
    }

    private static SystemTableRecord buildFullRecord(SequenceBean sequenceBean, boolean isNew) {
        SequenceOptRecord seqOptRecord;
        Pair<SequenceRecord, SequenceOptRecord> sequence = buildRecord(sequenceBean);

        Type type = sequenceBean.getType();
        if (type == null) {
            type = SequenceManagerProxy.getInstance()
                .checkIfExists(sequenceBean.getSchemaName(), sequenceBean.getSequenceName());
        }

        switch (type) {
        case SIMPLE:
            seqOptRecord = sequence.getValue();
            if (isNew) {
                seqOptRecord.cycle |= SequenceAttribute.CACHE_DISABLED;
            } else {
                if (sequenceBean.getStart() == null) {
                    seqOptRecord.value = 0;
                    seqOptRecord.startWith = 0;
                }
                if (sequenceBean.getIncrement() == null) {
                    seqOptRecord.incrementBy = 0;
                }
                if (sequenceBean.getMaxValue() == null) {
                    seqOptRecord.maxValue = 0;
                }
                if (sequenceBean.getCycle() == null) {
                    seqOptRecord.cycle = NA;
                }
            }
            return seqOptRecord;
        case TIME:
            seqOptRecord = sequence.getValue();
            seqOptRecord.value = 0;
            seqOptRecord.incrementBy = 0;
            seqOptRecord.startWith = 0;
            seqOptRecord.maxValue = 0;
            seqOptRecord.cycle = SequenceAttribute.TIME_BASED;
            return seqOptRecord;
        case GROUP:
        default:
            return sequence.getKey();
        }
    }

    private static Pair<SequenceRecord, SequenceOptRecord> buildRecord(SequenceBean sequenceBean) {
        SequenceRecord seqRecord = new SequenceRecord();
        SequenceOptRecord seqOptRecord = new SequenceOptRecord();

        if (sequenceBean.getSchemaName() != null) {
            seqRecord.schemaName = sequenceBean.getSchemaName();
            seqOptRecord.schemaName = sequenceBean.getSchemaName();
        }

        if (sequenceBean.getSequenceName() != null) {
            seqRecord.name = sequenceBean.getSequenceName();
            seqOptRecord.name = sequenceBean.getSequenceName();
        }

        if (sequenceBean.getNewSequenceName() != null) {
            seqRecord.newName = sequenceBean.getNewSequenceName();
            seqOptRecord.newName = sequenceBean.getNewSequenceName();
        }

        if (sequenceBean.getUnitCount() != null) {
            if (sequenceBean.getUnitCount() < DEFAULT_UNIT_COUNT
                || sequenceBean.getUnitCount() > UPPER_LIMIT_UNIT_COUNT) {
                seqRecord.unitCount = DEFAULT_UNIT_COUNT;
            } else {
                seqRecord.unitCount = sequenceBean.getUnitCount();
            }
        }

        if (sequenceBean.getUnitIndex() != null) {
            if (sequenceBean.getUnitIndex() < DEFAULT_UNIT_INDEX
                || sequenceBean.getUnitIndex() >= seqRecord.unitCount) {
                seqRecord.unitIndex = DEFAULT_UNIT_INDEX;
            } else {
                seqRecord.unitIndex = sequenceBean.getUnitIndex();
            }
        }

        if (sequenceBean.getInnerStep() != null) {
            if (sequenceBean.getInnerStep() < 1) {
                seqRecord.innerStep = DEFAULT_INNER_STEP;
            } else {
                seqRecord.innerStep = sequenceBean.getInnerStep();
            }
        }

        if (sequenceBean.getIncrement() != null) {
            if (sequenceBean.getIncrement() < DEFAULT_INCREMENT_BY || sequenceBean.getIncrement() > Integer.MAX_VALUE) {
                seqOptRecord.incrementBy = DEFAULT_INCREMENT_BY;
            } else {
                seqOptRecord.incrementBy = sequenceBean.getIncrement();
            }
        }

        if (sequenceBean.getMaxValue() != null) {
            if (sequenceBean.getMaxValue() < 1 || sequenceBean.getMaxValue() > Long.MAX_VALUE) {
                seqOptRecord.maxValue = Long.MAX_VALUE;
            } else {
                seqOptRecord.maxValue = sequenceBean.getMaxValue();
            }
        }

        if (sequenceBean.getCycle() != null) {
            seqOptRecord.cycle = sequenceBean.getCycle() ? SequenceAttribute.TRUE : SequenceAttribute.FALSE;
        }

        if (sequenceBean.getStart() != null) {
            if (sequenceBean.getStart() < 1 || sequenceBean.getStart() > Long.MAX_VALUE) {
                seqRecord.value = DEFAULT_START_WITH;
                seqOptRecord.value = DEFAULT_START_WITH;
                seqOptRecord.startWith = DEFAULT_START_WITH;
            } else {
                seqRecord.value = sequenceBean.getStart();
                seqOptRecord.value = sequenceBean.getStart();
                seqOptRecord.startWith = sequenceBean.getStart();
            }
        }

        // Recalculate start value for group sequence.
        if (seqRecord.value <= 1) {
            seqRecord.value = seqRecord.unitIndex > 0 ? seqRecord.unitIndex * seqRecord.innerStep : 0;
        }

        seqRecord.status = TableStatus.ABSENT.getValue();
        seqOptRecord.status = TableStatus.ABSENT.getValue();

        return new Pair<>(seqRecord, seqOptRecord);
    }

}
