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

package com.alibaba.polardbx.executor.ddl.job.meta;

import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.ast.AutoIncrementType;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.validator.SequenceValidator;
import com.alibaba.polardbx.gms.metadb.seq.SequenceBaseRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequenceOptRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequenceRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.sql.SQLException;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INCREMENT_BY;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_START_WITH;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_INDEX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UPPER_LIMIT_UNIT_COUNT;

public class SequenceMetaChanger {

    public static boolean createSequenceIfExists(String schemaName, String logicalTableName,
                                                 SequenceBean sequence, TablesExtRecord tablesExtRecord,
                                                 boolean isPartitioned, boolean ifNotExists, SqlKind sqlKind,
                                                 ExecutionContext executionContext) {
        if (sequence == null || !sequence.isNew()) {
            return false;
        }

        boolean isNewPartitionTable = DbInfoManager.getInstance().isNewPartitionDb(schemaName);

        // Check if it needs default sequence type.
        if (sequence.getType() == Type.NA) {
            if (!isNewPartitionTable) {
                TableRule tableRule;
                if (tablesExtRecord != null) {
                    tableRule = DdlJobDataConverter.buildTableRule(tablesExtRecord);
                } else {
                    tableRule = OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(logicalTableName);
                }
                if (tableRule != null && (isPartitioned || tableRule.isBroadcast())) {
                    sequence.setType(AutoIncrementType.GROUP);
                } else {
                    return false;
                }
            } else {
                sequence.setType(AutoIncrementType.NEW);
            }
        }

        String seqName = AUTO_SEQ_PREFIX + logicalTableName;

        Type existingSeqType = SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);
        if (existingSeqType != Type.NA) {
            boolean allowForCreateTable = sqlKind == SqlKind.CREATE_TABLE && ifNotExists;
            boolean allowForAlterTable = sqlKind == SqlKind.ALTER_TABLE;
            if (allowForCreateTable || allowForAlterTable) {
                return false;
            }

            if (compareSequence(schemaName, seqName, sequence)) {
                // The sequence has been created, so ignore it.
                return false;
            }

            // Warn user since the sequence already exists.
            StringBuilder errMsg = new StringBuilder();
            errMsg.append(existingSeqType).append(" SEQUENCE '");
            errMsg.append(seqName).append("' already exists. ");
            errMsg.append("Please try another name to create new sequence or alter an existing sequence instead.");
            throw new SequenceException(errMsg.toString());
        }

        // Use START WITH 1 by default when there is no table option
        // AUTO_INCREMENT = xx specified.
        if (sequence.getStart() == null) {
            sequence.setStart(DEFAULT_START_WITH);
        }

        sequence.setKind(SqlKind.CREATE_SEQUENCE);

        sequence.setName(seqName);

        SequenceValidator.validate(sequence, executionContext);

        return true;
    }

    private static boolean compareSequence(String schemaName, String seqName, SequenceBean sequence) {
        SequenceBaseRecord existingSequenceRecord;

        SequencesAccessor sequencesAccessor = new SequencesAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            sequencesAccessor.setConnection(metaDbConn);
            existingSequenceRecord = sequencesAccessor.query(schemaName, seqName);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            sequencesAccessor.setConnection(null);
        }

        if (existingSequenceRecord == null || sequence == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNEXPECTED, "invalid sequence record or input");
        }

        handleDefaultSequenceParams(sequence);

        if (existingSequenceRecord instanceof SequenceRecord) {
            SequenceRecord sequenceRecord = (SequenceRecord) existingSequenceRecord;
            return sequence.getUnitCount() != null && sequenceRecord.unitCount == sequence.getUnitCount()
                && sequence.getUnitIndex() != null && sequenceRecord.unitIndex == sequence.getUnitIndex()
                && sequence.getInnerStep() != null && sequenceRecord.innerStep == sequence.getInnerStep();
        } else if (existingSequenceRecord instanceof SequenceOptRecord) {
            SequenceOptRecord sequenceOptRecord = (SequenceOptRecord) existingSequenceRecord;
            return sequence.getIncrement() != null && sequenceOptRecord.incrementBy == sequence.getIncrement()
                && sequence.getStart() != null && sequenceOptRecord.startWith == sequence.getStart()
                && sequence.getMaxValue() != null && sequenceOptRecord.maxValue == sequence.getMaxValue()
                && sequence.getCycle() != null && sequenceOptRecord.cycle == (sequence.getCycle() ? 1 : 0);
        } else {
            return false;
        }
    }

    private static void handleDefaultSequenceParams(SequenceBean sequence) {
        if (sequence.getUnitCount() != null) {
            if (sequence.getUnitCount() < DEFAULT_UNIT_COUNT ||
                sequence.getUnitCount() > UPPER_LIMIT_UNIT_COUNT) {
                sequence.setUnitCount(DEFAULT_UNIT_COUNT);
            }
        }

        if (sequence.getUnitIndex() != null) {
            if (sequence.getUnitIndex() < DEFAULT_UNIT_INDEX ||
                sequence.getUnitIndex() >= sequence.getUnitCount()) {
                sequence.setUnitIndex(DEFAULT_UNIT_INDEX);
            }
        }

        if (sequence.getInnerStep() != null) {
            if (sequence.getInnerStep() < 1) {
                sequence.setInnerStep(DEFAULT_INNER_STEP);
            }
        }

        if (sequence.getIncrement() != null) {
            if (sequence.getIncrement() < DEFAULT_INCREMENT_BY ||
                sequence.getIncrement() > Integer.MAX_VALUE) {
                sequence.setIncrement(DEFAULT_INCREMENT_BY);
            }
        }

        if (sequence.getStart() != null) {
            if (sequence.getStart() < 1 || sequence.getStart() > Long.MAX_VALUE) {
                sequence.setStart(DEFAULT_START_WITH);
            }
        }

        if (sequence.getMaxValue() != null) {
            if (sequence.getMaxValue() < 1 || sequence.getMaxValue() > Long.MAX_VALUE) {
                sequence.setMaxValue(Long.MAX_VALUE);
            }
        }
    }

    protected static SequenceBean alterSequenceIfExists(String schemaName, String logicalTableName,
                                                        SequenceBean sequence, TablesExtRecord tablesExtRecord,
                                                        boolean isPartitioned, boolean ifNotExists, SqlKind sqlKind,
                                                        ExecutionContext executionContext) {
        // Check if any new AUTO_INCREMENT column exists and create
        // the corresponding sequence if any.
        boolean needToCreate =
            createSequenceIfExists(schemaName, logicalTableName, sequence, tablesExtRecord, isPartitioned, ifNotExists,
                sqlKind, executionContext);

        if (needToCreate) {
            if (sequence.getKind() == null && sqlKind == SqlKind.ALTER_TABLE) {
                sequence.setKind(SqlKind.ALTER_SEQUENCE);
            }
            return sequence;
        }

        // If no AUTO_INCREMENT column added and table options
        // exist, then check if table option AUTO_INCREMENT exists
        // and alter the existing sequence accordingly.

        String seqName = AUTO_SEQ_PREFIX + logicalTableName;

        Type existingType = SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);
        boolean seqTypeIgnored = existingType == Type.NA || existingType == Type.TIME;

        if (sequence != null && !sequence.isNew() && !seqTypeIgnored) {
            // Get table option AUTO_INCREMENT = xxx.
            if (sequence.getStart() != null && sequence.getStart() >= 0) {
                sequence.setSchemaName(schemaName);
                sequence.setName(seqName);

                if (sequence.getInnerStep() == null) {
                    sequence.setInnerStep(DEFAULT_INNER_STEP);
                }

                if (sequence.getToType() == null) {
                    sequence.setToType(Type.NA);
                }

                sequence.setKind(SqlKind.ALTER_SEQUENCE);

                SequenceValidator.validate(sequence, executionContext);

                return sequence;
            }
        }

        return null;
    }

    public static SequenceBean renameSequenceIfExists(String schemaName, String logicalTableName,
                                                      String newLogicalTableName) {
        String seqName = AUTO_SEQ_PREFIX + logicalTableName;
        String newSeqName = AUTO_SEQ_PREFIX + newLogicalTableName;
        Type existingType = SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);
        if (existingType != Type.NA) {
            SequenceBean sequence = new SequenceBean();
            sequence.setSchemaName(schemaName);
            sequence.setName(seqName);
            sequence.setNewName(newSeqName);
            sequence.setKind(SqlKind.RENAME_SEQUENCE);
            return sequence;
        }
        return null;
    }

    public static SequenceBean dropSequenceIfExists(String schemaName, String logicalTableName) {
        String seqName = AUTO_SEQ_PREFIX + logicalTableName;
        Type existingType = SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);
        if (existingType != Type.NA) {
            SequenceBean sequence = new SequenceBean();
            sequence.setSchemaName(schemaName);
            sequence.setName(seqName);
            sequence.setKind(SqlKind.DROP_SEQUENCE);
            return sequence;
        }
        return null;
    }

}
