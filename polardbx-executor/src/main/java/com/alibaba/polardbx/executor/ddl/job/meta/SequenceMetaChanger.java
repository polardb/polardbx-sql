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

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.ast.AutoIncrementType;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.validator.SequenceValidator;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequenceOptRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequenceRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.sequence.bean.DropSequence;
import com.alibaba.polardbx.optimizer.core.sequence.bean.SequenceFactory;
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

    public static SequenceBean createSequenceIfExists(String schemaName, String logicalTableName,
                                                      SequenceBean sequenceBean, TablesExtRecord tablesExtRecord,
                                                      boolean isPartitioned, boolean ifNotExists, SqlKind sqlKind,
                                                      ExecutionContext executionContext) {
        if (sequenceBean == null || !sequenceBean.isNew()) {
            return null;
        }

        boolean isNewPartitionTable = DbInfoManager.getInstance().isNewPartitionDb(schemaName);

        // Check if need default sequence type.
        if (sequenceBean.getType() == SequenceAttribute.Type.NA) {
            if (!isNewPartitionTable) {
                TableRule tableRule;
                if (tablesExtRecord != null) {
                    tableRule = DdlJobDataConverter.buildTableRule(tablesExtRecord);
                } else {
                    tableRule = OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(logicalTableName);
                }
                if (tableRule != null && (isPartitioned || tableRule.isBroadcast())) {
                    sequenceBean.setType(AutoIncrementType.GROUP);
                } else {
                    return null;
                }
            } else {
                sequenceBean.setType(AutoIncrementType.GROUP);
            }
        }

        String seqName = AUTO_SEQ_PREFIX + logicalTableName;

        SequenceAttribute.Type existingSeqType = SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);
        if (existingSeqType != SequenceAttribute.Type.NA) {
            boolean allowForCreateTable = sqlKind == SqlKind.CREATE_TABLE && ifNotExists;
            boolean allowForAlterTable = sqlKind == SqlKind.ALTER_TABLE;
            if (allowForCreateTable || allowForAlterTable) {
                return null;
            }

            if (compareSequence(schemaName, seqName, sequenceBean)) {
                // The sequence has been created, so ignore it.
                return null;
            }

            // Warn user since the sequence already exists.
            StringBuilder errMsg = new StringBuilder();
            errMsg.append(existingSeqType.getKeyword()).append(" SEQUENCE '");
            errMsg.append(seqName).append("' already exists. ");
            errMsg.append("Please try another name to create new sequence or alter an existing sequence instead.");
            throw new SequenceException(errMsg.toString());
        }

        // Use START WITH 1 by default when there is no table option
        // AUTO_INCREMENT = xx specified.
        if (sequenceBean.getStart() == null) {
            sequenceBean.setStart(DEFAULT_START_WITH);
        }

        sequenceBean.setKind(SqlKind.CREATE_SEQUENCE);

        sequenceBean.setSequenceName(seqName);

        SequenceValidator.validateSimpleSequence(sequenceBean, executionContext);

        return sequenceBean;
    }

    private static boolean compareSequence(String schemaName, String seqName, SequenceBean sequenceBean) {
        SystemTableRecord existingSequenceRecord;

        SequencesAccessor sequencesAccessor = new SequencesAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            sequencesAccessor.setConnection(metaDbConn);
            existingSequenceRecord = sequencesAccessor.query(schemaName, seqName);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            sequencesAccessor.setConnection(null);
        }

        if (existingSequenceRecord == null || sequenceBean == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNEXPECTED, "invalid sequence record or input");
        }

        handleDefaultSequenceParams(sequenceBean);

        if (existingSequenceRecord instanceof SequenceRecord) {
            SequenceRecord sequenceRecord = (SequenceRecord) existingSequenceRecord;
            return sequenceRecord.unitCount == sequenceBean.getUnitCount() &&
                sequenceRecord.unitIndex == sequenceBean.getUnitIndex() &&
                sequenceRecord.innerStep == sequenceBean.getInnerStep();
        } else if (existingSequenceRecord instanceof SequenceOptRecord) {
            SequenceOptRecord sequenceOptRecord = (SequenceOptRecord) existingSequenceRecord;
            return sequenceOptRecord.incrementBy == sequenceBean.getIncrement() &&
                sequenceOptRecord.startWith == sequenceBean.getStart() &&
                sequenceOptRecord.maxValue == sequenceBean.getMaxValue() &&
                sequenceOptRecord.cycle == (sequenceBean.getCycle() ? 1 : 0);
        } else {
            return false;
        }
    }

    private static void handleDefaultSequenceParams(SequenceBean sequenceBean) {
        if (sequenceBean.getUnitCount() != null) {
            if (sequenceBean.getUnitCount() < DEFAULT_UNIT_COUNT ||
                sequenceBean.getUnitCount() > UPPER_LIMIT_UNIT_COUNT) {
                sequenceBean.setUnitCount(DEFAULT_UNIT_COUNT);
            }
        }

        if (sequenceBean.getUnitIndex() != null) {
            if (sequenceBean.getUnitIndex() < DEFAULT_UNIT_INDEX ||
                sequenceBean.getUnitIndex() >= sequenceBean.getUnitCount()) {
                sequenceBean.setUnitIndex(DEFAULT_UNIT_INDEX);
            }
        }

        if (sequenceBean.getInnerStep() != null) {
            if (sequenceBean.getInnerStep() < 1) {
                sequenceBean.setInnerStep(DEFAULT_INNER_STEP);
            }
        }

        if (sequenceBean.getIncrement() != null) {
            if (sequenceBean.getIncrement() < DEFAULT_INCREMENT_BY ||
                sequenceBean.getIncrement() > Integer.MAX_VALUE) {
                sequenceBean.setIncrement(DEFAULT_INCREMENT_BY);
            }
        }

        if (sequenceBean.getStart() != null) {
            if (sequenceBean.getStart() < 1 || sequenceBean.getStart() > Long.MAX_VALUE) {
                sequenceBean.setStart(DEFAULT_START_WITH);
            }
        }

        if (sequenceBean.getMaxValue() != null) {
            if (sequenceBean.getMaxValue() < 1 || sequenceBean.getMaxValue() > Long.MAX_VALUE) {
                sequenceBean.setMaxValue(Long.MAX_VALUE);
            }
        }
    }

    protected static SequenceBean alterSequenceIfExists(String schemaName, String logicalTableName,
                                                        SequenceBean sequenceBean, TablesExtRecord tablesExtRecord,
                                                        boolean isPartitioned, boolean ifNotExists, SqlKind sqlKind,
                                                        ExecutionContext executionContext) {
        // Check if any new AUTO_INCREMENT column exists and create
        // the corresponding sequence if any.
        createSequenceIfExists(schemaName, logicalTableName, sequenceBean, tablesExtRecord, isPartitioned, ifNotExists,
            sqlKind, executionContext);

        String seqName = AUTO_SEQ_PREFIX + logicalTableName;

        if (sequenceBean != null && sequenceBean.isNew()) {
            if (sequenceBean.getKind() == null && sqlKind == SqlKind.ALTER_TABLE) {
                sequenceBean.setSequenceName(seqName);
                sequenceBean.setKind(SqlKind.ALTER_SEQUENCE);
            }
            return sequenceBean;
        }

        // If no AUTO_INCREMENT column added and table options
        // exist, then check if table option AUTO_INCREMENT exists
        // and alter the existing sequence accordingly.

        SequenceAttribute.Type existingType = SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);

        if (sequenceBean != null && !sequenceBean.isNew() && existingType != SequenceAttribute.Type.TIME) {
            // Get table option AUTO_INCREMENT = xxx.
            final Long start = sequenceBean.getStart();
            if (start != null && start >= 0) {
                // If it's a single table, maybe there's no sequence.
                if (SequenceManagerProxy.getInstance().isUsingSequence(schemaName, logicalTableName)) {
                    SequenceValidator.validateSimpleSequence(sequenceBean, executionContext);

                    sequenceBean.setSchemaName(schemaName);
                    sequenceBean.setSequenceName(seqName);
                    if (sequenceBean.getInnerStep() == null) {
                        sequenceBean.setInnerStep(DEFAULT_INNER_STEP);
                    }
                    if (sequenceBean.getToType() == null) {
                        sequenceBean.setToType(SequenceAttribute.Type.NA);
                    }
                    sequenceBean.setKind(SqlKind.ALTER_SEQUENCE);
                    return sequenceBean;
                }
            }
        }

        return null;
    }

    public static SequenceBean renameSequenceIfExists(String schemaName, String logicalTableName,
                                                      String newLogicalTableName) {
        String seqName = AUTO_SEQ_PREFIX + logicalTableName;
        String newSeqName = AUTO_SEQ_PREFIX + newLogicalTableName;
        SequenceAttribute.Type existingType = SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);
        if (existingType != SequenceAttribute.Type.NA) {
            SequenceBean sequenceBean = new SequenceBean();
            sequenceBean.setSchemaName(schemaName);
            sequenceBean.setSequenceName(seqName);
            sequenceBean.setNewSequenceName(newSeqName);
            sequenceBean.setKind(SqlKind.RENAME_SEQUENCE);
            return sequenceBean;
        }
        return null;
    }

    public static SequenceBean dropSequenceIfExists(String schemaName, String logicalTableName) {
        String seqName = AUTO_SEQ_PREFIX + logicalTableName;
        DropSequence dropSequence = SequenceFactory.getDropSequence(schemaName, seqName, true);
        if (dropSequence != null) {
            SequenceBean sequenceBean = new SequenceBean();
            sequenceBean.setSchemaName(schemaName);
            sequenceBean.setSequenceName(seqName);
            sequenceBean.setKind(SqlKind.DROP_SEQUENCE);
            return sequenceBean;
        }
        return null;
    }

}
