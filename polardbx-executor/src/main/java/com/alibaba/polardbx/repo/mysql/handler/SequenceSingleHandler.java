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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.validator.SequenceValidator;
import com.alibaba.polardbx.executor.ddl.sync.ClearPlanCacheSyncAction;
import com.alibaba.polardbx.executor.gms.util.SequenceUtil;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.SequenceSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.sequence.bean.AlterSequence;
import com.alibaba.polardbx.optimizer.core.sequence.bean.CreateSequence;
import com.alibaba.polardbx.optimizer.core.sequence.bean.SequenceFactory;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyDdlTableCursor;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public class SequenceSingleHandler extends HandlerCommon {
    public SequenceSingleHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode relNode, ExecutionContext executionContext) {
        if (relNode instanceof PhyDdlTableOperation) {
            final PhyDdlTableOperation tableOperation = (PhyDdlTableOperation) relNode;
            final SequenceBean sequence = tableOperation.getSequence();

            sequence.setKind(tableOperation.getKind());

            String ret = ((PhyDdlTableOperation) relNode).getSchemaName();
            String schemaName = StringUtils.isEmpty(sequence.getSchemaName()) ? ret : sequence.getSchemaName();

            SequenceValidator.validate(sequence, executionContext);

            Cursor cursor = handleGMS(sequence, executionContext);

            LoggerInit.TDDL_SEQUENCE_LOG
                .info("Sequence operation was successful, the operation is " + tableOperation.getKind().name());

            SyncManagerHelper.sync(new SequenceSyncAction(schemaName, sequence.getSequenceName()), schemaName,
                SyncScope.CURRENT_ONLY);

            clearPlanCache(schemaName, sequence.getSequenceName(), tableOperation);

            return cursor;
        } else {
            throw new SequenceException("Unexpected Sequence DDL operation");
        }

    }

    private Cursor handleGMS(SequenceBean sequenceBean, ExecutionContext executionContext) {
        int affectedRows = 0;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            SequencesAccessor sequencesAccessor = new SequencesAccessor();
            sequencesAccessor.setConnection(metaDbConn);

            SystemTableRecord record = SequenceUtil.convert(sequenceBean, null, executionContext);

            String seqSchema = sequenceBean.getSchemaName();

            switch (sequenceBean.getKind()) {
            case CREATE_SEQUENCE:
                generateCreate(sequenceBean, seqSchema);
                affectedRows = sequencesAccessor.insert(record);
                break;
            case ALTER_SEQUENCE:
                boolean alterWithoutChange = true;
                generateAlter(sequenceBean, sequenceBean.getSequenceName());
                Type existingType =
                    SequenceManagerProxy.getInstance().checkIfExists(seqSchema, sequenceBean.getSequenceName());
                if (sequenceBean.getToType() != null && sequenceBean.getToType() != Type.NA) {
                    Pair<SystemTableRecord, SystemTableRecord> recordPair =
                        SequenceUtil.change(sequenceBean, null, executionContext);
                    if (recordPair != null) {
                        affectedRows = sequencesAccessor.change(recordPair, seqSchema);
                        alterWithoutChange = false;
                    }
                }
                if (alterWithoutChange && existingType != Type.TIME) {
                    affectedRows = sequencesAccessor.update(record);
                }
                break;
            case DROP_SEQUENCE:
                if (!ConfigDataMode.isSupportDropAutoSeq()
                    && TStringUtil.startsWithIgnoreCase(seqSchema, AUTO_SEQ_PREFIX)) {
                    throw new SequenceException(
                        "A sequence associated with a table is not allowed to be dropped separately");
                }
                affectedRows = sequencesAccessor.delete(record);
                break;
            case RENAME_SEQUENCE:
                affectedRows = sequencesAccessor.rename(record);
                break;
            default:
                throw new SequenceException("Unexpected Sequence operation: " + sequenceBean.getType());
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
        return new AffectRowCursor(new int[] {affectedRows});
    }

    private String generateCreate(SequenceBean seqAttr, String schemaName) {
        String name = seqAttr.getSequenceName();
        String ret = StringUtils.isEmpty(seqAttr.getSchemaName()) ? schemaName : seqAttr.getSchemaName();
        try {
            CreateSequence seq = SequenceFactory.getCreateSequence(seqAttr.getType(), name, ret);
            if (seqAttr.getInnerStep() != null) {
                seq.setInnerStep(seqAttr.getInnerStep());
            }

            if (seqAttr.getStart() != null) {
                seq.setStart(seqAttr.getStart());
            }

            if (seqAttr.getIncrement() != null) {
                seq.setIncrement(seqAttr.getIncrement());
            }

            if (seqAttr.getMaxValue() != null) {
                seq.setMaxValue(seqAttr.getMaxValue());
            }

            if (seqAttr.getUnitCount() != null) {
                seq.setUnitCount(seqAttr.getUnitCount());
            }

            if (seqAttr.getUnitIndex() != null) {
                seq.setUnitIndex(seqAttr.getUnitIndex());
            }

            if (seqAttr.getInnerStep() != null) {
                seq.setInnerStep(seqAttr.getInnerStep());
            }

            if (seqAttr.getCycle() != null) {
                if (seqAttr.getCycle()) {
                    seq.setCycle(SequenceAttribute.TRUE);
                } else {
                    seq.setCycle(SequenceAttribute.FALSE);
                }
            }

            Type existingSeqType = SequenceManagerProxy.getInstance()
                .checkIfExists(seqAttr.getSchemaName(), seq.getName());
            if (existingSeqType == Type.NA) {
                return seq.getSql();
            } else {
                // Warn user since the sequence already exists.
                StringBuilder errMsg = new StringBuilder();
                errMsg.append(existingSeqType.getKeyword()).append(" SEQUENCE '");
                errMsg.append(seq.getName()).append("' already exists. ");
                errMsg.append(
                    "Please try another name to create new sequence or alter an existing sequence instead.");
                throw new SequenceException(errMsg.toString());
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private String generateAlter(SequenceBean seqAttr, String schemaName) {
        String name = seqAttr.getSequenceName();
        try {
            String ret = StringUtils.isEmpty(seqAttr.getSchemaName()) ? schemaName : seqAttr.getSchemaName();
            AlterSequence seq = SequenceFactory.getAlterSequence(ret, seqAttr.getToType(), name);

            if (seqAttr.getToType() != null && seqAttr.getToType() != Type.NA) {
                seq.setType(seqAttr.getToType());
            }

            if (seqAttr.getStart() != null) {
                seq.setStart(seqAttr.getStart());
            }

            if (seqAttr.getIncrement() != null) {
                seq.setIncrement(seqAttr.getIncrement());
            }

            if (seqAttr.getMaxValue() != null) {
                seq.setMaxValue(seqAttr.getMaxValue());
            }

            if (seqAttr.getCycle() != null) {
                if (seqAttr.getCycle()) {
                    seq.setCycle(SequenceAttribute.TRUE);
                } else {
                    seq.setCycle(SequenceAttribute.FALSE);
                }
            }

            SequenceAttribute.Type existingSeqType = SequenceManagerProxy.getInstance()
                .checkIfExists(seq.getSchemaName(), seq.getName());
            if (existingSeqType != Type.NA) {
                LoggerInit.TDDL_SEQUENCE_LOG
                    .info(existingSeqType + " sequence '" + seq.getName() + "' has been changed"
                        + (existingSeqType != seq.getType() ? " (type to " + seq.getType() + ")" : ""));
                return seq.getSql(existingSeqType);
            } else {
                // Type.NA, i.e. such a sequence doesn't exist at all.
                // Warn user not to alter a non-existent sequence.
                StringBuilder errMsg = new StringBuilder();
                errMsg.append("SEQUENCE '").append(seq.getName()).append("' doesn't exist. ");
                errMsg.append("Please alter an existing sequence.");
                throw new SequenceException(errMsg.toString());
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private Cursor execute(PhyDdlTableOperation relNode, ExecutionContext executionContext) {
        // Execute the SQL statement and return the number of affected rows.
        Cursor cursor = repo.getCursorFactory().repoCursor(executionContext, relNode);
        int affectRows = ((MyPhyDdlTableCursor) cursor).getAffectedRows();
        return new AffectRowCursor(new int[] {affectRows});
    }

    private void clearPlanCache(String schemaName, String seqName, PhyDdlTableOperation operation) {
        if (TStringUtil.startsWithIgnoreCase(seqName, AUTO_SEQ_PREFIX) &&
            operation.getKind() == SqlKind.CREATE_SEQUENCE) {
            // Only separate sequence DDL, i.e. create sequence, will be handled here,
            // so we don't have to check its parent.
            // Avoid unnecessary plan cache cleanup since it's schema-level.
                SyncManagerHelper.sync(new ClearPlanCacheSyncAction(schemaName), schemaName);
        }
    }

}
