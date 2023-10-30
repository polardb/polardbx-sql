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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.properties.ConnectionParams;
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
import com.alibaba.polardbx.gms.metadb.seq.SequenceBaseRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalSequenceDdl;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSequence;

import java.sql.Connection;
import java.sql.SQLException;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;

public class LogicalSequenceDdlHandler extends HandlerCommon {
    public LogicalSequenceDdlHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalSequenceDdl sequenceDdl = (LogicalSequenceDdl) logicalPlan;
        final SequenceBean sequence = ((SqlSequence) sequenceDdl.relDdl.sqlNode).getSequenceBean();

        String schemaName = sequence.getSchemaName();
        if (TStringUtil.isBlank(schemaName)) {
            schemaName = executionContext.getSchemaName();
            sequence.setSchemaName(schemaName);
        }
        String seqName = sequence.getName();

        SequenceValidator.validate(sequence, executionContext);

        Cursor cursor = handleSequence(sequence, executionContext);

        LoggerInit.TDDL_SEQUENCE_LOG.info(String.format("Sequence operation %s for %s was successful in %s",
            sequence.getKind(), seqName, schemaName));

        SyncManagerHelper.sync(new SequenceSyncAction(schemaName, seqName), schemaName, SyncScope.CURRENT_ONLY);

        // Clean up plan cache, but avoid unnecessary cleanup since it's schema-level.
        if (TStringUtil.startsWithIgnoreCase(seqName, AUTO_SEQ_PREFIX) &&
            sequence.getKind() == SqlKind.CREATE_SEQUENCE) {
            SyncManagerHelper.sync(new ClearPlanCacheSyncAction(schemaName), schemaName);
        }

        return cursor;
    }

    private Cursor handleSequence(SequenceBean sequence, ExecutionContext executionContext) {
        int affectedRows = 0;

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            SequencesAccessor sequencesAccessor = new SequencesAccessor();
            sequencesAccessor.setConnection(metaDbConn);

            final String seqSchema = sequence.getSchemaName();
            final String seqName = sequence.getName();

            SequenceBaseRecord record = SequenceUtil.convert(sequence, null, executionContext);

            long newSeqCacheSize = executionContext.getParamManager().getLong(ConnectionParams.NEW_SEQ_CACHE_SIZE);
            newSeqCacheSize = newSeqCacheSize < 1 ? 0 : newSeqCacheSize;

            switch (sequence.getKind()) {
            case CREATE_SEQUENCE:
                affectedRows = sequencesAccessor.insert(record, newSeqCacheSize,
                    SequenceUtil.buildFailPointInjector(executionContext));
                break;
            case ALTER_SEQUENCE:
                boolean alterWithoutTypeChange = true;

                if (sequence.getToType() != null && sequence.getToType() != Type.NA) {
                    Pair<SequenceBaseRecord, SequenceBaseRecord> recordPair =
                        SequenceUtil.change(sequence, null, executionContext);
                    if (recordPair != null) {
                        affectedRows = sequencesAccessor.change(recordPair, newSeqCacheSize,
                            SequenceUtil.buildFailPointInjector(executionContext));
                        alterWithoutTypeChange = false;
                    }
                }

                if (alterWithoutTypeChange) {
                    Type existingType = SequenceManagerProxy.getInstance().checkIfExists(seqSchema, seqName);
                    if (existingType != Type.TIME) {
                        affectedRows = sequencesAccessor.update(record, newSeqCacheSize);
                    }
                }

                break;
            case DROP_SEQUENCE:
                if (!ConfigDataMode.isSupportDropAutoSeq()
                    && TStringUtil.startsWithIgnoreCase(seqName, AUTO_SEQ_PREFIX)) {
                    throw new SequenceException(
                        "A sequence associated with a table is not allowed to be dropped separately");
                }

                affectedRows = sequencesAccessor.delete(record);

                break;
            case RENAME_SEQUENCE:
                affectedRows = sequencesAccessor.rename(record);
                break;
            default:
                throw new SequenceException("Unexpected operation: " + sequence.getKind());
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }

        return new AffectRowCursor(new int[] {affectedRows});
    }

}
