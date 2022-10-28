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

package com.alibaba.polardbx.executor.ddl.job.validator;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.ddl.job.meta.delegate.TableInfoManagerDelegate;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.util.SeqTypeUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;

import java.util.Set;

public class SequenceValidator {

    public static void validate(SequenceBean sequence, ExecutionContext executionContext) {
        if (sequence == null || sequence.getName() == null) {
            throw new SequenceException("Invalid sequence bean");
        }

        String schemaName = sequence.getSchemaName();
        if (TStringUtil.isBlank(schemaName)) {
            schemaName = executionContext.getSchemaName();
            sequence.setSchemaName(schemaName);
        }

        validateNewSequence(sequence);

        validateSimpleSequence(sequence, executionContext);

        validateExistence(sequence);

        String newSeqName = sequence.getNewName();
        if (sequence.getKind() == SqlKind.RENAME_SEQUENCE && StringUtils.isEmpty(newSeqName)) {
            throw new SequenceException("New sequence name must be provided for RENAME SEQUENCE");
        }

        validateSequenceLimits(sequence);
    }

    private static void validateNewSequence(SequenceBean sequence) {
        if (sequence == null ||
            sequence.getKind() == null ||
            SeqTypeUtil.isNewSeqSupported(sequence.getSchemaName())) {
            return;
        }

        Type seqType = sequence.getType();
        Type toType = sequence.getToType();

        boolean allowed = true;

        switch (sequence.getKind()) {
        case CREATE_SEQUENCE:
            if (seqType != null && seqType == Type.NEW) {
                allowed = false;
            }
            break;
        case ALTER_SEQUENCE:
            if (toType != null && toType == Type.NEW) {
                allowed = false;
            }
            break;
        default:
            break;
        }

        if (!allowed) {
            throw new SequenceException("New Sequence is only supported in a database with the AUTO mode");
        }
    }

    public static void validateSimpleSequence(SequenceBean sequence, ExecutionContext executionContext) {
        if (sequence == null ||
            sequence.getKind() == null ||
            ConfigDataMode.isAllowSimpleSequence() ||
            executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_SIMPLE_SEQUENCE)) {
            return;
        }

        Type seqType = sequence.getType();
        Type toType = sequence.getToType();

        boolean allowed = true;

        switch (sequence.getKind()) {
        case CREATE_SEQUENCE:
            if (seqType != null && seqType == Type.SIMPLE) {
                allowed = false;
            }
            break;
        case ALTER_SEQUENCE:
            if (toType != null && toType == Type.SIMPLE) {
                allowed = false;
            }
            break;
        default:
            break;
        }

        if (!allowed) {
            String errMsg = "Simple Sequence is not allowed to be created or changed by default in PolarDB-X 2.0";
            throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, errMsg);
        }
    }

    private static void validateExistence(SequenceBean sequence) {
        final String seqSchema = sequence.getSchemaName();
        final String seqName = sequence.getName();

        Type existingSeqType = SequenceManagerProxy.getInstance().checkIfExists(seqSchema, seqName);
        boolean seqExists = existingSeqType != Type.NA;

        String errorMessage = null;

        switch (sequence.getKind()) {
        case CREATE_SEQUENCE:
            if (seqExists) {
                errorMessage = String.format("Sequence %s already exists.", seqName);
            }
            break;
        case ALTER_SEQUENCE:
            if (!seqExists) {
                errorMessage = String.format("Sequence %s doesn't exist.", seqName);
            }
            break;
        case DROP_SEQUENCE:
            // Don't fail if sequence doesn't exist for compatibility.
            break;
        case RENAME_SEQUENCE:
            if (!seqExists) {
                errorMessage = String.format("Source sequence %s doesn't exist.", seqName);
            }
            final String seqNewName = sequence.getNewName();
            Type existingNewSeqType = SequenceManagerProxy.getInstance().checkIfExists(seqSchema, seqNewName);
            if (existingNewSeqType != Type.NA) {
                errorMessage = String.format("Target sequence %s already exists.", seqNewName);
            }
            break;
        default:
            throw new SequenceException("Unexpected operation: " + sequence.getKind());
        }

        if (TStringUtil.isNotBlank(errorMessage)) {
            throw new SequenceException(errorMessage);
        }
    }

    public static void validateExistenceForRename(String schemaName, String tableName) {
        Set<String> sequenceTableNames = new TableInfoManagerDelegate<Set<String>>(new TableInfoManager()) {
            @Override
            protected Set<String> invoke() {
                return tableInfoManager.queryAllSequenceNames(schemaName);
            }
        }.execute();

        String sequenceName = SequenceAttribute.AUTO_SEQ_PREFIX + tableName;

        if (sequenceTableNames.contains(sequenceName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE,
                String.format("Sequence of table '%s' already exists", tableName));
        }
    }

    private static void validateSequenceLimits(SequenceBean sequence) {
        if (sequence.getKind() == SqlKind.CREATE_SEQUENCE) {
            // Check the sequence name length.
            LimitValidator.validateSequenceNameLength(sequence.getName());
            // Check total sequence count.
            LimitValidator.validateSequenceCount(sequence.getSchemaName());
        }
        if (sequence.getKind() == SqlKind.RENAME_SEQUENCE) {
            // Check new sequence name length.
            LimitValidator.validateSequenceNameLength(sequence.getNewName());
        }
    }

}
