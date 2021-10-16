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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.ddl.job.meta.delegate.TableInfoManagerDelegate;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;

import java.util.Set;

public class SequenceValidator {

    public static void validate(SequenceBean sequenceBean, ExecutionContext executionContext) {
        if (sequenceBean == null || sequenceBean.getSequenceName() == null) {
            throw new SequenceException("Invalid sequence bean");
        }

        validateSimpleSequence(sequenceBean, executionContext);

        String newSequenceName = sequenceBean.getNewSequenceName();
        if (sequenceBean.getKind() == SqlKind.RENAME_SEQUENCE && StringUtils.isEmpty(newSequenceName)) {
            throw new SequenceException("Unexpected new sequence name: " + newSequenceName);
        }

        validateSequenceLimits(sequenceBean);
    }

    public static void validateSimpleSequence(SequenceBean sequenceBean, ExecutionContext executionContext) {
        if (sequenceBean == null ||
            ConfigDataMode.isAllowSimpleSequence() ||
            executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_SIMPLE_SEQUENCE)) {
            return;
        }

        SequenceAttribute.Type seqType = sequenceBean.getType();
        SequenceAttribute.Type toType = sequenceBean.getToType();

        boolean allowed = true;

        switch (sequenceBean.getKind()) {
        case CREATE_SEQUENCE:
            if (seqType != null && seqType == SequenceAttribute.Type.SIMPLE) {
                allowed = false;
            }
            break;
        case ALTER_SEQUENCE:
            if (toType != null && toType == SequenceAttribute.Type.SIMPLE) {
                allowed = false;
            }
            break;
        default:
            break;
        }

        if (!allowed) {
            String errMsg = "Simple Sequence is not allowed to be created or changed by default in PolarDB-X";
            throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, errMsg);
        }
    }

    private static void validateSequenceLimits(SequenceBean sequenceBean) {
        if (sequenceBean.getKind() == SqlKind.CREATE_SEQUENCE) {
            // Check the sequence name length.
            LimitValidator.validateSequenceNameLength(sequenceBean.getSequenceName());
            // Check total sequence count.
            LimitValidator.validateSequenceCount(sequenceBean.getSchemaName());
        }
        if (sequenceBean.getKind() == SqlKind.RENAME_SEQUENCE) {
            // Check new sequence name length.
            LimitValidator.validateSequenceNameLength(sequenceBean.getNewSequenceName());
        }
    }

    public static void validateSequenceExistence(String schemaName, String tableName) {
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

}
