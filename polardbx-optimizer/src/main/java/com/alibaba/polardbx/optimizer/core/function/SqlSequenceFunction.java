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

package com.alibaba.polardbx.optimizer.core.function;

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SqlSequenceFunction extends SqlFunction {

    public SqlSequenceFunction(String funcName) {
        super(funcName,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BIGINT,
            null,
            OperandTypes.or(OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.BOOLEAN),
                OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.BOOLEAN))
            ,
            SqlFunctionCategory.NUMERIC);
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public void validateCall(SqlCall call, SqlValidator validator, SqlValidatorScope scope,
                             SqlValidatorScope operandScope) {
        String defaultSchemaName = DefaultSchema.getSchemaName();
        if (OptimizerContext.getContext(defaultSchemaName).isSqlMock()) {
            return;
        }
        List<SqlNode> operands = call.getOperandList();

        int operandSize = operands.size();

        String seqName = null;
        if (operandSize == 2) {
            seqName = ((SqlLiteral) operands.get(0)).getValueAs(String.class);
            SequenceManagerProxy.getInstance().checkIfExists(defaultSchemaName, seqName);
        } else if (operandSize == 3) {
            String schemaName = null;

            schemaName = ((SqlLiteral) operands.get(0)).getValueAs(String.class);
            seqName = ((SqlLiteral) operands.get(1)).getValueAs(String.class);

            scope.getValidator().getCatalogReader().getRootSchema().getSubSchema(schemaName, false);
            SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);
        }

    }

    /**
     * @param beginSequenceVal the first sequence value for multiple values INSERT.
     * May be null, if it's the first value.
     * @return 1. assigned value for current NEXTVAL.
     * 2. lastInsertId, stands for last_insert_id().
     * 3. the first sequence value for multiple values INSERT.
     */
    public static Long[] assignValue(String schemaName, SqlCall call, Parameters parameterSettings,
                                     Long beginSequenceVal) {
        List<SqlNode> operands = call.getOperandList();

        String schemaNameOfSeq = null;
        String seqName = null;
        Boolean autoIncrement = true;

        if (operands.size() == 2) {

            schemaNameOfSeq = OptimizerContext.getContext(schemaName).getSchemaName();

            seqName = ((SqlLiteral) operands.get(0)).getValueAs(String.class);
            // For auto increment column, last insert id should be changed.
            // Otherwise, like explicitly calling to NEXTVAL, last insert id won't
            // be changed.
            autoIncrement = ((SqlLiteral) operands.get(1)).getValueAs(Boolean.class);

        } else if (operands.size() == 3) {

            schemaNameOfSeq = ((SqlLiteral) operands.get(0)).getValueAs(String.class);

            seqName = ((SqlLiteral) operands.get(1)).getValueAs(String.class);
            // For auto increment column, last insert id should be changed.
            // Otherwise, like explicitly calling to NEXTVAL, last insert id won't
            // be changed.
            autoIncrement = ((SqlLiteral) operands.get(2)).getValueAs(Boolean.class);
        }

        return assignValue(schemaNameOfSeq, seqName, autoIncrement, parameterSettings, beginSequenceVal);
    }

    public static Long[] assignValue(String schemaName, String seqName, Boolean autoIncrement,
                                     Parameters parameterSettings, Long beginSequenceVal) {
        // Type of sequence
        final Type seqType = SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);

        // The increment step size of sequence
        final Integer seqIncrement = SequenceManagerProxy.getInstance().getIncrement(schemaName, seqName);

        return assignValue(schemaName, seqName, autoIncrement, parameterSettings, beginSequenceVal, seqType,
            seqIncrement);
    }

    public static Long[] assignValue(String schemaName, String seqName, Boolean autoIncrement,
                                     Parameters parameterSettings, Long beginSequenceVal,
                                     Type seqType, Integer seqIncrement) {
        Long nextVal = null;
        Long lastInsertId = null;
        if (autoIncrement && parameterSettings != null && parameterSettings.getSequenceSize().get() > 0) {
            final AtomicInteger seqSize = parameterSettings.getSequenceSize();
            final AtomicInteger seqIndex = parameterSettings.getSequenceIndex();

            if (beginSequenceVal == null) {
                // prefetch all the values at once to keep them consecutive, and return the max value
                beginSequenceVal = SequenceManagerProxy.getInstance().nextValue(schemaName, seqName, seqSize.get());
                if (seqType == Type.TIME) {
                    lastInsertId = computeTimeBasedSeqVal(beginSequenceVal, seqSize.get(), 0);
                } else {
                    lastInsertId =
                        beginSequenceVal = beginSequenceVal - (long) seqSize.get() * seqIncrement + seqIncrement;
                }
            }

            if (seqType == Type.TIME) {
                nextVal = computeTimeBasedSeqVal(beginSequenceVal, seqSize.get(), seqIndex.getAndIncrement());
            } else {
                nextVal = beginSequenceVal + (long) seqIndex.getAndIncrement() * seqIncrement;
            }
        } else {
            nextVal = SequenceManagerProxy.getInstance().nextValue(schemaName, seqName);
            if (autoIncrement) {
                lastInsertId = nextVal;
            }
        }
        return new Long[] {nextVal, lastInsertId, beginSequenceVal};
    }

    private static long computeTimeBasedSeqVal(long baseSeqVal, int size, int index) {
        long timestamp = IdGenerator.extractTimestamp(baseSeqVal);
        long workerId = IdGenerator.extractWorkerId(baseSeqVal);
        long sequence = IdGenerator.extractSequence(baseSeqVal);

        long currentTimestamp;
        long currentSequence;

        long numOfIdsInLastRange = sequence + 1;
        if (size <= numOfIdsInLastRange) {
            // The batch of ids don't cross ranges, so we can simply calculate and return.
            currentTimestamp = timestamp;
            currentSequence = numOfIdsInLastRange - size + index;
        } else {
            long restOfIds = size - numOfIdsInLastRange;
            long numOfFullRanges = restOfIds / IdGenerator.MAX_NUM_OF_IDS_PER_RANGE;
            long numOfIdsInFirstRange = restOfIds % IdGenerator.MAX_NUM_OF_IDS_PER_RANGE;

            if (numOfIdsInFirstRange == 0) {
                numOfFullRanges--;
                numOfIdsInFirstRange = IdGenerator.MAX_NUM_OF_IDS_PER_RANGE;
            }

            long firstTimestamp = timestamp - 1 - numOfFullRanges;
            long firstSequence = IdGenerator.MAX_NUM_OF_IDS_PER_RANGE - numOfIdsInFirstRange;

            if (index < numOfIdsInFirstRange) {
                currentTimestamp = firstTimestamp;
                currentSequence = firstSequence + index;
            } else {
                index -= numOfIdsInFirstRange;
                currentTimestamp = firstTimestamp + 1 + index / IdGenerator.MAX_NUM_OF_IDS_PER_RANGE;
                currentSequence = index % IdGenerator.MAX_NUM_OF_IDS_PER_RANGE;
            }
        }

        return IdGenerator.assembleId(currentTimestamp, workerId, currentSequence);
    }

}
