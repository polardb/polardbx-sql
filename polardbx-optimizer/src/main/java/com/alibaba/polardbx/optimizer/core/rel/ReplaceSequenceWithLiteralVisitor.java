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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.function.SqlSequenceFunction;
import com.alibaba.polardbx.optimizer.sequence.ISequenceManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSequenceParam;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallParam;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlReplace;
import org.apache.calcite.sql.SqlSequenceParam;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author minggong.zm 2018/2/7
 */
public class ReplaceSequenceWithLiteralVisitor extends SqlShuttle {

    // Collect assigned sequence values for sharding, because RexNode is used
    // for sharding, not RelNode.
    // One list for each row. Format in each row is: fieldIndex -> seqVal.
    private List<Map<Integer, Long>> sequenceValues = new ArrayList<>();
    private Parameters parameterSettings;
    private int batchSize = 0;
    private Long lastInsertId = null;
    private boolean autoValueOnZero;
    // The first implicit value for auto increment column. If none, it's the
    // last explicit value.
    private Long returnedLastInsertId = null;
    private Long beginSequenceVal = null;
    private int curFieldIndex = 0;
    // auto increment field index.
    private int autoIncrementColumn;
    private String seqName = null;
    private int curRowIndex = 0;
    private String schemaName = null;

    private final Map<String, Map<String, SeqBean>> seqCache = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private static class SeqBean {
        private String schemaName = null;
        private String seqName = null;
        private SequenceAttribute.Type seqType;
        private Integer seqIncrement;

        public SeqBean(String schemaName, String seqName) {
            this.schemaName = schemaName;
            this.seqName = seqName;
            this.seqType = SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);
            this.seqIncrement = SequenceManagerProxy.getInstance().getIncrement(schemaName, seqName);
        }
    }

    public ReplaceSequenceWithLiteralVisitor(Parameters parameterSettings, int autoIncrementColumn, String schemaName,
                                             String tableName, boolean autoValueOnZero) {
        super();
        this.parameterSettings = parameterSettings;
        if (parameterSettings != null) {
            batchSize = parameterSettings.getBatchSize();
        }
        this.autoIncrementColumn = autoIncrementColumn;
        this.schemaName = schemaName;
        this.seqName = ISequenceManager.AUTO_SEQ_PREFIX + tableName;
        this.autoValueOnZero = autoValueOnZero;
    }

    public List<Map<Integer, Long>> getSequenceValues() {
        return sequenceValues;
    }

    public Long getLastInsertId() {
        return lastInsertId;
    }

    public Long getReturnedLastInsertId() {
        // Only when all values are explicitly assigned should
        // returnedLastInsertId work.
        if (lastInsertId == null || lastInsertId == 0) {
            return returnedLastInsertId;
        } else {
            return lastInsertId;
        }
    }

    public Parameters getParameters() {
        return parameterSettings;
    }

    /**
     * Optimized sequence calculation in single INSERT VALUES. 1. SqlNode copy
     * is not needed; 2. param index is already calculated in optimizer; 3.
     * batch size is always 1.
     *
     * @param dynamicParamIndex param index for sequence
     */
    public void replaceDynamicParam(int dynamicParamIndex) {
        // in SPLIT mode
        if (parameterSettings.getSequenceSize().get() == 0) {
            parameterSettings.getSequenceSize().set(1);
            parameterSettings.getSequenceIndex().set(0);
        }

        Map<Integer, ParameterContext> curParams = parameterSettings.getCurrentParameter();
        ParameterContext oldPc = curParams.get(dynamicParamIndex + 1);

        Long newValue = null;
        // if oldPc == null, it was `null`(Literal), and was replaced with
        // DynamicParam in Optimizer.
        if (oldPc != null) {
            Object autoIncValue = oldPc.getValue();
            newValue = RexUtils.valueOfObject1(autoIncValue);
        }

        // if NO_AUTO_VALUE_ON_ZERO is set, last_insert_id and returned_last_insert_id won't change.
        final boolean explicitLastInsertId = (null != newValue) && (0L != newValue);
        final boolean assignSequence = (null == newValue) || (0L == newValue && autoValueOnZero);

        if (explicitLastInsertId) {
            returnedLastInsertId = newValue;
            SequenceManagerProxy.getInstance().updateValue(schemaName, seqName, newValue);
        } else if (assignSequence) {
            Long nextVal = assignImplicitValue(false);
            ParameterContext newPc = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                dynamicParamIndex + 1, nextVal});
            curParams.put(dynamicParamIndex + 1, newPc);
        }
    }

    /**
     * Assign sequence values
     * 1. Assign implicit sequence first, with guarantee of continuity
     * 2. Assign explicit sequence call one by one
     *
     * @param values value expressions
     * @return Whether auto_increment column using sequence
     */
    public boolean replaceDynamicParam(List<RexNode> values, Map<Integer, ParameterContext> curParams,
                                       Set<Integer> autoIncColumnIndex) {
        if (GeneralUtil.isEmpty(autoIncColumnIndex)) {
            return false;
        }

        // Parameter index set which will be replaced with same sequence value
        final Set<Integer> implicitSeqParamIndex = implicitSeqParamIndex(values, autoIncColumnIndex, curParams);

        // Replace parameter with sequence value
        boolean autoIncrementUsingSeq = false;
        for (Ord<RexNode> o : Ord.zip(values)) {
            final Integer columnIndex = o.getKey();
            final RexNode value = o.getValue();

            if (!(value instanceof RexDynamicParam)) {
                continue;
            }

            final int paramIndex = ((RexDynamicParam) value).getIndex();

            if (implicitSeqParamIndex.contains(paramIndex)) {
                // Implicit sequence call
                final Long seqValue = assignImplicitValue(false);

                ParameterContext newPc = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                    paramIndex + 1, seqValue});
                curParams.put(paramIndex + 1, newPc);
                autoIncrementUsingSeq = true;
                continue;
            }

            final boolean isSeqCall = value instanceof RexSequenceParam;
            final boolean isAutoIncColumn = autoIncColumnIndex.contains(columnIndex);

            if (isSeqCall) {
                // Explicit sequence call
                final String seqName = ((RexSequenceParam) value).getSequenceName();

                // Assign sequence value independently
                final Long nextVal = SequenceManagerProxy.getInstance().nextValue(schemaName, seqName);
                ParameterContext newPc = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                    paramIndex + 1, nextVal});
                curParams.put(paramIndex + 1, newPc);

                if (isAutoIncColumn) {
                    autoIncrementUsingSeq = true;
                }
            } else if (isAutoIncColumn) {
                // Update sequence with specified value of auto increment column
                final ParameterContext autoIncPc = curParams.get(paramIndex + 1);
                final long newValue = RexUtils.valueOfObject1(autoIncPc.getValue());

                returnedLastInsertId = newValue;
                SequenceManagerProxy.getInstance().updateValue(schemaName, seqName, newValue);
            }
        }

        return autoIncrementUsingSeq;
    }

    /**
     * Count rows require implicit sequence call
     *
     * @param mergeRow Value clause of insert, might contains more than one implicit call
     * @param curParams Parameters
     * @param autoIncColumnIndex Auto increment column index
     * @return Sequence batch size
     */
    public int getImplicitSequenceBatchSize(List<RexNode> mergeRow, Map<Integer, ParameterContext> curParams,
                                            Set<Integer> autoIncColumnIndex) {
        if (GeneralUtil.isEmpty(autoIncColumnIndex)) {
            return 0;
        }

        // Parameter index set which will be replaced with same sequence value
        final Set<Integer> implicitSeqParamIndex = implicitSeqParamIndex(mergeRow, autoIncColumnIndex, curParams);

        // Replace parameter with sequence value
        int seqCount = 0;
        for (Ord<RexNode> o : Ord.zip(mergeRow)) {
            final RexNode value = o.getValue();

            if (!(value instanceof RexDynamicParam)) {
                // Value of auto increment column should be replaced with RexSequenceParam/RexCallParam in
                // {@link OptimizeLogicalInsertRule.processRexCall}
                continue;
            }

            final int paramIndex = ((RexDynamicParam) value).getIndex();

            if (implicitSeqParamIndex.contains(paramIndex)) {
                // Implicit sequence call
                // There can be only one AUTO_INCREMENT column per table
                // So that for each implicit sequence call, a new sequence value is required
                seqCount++;
            }

//            final boolean isSeqCall = value instanceof RexSequenceParam;
//
//            if (isSeqCall) {
//                // Explicit sequence call
//                final RexSequenceParam seqParam = (RexSequenceParam) value;
//                final RexCall seqCall = (RexCall) seqParam.getSequenceCall();
//                final RexLiteral seqNameLiteral = (RexLiteral) seqCall.getOperands().get(0);
//                final String seqName = seqNameLiteral.getValueAs(String.class);
//
//                if (!TStringUtil.equalsIgnoreCase(this.seqName, seqName)) {
//                    return 0;
//                }
//
//                seqCount++;
//            }
        }

        return seqCount;
    }

    /**
     * There can be only one AUTO_INCREMENT column per table,
     * So that there can only be one (at most) implicit sequence call per row.
     * But values might contains more than one row
     */
    public Set<Integer> implicitSeqParamIndex(List<RexNode> values, Set<Integer> autoIncColumnIndex,
                                              Map<Integer, ParameterContext> curParams) {
        final Set<Integer> tableSeqParamIndex = new HashSet<>();
        for (Ord<RexNode> o : Ord.zip(values)) {
            final Integer columnIndex = o.getKey();
            final RexNode value = o.getValue();

            final boolean isAutoIncColumn = autoIncColumnIndex.contains(columnIndex);

            if (!isAutoIncColumn) {
                continue;
            }

            if (!(value instanceof RexDynamicParam)) {
                // Value of auto increment column should be replaced with RexSequenceParam/RexCallParam in
                // {@link OptimizeLogicalInsertRule.processRexCall}
                throw new IllegalArgumentException("Unexpected value type for auto_increment column, " + value);
            }

            final int paramIndex = ((RexDynamicParam) value).getIndex();
            final boolean isSeqCall = value instanceof RexSequenceParam;

            if (isSeqCall) {
                final String seqName = ((RexSequenceParam) value).getSequenceName();

                if (TStringUtil.equalsIgnoreCase(this.seqName, seqName)) {
                    // Auto increment column with explicit seq-of-target-table.nextVal specified
                    tableSeqParamIndex.add(paramIndex);
                }

                continue;
            }

            final ParameterContext autoIncPc = curParams.get(paramIndex + 1);
            final Object oriValue = autoIncPc.getValue();

            if (null == oriValue) {
                // Auto increment column with NULL specified
                tableSeqParamIndex.add(paramIndex);
            } else {
                final long autoIncValue = RexUtils.valueOfObject1(oriValue);
                if (autoIncValue == 0 && autoValueOnZero) {
                    // Auto increment column with 0 specified
                    tableSeqParamIndex.add(paramIndex);
                }
            }
        }
        return tableSeqParamIndex;
    }

    public SqlInsert visit(SqlInsert insert) {
        List<SqlNode> operandList = insert.getOperandList();
        SqlNode oldSource = operandList.get(2);
        SqlNode newSource = oldSource.accept(this);
        SqlInsert sqlInsert = null;
        if (insert.getKind() == SqlKind.INSERT) {
            sqlInsert = new SqlInsert(insert.getParserPosition(),
                (SqlNodeList) operandList.get(0),
                operandList.get(1),
                newSource,
                (SqlNodeList) operandList.get(3),
                (SqlNodeList) operandList.get(4),
                insert.getBatchSize(),
                insert.getHints());
        } else {
            sqlInsert = new SqlReplace(insert.getParserPosition(),
                (SqlNodeList) operandList.get(0),
                operandList.get(1),
                newSource,
                (SqlNodeList) operandList.get(3),
                insert.getBatchSize(),
                insert.getHints());
        }
        return sqlInsert;
    }

    public SqlNode visit(SqlCall call) {
        if (call instanceof SqlBasicCall) {
            if (call.getKind() == SqlKind.VALUES) {
                return visitSqlValues((SqlBasicCall) call);
            } else if (call.getKind() == SqlKind.ROW) {
                return visitSqlRow((SqlBasicCall) call);
            } else if (call.getKind() == SqlKind.OTHER_FUNCTION) {
                return visitFunction(call);
            }
        }
        return SqlNode.clone(call);
    }

    private SqlBasicCall visitSqlValues(SqlBasicCall call) {
        SqlNode[] oldRows = call.getOperands();
        SqlNode[] valuesOperands;

        if (batchSize > 0) {
            computeSequenceSize(batchSize, oldRows);
            SqlNode operand = oldRows[0].accept(this);
            valuesOperands = new SqlNode[] {operand};
        } else {
            valuesOperands = new SqlBasicCall[oldRows.length];
            computeSequenceSize(oldRows.length, oldRows);
            for (int i = 0; i < oldRows.length; i++) {
                curRowIndex = i;
                valuesOperands[i] = oldRows[i].accept(this); // ROW
            }
        }

        return new SqlBasicCall(call.getOperator(),
            valuesOperands,
            call.getParserPosition(),
            call.isExpanded(),
            call.getFunctionQuantifier());
    }

    /**
     * Compute count of values to apply sequence. This count may be greater than
     * we want. For instance, user used some explicit values, which shouldn't be
     * overwrite by assigned sequence. But we don't guarantee continuity of
     * sequence between requests, we only guarantee continuity in each request.
     * So we can prefetch as many sequences as we may need at once. Specially,
     * if batch_insert_policy = 'SPLIT', values num is calculated before
     * executing sql. To ensure the continuity of sequence, we must assign
     * sequence in the first batch.
     */
    private void computeSequenceSize(int rowCount, SqlNode[] rows) {
        if (parameterSettings != null) {
            int sequenceSize = 0;
            if (autoIncrementColumn >= 0) {
                sequenceSize = rowCount;
                if (!isAllUsingSequence(rows)) {
                    sequenceSize = 0;
                }
            }

            // in SPLIT mode
            if (parameterSettings.getSequenceSize().get() == 0) {
                parameterSettings.getSequenceSize().set(sequenceSize);
                parameterSettings.getSequenceIndex().set(0);
            }
        }

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            sequenceValues.add(new HashMap<Integer, Long>());
        }
    }

    /**
     * create table tb(id int primary key auto_increment by simple) dbpartition
     * by hash(id); insert into tb(id) values(10), (0), (8), (0); In MySQL,
     * inserted values should be 10, 11, 8, 12. That is, if there're both
     * sequence and user values, we should always assign sequence one by one,
     * instead of assigning a batch once. Plus, we should update sequence table
     * when there's a user value, to make sure that next sequence will be larger
     * than it.
     */
    private boolean isAllUsingSequence(SqlNode[] rows) {
        for (int i = 0; i < rows.length; i++) {
            if (!(rows[i] instanceof SqlBasicCall)) { // impossible
                return false;
            }

            SqlBasicCall row = (SqlBasicCall) rows[i];
            SqlNode[] operands = row.getOperands();
            if (operands == null || operands.length <= autoIncrementColumn) { // impossible
                return false;
            }

            SqlNode operand = row.getOperands()[autoIncrementColumn];
            if (!isRowUsingSequence(operand)) {
                return false;
            }
        }
        return true;
    }

    private boolean isRowUsingSequence(SqlNode sqlNode) {
        if (sqlNode instanceof SqlLiteral) {
            SqlLiteral literal = (SqlLiteral) sqlNode;

            SqlTypeFamily typeFamily = literal.getTypeName().getFamily();
            if (typeFamily == SqlTypeFamily.NUMERIC || typeFamily == SqlTypeFamily.EXACT_NUMERIC) {
                return literal.getValueAs(Long.class) == 0;
            } else {
                return typeFamily == SqlTypeFamily.NULL;
            }
        } else if (sqlNode instanceof SqlCallParam) {
            return null != ((SqlCallParam) sqlNode).getSequenceCall();
        } else if (sqlNode instanceof SqlSequenceParam) {
            return true;
        } else if (sqlNode instanceof SqlDynamicParam) {
            if (parameterSettings == null) {
                return false; // explain
            }

            SqlDynamicParam dynamicParam = (SqlDynamicParam) sqlNode;

            if (batchSize > 0) {
                int dynamicParamIndex = dynamicParam.getIndex();
                List<Map<Integer, ParameterContext>> batchParameters = parameterSettings.getBatchParameters();

                for (int rowIndex = 0; rowIndex < batchSize; rowIndex++) {
                    Map<Integer, ParameterContext> curParams = batchParameters.get(rowIndex);
                    ParameterContext parameterContext = curParams.get(dynamicParamIndex + 1);

                    Object autoIncValue = parameterContext.getValue();

                    final Long newValue = RexUtils.valueOfObject1(autoIncValue);
                    final boolean assignSequence = (null == newValue) || (0L == newValue && autoValueOnZero);
                    if (!assignSequence) {
                        return false;
                    }
                }
                return true;

            } else {
                int dynamicParamIndex = dynamicParam.getIndex();
                Map<Integer, ParameterContext> curParams = parameterSettings.getCurrentParameter();
                ParameterContext parameterContext = curParams.get(dynamicParamIndex + 1);

                final Long newValue = RexUtils.valueOfObject1(parameterContext.getValue());
                return (null == newValue) || (0L == newValue && autoValueOnZero);
            }
        } else {
            return false; // SqlCall
        }
    }

    /**
     * If it's in batch, scan by column; if not, scan by row.
     *
     * @param call one row
     */
    private SqlNode visitSqlRow(SqlBasicCall call) {
        SqlNode[] operands = call.getOperands();
        SqlNode[] newOperands = new SqlNode[operands.length];
        for (int columnIndex = 0; columnIndex < operands.length; columnIndex++) {
            curFieldIndex = columnIndex;
            newOperands[columnIndex] = operands[columnIndex].accept(this);
        }

        return new SqlBasicCall(call.getOperator(),
            newOperands,
            SqlParserPos.ZERO,
            call.isExpanded(),
            call.getFunctionQuantifier());
    }

    public SqlNode visit(SqlLiteral literal) {
        if (batchSize > 0) {
            return visitAllLiteral(literal);
        } else {
            return visitOneLiteral(literal);
        }
    }

    /**
     * Not in batch mode. Check the value. If it's null or 0, assign value for
     * it.
     */
    private SqlNode visitOneLiteral(SqlLiteral literal) {
        if (autoIncrementColumn != curFieldIndex) {
            return SqlNode.clone(literal);
        }

        boolean shouldReplace = false;
        long value = 0;
        SqlTypeFamily typeFamily = literal.getTypeName().getFamily();
        switch (typeFamily) {
        case NUMERIC:
        case EXACT_NUMERIC:
            value = literal.getValueAs(Long.class);
            if (value == 0) {
                shouldReplace = true;
            } else {
                returnedLastInsertId = value;
            }
            break;
        case NULL:
            shouldReplace = true;
            break;
        default:
            break;
        }

        if (shouldReplace) {
            Long nextVal = assignImplicitValue(true);
            return SqlLiteral.createExactNumeric(String.valueOf(nextVal), literal.getParserPosition());
        } else {
            String schema = schemaName;
            if (schema == null) {
                schema = DefaultSchema.getSchemaName();
            }
            SequenceManagerProxy.getInstance().updateValue(schema, seqName, value);
            return SqlNode.clone(literal);
        }
    }

    /**
     * In batch mode. if the auto increment column is SqlLiteral, create a
     * SqlDynamicParam and add corresponding value to the parameter map.
     */
    private SqlNode visitAllLiteral(SqlLiteral literal) {
        if (autoIncrementColumn != curFieldIndex) {
            return SqlNode.clone(literal);
        }

        // In this case, the value must be the default value (NULL), which is
        // added automatically in optimizer.
        // We needn't check the values, just replace SqlLiteral with
        // SqlDynamicParam.
        List<Map<Integer, ParameterContext>> batchParameters = parameterSettings.getBatchParameters();
        int dynamicParamIndex = batchParameters.get(0).size();
        for (int rowIndex = 0; rowIndex < batchSize; rowIndex++) {
            curRowIndex = rowIndex;
            Long nextVal = assignImplicitValue(true);

            Map<Integer, ParameterContext> curParams = batchParameters.get(rowIndex);
            ParameterContext newPc = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                dynamicParamIndex + 1, nextVal});

            curParams.put(dynamicParamIndex + 1, newPc);
        }
        return new SqlDynamicParam(dynamicParamIndex, literal.getParserPosition(), null);
    }

    public SqlDynamicParam visit(SqlDynamicParam dynamicParam) {
        if (batchSize > 0) {
            return visitAllDynamicParam(dynamicParam);
        } else {
            return visitOneDynamicParam(dynamicParam);
        }
    }

    /**
     * Not in batch mode. Check its value. If it's null or 0, assign a value and
     * set it to the parameter map.
     */
    private SqlDynamicParam visitOneDynamicParam(SqlDynamicParam dynamicParam) {
        if (autoIncrementColumn != curFieldIndex) {
            return SqlNode.clone(dynamicParam);
        }

        final Map<Integer, ParameterContext> curParams = parameterSettings.getCurrentParameter();

        final int dynamicParamIndex = dynamicParam.getIndex();
        final ParameterContext oldPc = curParams.get(dynamicParamIndex + 1);
        final Object autoIncValue = oldPc.getValue();
        final Long newValue = RexUtils.valueOfObject1(autoIncValue);

        // if NO_AUTO_VALUE_ON_ZERO is set, last_insert_id and returned_last_insert_id won't change.
        boolean explicitLastInsertId = (null != newValue) && (0L != newValue);
        boolean assignSequence = (null == newValue) || (0L == newValue && autoValueOnZero);

        if (explicitLastInsertId) {
            returnedLastInsertId = newValue;
            SequenceManagerProxy.getInstance().updateValue(schemaName, seqName, newValue);
        } else if (assignSequence) {
            Long nextVal = assignImplicitValue(true);
            // use setObject1 instead of oldPc.getParameterMethod, because
            // the method is 'setString' when users inserts '0'.
            ParameterContext newPc = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                dynamicParamIndex + 1, nextVal});
            curParams.put(dynamicParamIndex + 1, newPc);
        }

        return SqlNode.clone(dynamicParam);
    }

    /**
     * In batch mode. Check its value. If it's null or 0, assign a value and set
     * it to the parameter map.
     */
    private SqlDynamicParam visitAllDynamicParam(SqlDynamicParam dynamicParam) {
        if (autoIncrementColumn != curFieldIndex) {
            return SqlNode.clone(dynamicParam);
        }

        int dynamicParamIndex = dynamicParam.getIndex();
        List<Map<Integer, ParameterContext>> batchParameters = parameterSettings.getBatchParameters();
        for (int rowIndex = 0; rowIndex < batchSize; rowIndex++) {
            Map<Integer, ParameterContext> curParams = batchParameters.get(rowIndex);
            ParameterContext oldPc = curParams.get(dynamicParamIndex + 1);

            Object autoIncValue = oldPc.getValue();
            final Long newValue = RexUtils.valueOfObject1(autoIncValue);

            // if NO_AUTO_VALUE_ON_ZERO is set, last_insert_id and returned_last_insert_id won't change.
            boolean explicitLastInsertId = (null != newValue) && (0L != newValue);
            boolean assignSequence = (null == newValue) || (0L == newValue && autoValueOnZero);

            if (explicitLastInsertId) {
                returnedLastInsertId = newValue;
                SequenceManagerProxy.getInstance().updateValue(schemaName, seqName, newValue);
            } else if (assignSequence) {
                curRowIndex = rowIndex;
                Long nextVal = assignImplicitValue(true);

                // use setObject1 instead of oldPc.getParameterMethod,
                // because the method is 'setString' when users inserts '0'.
                ParameterContext newPc = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                    dynamicParamIndex + 1, nextVal});
                curParams.put(dynamicParamIndex + 1, newPc);
            }
        }
        return SqlNode.clone(dynamicParam);
    }

    /**
     * NEXTVAL won't appear in batch mode.
     *
     * @param call function
     */
    private SqlNode visitFunction(SqlCall call) {
        SqlOperator operator = call.getOperator();
        if (operator == TddlOperatorTable.NEXTVAL) {
            Long[] result = SqlSequenceFunction.assignValue(schemaName, call, parameterSettings, beginSequenceVal);
            Long nextVal = result[0];
            if (result[1] != null) {
                lastInsertId = result[1];
            }
            beginSequenceVal = result[2];

            sequenceValues.get(curRowIndex).put(curFieldIndex, nextVal);
            return SqlLiteral.createExactNumeric(String.valueOf(nextVal), call.getParserPosition());
        }
        return SqlNode.clone(call);
    }

    private Long assignImplicitValue(boolean recordSeqValue) {
        return assignImplicitValue(recordSeqValue, seqName);
    }

    private Long assignImplicitValue(boolean recordSeqValue, String seqName) {
        // In SPLIT mode
        if (beginSequenceVal == null && parameterSettings.getSequenceBeginVal() != null) {
            beginSequenceVal = parameterSettings.getSequenceBeginVal();
        }
        final SeqBean seqBean = getSeqBean(schemaName, seqName);
        Long[] result = SqlSequenceFunction
            .assignValue(schemaName, seqName, true, parameterSettings, beginSequenceVal, seqBean.seqType,
                seqBean.seqIncrement);
        Long nextVal = result[0];
        if (result[1] != null) {
            final boolean isBatchSeq = parameterSettings.getSequenceSize().get() > 0;
            final boolean isNewSeq = (seqBean.seqType == SequenceAttribute.Type.NEW);
            final boolean isSimpleSeq = (seqBean.seqType == SequenceAttribute.Type.SIMPLE);

            if (isBatchSeq || (!isNewSeq && !isSimpleSeq) || null == lastInsertId) {
                lastInsertId = result[1];
            }
        }
        beginSequenceVal = result[2];
        parameterSettings.setSequenceBeginVal(beginSequenceVal);

        if (recordSeqValue) {
            sequenceValues.get(curRowIndex).put(curFieldIndex, nextVal);
        }
        return nextVal;
    }

    private SeqBean getSeqBean(String schemaName, String seqName) {
        final Map<String, SeqBean> seqCache = this.seqCache.computeIfAbsent(schemaName, (k) -> new TreeMap<>());
        return seqCache.computeIfAbsent(seqName, (k) -> new SeqBean(schemaName, k));
    }

    public SequenceAttribute.Type getSeqType() {
        return getSeqBean(schemaName, seqName).seqType;
    }
}
