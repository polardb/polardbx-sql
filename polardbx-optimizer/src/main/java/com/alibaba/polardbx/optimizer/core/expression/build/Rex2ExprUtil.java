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

package com.alibaba.polardbx.optimizer.core.expression.build;

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.type.MySQLResultType;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.TimeType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.polardbx.common.type.MySQLResultType.DECIMAL_RESULT;
import static com.alibaba.polardbx.common.type.MySQLResultType.INT_RESULT;
import static com.alibaba.polardbx.common.type.MySQLResultType.REAL_RESULT;
import static com.alibaba.polardbx.common.type.MySQLResultType.STRING_RESULT;
import static com.alibaba.polardbx.common.type.MySQLStandardFieldType.MYSQL_TYPE_JSON;
import static com.alibaba.polardbx.common.type.MySQLStandardFieldType.MYSQL_TYPE_TIME;

public class Rex2ExprUtil {
    public static CollationName fixCollation(RexCall call, ExecutionContext executionContext) {
        List<ImmutablePair<CollationName, SqlCollation.Coercibility>> collationPairs = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            ImmutablePair<CollationName, SqlCollation.Coercibility> pair;
            if (operand instanceof RexDynamicParam) {
                // for partition table build PartitionPruneStep, executionContext maybe null
                if (executionContext == null) {
                    pair = ImmutablePair.of(
                        CollationName.defaultCollation(),
                        SqlCollation.Coercibility.COERCIBLE
                    );
                } else {
                    Map<Integer, NlsString> parameterNlsStrings = executionContext.getParameterNlsStrings();
                    NlsString nlsString = null;
                    if (parameterNlsStrings != null
                        && (nlsString = parameterNlsStrings.get(((RexDynamicParam) operand).getIndex())) != null) {
                        pair = ImmutablePair.of(
                            CollationName.of(nlsString.getCollation().getCollationName()),
                            nlsString.getCollation().getCoercibility());
                    } else {
                        pair = ImmutablePair.of(
                            CollationName.defaultCollation(),
                            SqlCollation.Coercibility.COERCIBLE
                        );
                    }
                }
            } else {
                pair = Optional.ofNullable(operand.getType())
                    .filter(SqlTypeUtil::isCharacter)
                    .map(RelDataType::getCollation)
                    .map(c -> ImmutablePair.of(CollationName.of(c.getCollationName()), c.getCoercibility()))
                    .orElseGet(
                        () -> ImmutablePair.of(CollationName.defaultCollation(), SqlCollation.Coercibility.COERCIBLE)
                    );
            }
            collationPairs.add(pair);
        }

        // get collation of runtime function.
        // 分成三类：explicit(字面量) > implicit(列) > coercible(普通常量)
        CollationName collation = SqlCollation.getMixOfCollation(collationPairs);
        return collation;
    }

    private static final Map<MySQLResultType, DataType> CMP_TYPE_MAP = ImmutableMap.of(
        STRING_RESULT, VarcharType.DEFAULT_COLLATION_VARCHAR_TYPE,
        REAL_RESULT, DataTypes.DoubleType,
        INT_RESULT, DataTypes.LongType,
        DECIMAL_RESULT, DataTypes.DecimalType
    );

    /**
     * Get compare type of binary comparison operators.
     * For scalar functions.
     *
     * @param leftType left operand type.
     * @param rightType right operand type.
     * @return The comparison type chosen to do compare.
     */
    public static DataType compareTypeOf(DataType leftType, DataType rightType) {
        MySQLResultType cmpType = cmpType(leftType, rightType);

        MySQLStandardFieldType lFieldType = leftType.fieldType();
        MySQLStandardFieldType rFieldType = rightType.fieldType();

        MySQLResultType lResultType = leftType.fieldType().toResultType();
        MySQLResultType rResultType = leftType.fieldType().toResultType();

        if ((lResultType == STRING_RESULT && lFieldType == MYSQL_TYPE_JSON)
            || (rResultType == STRING_RESULT && rFieldType == MYSQL_TYPE_JSON)) {
            // Use the JSON comparator if at least one of the arguments is JSON.
            return DataTypes.JsonType;
        }

        // test can compare as dates
        boolean canCompareAsDate;
        if (MySQLStandardFieldType.isTemporalTypeWithDate(lFieldType)) {
            if (MySQLStandardFieldType.isTemporalTypeWithDate(rFieldType)) {
                //  date[time] + date
                canCompareAsDate = true;
            } else if (rResultType == STRING_RESULT) {
                // date[time] + string
                canCompareAsDate = true;
            } else {
                // date[time] + number
                canCompareAsDate = false;
            }
        } else if (MySQLStandardFieldType.isTemporalTypeWithDate(rFieldType)
            && lResultType == STRING_RESULT) {
            // string + date[time]
            canCompareAsDate = true;
        } else {
            // No date[time] items found
            canCompareAsDate = false;
        }
        if (canCompareAsDate) {
            return DateTimeType.DATE_TIME_TYPE_6;
        }

        if ((cmpType == STRING_RESULT || cmpType == REAL_RESULT)
            && lFieldType == MYSQL_TYPE_TIME && rFieldType == MYSQL_TYPE_TIME) {
            // When comparing time field
            // Compare TIME values as integers.
            return TimeType.TIME_TYPE_6;
        }

        if (cmpType == STRING_RESULT
            && lResultType == STRING_RESULT
            && rResultType == STRING_RESULT) {
            // we will set collation to function.
            return VarcharType.DEFAULT_COLLATION_VARCHAR_TYPE;
        }

        // fix cmp type
        if (cmpType == REAL_RESULT
            && (lResultType == DECIMAL_RESULT && rResultType == STRING_RESULT)
            || (rResultType == DECIMAL_RESULT && lResultType == STRING_RESULT)) {
            cmpType = DECIMAL_RESULT;
        }

        DataType tmpResType = CMP_TYPE_MAP.get(cmpType);

        switch (cmpType) {
        case STRING_RESULT:
            // we don't handle collation there.
        case DECIMAL_RESULT:
        case REAL_RESULT:
            break;
        case INT_RESULT: {
            if (MySQLStandardFieldType.isTemporalType(lFieldType)
                && MySQLStandardFieldType.isTemporalType(rFieldType)) {
                tmpResType = TimeType.TIME_TYPE_6;
            } else if (leftType.isUnsigned() || rightType.isUnsigned()) {
                tmpResType = DataTypes.ULongType;
            }
            break;
        }
        }

        return tmpResType;
    }

    private static MySQLResultType cmpType(DataType leftType, DataType rightType) {
        MySQLResultType resType;
        MySQLResultType l = leftType.fieldType().toResultType();
        MySQLResultType r = rightType.fieldType().toResultType();

        if (l == STRING_RESULT && r == STRING_RESULT) {
            resType = STRING_RESULT;
        } else if (l == INT_RESULT && r == INT_RESULT) {
            resType = MySQLResultType.INT_RESULT;
        } else if ((l == INT_RESULT || l == DECIMAL_RESULT)
            && (r == INT_RESULT || r == DECIMAL_RESULT)) {
            resType = MySQLResultType.DECIMAL_RESULT;
        } else {
            resType = MySQLResultType.REAL_RESULT;
        }
        return resType;
    }
}
