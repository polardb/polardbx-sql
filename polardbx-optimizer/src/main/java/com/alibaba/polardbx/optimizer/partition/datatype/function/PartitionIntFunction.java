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

package com.alibaba.polardbx.optimizer.partition.datatype.function;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.function.SqlSubStrFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlSubstringFunction;

import java.util.Arrays;
import java.util.List;

import java.util.List;

public abstract class PartitionIntFunction extends AbstractScalarFunction {
    public PartitionIntFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, DataTypes.LongType);
    }

    protected static final long SINGED_MIN_LONG = -0x7fffffffffffffffL - 1;

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return null;
    }

    /**
     * Get the monotonicity of function according to the field type.
     */
    public abstract Monotonicity getMonotonicity(DataType<?> fieldType);

    public abstract SqlOperator getSqlOperator();

    /**
     * The intervalType for enumerating partitions when do the partition pruning
     */
    public abstract MySQLIntervalType getIntervalType();

    /**
     * Return integer representation of expression.
     */
    abstract public long evalInt(PartitionField partitionField, SessionProperties sessionProperties);

    /**
     * Convert "func_arg $CMP$ const" half-interval into "FUNC(func_arg) $CMP2$ const2"
     * <p>
     * SYNOPSIS
     * val_int_endpoint()
     * left_endp  FALSE  <=> The interval is "x < const" or "x <= const"
     * TRUE   <=> The interval is "x > const" or "x >= const"
     * <p>
     * incl_endp  IN   FALSE <=> the comparison is '<' or '>'
     * TRUE  <=> the comparison is '<=' or '>='
     * OUT  The same but for the "F(x) $CMP$ F(const)" comparison
     * <p>
     * DESCRIPTION
     * This function is defined only for unary monotonic functions. The caller
     * supplies the source half-interval
     * <p>
     * x $CMP$ const
     * <p>
     * The value of const is supplied implicitly as the value this item's
     * argument, the form of $CMP$ comparison is specified through the
     * function's arguments. The calle returns the result interval
     * <p>
     * F(x) $CMP2$ F(const)
     * <p>
     * passing back F(const) as the return value, and the form of $CMP2$
     * through the out parameter. NULL values are assumed to be comparable and
     * be less than any non-NULL values.
     *
     * @param endpoints left_endp = endpoints[0], incl_endp = endpoints[1].
     * @return The output range bound, which equal to the value of val_int(). If the value of the function is NULL then the bound is the smallest possible value of LLONG_MIN
     */
    abstract public long evalIntEndpoint(PartitionField partitionField, SessionProperties sessionProperties,
                                         boolean[] endpoints);

    /**
     * eval object by the input of full params fields
     */
    public Object evalEndpoint(List<PartitionField> allParamsFields,
                               SessionProperties sessionProperties,
                               boolean[] booleans) {
        throw new NotSupportException();
    }

    public static PartitionIntFunction create(final SqlOperator sqlOperator) {
        if (sqlOperator == TddlOperatorTable.YEAR) {
            return new YearPartitionIntFunction(null, null);
        } else if (sqlOperator == TddlOperatorTable.DAYOFMONTH) {
            return new DayOfMonthPartitionIntFunction(null, null);
        } else if (sqlOperator == TddlOperatorTable.DAYOFWEEK) {
            return new DayOfWeekPartitionIntFunction(null, null);
        } else if (sqlOperator == TddlOperatorTable.DAYOFYEAR) {
            return new DayOfYearPartitionIntFunction(null, null);
        } else if (sqlOperator == TddlOperatorTable.WEEKOFYEAR) {
            return new WeekOfYearPartitionIntFunction(null, null);
        } else if (sqlOperator == TddlOperatorTable.TO_DAYS) {
            return new ToDaysPartitionIntFunction(null, null);
        } else if (sqlOperator == TddlOperatorTable.TO_MONTHS) {
            return new ToMonthsPartitionIntFunction(null, null);
        } else if (sqlOperator == TddlOperatorTable.TO_WEEKS) {
            return new ToWeeksPartitionIntFunction(null, null);
        } else if (sqlOperator == TddlOperatorTable.TO_SECONDS) {
            return new ToSecondsPartitionIntFunction(null, null);
        } else if (sqlOperator == TddlOperatorTable.UNIX_TIMESTAMP) {
            return new UnixTimestampPartitionIntFunction(null, null);
        } else if (sqlOperator == TddlOperatorTable.MONTH) {
            return new MonthPartitionIntFunction(null, null);
        } else if (sqlOperator instanceof SqlSubStrFunction) {
            SubStrPartitionFunction partFunc = new SubStrPartitionFunction(null, null);
            partFunc.setPosition(((SqlSubStrFunction) sqlOperator).getPosition());
            return partFunc;
        }
        return null;
    }

    public static PartitionIntFunction create(SqlOperator sqlOperator, List<SqlNode> operands) {
        if (sqlOperator instanceof SqlSubStrFunction || sqlOperator instanceof SqlSubstringFunction) {
            SubStrPartitionFunction partFunc = new SubStrPartitionFunction(null, null);
            if (operands.size() >= 2 && operands.get(1) instanceof SqlLiteral) {
                partFunc.setPosition(((SqlLiteral) operands.get(1)).intValue(true));
                if (operands.size() >= 3 && operands.get(2) instanceof SqlLiteral) {
                    partFunc.setLength(((SqlLiteral) operands.get(2)).intValue(true));
                }
                return partFunc;
            }
            return null;
        } else {
            return create(sqlOperator);
        }
    }

    /**
     * get the full params fields of current partition function defined in partitionBy
     * <pre>
     *      e.g
     *     partitionFunction: SUBSTR (strObj, pos, len)
     *
     *      ddl: create table tbl ( id ...)
     *           partition by hash(substr(str_col, 1, 3)) partitions 8
     *
     *     query: select * from tbl where id=1000
     *
     *     then:
     *
     *      the result of getFullParamsByPartColFields is
     *          1000,
     *          1,
     *          3
     * </pre>
     */
    public List<PartitionField> getFullParamsByPartColFields(List<PartitionField> partColFields) {
        return partColFields;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[] {getSqlOperator()});
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof PartitionIntFunction)) {
            return false;
        }
        PartitionIntFunction otherFn = (PartitionIntFunction) obj;
        if (!this.getSqlOperator().equals(otherFn.getSqlOperator())) {
            return false;
        }
        return true;
    }

}
