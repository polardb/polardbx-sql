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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.SqlSubStrFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.function.udf.UdfJavaFunctionHelper;
import com.alibaba.polardbx.optimizer.partition.datatype.function.udf.UdfJavaFunctionMeta;
import com.alibaba.polardbx.optimizer.partition.datatype.function.udf.UdfPartitionIntFunctionTemplate;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlSubstringFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author chenghui.lch
 */
public class PartitionFunctionBuilder {

    protected static Set<String> supportedBuiltInFunctions = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    protected static Set<PartitionStrategy> strategiesOfAllowedUsingPartFunc = new HashSet<PartitionStrategy>();

    static {

        supportedBuiltInFunctions.add(TddlOperatorTable.YEAR.getName());
        supportedBuiltInFunctions.add(TddlOperatorTable.TO_DAYS.getName());
        supportedBuiltInFunctions.add(TddlOperatorTable.TO_SECONDS.getName());
        supportedBuiltInFunctions.add(TddlOperatorTable.TO_MONTHS.getName());
        supportedBuiltInFunctions.add(TddlOperatorTable.TO_WEEKS.getName());
        supportedBuiltInFunctions.add(TddlOperatorTable.UNIX_TIMESTAMP.getName());
        supportedBuiltInFunctions.add(TddlOperatorTable.MONTH.getName());
        supportedBuiltInFunctions.add(TddlOperatorTable.DAYOFMONTH.getName());
        supportedBuiltInFunctions.add(TddlOperatorTable.DAYOFWEEK.getName());
        supportedBuiltInFunctions.add(TddlOperatorTable.DAYOFYEAR.getName());
        /**
         *  temporarily close the 'weekofyear' partition int function
         * */
        supportedBuiltInFunctions.add(TddlOperatorTable.WEEKOFYEAR.getName());
        supportedBuiltInFunctions.add(TddlOperatorTable.SUBSTR.getName());
        supportedBuiltInFunctions.add(TddlOperatorTable.SUBSTRING.getName());

        strategiesOfAllowedUsingPartFunc.add(PartitionStrategy.HASH);
        strategiesOfAllowedUsingPartFunc.add(PartitionStrategy.RANGE);
        strategiesOfAllowedUsingPartFunc.add(PartitionStrategy.LIST);
        strategiesOfAllowedUsingPartFunc.add(PartitionStrategy.UDF_HASH);
    }

    public static Set<String> getAllSupportedPartitionFunctions() {
        return supportedBuiltInFunctions;
    }

    public static PartitionIntFunction create(SqlOperator sqlOperator, List<SqlNode> operands) {
        return createPartitionFunction(sqlOperator, operands);
    }

    protected static PartitionIntFunction createPartitionFunction(SqlOperator sqlOperator,
                                                                  List<SqlNode> operands) {
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
            PartitionFunctionMeta partFuncMata = createPartitionFunctionMeta(sqlOperator, operands);
            PartitionFunctionProxy funcProxy = new PartitionFunctionProxy(partFuncMata);
            return funcProxy;
        } else if (sqlOperator instanceof SqlUserDefinedFunction) {
            PartitionFunctionMeta partFuncMata = createPartitionFunctionMeta(sqlOperator, operands);
            PartitionFunctionProxy funcProxy = new PartitionFunctionProxy(partFuncMata);
            return funcProxy;
        }
        return null;
    }

    protected static PartitionFunctionMeta createPartitionFunctionMeta(SqlOperator sqlOperator,
                                                                       List<SqlNode> operands) {
        if (sqlOperator instanceof SqlSubStrFunction || sqlOperator instanceof SqlSubstringFunction) {
            SubStrPartitionFunction partFunc = new SubStrPartitionFunction();
            List<DataType> fullParamDatatypes = new ArrayList<>();
            fullParamDatatypes.add(DataTypes.StringType);
            fullParamDatatypes.add(DataTypes.LongType);
            fullParamDatatypes.add(DataTypes.LongType);
            PartitionFunctionMeta subStrPartFuncMeta =
                new PartitionFunctionMetaImpl(partFunc, sqlOperator, operands, fullParamDatatypes);
            return subStrPartFuncMeta;
        } else if (sqlOperator instanceof SqlUserDefinedFunction || sqlOperator instanceof SqlUnresolvedFunction) {
            String udfName = sqlOperator.getName();
            UdfJavaFunctionMeta udfJavaFuncMeta =
                UdfJavaFunctionHelper.createUdfJavaFunctionMetaByName(udfName, sqlOperator);
            UdfPartitionIntFunctionTemplate partFuncTemp = new UdfPartitionIntFunctionTemplate(udfJavaFuncMeta);
            PartitionFunctionMeta udfFuncMeta = new PartitionFunctionMetaImpl(partFuncTemp, sqlOperator, operands);
            return udfFuncMeta;
        }
        return null;
    }

    /**
     * Create a part func by the sqlcall ast like "xxx_fn(part_col, param1, param2, ....)"
     */
    public static PartitionIntFunction createPartFuncByPartFuncCal(SqlNode partFuncCall) {
        if (partFuncCall instanceof SqlCall) {
            SqlCall call = (SqlCall) partFuncCall;
            SqlOperator operator = call.getOperator();
            List<SqlNode> operands = call.getOperandList();
            PartitionIntFunction partIntFunc = createPartitionFunction(operator, operands);
            return partIntFunc;
        }
        return null;
    }

    /**
     * Get the sqlcall ast like "xxx_fn(part_col, param1, param2, ....)" by partInfo and partLevel
     */
    public static SqlCall getPartFuncCall(PartKeyLevel level, int partKeyIndex, PartitionInfo partInfo) {
        List<SqlNode> partColExprList = new ArrayList<>();
        if (level == PartKeyLevel.PARTITION_KEY) {
            partColExprList = partInfo.getPartitionBy().getPartitionExprList();
        } else if (level == PartKeyLevel.SUBPARTITION_KEY) {
            partColExprList = partInfo.getPartitionBy().getSubPartitionBy().getPartitionExprList();
        }
        SqlNode partKeyExpr = partColExprList.get(partKeyIndex);
        if (partKeyExpr instanceof SqlIdentifier) {
            // The part col is only
            // so ignore.
            return null;
        } else if (partKeyExpr instanceof SqlCall) {
            SqlCall partKeyExprSqlCall = (SqlCall) partKeyExpr;

            return partKeyExprSqlCall;
        } else {
            throw new NotSupportException("should not be here");
        }
    }

    public static boolean isBuildInPartFunc(PartitionIntFunction partFunc) {
        return isBuildInPartFuncName(partFunc.getSqlOperator().getName());
    }

    public static boolean isBuildInPartFuncName(String partFuncName) {
        return supportedBuiltInFunctions.contains(partFuncName);
    }

    public static boolean checkStrategySupportPartFunc(PartitionStrategy strategy) {
        return strategiesOfAllowedUsingPartFunc.contains(strategy);
    }

    public static void validatePartitionFunction(String partFuncName) {

        /**
         * Check if a built-in partition function
         */
        boolean isBuiltInPartFunc = isBuildInPartFuncName(partFuncName);
        if (isBuiltInPartFunc) {
            return;
        }

        /**
         * Check if the udf java function exists
         */
        boolean isUdfExists = UdfJavaFunctionHelper.checkIfUdfJavaFunctionExists(partFuncName);
        if (!isUdfExists) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("user-defined function %s does not exists", partFuncName));
        }

        /**
         * Check if the user-defined function is allowed using as partition function
         */
        checkIfUdfAllowedUsingAsPartFunc(partFuncName);

    }

    /**
     * Check if the dataTypes of partition column match the input datatypes of user-defined function
     */
    public static void checkIfPartColDataTypesMatchUdfInputDataTypes(SqlNode partFuncCallAst,
                                                                     List<DataType> partColDataTypes) {
        if (!(partFuncCallAst instanceof SqlCall)) {
            return;
        }

        SqlCall call = (SqlCall) partFuncCallAst;
        SqlOperator operator = call.getOperator();
        List<SqlNode> operands = call.getOperandList();
        String udfPartFuncName = operator.getName();
        if (PartitionFunctionBuilder.isBuildInPartFuncName(udfPartFuncName)) {
            return;
        }

        PartitionFunctionMeta partFuncMeta = createPartitionFunctionMeta(operator, operands);
        List<Integer> partColPositions = partFuncMeta.getPartColInputPositions();
        if (partColPositions.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("No found any partition columns as input of partition function %s", udfPartFuncName));
        }

        if (partColDataTypes.size() != partColPositions.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("The partition column count mismatch the partition function %s", udfPartFuncName));
        }

        List<DataType> allInputDataTypes = partFuncMeta.getInputDataTypes();
        for (int i = 0; i < partColDataTypes.size(); i++) {
            DataType partColType = partColDataTypes.get(i);
            Integer pos = partColPositions.get(i);
            DataType inputTypeOfFunc = allInputDataTypes.get(pos);

            if (!DataTypeUtil.equalsSemantically(partColType, inputTypeOfFunc)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format(
                        "The partition column datatype [%s] mismatch %s-th input datatype [%s] the partition function %s",
                        partColType.getStringSqlType(),
                        pos,
                        inputTypeOfFunc.getStringSqlType(),
                        udfPartFuncName));
            }
        }
        return;
    }

    /**
     * Check if the user-defined function is allowed using as partition function
     */
    public static boolean checkIfUdfAllowedUsingAsPartFunc(String udfFuncName) {
        SqlOperator udfFuncOp = UdfJavaFunctionHelper.getUdfFuncOperator(udfFuncName);

        if (udfFuncOp == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("No found operator for the user-defined function %s", udfFuncName));
        }

        UdfJavaFunctionMeta udfJavaFuncMeta =
            UdfJavaFunctionHelper.createUdfJavaFunctionMetaByName(udfFuncName, udfFuncOp);

        /**
         * Check udfFunc state
         */

        DataType outputDataType = udfJavaFuncMeta.getOutputDataType();
        List<DataType> inputDataTypes = udfJavaFuncMeta.getInputDataTypes();

        if (!DataTypeUtil.isUnderBigintType(outputDataType)) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("The output datatype of user-defined function[%s] is not allowed in partition function",
                    udfFuncName));
        }

        for (int i = 0; i < inputDataTypes.size(); i++) {
            DataType inputDataType = inputDataTypes.get(i);

            boolean isIntClass =
                DataTypeUtil.isUnderBigintType(inputDataType) || DataTypeUtil.isBigintUnsigned(inputDataType);
            boolean isTimeClass =
                DataTypeUtil.isDateType(inputDataType) && !DataTypeUtil.isTimezoneDependentType(inputDataType);
            boolean isStrClass = DataTypeUtil.isStringSqlType(inputDataType);

            if (!(isIntClass || isTimeClass || isStrClass)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format(
                        "The input datatypes of user-defined function[%s] is not allowed in partition function",
                        udfFuncName));
            }
        }

        return true;
    }

    /**
     * Check if the partition column datatypes are allowed using as input of user-defined partition function
     * or on UDF_HASH partition strategy
     */
    public static boolean checkIfPartColDataTypesAllowedUsingOnUdfPartitionFunction(List<ColumnMeta> partColMetas) {
        for (int i = 0; i < partColMetas.size(); i++) {
            ColumnMeta partColMeta = partColMetas.get(i);
            String colName = partColMeta.getName();
            DataType partColDataType = partColMeta.getDataType();
            boolean isNullable = partColMeta.isNullable();

            boolean isIntClass =
                DataTypeUtil.isUnderBigintType(partColDataType) || DataTypeUtil.isBigintUnsigned(partColDataType);
            boolean isTimeClass =
                DataTypeUtil.isDateType(partColDataType) && !DataTypeUtil.isTimezoneDependentType(partColDataType);
            boolean isStrClass = DataTypeUtil.isStringSqlType(partColDataType);

            if (!(isIntClass || isTimeClass || isStrClass)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format(
                        "The datatype[%s] of partition column [%s] is not allow to use as input of partition function",
                        partColDataType.getStringSqlType(), colName));
            }

            if (isNullable) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format(
                        "The nullable partition column [%s] is not allowed using as input of partition function",
                        colName));
            }
        }
        return true;
    }

}
