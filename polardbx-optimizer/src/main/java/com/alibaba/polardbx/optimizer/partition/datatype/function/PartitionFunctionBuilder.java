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

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlSubstringFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.jetbrains.annotations.NotNull;

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
    protected static Set<String> supportedBuiltInStringFamilyPartFunctions =
        new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

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
        supportedBuiltInFunctions.add(TddlOperatorTable.RIGHT.getName());
        supportedBuiltInFunctions.add(TddlOperatorTable.LEFT.getName());

        strategiesOfAllowedUsingPartFunc.add(PartitionStrategy.HASH);
        strategiesOfAllowedUsingPartFunc.add(PartitionStrategy.RANGE);
        strategiesOfAllowedUsingPartFunc.add(PartitionStrategy.LIST);
        strategiesOfAllowedUsingPartFunc.add(PartitionStrategy.UDF_HASH);
        strategiesOfAllowedUsingPartFunc.add(PartitionStrategy.CO_HASH);

        supportedBuiltInStringFamilyPartFunctions.add(TddlOperatorTable.SUBSTR.getName());
        supportedBuiltInStringFamilyPartFunctions.add(TddlOperatorTable.SUBSTRING.getName());
        supportedBuiltInStringFamilyPartFunctions.add(TddlOperatorTable.RIGHT.getName());
        supportedBuiltInStringFamilyPartFunctions.add(TddlOperatorTable.LEFT.getName());
    }

    public static boolean isStringFamilyPartitionFunction(String partFunOpName) {
        return supportedBuiltInStringFamilyPartFunctions.contains(partFunOpName);
    }

    public static Set<String> getAllSupportedPartitionFunctions() {
        return supportedBuiltInFunctions;
    }

    public static PartitionIntFunction create(SqlOperator sqlOperator, List<SqlNode> operands) {
        return createPartitionFunction(sqlOperator, operands, null);
    }

    public static PartitionIntFunction create(SqlOperator sqlOperator, List<SqlNode> operands,
                                              List<ColumnMeta> partColMetas) {
        return createPartitionFunction(sqlOperator, operands, partColMetas);
    }

    protected static PartitionIntFunction createPartitionFunction(SqlOperator sqlOperator,
                                                                  List<SqlNode> operands,
                                                                  List<ColumnMeta> partColMetas) {
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
        } else if (supportedBuiltInFunctions.contains(sqlOperator.getName())) {
            PartitionFunctionMeta partFuncMata = createPartitionFunctionMeta(sqlOperator, operands, partColMetas);
            PartitionFunctionProxy funcProxy = new PartitionFunctionProxy(partFuncMata);
            return funcProxy;
        } else if (sqlOperator instanceof SqlUserDefinedFunction) {
            PartitionFunctionMeta partFuncMata = createPartitionFunctionMeta(sqlOperator, operands, partColMetas);
            PartitionFunctionProxy funcProxy = new PartitionFunctionProxy(partFuncMata);
            return funcProxy;
        }
        return null;
    }

    protected static PartitionFunctionMeta createPartitionFunctionMeta(SqlOperator sqlOperator,
                                                                       List<SqlNode> operands,
                                                                       List<ColumnMeta> partColMetas) {
        if (sqlOperator instanceof SqlSubStrFunction || sqlOperator instanceof SqlSubstringFunction) {
            SubStrPartitionFunction partFunc = new SubStrPartitionFunction();
            List<DataType> fullParamDatatypes =
                prepareParamsDatatypesForStringFamilyPartFunc(sqlOperator, operands, partColMetas, partFunc);
            PartitionFunctionMeta subStrPartFuncMeta =
                new PartitionFunctionMetaImpl(partFunc, sqlOperator, operands, fullParamDatatypes);
            return subStrPartFuncMeta;
        } else if (sqlOperator instanceof SqlFunction && (sqlOperator.getName().equalsIgnoreCase("RIGHT"))) {
            if (operands.size() != 2) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("Only two operands are allowed on partition function %s",
                        sqlOperator.getName()));
            }
            SqlNode lenAst = operands.get(1);
            if (!(lenAst instanceof SqlNumericLiteral)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("The length operand must be an integer on partition function %s",
                        sqlOperator.getName()));
            }
            SqlNumericLiteral lenNum = (SqlNumericLiteral) lenAst;
            Long lenNumLongVal = lenNum.longValue(false);
            if (lenNumLongVal <= 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("The length operand must be more than 1 on partition function %s",
                        sqlOperator.getName()));
            }
//            SqlNumericLiteral newPostNum =
//                SqlLiteral.createLiteralForIntTypes(-1 * lenNumLongVal, SqlParserPos.ZERO, SqlTypeName.INTEGER);
//            List<SqlNode> newOps = new ArrayList<>();
//            newOps.add(operands.get(0));
//            newOps.add(newPostNum);
//            List<DataType> fullParamDatatypes =
//                prepareParamsDatatypesForStringFamilyPartFunc(partFunc.getSqlOperator(), newOps, partColMetas, partFunc);
//            PartitionFunctionMeta subStrPartFuncMeta =
//                new PartitionFunctionMetaImpl(partFunc, partFunc.getSqlOperator(), newOps, fullParamDatatypes);
//            return subStrPartFuncMeta;

            RightPartitionFunction partFunc = new RightPartitionFunction(null, null);
            List<DataType> fullParamDatatypes =
                prepareParamsDatatypesForStringFamilyPartFunc(partFunc.getSqlOperator(), operands, partColMetas,
                    partFunc);
            PartitionFunctionMeta rightPartFuncMeta =
                new PartitionFunctionMetaImpl(partFunc, sqlOperator, operands, fullParamDatatypes);
            return rightPartFuncMeta;
        } else if (sqlOperator instanceof SqlFunction && (sqlOperator.getName().equalsIgnoreCase("LEFT"))) {
            if (operands.size() != 2) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("Only two operands are allowed on partition function %s",
                        sqlOperator.getName()));
            }
            SqlNode lenAst = operands.get(1);
            if (!(lenAst instanceof SqlNumericLiteral)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("The second operand must be an integer on partition function %s",
                        sqlOperator.getName()));
            }
            SqlNumericLiteral lenNum = (SqlNumericLiteral) lenAst;
            Long lenNumLongVal = lenNum.longValue(false);
            if (lenNumLongVal <= 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("The length operand must be more than 1 on partition function %s",
                        sqlOperator.getName()));
            }
//            SqlNumericLiteral newPostNum = SqlNumericLiteral.createExactNumeric("1", SqlParserPos.ZERO);
//            List<SqlNode> newOps = new ArrayList<>();
//            newOps.add(operands.get(0));
//            newOps.add(newPostNum);
//            newOps.add(lenNum);
//            List<DataType> fullParamDatatypes =
//                prepareParamsDatatypesForStringFamilyPartFunc(partFunc.getSqlOperator(), newOps, partColMetas, partFunc);
//            PartitionFunctionMeta subStrPartFuncMeta =
//                new PartitionFunctionMetaImpl(partFunc, partFunc.getSqlOperator(), newOps, fullParamDatatypes);
//            return subStrPartFuncMeta;

            LeftPartitionFunction partFunc = new LeftPartitionFunction(null, null);
            List<DataType> fullParamDatatypes =
                prepareParamsDatatypesForStringFamilyPartFunc(partFunc.getSqlOperator(), operands, partColMetas,
                    partFunc);
            PartitionFunctionMeta leftPartFuncMeta =
                new PartitionFunctionMetaImpl(partFunc, sqlOperator, operands, fullParamDatatypes);
            return leftPartFuncMeta;
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

    @NotNull
    private static List<DataType> prepareParamsDatatypesForStringFamilyPartFunc(SqlOperator sqlOperator,
                                                                                List<SqlNode> operands,
                                                                                List<ColumnMeta> partColMetas,
                                                                                PartitionIntFunction partFunc) {
        List<DataType> fullParamDatatypes = new ArrayList<>();
        ColumnMeta partCm = null;
        partCm = findPartColMetaFromFirstFuncOperand(operands, partColMetas, partCm);
        if (partCm == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("No found any partition columns as input of partition function %s",
                    sqlOperator.getName()));
        }
        DataType rsFldDt = partCm.getDataType();
        if (DataTypeUtil.isStringSqlType(rsFldDt)) {
            /**
             * For the columns of string type such as varchar/char,
             * the result datatype and collation must be the same as the columns defined
             */
            Field rsFld = new Field(partCm.getField().getRelType());
            CollationName collationName = CollationName.findCollationName(partCm.getField().getCollationName());
            if (collationName == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("Collation %s is not support used on partition columns",
                        partCm.getField().getCollationName()));
            }
            partFunc.setCollation(collationName);
            partFunc.setResultField(rsFld);
            fullParamDatatypes.add(rsFldDt);
        } else if (DataTypeUtil.isUnderBigintType(rsFldDt) || DataTypeUtil.isDecimalType(rsFldDt)) {
            /**
             * For the columns of non-string type such as bigint/int/mediumint/tinyint/decimal,
             * the input datatype and collation must be always be the utf8_general_ci collation
             */
            fullParamDatatypes.add(DataTypes.VarcharType);
            partFunc.setResultField(null);
        } else {
            /**
             * Not Supported datatypes
             */
        }

        if (partFunc.getSqlOperator() == TddlOperatorTable.SUBSTR) {
            /**
             *  SUBSTR(str,pos,len) or SUBSTR(str FROM pos FOR len)
             */

            /**
             * datatype for pos params
             */
            fullParamDatatypes.add(DataTypes.LongType);

            /**
             * datatype for len params
             */
            fullParamDatatypes.add(DataTypes.LongType);
        } else if (partFunc.getSqlOperator() == TddlOperatorTable.RIGHT) {
            /**
             *  RIGHT(str,len)
             */

            /**
             * datatype for len params
             */
            fullParamDatatypes.add(DataTypes.LongType);
        } else if (partFunc.getSqlOperator() == TddlOperatorTable.LEFT) {
            /**
             *  LEFT(str,len)
             */

            /**
             * datatype for len params
             */
            fullParamDatatypes.add(DataTypes.LongType);
        } else {
            /**
             * do nothings
             */
            throw new NotSupportException("unknown partition function");
        }

        return fullParamDatatypes;
    }

    private static ColumnMeta findPartColMetaFromFirstFuncOperand(List<SqlNode> operands, List<ColumnMeta> partColMetas,
                                                                  ColumnMeta partCm) {
        if (operands == null || operands.isEmpty()) {
            return null;
        }
        SqlNode colAst = operands.get(0);
        if (colAst instanceof SqlIdentifier) {
            SqlIdentifier colId = (SqlIdentifier) colAst;
            String colNameStr = SQLUtils.normalizeNoTrim(colId.getLastName());
            for (int i = 0; i < partColMetas.size(); i++) {
                ColumnMeta cm = partColMetas.get(i);
                String fldName = cm.getName();
                if (fldName.equalsIgnoreCase(colNameStr)) {
                    partCm = cm;
                    break;
                }
            }
        }
        return partCm;
    }

    /**
     * Create a part func by the sqlcall ast like "xxx_fn(part_col, param1, param2, ....)"
     */
    public static PartitionIntFunction createPartFuncByPartFuncCal(SqlNode partFuncCall,
                                                                   List<ColumnMeta> partColMetas) {
        if (partFuncCall instanceof SqlCall) {
            SqlCall call = (SqlCall) partFuncCall;
            SqlOperator operator = call.getOperator();
            List<SqlNode> operands = call.getOperandList();
            PartitionIntFunction partIntFunc = createPartitionFunction(operator, operands, partColMetas);
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
                                                                     PartitionStrategy partStrategy,
                                                                     List<ColumnMeta> partColMetas) {
        List<DataType> partColDataTypes = new ArrayList<>();
        for (int i = 0; i < partColMetas.size(); i++) {
            partColDataTypes.add(partColMetas.get(i).getDataType());
        }

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

        PartitionFunctionMeta partFuncMeta = createPartitionFunctionMeta(operator, operands, partColMetas);
        List<Integer> partColPositions = partFuncMeta.getPartColInputPositions();
        if (partColPositions.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("No found any partition columns as input of partition function %s", udfPartFuncName));
        }

        if (partStrategy != PartitionStrategy.CO_HASH) {

            if (partColDataTypes.size() != partColPositions.size()) {
                /**
                 * Not supported for such as partition by hash (func(c1,c2,c3)) but part cols are c1,c2
                 */
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
        } else {
            /**
             * To Be check for co_hash
             */
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

            boolean isIntClass = DataTypeUtil.isUnderBigintUnsignedType(partColDataType);
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
