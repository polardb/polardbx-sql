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

package com.alibaba.polardbx.optimizer.core;

import com.alibaba.polardbx.optimizer.core.function.AddTimeFunction;
import com.alibaba.polardbx.optimizer.core.function.FunctionWithVariadicArg;
import com.alibaba.polardbx.optimizer.core.function.FunctionWithoutArg;
import com.alibaba.polardbx.optimizer.core.function.MySQLMatchAgainst;
import com.alibaba.polardbx.optimizer.core.function.SqlBitAndFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlBitOrFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlBitXorFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlConcatFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlDateDiffFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlDateFormatFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlDateManipulationFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlDayOfYearFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlFunctionWithOneStringArg;
import com.alibaba.polardbx.optimizer.core.function.SqlGroupConcatFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlHyperLogLogFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlIfFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlIfNullFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlLocateFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlNumericConvFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlNumericCrc32Function;
import com.alibaba.polardbx.optimizer.core.function.SqlNumericOneFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlPadFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlRepeatFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlReplaceFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlReverseFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlSequenceFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlSpaceFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlStrCmpFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlSubStrFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlTimeStampFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlTrimFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlValuesFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlWeekYearFunction;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexHintOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.fun.SqlNoParameterTimeFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.MySQLStandardTypeInference;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Set;

/**
 * 在 MySQL 中执行以下 SQL, 可以查看对应函数的说明
 *
 * <pre>
 *     If you add a operator, check the Precedence at
 *     https://dev.mysql.com/doc/refman/5.7/en/operator-precedence.html
 * </pre>
 *
 * <pre>
 *     SELECT * from help_topic JOIN help_category
 *     ON help_category.help_category_id = help_topic.help_category_id
 *     WHERE help_category.help_category_id IN (
 *      SELECT help_category_id FROM help_category WHERE parent_category_id in (38,39) )
 *      and help_topic.name like 'BINARY OPERATOR%';
 * </pre>
 *
 * @author lingce.ldm 2017-10-26 15:58
 */
public class TddlOperatorTable extends SqlStdOperatorTable {

    private static volatile TddlOperatorTable instance;

    /**
     * Returns the TDDL operator table, creating it if necessary.
     */
    public static TddlOperatorTable instance() {
        if (instance == null) {
            synchronized (TddlOperatorTable.class) {
                if (instance == null) {
                    // Creates and initializes the TDDL operator table.
                    // Uses two-phase construction, because we can't initialize
                    // the
                    // table until the constructor of the sub-class has
                    // completed.
                    instance = new TddlOperatorTable();
                    instance.init();
                }
            }
        }
        return instance;
    }

    /**
     * unsupported operators
     */
    public final Set<SqlOperator> UNSUPPORTED_OPERATORS = ImmutableSet.of(
        IS_DISTINCT_FROM
    );

    /**
     * ! != % & << <=> >>
     */
    public static SqlFunction ADDDATE = new SqlDateManipulationFunction("ADDDATE");
    public static SqlFunction ADDTIME = new AddTimeFunction();

    public static SqlFunction AES_DECRYPT = new SqlFunction("AES_DECRYPT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.STRING_STRING_OPTIONAL_STRING,
        SqlFunctionCategory.STRING);

    public static SqlFunction AES_ENCRYPT = new SqlFunction("AES_ENCRYPT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_BINARY,
        InferTypes.FIRST_KNOWN,
        OperandTypes.STRING_STRING_OPTIONAL_STRING,
        SqlFunctionCategory.STRING);

    // TODO: ANY_VALUE

    public static SqlFunction ASCII = new SqlFunction("ASCII",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.STRING,
        SqlFunctionCategory.SYSTEM);

    public static final SqlBinaryOperator ASSIGNMENT =
        new SqlBinaryOperator(
            ":=",
            SqlKind.ASSIGNMENT,
            30,
            true,
            ReturnTypes.VARCHAR_2000,
            InferTypes.VARCHAR_2000,
            OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

    // Todo: BENCHMARK

    public static SqlFunction BIN = new SqlFunction("BIN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_BINARY,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.STRING);

    // Todo: BINARY OPERATOR: The BINARY operator converts the expression to a
    // binary string.
//    public static SqlFunction BIT_AND = new SqlFunction("BIT_AND",
//        SqlKind.OTHER_FUNCTION,
//        ReturnTypes.BIGINT_UNSIGNED_NULLABLE,
//        InferTypes.BIGINT,
//        OperandTypes.family(SqlTypeFamily.ANY),
//        SqlFunctionCategory.NUMERIC);

    public static SqlBinaryOperator BITWISE_XOR = new SqlMonotonicBinaryOperator("^",
        SqlKind.OTHER_FUNCTION,
        50,
        true,
        ReturnTypes.BIGINT_UNSIGNED_NULLABLE,
        InferTypes.BIGINT,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY));
    public static SqlBinaryOperator BITWISE_AND = new SqlMonotonicBinaryOperator("&",
        SqlKind.OTHER_FUNCTION,
        44,
        true,
        ReturnTypes.BIGINT_UNSIGNED_NULLABLE,
        InferTypes.BIGINT,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY));

    public static SqlFunction BIT_COUNT = new SqlFunction("BIT_COUNT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.NUMERIC);

    public static SqlFunction BENCHMARK = new SqlFunction("BENCHMARK",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MINUS_PREFIX = new SqlFunction("MINUS_PREFIX",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.NUMERIC);

    public static SqlFunction BIT_NOT = new SqlFunction("BIT_NOT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.NUMERIC);

    public static SqlFunction BIT_LENGTH = new SqlFunction("BIT_LENGTH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.STRING);
    public static SqlBinaryOperator BITWISE_OR = new SqlBinaryOperator("|",
        SqlKind.OTHER_FUNCTION,
        42,
        true,
        ReturnTypes.BIGINT_UNSIGNED_NULLABLE,
        InferTypes.BIGINT,
        OperandTypes.ANY_ANY);

    public static final SqlFunction CONVERT_TZ = new SqlFunction("CONVERT_TZ",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.CONVERT_TZ,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY_ANY_ANY,
        SqlFunctionCategory.TIMEDATE);

    public static SqlFunction CONNECTION_ID = FunctionWithoutArg.CONNECTION_ID;
    public static SqlFunction TSO_TIMESTAMP = FunctionWithoutArg.TSO_TIMESTAMP;
    public static SqlFunction CEILING = new SqlNumericOneFunction("CEILING",
        ReturnTypes.BIGINT);

    /**
     * TODO: CHAR FUNCTION CHARSET COERCIBILITY COLLATION COMPRESS
     */
    public static SqlFunction CHARSET = new SqlFunction("CHARSET",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM);
    public static SqlFunction COLLATION = new SqlFunction("COLLATION",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM);
    public static SqlFunction CONCAT = new SqlConcatFunction("CONCAT");
    public static SqlFunction CONCAT_WS = new SqlConcatFunction("CONCAT_WS");
    // Todo:CONNECTION_ID
    public static SqlFunction CONV = new SqlNumericConvFunction("CONV");
    // public static SqlFunction CONVERT = new CoronaDBSqlConvertFunction();

    public static SqlFunction COMPRESS = new SqlFunction("COMPRESS",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.STRING);
    public static SqlFunction CRC32 = new SqlNumericCrc32Function("CRC32");
    public static SqlFunction CURDATE = new SqlFunction("CURDATE",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.DATE_0,
        InferTypes.FIRST_KNOWN,
        OperandTypes.NILADIC,
        SqlFunctionCategory.TIMEDATE);
    public static SqlFunction CURTIME = new SqlFunction("CURTIME",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.TIME_WITH_FSP,
        InferTypes.FIRST_KNOWN,
        OperandTypes.or(OperandTypes.NILADIC, OperandTypes.ANY),
        SqlFunctionCategory.TIMEDATE);

    public static SqlFunction CURRENT_USER = new FunctionWithoutArg("CURRENT_USER",
        ReturnTypes.VARCHAR_2000);
    public static SqlFunction DATABASE = FunctionWithoutArg.DATABASE;
    public static SqlFunction DATE = new SqlFunction("DATE",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.DATE_0,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE);
    public static SqlFunction DATEDIFF = new SqlDateDiffFunction();

    public static SqlFunction CHECK_FIREWORKS = FunctionWithoutArg.CHECK_FIREWORKS;

    public static SqlFunction DATE_FORMAT = new SqlDateFormatFunction("DATE_FORMAT");

    public static SqlFunction DAY = new SqlFunction("DAY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE);

    public static SqlFunction DAYNAME = new SqlFunction("DAYNAME",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE);

    // Todo: DAYOFWEEK
    public static SqlFunction DAYOFYEAR = new SqlDayOfYearFunction("DAYOFYEAR");

    public static SqlFunction DATE_SUB = new SqlDateManipulationFunction("DATE_SUB");
    public static SqlFunction DATE_ADD = new SqlDateManipulationFunction("DATE_ADD");
    /**
     * Arithmetic division operator, '<code>/</code>'.
     */
    public static final SqlBinaryOperator DIVIDE_INTEGER = new SqlBinaryOperator("DIV",
        SqlKind.DIVIDE,
        60,
        true,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.DIVISION_OPERATOR);

    /**
     * Prefix arithmetic plus operator, '<code>+</code>'.
     * <p>
     * Its precedence is greater than the infix '{@link #PLUS +}' and '
     * {@link #MINUS -}' operators.
     */
    public static final SqlPrefixOperator UNARY_PLUS = new SqlPrefixOperator("+",
        SqlKind.PLUS_PREFIX,
        80,
        ReturnTypes.ARG0,
        InferTypes.RETURN_TYPE,
        OperandTypes.NUMERIC_OR_INTERVAL);
    // /**
    // * Arithmetic division operator, '<code>/</code>'.
    // */
    // public static final SqlBinaryOperator DIVIDE = new SqlBinaryOperator("/",
    // SqlKind.DIVIDE,
    // 60,
    // true,
    // ReturnTypes.DECIMAL_SCALE0_NULLABLE,
    // InferTypes.FIRST_KNOWN,
    // OperandTypes.DIVISION_OPERATOR);
    public static SqlFunction EXPORTSET = FunctionWithVariadicArg.exportSet;
    /**
     * Todo: DECODE DES_DECRYPT DES_ENCRYPT DIV
     */

    /**
     * ELT ENCODE ENCRYPT EXPORT_SET EXTRACTVALUE
     */
    public static final SqlFunction ELT = new SqlFunction("ELT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.MIX_OF_COLLATION_RETURN_VARCHAR,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.STRING);

    /**
     * FIELD FIND_IN_SET FORMAT FOUND_ROWS
     */

    public static SqlFunction FORMAT = new SqlFunction("FORMAT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY_ANY_OR_ANY_ANY_ANY,
        SqlFunctionCategory.STRING);

    /**
     * FROM_BASE64
     */
    public static SqlFunction FROM_DAYS = new SqlFunction("FROM_DAYS",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.DATE_0,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE);

    public static SqlFunction FROM_UNIXTIME = new SqlTimeStampFunction("FROM_UNIXTIME",
        ReturnTypes.DATETIMEWITHFORMAT);

    public static SqlFunction FIND_IN_SET = new SqlFunction("FIND_IN_SET",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.MIX_OF_COLLATION_RETURN_BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
        SqlFunctionCategory.STRING);
    public static SqlFunction FOUND_ROWS = new FunctionWithoutArg("FOUND_ROWS",
        ReturnTypes.BIGINT);
    public static SqlFunction ROW_COUNT = new FunctionWithoutArg("ROW_COUNT",
        ReturnTypes.BIGINT);
    public static SqlFunction FIELD = new FunctionWithVariadicArg("FIELD",
        ReturnTypes.INTEGER_NULLABLE,
        SqlFunctionCategory.STRING);
    public static SqlFunction FROM_BASE64 = new SqlFunction("FROM_BASE64",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_BINARY,
        InferTypes.RETURN_TYPE,
        OperandTypes.ANY,
        SqlFunctionCategory.STRING);
    /**
     * GET_FORMAT GET_LOCK GREATEST
     */
    public static SqlFunction GET_FORMAT = new SqlFunction("GET_FORMAT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.TIMEDATE);
    public static SqlFunction GET_LOCK = new SqlFunction("GET_LOCK",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER,
        InferTypes.FIRST_KNOWN,
        OperandTypes.STRING_NUMERIC,
        SqlFunctionCategory.SYSTEM);

    /**
     * HEX
     */
    public static SqlFunction HEX = new SqlFunction("HEX",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.NUMERIC);

    public static SqlFunction IF = new SqlIfFunction();
    public static SqlFunction IFNULL = new SqlIfNullFunction();
    public static SqlFunction ISNULL = new SqlFunction("ISNULL",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.VARCHAR_1024,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM);
    public static SqlFunction INTERVAL = new SqlFunction("INTERVAL",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.STRING);

    public static SqlFunction INSTR = new SqlFunction("INSTR",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.MIX_OF_COLLATION_RETURN_BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.STRING);
    public static SqlFunction INSERT = new SqlFunction("INSERT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.MIX_OF_COLLATION_RETURN_VARCHAR,
        InferTypes.FIRST_KNOWN,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY),
        SqlFunctionCategory.STRING);
    public static SqlFunction INTERVAL_PRIMARY = new SqlFunction("INTERVAL_PRIMARY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.INTERVAL),
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.TIMEDATE) {

        @Override
        public void unparse(SqlWriter writer,
                            SqlCall call,
                            int leftPrec,
                            int rightPrec) {
            writer.keyword("INTERVAL");
            for (SqlNode operand : call
                .getOperandList()) {
                operand.unparse(writer, 0, 0);
            }
        }

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            return opBinding.getOperandType(1);
        }
    };
    /**
     * INET6_ATON INET6_NTOA INET_ATON INET_NTOA INSERT FUNCTION INSTR INTERVAL
     * ISNULL IS_FREE_LOCK IS_IPV4 IS_IPV4_COMPAT IS_IPV4_MAPPED IS_IPV6
     * IS_USED_LOCK
     */
    // public static SqlFunction JSON_ARRAY = new SqlFunction("JSON_ARRAY",
    // SqlKind.OTHER_FUNCTION,
    // ReturnTypes.VARCHAR_2000,
    // null,
    // OperandTypes.VARIADIC,
    // SqlFunctionCategory.STRING);
    // public static SqlFunction JSON_VALID = new SqlFunction("JSON_VALID",
    // SqlKind.OTHER_FUNCTION,
    // ReturnTypes.INTEGER,
    // null,
    // OperandTypes.ANY,
    // SqlFunctionCategory.STRING);
    // public static SqlFunction JSON_MERGE = new SqlFunction("JSON_MERGE",
    // SqlKind.OTHER_FUNCTION,
    // ReturnTypes.VARCHAR_2000,
    // null,
    // OperandTypes.ANY_ANY,
    // SqlFunctionCategory.STRING);
    public static SqlFunction LASTDAY = new SqlFunction("LAST_DAY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DATE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE);
    public static final SqlFunction LEFT = new SqlFunction("LEFT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.FIRST_STRING_TYPE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
        SqlFunctionCategory.STRING);
    public static final SqlFunction IS_FREE_LOCK = new SqlFunction("IS_FREE_LOCK",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER,
        InferTypes.FIRST_KNOWN,
        OperandTypes.STRING,
        SqlFunctionCategory.SYSTEM);
    public static final SqlFunction IS_USED_LOCK = new SqlFunction("IS_USED_LOCK",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.STRING,
        SqlFunctionCategory.SYSTEM);
    public static SqlFunction LAST_INSERT_ID = FunctionWithoutArg.LAST_INSERT_ID;
    /**
     * LCASE LEAST LEFT
     */
    public static SqlFunction LENGTH = new SqlFunctionWithOneStringArg("LENGTH",
        ReturnTypes.BIGINT);
    public static SqlFunction OCTET_LENGTH = new SqlFunctionWithOneStringArg(
        "OCTET_LENGTH",
        ReturnTypes.INTEGER);

    // Todo: LOAD_FILE
    public static SqlFunction LOCATE = new SqlLocateFunction();
    public static SqlFunction LOG = new SqlNumericOneFunction("LOG",
        ReturnTypes.DOUBLE_NULLABLE);
    public static SqlFunction LOG2 = new SqlNumericOneFunction("LOG2",
        ReturnTypes.DOUBLE_NULLABLE);
    public static SqlFunction LPAD = new SqlPadFunction("LPAD");
    public static SqlFunction LTRIM = new SqlTrimFunction("LTRIM");

    public static SqlFunction MICROSECOND = new SqlFunction("MICROSECOND",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE);
    public static SqlFunction MONTHNAME = new SqlFunction("MONTHNAME",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE);
    public static SqlFunction MAKETIME = new SqlFunction("MAKETIME",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.MAKE_TIME,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY_ANY_ANY,
        SqlFunctionCategory.TIMEDATE);
    public static SqlFunction MAKEDATE = new SqlFunction("MAKEDATE",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.DATE_0,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.TIMEDATE);
    public static SqlFunction MAKESET = new SqlFunction("MAKE_SET",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.STRING);
    public static SqlFunction MD5 = new SqlFunction("MD5",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.STRING);
    public static SqlFunction RANDOM_BYTES = new SqlFunction("RANDOM_BYTES",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_BINARY,
        InferTypes.FIRST_KNOWN,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.STRING);
    public static SqlFunction SHA = new SqlFunction("SHA",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.STRING);
    public static SqlFunction SHA1 = new SqlFunction("SHA1",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.STRING);
    public static SqlFunction SHA2 = new SqlFunction("SHA2",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY_NUMERIC,
        SqlFunctionCategory.STRING);
    /**
     * NAME_CONST NOT REGEXP
     */

    public static SqlFunction NOW = new SqlFunction("NOW",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.DATETIME_WITH_FSP,
        InferTypes.FIRST_KNOWN,
        OperandTypes.or(OperandTypes.NILADIC, OperandTypes.ANY),
        SqlFunctionCategory.TIMEDATE);

    public static SqlBinaryOperator NOT_REGEXP = new SqlBinaryOperator("NOT REGEXP",
        SqlKind.MATCH_RECOGNIZE,
        30,
        true,
        ReturnTypes.INTEGER,
        InferTypes.VARCHAR_2000,
        OperandTypes.ANY_ANY);

    public static SqlBinaryOperator NULL_SAFE_EQUAL = new SqlBinaryOperator("<=>",
        SqlKind.IS_NOT_DISTINCT_FROM,
        60,
        true,
        ReturnTypes.BIGINT,
        InferTypes.FIRST_KNOWN,
        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

    /**
     * PASSWORD PERIOD_ADD PERIOD_DIFF
     */
    public static SqlFunction PERIODADD = new SqlNumericOneFunction("PERIOD_ADD",
        ReturnTypes.BIGINT_NULLABLE);
    public static SqlFunction PERIODDIFF = new SqlNumericOneFunction("PERIOD_DIFF",
        ReturnTypes.BIGINT_NULLABLE);

    public static SqlFunction POW = new SqlNumericOneFunction("POW",
        ReturnTypes.DOUBLE_NULLABLE);
    public static SqlFunction PI = FunctionWithoutArg.PI;
    public static SqlFunction SCHEMA = FunctionWithoutArg.SCHEMA;

    /**
     * QUOTE RANDOM_BYTES REGEXP RELEASE_ALL_LOCKS RELEASE_LOCK
     */
    public static SqlFunction RELEASE_LOCK = new SqlFunction("RELEASE_LOCK",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER,
        InferTypes.FIRST_KNOWN,
        OperandTypes.STRING,
        SqlFunctionCategory.SYSTEM);
    public static SqlFunction RELEASE_ALL_LOCKS = new FunctionWithoutArg("RELEASE_ALL_LOCKS", ReturnTypes.INTEGER);
    public static SqlFunction REPEAT = new SqlRepeatFunction();
    public static SqlFunction REPLACE = new SqlReplaceFunction();
    public static SqlFunction REVERSE = new SqlReverseFunction();
    public static SqlBinaryOperator REGEXP = new SqlBinaryOperator("REGEXP",
        SqlKind.MATCH_RECOGNIZE,
        30,
        true,
        ReturnTypes.INTEGER,
        InferTypes.VARCHAR_2000,
        OperandTypes.ANY_ANY);
    public static SqlBinaryOperator RLIKE = new SqlBinaryOperator("RLIKE",
        SqlKind.MATCH_RECOGNIZE,
        30,
        true,
        ReturnTypes.INTEGER,
        InferTypes.VARCHAR_2000,
        OperandTypes.ANY_ANY);
    /**
     * RIGHT ROW_COUNT
     */
    public static SqlFunction RPAD = new SqlPadFunction("RPAD");
    public static SqlFunction RTRIM = new SqlTrimFunction("RTRIM");

    /**
     * SCHEMA SECOND SEC_TO_TIME SHA1 SHA2
     */
    public static SqlBinaryOperator BITLSHIFT = new SqlBinaryOperator("<<",
        SqlKind.OTHER_FUNCTION,
        46,
        true,
        ReturnTypes.BIGINT_UNSIGNED_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY_ANY);
    public static SqlBinaryOperator BITRSHIFT = new SqlBinaryOperator(">>",
        SqlKind.OTHER_FUNCTION,
        46,
        true,
        ReturnTypes.BIGINT_UNSIGNED_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY_ANY);
    public static SqlFunction CHAR = FunctionWithVariadicArg.charFunc;

    public static SqlFunction SEC_TO_TIME = new SqlFunction("SEC_TO_TIME",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.SEC_TO_TIME,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE);

    public static SqlFunction SLEEP = new SqlFunction("SLEEP",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER,
        InferTypes.FIRST_KNOWN,
        OperandTypes.family(SqlTypeFamily.INTEGER),
        SqlFunctionCategory.SYSTEM);
    public static SqlFunction SUBTIME = new SqlFunction(
        "SUBTIME",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.ADD_TIME,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.TIMEDATE
    );

    public static SqlFunction SUBSTRING_INDEX = new SqlFunction("SUBSTRING_INDEX",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.MIX_OF_COLLATION_RETURN_VARCHAR,
        InferTypes.FIRST_KNOWN,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY),
        SqlFunctionCategory.STRING);

    public static SqlFunction CAN_ACCESS_TABLE = new SqlFunction("CAN_ACCESS_TABLE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
        SqlFunctionCategory.SYSTEM);

    /**
     * SOUNDEX SOUNDS LIKE
     */
    public static SqlFunction SPACE = new SqlSpaceFunction();
    public static SqlFunction STRCMP = new SqlStrCmpFunction("STRCMP");

    /**
     * STR_TO_DATE
     */
    public static SqlFunction SUBDATE = new SqlDateManipulationFunction("SUBDATE");
    public static SqlFunction SUBSTRING = new SqlSubStrFunction("SUBSTRING");
    public static SqlFunction SUBSTR = new SqlSubStrFunction("SUBSTR");
    public static SqlFunction STRTODATE = new SqlFunction("STR_TO_DATE",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.STR_TO_DATE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.TIMEDATE);

    /**
     * SUBSTRING_INDEX SUBTIME
     */
    public static SqlFunction SYSDATE = new SqlFunction("SYSDATE",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.DATETIME_WITH_FSP,
        InferTypes.FIRST_KNOWN,
        OperandTypes.or(OperandTypes.NILADIC, OperandTypes.ANY),
        SqlFunctionCategory.TIMEDATE);

    public static SqlFunction SOUNDEX = new SqlFunction("SOUNDEX",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.FIRST_STRING_TYPE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM);
    public static SqlFunction TIME = new SqlFunction("TIME",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.DYNAMIC_TIME,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE);
    public static SqlFunction TIMEDIFF = new SqlFunction("TIMEDIFF",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.TIME_DIFF,
        InferTypes.FIRST_KNOWN,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
        SqlFunctionCategory.TIMEDATE);

    public static SqlFunction TIME_TO_SEC = new SqlFunction("TIME_TO_SEC",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE);
    public static SqlFunction TIME_FORMAT = new SqlFunction("TIME_FORMAT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
        SqlFunctionCategory.TIMEDATE);
    public static SqlFunction TO_DAYS = new SqlFunction("TO_DAYS",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE);
    public static SqlFunction TO_SECONDS = new SqlFunction("TO_SECONDS",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE);
    public static SqlFunction TO_BASE64 = new SqlFunction("TO_BASE64",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.RETURN_TYPE,
        OperandTypes.ANY,
        SqlFunctionCategory.STRING);

    /**
     * TIMEDIFF TIMESTAMP FUNCTION TIME_FORMAT TIME_TO_SEC TO_BASE64 TO_DAYS
     * TO_SECONDS UCASE UNCOMPRESS UNCOMPRESSED_LENGTH UNHEX
     */

    public static SqlFunction UNHEX = new SqlFunction("UNHEX",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_BINARY,
        InferTypes.RETURN_TYPE,
        OperandTypes.ANY,
        SqlFunctionCategory.NUMERIC);
    public static SqlFunction UNIX_TIMESTAMP = new SqlFunction("UNIX_TIMESTAMP",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.UNIX_TIMESTAMP,
        InferTypes.FIRST_KNOWN,
        OperandTypes.or(OperandTypes.ANY, OperandTypes.NILADIC),
        SqlFunctionCategory.TIMEDATE);

    public static SqlFunction TIMESTAMP = new SqlFunction("TIMESTAMP",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.TIMESTAMP_FUNC,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY_OR_ANY_ANY,
        SqlFunctionCategory.TIMEDATE);

    public static SqlFunction WEEKOFYEAR = new SqlWeekYearFunction("WEEKOFYEAR");
    public static SqlFunction YEARWEEK = new SqlWeekYearFunction("YEARWEEK");
    public static SqlFunction UNCOMPRESS = new SqlFunction("UNCOMPRESS",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.STRING,
        SqlFunctionCategory.STRING);
    /**
     * UPDATEXML
     */
    public static SqlFunction UTC_DATE = new SqlNoParameterTimeFunction("UTC_DATE",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.DATE_0,
        InferTypes.FIRST_KNOWN,
        OperandTypes.NILADIC);

    public static SqlFunction UTC_TIME = new SqlNoParameterTimeFunction("UTC_TIME",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.TIME_WITH_FSP,
        InferTypes.FIRST_KNOWN,
        OperandTypes.or(OperandTypes.NILADIC, OperandTypes.ANY));

    public static SqlFunction UTC_TIMESTAMP = new SqlNoParameterTimeFunction("UTC_TIMESTAMP",
        SqlKind.OTHER_FUNCTION,
        MySQLStandardTypeInference.DATETIME_WITH_FSP,
        InferTypes.FIRST_KNOWN,
        OperandTypes.or(OperandTypes.NILADIC, OperandTypes.ANY));

    public static SqlFunction UUID = FunctionWithoutArg.UUID;
    public static SqlFunction UUID_SHORT = FunctionWithoutArg.UUID_SHORT;
    /**
     * VALIDATE_PASSWORD_STRENGTH VERSION WEEK WEEKDAY WEEKOFYEAR WEIGHT_STRING XOR
     */
    // public static final SqlFunction UPPER = new SqlFunction("UPPER",
    // SqlKind.OTHER_FUNCTION,
    // ReturnTypes.ARG0_NULLABLE,
    // InferTypes.FIRST_KNOWN,
    // OperandTypes.CHARACTER,
    // SqlFunctionCategory.STRING);
    public static SqlFunction VERSION = new FunctionWithoutArg("VERSION",
        ReturnTypes.VARCHAR_2000);

    public static SqlFunction WEEKDAY = new SqlFunction("WEEKDAY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE);

    public static SqlFunction OCT = new SqlFunction("OCT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC);
    public static SqlFunction ORD = new SqlFunction("ORD",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT,
        InferTypes.FIRST_KNOWN,
        OperandTypes.STRING,
        SqlFunctionCategory.STRING);
    public static final SqlFunction QUOTE = new SqlFunction("QUOTE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.FIRST_STRING_TYPE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.family(SqlTypeFamily.ANY),
        SqlFunctionCategory.STRING);
    public static final SqlFunction RIGHT = new SqlFunction("RIGHT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.FIRST_STRING_TYPE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
        SqlFunctionCategory.STRING);
    public static SqlBinaryOperator XOR = new SqlMonotonicBinaryOperator("XOR",
        SqlKind.OTHER_FUNCTION,
        23,
        true,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY));
    /**
     * YEARWEEK ^ | ~
     */

    /**
     * Overload functions.
     */

    /**
     * Other functions
     */
    public static SqlFunction HYPERLOGLOG = new SqlHyperLogLogFunction();

    /**
     * 聚合函数
     */
    public static SqlFunction GROUP_CONCAT = new SqlGroupConcatFunction();

    public static SqlFunction BIT_OR = new SqlBitOrFunction();

    public static SqlFunction BIT_AND = new SqlBitAndFunction();

    public static SqlFunction BIT_XOR = new SqlBitXorFunction();

    /**
     * Variadic operands functions
     */
    public static SqlFunction GREATEST = new SqlFunction("GREATEST",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.LEAST_RESTRICTIVE_VARCHAR,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);
    public static SqlFunction LEAST = new SqlFunction("LEAST",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.LEAST_RESTRICTIVE_VARCHAR,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlPrefixOperator BINARY = new SqlPrefixOperator("BINARY",
        SqlKind.OTHER,
        26,
        ReturnTypes.VARCHAR_BINARY,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY);

    public static final SqlPrefixOperator NOT = new SqlPrefixOperator("NOT",
        SqlKind.NOT,
        26,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.BOOLEAN,
        OperandTypes.ANY);
    public static SqlFunction MID = new SqlSubStrFunction("MID");

    public static SqlFunction NEXTVAL = new SqlSequenceFunction("NEXTVAL");

    // VALUES after ON DUPLICATE KEY UPDATE
    public static SqlFunction VALUES = new SqlValuesFunction("VALUES");

    public static SqlIndexHintOperator INTERVAL_PRE = SqlIndexHintOperator.INDEX_OPERATOR;

    // GEO functions
    public static SqlFunction GeomFromText = new SqlFunction("GeomFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_GeomFromText = new SqlFunction("ST_GeomFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction GeometryFromText = new SqlFunction("GeometryFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_GeometryFromText = new SqlFunction("ST_GeometryFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_GeomCollFromText = new SqlFunction("ST_GeomCollFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_GeometryCollectionFromText = new SqlFunction(
        "ST_GeometryCollectionFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_GeomCollFromTxt = new SqlFunction("ST_GeomCollFromTxt",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction GeomCollFromText = new SqlFunction("GeomCollFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction GeometryCollectionFromText = new SqlFunction("GeometryCollectionFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_LineFromText = new SqlFunction("ST_LineFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_LineStringFromText = new SqlFunction("ST_LineStringFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction LineFromText = new SqlFunction("LineFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction LineStringFromText = new SqlFunction("LineStringFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MLineFromText = new SqlFunction("MLineFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MultiLineStringFromText = new SqlFunction("MultiLineStringFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_MLineFromText = new SqlFunction("ST_MLineFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_MultiLineStringFromText = new SqlFunction("ST_MultiLineStringFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_MPointFromText = new SqlFunction("ST_MPointFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_MultiPointFromText = new SqlFunction("ST_MultiPointFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MPointFromText = new SqlFunction("MPointFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MultiPointFromText = new SqlFunction("MultiPointFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MPolyFromText = new SqlFunction("MPolyFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MultiPolygonFromText = new SqlFunction("MultiPolygonFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_MPolyFromText = new SqlFunction("ST_MPolyFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_MultiPolygonFromText = new SqlFunction("ST_MultiPolygonFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_PointFromText = new SqlFunction("ST_PointFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction PointFromText = new SqlFunction("PointFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction PolyFromText = new SqlFunction("PolyFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction PolygonFromText = new SqlFunction("PolygonFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_PolyFromText = new SqlFunction("ST_PolyFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_PolygonFromText = new SqlFunction("ST_PolygonFromText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_AsBinary = new SqlFunction("ST_AsBinary",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_AsWKB = new SqlFunction("ST_AsWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction AsBinary = new SqlFunction("AsBinary",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction AsWKB = new SqlFunction("AsWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_AsText = new SqlFunction("ST_AsText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_AsWKT = new SqlFunction("ST_AsWKT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction AsText = new SqlFunction("AsText",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction AsWKT = new SqlFunction("AsWKT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction GeomFromWKB = new SqlFunction("GeomFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction GeometryFromWKB = new SqlFunction("GeometryFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_GeomFromWKB = new SqlFunction("ST_GeomFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_GeometryFromWKB = new SqlFunction("ST_GeometryFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_GeomCollFromWKB = new SqlFunction("ST_GeomCollFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_GeometryCollectionFromWKB = new SqlFunction(
        "ST_GeometryCollectionFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction GeomCollFromWKB = new SqlFunction("GeomCollFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction GeometryCollectionFromWKB = new SqlFunction("GeometryCollectionFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction LineFromWKB = new SqlFunction("LineFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction LineStringFromWKB = new SqlFunction("LineStringFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_LineFromWKB = new SqlFunction("ST_LineFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_LineStringFromWKB = new SqlFunction("ST_LineStringFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MLineFromWKB = new SqlFunction("MLineFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MultiLineStringFromWKB = new SqlFunction("MultiLineStringFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_MLineFromWKB = new SqlFunction("ST_MLineFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_MultiLineStringFromWKB = new SqlFunction("ST_MultiLineStringFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MPointFromWKB = new SqlFunction("MPointFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MultiPointFromWKB = new SqlFunction("MultiPointFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_MPointFromWKB = new SqlFunction("ST_MPointFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_MultiPointFromWKB = new SqlFunction("ST_MultiPointFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MPolyFromWKB = new SqlFunction("MPolyFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MultiPolygonFromWKB = new SqlFunction("MultiPolygonFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_MPolyFromWKB = new SqlFunction("ST_MPolyFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_MultiPolygonFromWKB = new SqlFunction("ST_MultiPolygonFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction PointFromWKB = new SqlFunction("PointFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_PointFromWKB = new SqlFunction("ST_PointFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction PolyFromWKB = new SqlFunction("PolyFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction PolygonFromWKB = new SqlFunction("PolygonFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_PolyFromWKB = new SqlFunction("ST_PolyFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_PolygonFromWKB = new SqlFunction("ST_PolygonFromWKB",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction GeometryCollection = new SqlFunction("GeometryCollection",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction LineString = new SqlFunction("LineString",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MultiLineString = new SqlFunction("MultiLineString",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MultiPoint = new SqlFunction("MultiPoint",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MultiPolygon = new SqlFunction("MultiPolygon",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Point = new SqlFunction("Point",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Polygon = new SqlFunction("Polygon",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Dimension = new SqlFunction("ST_Dimension",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Dimension = new SqlFunction("Dimension",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Envelope = new SqlFunction("ST_Envelope",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Envelope = new SqlFunction("Envelope",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_GeometryType = new SqlFunction("ST_GeometryType",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction GeometryType = new SqlFunction("GeometryType",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_IsEmpty = new SqlFunction("ST_IsEmpty",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction IsEmpty = new SqlFunction("IsEmpty",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_IsSimple = new SqlFunction("ST_IsSimple",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction IsSimple = new SqlFunction("IsSimple",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_SRID = new SqlFunction("ST_SRID",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction SRID = new SqlFunction("SRID",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_X = new SqlFunction("ST_X",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction X = new SqlFunction("X",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Y = new SqlFunction("ST_Y",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Y = new SqlFunction("Y",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_EndPoint = new SqlFunction("ST_EndPoint",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction EndPoint = new SqlFunction("EndPoint",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_IsClosed = new SqlFunction("ST_IsClosed",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction IsClosed = new SqlFunction("IsClosed",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Length = new SqlFunction("ST_Length",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction GLength = new SqlFunction("GLength",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_NumPoints = new SqlFunction("ST_NumPoints",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction NumPoints = new SqlFunction("NumPoints",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_PointN = new SqlFunction("ST_PointN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction PointN = new SqlFunction("PointN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_StartPoint = new SqlFunction("ST_StartPoint",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction StartPoint = new SqlFunction("StartPoint",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Area = new SqlFunction("ST_Area",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Area = new SqlFunction("Area",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Centroid = new SqlFunction("ST_Centroid",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Centroid = new SqlFunction("Centroid",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_ExteriorRing = new SqlFunction("ST_ExteriorRing",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ExteriorRing = new SqlFunction("ExteriorRing",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_InteriorRingN = new SqlFunction("ST_InteriorRingN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction InteriorRingN = new SqlFunction("InteriorRingN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_NumInteriorRing = new SqlFunction("ST_NumInteriorRing",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_NumInteriorRings = new SqlFunction("ST_NumInteriorRings",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction NumInteriorRings = new SqlFunction("NumInteriorRings",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_GeometryN = new SqlFunction("ST_GeometryN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction GeometryN = new SqlFunction("GeometryN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_NumGeometries = new SqlFunction("ST_NumGeometries",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction NumGeometries = new SqlFunction("NumGeometries",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Buffer = new SqlFunction("ST_Buffer",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Buffer = new SqlFunction("Buffer",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Buffer_Strategy = new SqlFunction("ST_Buffer_Strategy",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_ConvexHull = new SqlFunction("ST_ConvexHull",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ConvexHull = new SqlFunction("ConvexHull",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Difference = new SqlFunction("ST_Difference",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Intersection = new SqlFunction("ST_Intersection",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_SymDifference = new SqlFunction("ST_SymDifference",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Union = new SqlFunction("ST_Union",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Contains = new SqlFunction("ST_Contains",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Crosses = new SqlFunction("ST_Crosses",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Crosses = new SqlFunction("Crosses",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Disjoint = new SqlFunction("ST_Disjoint",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Distance = new SqlFunction("ST_Distance",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Distance = new SqlFunction("Distance",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Equals = new SqlFunction("ST_Equals",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Intersects = new SqlFunction("ST_Intersects",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Overlaps = new SqlFunction("ST_Overlaps",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Touches = new SqlFunction("ST_Touches",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Touches = new SqlFunction("Touches",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Within = new SqlFunction("ST_Within",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MBRContains = new SqlFunction("MBRContains",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Contains = new SqlFunction("Contains",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MBRCoveredBy = new SqlFunction("MBRCoveredBy",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MBRCovers = new SqlFunction("MBRCovers",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MBRDisjoint = new SqlFunction("MBRDisjoint",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Disjoint = new SqlFunction("Disjoint",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MBREqual = new SqlFunction("MBREqual",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MBREquals = new SqlFunction("MBREquals",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Equals = new SqlFunction("Equals",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MBRIntersects = new SqlFunction("MBRIntersects",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Intersects = new SqlFunction("Intersects",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MBROverlaps = new SqlFunction("MBROverlaps",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Overlaps = new SqlFunction("Overlaps",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MBRTouches = new SqlFunction("MBRTouches",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MBRWithin = new SqlFunction("MBRWithin",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction Within = new SqlFunction("Within",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_GeoHash = new SqlFunction("ST_GeoHash",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_LatFromGeoHash = new SqlFunction("ST_LatFromGeoHash",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_LongFromGeoHash = new SqlFunction("ST_LongFromGeoHash",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_PointFromGeoHash = new SqlFunction("ST_PointFromGeoHash",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_AsGeoJSON = new SqlFunction("ST_AsGeoJSON",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_GeomFromGeoJSON = new SqlFunction("ST_GeomFromGeoJSON",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Distance_Sphere = new SqlFunction("ST_Distance_Sphere",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_IsValid = new SqlFunction("ST_IsValid",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_MakeEnvelope = new SqlFunction("ST_MakeEnvelope",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Simplify = new SqlFunction("ST_Simplify",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction ST_Validate = new SqlFunction("ST_Validate",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    // JSON functions
    public static SqlFunction JSON_EXTRACT = new SqlFunction("JSON_EXTRACT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_UNQUOTE = new SqlFunction("JSON_UNQUOTE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_APPEND = new SqlFunction("JSON_APPEND",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_ARRAY_APPEND = new SqlFunction("JSON_ARRAY_APPEND",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_ARRAY = new SqlFunction("JSON_ARRAY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_ARRAY_INSERT = new SqlFunction("JSON_ARRAY_INSERT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_CONTAINS = new SqlFunction("JSON_CONTAINS",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_CONTAINS_PATH = new SqlFunction("JSON_CONTAINS_PATH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_DEPTH = new SqlFunction("JSON_DEPTH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_INSERT = new SqlFunction("JSON_INSERT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_KEYS = new SqlFunction("JSON_KEYS",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_LENGTH = new SqlFunction("JSON_LENGTH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_MERGE = new SqlFunction("JSON_MERGE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_MERGE_PATCH = new SqlFunction("JSON_MERGE_PATCH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_MERGE_PRESERVE = new SqlFunction("JSON_MERGE_PRESERVE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_OBJECT = new SqlFunction("JSON_OBJECT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_PRETTY = new SqlFunction("JSON_PRETTY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_QUOTE = new SqlFunction("JSON_QUOTE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_REMOVE = new SqlFunction("JSON_REMOVE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_REPLACE = new SqlFunction("JSON_REPLACE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_SEARCH = new SqlFunction("JSON_SEARCH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_SET = new SqlFunction("JSON_SET",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_STORAGE_SIZE = new SqlFunction("JSON_STORAGE_SIZE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_TYPE = new SqlFunction("JSON_TYPE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_VALID = new SqlFunction("JSON_VALID",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_ARRAYAGG = new SqlFunction("JSON_ARRAYAGG",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction JSON_OBJECTAGG = new SqlFunction("JSON_OBJECTAGG",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction USER = new SqlFunction("USER",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        InferTypes.FIRST_KNOWN,
        OperandTypes.NILADIC,
        SqlFunctionCategory.SYSTEM);

    public static SqlFunction MATCH_AGAINST = new MySQLMatchAgainst();

    @Override
    public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax,
                                        List<SqlOperator> operatorList) {
        // check nextval
        if ((opName.names.size() == 2 && opName.names.get(1).equalsIgnoreCase(SqlFunction.NEXTVAL_FUNC_NAME))
            || (opName.names.size() == 3 && opName.names.get(2).equalsIgnoreCase(SqlFunction.NEXTVAL_FUNC_NAME))) {
            operatorList.add(NEXTVAL);
        } else {
            super.lookupOperatorOverloads(opName, category, syntax, operatorList);
        }
    }

    public static final Set<SqlOperator> VECTORIZED_BINARY_COMPARISON_OPERATORS = ImmutableSet.of(
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        NOT_EQUALS,
        EQUALS,
        IS_NOT_DISTINCT_FROM
    );

    public static final Set<SqlOperator> VECTORIZED_UNARY_COMPARISON_OPERATORS = ImmutableSet.of(
        IS_NULL,
        IS_NOT_NULL,
        IS_TRUE,
        IS_FALSE,
        IS_UNKNOWN,
        IS_NOT_FALSE,
        IS_NOT_UNKNOWN
    );

    /**
     * vectorized comparison operators.
     */
    public static final Set<SqlOperator> VECTORIZED_COMPARISON_OPERATORS = ImmutableSet.<SqlOperator>builder()
        .addAll(VECTORIZED_BINARY_COMPARISON_OPERATORS)
        .addAll(VECTORIZED_UNARY_COMPARISON_OPERATORS)
        .build();

    /**
     * vectorized arithmetic operators.
     */
    public static final Set<SqlOperator> VECTORIZED_ARITHMETIC_OPERATORS = ImmutableSet.of(
        PLUS,
        MINUS,
        MULTIPLY,
        DIVIDE,
        MOD
    );

    /**
     * vectorized arithmetic and comparison operators.
     */
    public static final Set<SqlOperator> BASE_VECTORIZED_OPERATORS = ImmutableSet.<SqlOperator>builder()
        .addAll(VECTORIZED_COMPARISON_OPERATORS)
        .addAll(VECTORIZED_ARITHMETIC_OPERATORS)
        .build();

    /**
     * vectorized branch operators
     */
    public static final Set<SqlOperator> CONTROL_FLOW_VECTORIZED_OPERATORS = ImmutableSet.of(
        CASE,
        IFNULL,
        IF,
        COALESCE,
        NULLIF
    );

    /**
     * vectorized logic operators
     */
    public static final Set<SqlOperator> LOGIC_VECTORIZED_OPERATORS = ImmutableSet.of(
        AND,
        OR,
        NOT,
        XOR
    );

    /**
     * all vectorized operators
     */
    public static final Set<SqlOperator> DRDS_VECTORIZED_OPERATORS = ImmutableSet.<SqlOperator>builder()
        .addAll(BASE_VECTORIZED_OPERATORS)
        .addAll(CONTROL_FLOW_VECTORIZED_OPERATORS)
        .addAll(LOGIC_VECTORIZED_OPERATORS)
        .build();

    static {
        VECTORIZED_OPERATORS.addAll(DRDS_VECTORIZED_OPERATORS);
        TYPE_COERCION_ENABLE_OPERATORS.addAll(VECTORIZED_OPERATORS);
        TYPE_COERCION_ENABLE_OPERATORS.add(ABS);
    }

    public static final Set<SqlOperator> STRING_FUNCTIONS = ImmutableSet.of(
        ASCII, //Return numeric value of left-most character
        BIN, //Return a string containing binary representation of a number
        BIT_LENGTH, //Return length of argument in bits
        CHAR, //Return the character for each integer passed
        CHAR_LENGTH, //Return number of characters in argument
        CHARACTER_LENGTH, //Synonym for CHAR_LENGTH()
        CONCAT, //Return concatenated string
        CONCAT_WS, //Return concatenate with separator
        ELT, //Return string at index number
        EXPORTSET,
        //Return a string such that for every bit set in the value bits, you get an on string and for every unset bit, you get an off string
        FIELD, //Index (position) of first argument in subsequent arguments
        FIND_IN_SET, //Index (position) of first argument within second argument
        FORMAT, //Return a number formatted to specified number of decimal places
        FROM_BASE64, //Decode base64 encoded string and return result
        HEX, //Hexadecimal representation of decimal or string value
        INSERT, //Insert substring at specified position up to specified number of characters
        INSTR, //Return the index of the first occurrence of substring
        LEFT, //Return the leftmost number of characters as specified
        LENGTH, //Return the length of a string in bytes
        LIKE, //Simple pattern matching
        LOCATE, //Return the position of the first occurrence of substring
        LOWER, //Return the argument in lowercase
        LPAD, //Return the string argument, left-padded with the specified string
        LTRIM, //Remove leading spaces
        MAKESET, //Return a set of comma-separated strings that have the corresponding bit in bits set
        MID, //Return a substring starting from the specified position
        NOT_LIKE, //Negation of simple pattern matching
        NOT_REGEXP, //Negation of REGEXP
        OCT, //Return a string containing octal representation of a number
        OCTET_LENGTH, //Synonym for LENGTH()
        ORD, //Return character code for leftmost character of the argument
        POSITION, //Synonym for LOCATE()
        QUOTE, //Escape the argument for use in an SQL statement
        REGEXP, //Whether string matches regular expression
        REPEAT, //Repeat a string the specified number of times
        REPLACE, //Replace occurrences of a specified string
        REVERSE, //Reverse the characters in a string
        RIGHT, //Return the specified rightmost number of characters
        RLIKE, //Whether string matches regular expression
        RPAD, //Append string the specified number of times
        RTRIM, //Remove trailing spaces
        SOUNDEX, //Return a soundex string
        SPACE, //Return a string of the specified number of spaces
        STRCMP, //Compare two strings
        SUBSTR, //Return the substring as specified
        SUBSTRING, //Return the substring as specified
        SUBSTRING_INDEX, //Return a substring from a string before the specified number of occurrences of the delimiter
        TO_BASE64, //Return the argument converted to a base-64 string
        TRIM, //Remove leading and trailing spaces
        UNHEX, //Return a string containing hex representation of a number
        UPPER //Convert to uppercase
    );

    public static final Set<SqlOperator> ENCRYPTION_FUNCTIONS = ImmutableSet.of(
        AES_DECRYPT, //Decrypt using AES
        AES_ENCRYPT, //Encrypt using AES
        COMPRESS, //Return result as a binary string
        MD5, //Calculate MD5 checksum
        RANDOM_BYTES, //Return a random byte vector
        SHA, SHA1, //Calculate an SHA-1 160-bit checksum
        SHA2,      //Calculate an SHA-2 checksum
        //        PASSWORD, //(deprecated) Calculate and return a password string
        UNCOMPRESS //Uncompress a string compressed
        //        UNCOMPRESSED_LENGTH, //Return the length of a string before compression
        //        VALIDATE_PASSWORD_STRENGTH, //Determine strength of password
    );
}
