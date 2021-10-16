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

package com.alibaba.polardbx.optimizer.core.datatype;

import org.apache.calcite.sql.SqlKind;

import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT_UNSIGNED;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR;
import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.DATETIME;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.FLOAT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.JSON;
import static org.apache.calcite.sql.type.SqlTypeName.MEDIUMINT;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public class TypeCoercionTest extends DataTypeTestBase {
    @Test
    public void testArithmetic() {
        // exact type, approximate type -> double
        sql("select decimal_test + double_test from test").type(DOUBLE);
        sql("select decimal_test - double_test from test").type(DOUBLE);
        sql("select decimal_test * double_test from test").type(DOUBLE);
        sql("select decimal_test / double_test from test").type(DOUBLE);

        /*
         * PLUS '+'
         */
        // time, numeric -> bigint, numeric
        sql("select "
            + "integer_test + datetime_test, "
            + "integer_test + date_test, "
            + "integer_test + timestamp_test, "
            + "integer_test + time_test, "
            + "double_test + datetime_test, "
            + "double_test + date_test, "
            + "double_test + timestamp_test, "
            + "double_test + time_test from test ")
            .type(BIGINT, BIGINT, BIGINT, BIGINT, DOUBLE, DOUBLE, DOUBLE, DOUBLE)
            .cast(DATETIME, BIGINT)
            .cast(DATE, BIGINT)
            .cast(TIME, BIGINT)
            .cast(TIMESTAMP, BIGINT);

        // varchar, numeric -> double, numeric
        sql("select "
            + "integer_test + varchar_test, "
            + "bigint_test + varchar_test, "
            + "decimal_test + varchar_test, "
            + "double_test + varchar_test from test")
            .type(DOUBLE, DOUBLE, DOUBLE, DOUBLE)
            .cast(VARCHAR, DOUBLE);

        /*
         * MINUS '-'
         */
        // time, numeric -> bigint, numeric
        sql("select "
            + "integer_test - datetime_test, "
            + "integer_test - date_test, "
            + "integer_test - timestamp_test, "
            + "integer_test - time_test, "
            + "double_test - datetime_test, "
            + "double_test - date_test, "
            + "double_test - timestamp_test, "
            + "double_test - time_test from test ")
            .type(BIGINT, BIGINT, BIGINT, BIGINT, DOUBLE, DOUBLE, DOUBLE, DOUBLE)
            .cast(DATETIME, BIGINT)
            .cast(DATE, BIGINT)
            .cast(TIME, BIGINT)
            .cast(TIMESTAMP, BIGINT);

        // varchar, numeric -> double, numeric
        sql("select "
            + "integer_test - varchar_test, "
            + "bigint_test - varchar_test, "
            + "decimal_test - varchar_test, "
            + "double_test - varchar_test from test")
            .type(DOUBLE, DOUBLE, DOUBLE, DOUBLE)
            .cast(VARCHAR, DOUBLE);

        /*
         * MULTIPLY '*'
         */
        // time, numeric -> bigint, numeric
        sql("select "
            + "integer_test * datetime_test, "
            + "integer_test * date_test, "
            + "integer_test * timestamp_test, "
            + "integer_test * time_test, "
            + "double_test * datetime_test, "
            + "double_test * date_test, "
            + "double_test * timestamp_test, "
            + "double_test * time_test from test ")
            .type(BIGINT, BIGINT, BIGINT, BIGINT, DOUBLE, DOUBLE, DOUBLE, DOUBLE)
            .cast(DATETIME, BIGINT)
            .cast(DATE, BIGINT)
            .cast(TIME, BIGINT)
            .cast(TIMESTAMP, BIGINT);

        // varchar, numeric -> double, numeric
        sql("select "
            + "integer_test * varchar_test, "
            + "bigint_test * varchar_test, "
            + "decimal_test * varchar_test, "
            + "double_test * varchar_test from test")
            .type(DOUBLE, DOUBLE, DOUBLE, DOUBLE)
            .cast(VARCHAR, DOUBLE);

        /*
         * DIVIDE '/'
         */
        // exact type / approximate type -> double
        sql("select decimal_test + double_test from test").type(DOUBLE);
        sql("select decimal_test - double_test from test").type(DOUBLE);
        sql("select decimal_test * double_test from test").type(DOUBLE);
        sql("select decimal_test / double_test from test").type(DOUBLE);

        // exact type / exact type -> decimal
        sql("select 4 / 2 from test")
            .type(DECIMAL);

        // time, numeric -> bigint, numeric
        sql("select "
            + "integer_test / datetime_test, "
            + "integer_test / date_test, "
            + "integer_test / timestamp_test, "
            + "integer_test / time_test, "
            + "double_test / datetime_test, "
            + "double_test / date_test, "
            + "double_test / timestamp_test, "
            + "double_test / time_test from test ")
            .type(DECIMAL, DECIMAL, DECIMAL, DECIMAL, DOUBLE, DOUBLE, DOUBLE, DOUBLE)
            .cast(DATETIME, DECIMAL)
            .cast(DATE, DECIMAL)
            .cast(TIME, DECIMAL)
            .cast(TIMESTAMP, DECIMAL);

        // varchar, numeric -> double, numeric
        sql("select "
            + "integer_test / varchar_test, "
            + "bigint_test / varchar_test, "
            + "decimal_test / varchar_test, "
            + "double_test / varchar_test from test")
            .type(DOUBLE, DOUBLE, DOUBLE, DOUBLE)
            .cast(VARCHAR, DOUBLE);

        /*
         * MOD '%'
         */
        // time, numeric -> bigint, numeric
        sql("select "
            + "integer_test % datetime_test, "
            + "integer_test % date_test, "
            + "integer_test % timestamp_test, "
            + "integer_test % time_test, "
            + "double_test % datetime_test, "
            + "double_test % date_test, "
            + "double_test % timestamp_test, "
            + "double_test % time_test from test ")
            .type(BIGINT, BIGINT, BIGINT, BIGINT, DOUBLE, DOUBLE, DOUBLE, DOUBLE)
            .cast(DATETIME, BIGINT)
            .cast(DATE, BIGINT)
            .cast(TIME, BIGINT)
            .cast(TIMESTAMP, BIGINT);

        // varchar, numeric -> double, numeric
        sql("select "
            + "integer_test % varchar_test, "
            + "bigint_test % varchar_test, "
            + "decimal_test % varchar_test, "
            + "double_test % varchar_test from test")
            .type(DOUBLE, DOUBLE, DOUBLE, DOUBLE)
            .cast(VARCHAR, DOUBLE);
    }

    @Test
    public void testNumericArithmetic() {
        // numeric + type check.
        // TINYINT
        sql("select tinyint_test + tinyint_test from test").type(BIGINT);
        sql("select tinyint_test + u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test + smallint_test from test").type(BIGINT);
        sql("select tinyint_test + u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test + mediumint_test from test").type(BIGINT);
        sql("select tinyint_test + u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test + integer_test from test").type(BIGINT);
        sql("select tinyint_test + u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test + bigint_test from test").type(BIGINT);
        sql("select tinyint_test + u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test + decimal_test from test").type(DECIMAL);
        sql("select tinyint_test + float_test from test").type(DOUBLE);
        sql("select tinyint_test + double_test from test").type(DOUBLE);
        // TINYINT_UNSIGNED
        sql("select u_tinyint_test + tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test + u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test + smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test + u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test + mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test + u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test + integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test + u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test + bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test + u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test + decimal_test from test").type(DECIMAL);
        sql("select u_tinyint_test + float_test from test").type(DOUBLE);
        sql("select u_tinyint_test + double_test from test").type(DOUBLE);
        // SMALLINT
        sql("select smallint_test + tinyint_test from test").type(BIGINT);
        sql("select smallint_test + u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test + smallint_test from test").type(BIGINT);
        sql("select smallint_test + u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test + mediumint_test from test").type(BIGINT);
        sql("select smallint_test + u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test + integer_test from test").type(BIGINT);
        sql("select smallint_test + u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test + bigint_test from test").type(BIGINT);
        sql("select smallint_test + u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test + decimal_test from test").type(DECIMAL);
        sql("select smallint_test + float_test from test").type(DOUBLE);
        sql("select smallint_test + double_test from test").type(DOUBLE);
        // SMALLINT_UNSIGNED
        sql("select u_smallint_test + tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test + u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test + smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test + u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test + mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test + u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test + integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test + u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test + bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test + u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test + decimal_test from test").type(DECIMAL);
        sql("select u_smallint_test + float_test from test").type(DOUBLE);
        sql("select u_smallint_test + double_test from test").type(DOUBLE);
        // MEDIUMINT
        sql("select mediumint_test + tinyint_test from test").type(BIGINT);
        sql("select mediumint_test + u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test + smallint_test from test").type(BIGINT);
        sql("select mediumint_test + u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test + mediumint_test from test").type(BIGINT);
        sql("select mediumint_test + u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test + integer_test from test").type(BIGINT);
        sql("select mediumint_test + u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test + bigint_test from test").type(BIGINT);
        sql("select mediumint_test + u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test + decimal_test from test").type(DECIMAL);
        sql("select mediumint_test + float_test from test").type(DOUBLE);
        sql("select mediumint_test + double_test from test").type(DOUBLE);
        // MEDIUMINT_UNSIGNED
        sql("select u_mediumint_test + tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test + u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test + smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test + u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test + mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test + u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test + integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test + u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test + bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test + u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test + decimal_test from test").type(DECIMAL);
        sql("select u_mediumint_test + float_test from test").type(DOUBLE);
        sql("select u_mediumint_test + double_test from test").type(DOUBLE);
        // INTEGER
        sql("select integer_test + tinyint_test from test").type(BIGINT);
        sql("select integer_test + u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test + smallint_test from test").type(BIGINT);
        sql("select integer_test + u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test + mediumint_test from test").type(BIGINT);
        sql("select integer_test + u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test + integer_test from test").type(BIGINT);
        sql("select integer_test + u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test + bigint_test from test").type(BIGINT);
        sql("select integer_test + u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test + decimal_test from test").type(DECIMAL);
        sql("select integer_test + float_test from test").type(DOUBLE);
        sql("select integer_test + double_test from test").type(DOUBLE);
        // INTEGER_UNSIGNED
        sql("select u_integer_test + tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test + u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test + smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test + u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test + mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test + u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test + integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test + u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test + bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test + u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test + decimal_test from test").type(DECIMAL);
        sql("select u_integer_test + float_test from test").type(DOUBLE);
        sql("select u_integer_test + double_test from test").type(DOUBLE);
        // BIGINT
        sql("select bigint_test + tinyint_test from test").type(BIGINT);
        sql("select bigint_test + u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test + smallint_test from test").type(BIGINT);
        sql("select bigint_test + u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test + mediumint_test from test").type(BIGINT);
        sql("select bigint_test + u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test + integer_test from test").type(BIGINT);
        sql("select bigint_test + u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test + bigint_test from test").type(BIGINT);
        sql("select bigint_test + u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test + decimal_test from test").type(DECIMAL);
        sql("select bigint_test + float_test from test").type(DOUBLE);
        sql("select bigint_test + double_test from test").type(DOUBLE);
        // BIGINT_UNSIGNED
        sql("select u_bigint_test + tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test + u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test + smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test + u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test + mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test + u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test + integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test + u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test + bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test + u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test + decimal_test from test").type(DECIMAL);
        sql("select u_bigint_test + float_test from test").type(DOUBLE);
        sql("select u_bigint_test + double_test from test").type(DOUBLE);
        // DECIMAL
        sql("select decimal_test + tinyint_test from test").type(DECIMAL);
        sql("select decimal_test + u_tinyint_test from test").type(DECIMAL);
        sql("select decimal_test + smallint_test from test").type(DECIMAL);
        sql("select decimal_test + u_smallint_test from test").type(DECIMAL);
        sql("select decimal_test + mediumint_test from test").type(DECIMAL);
        sql("select decimal_test + u_mediumint_test from test").type(DECIMAL);
        sql("select decimal_test + integer_test from test").type(DECIMAL);
        sql("select decimal_test + u_integer_test from test").type(DECIMAL);
        sql("select decimal_test + bigint_test from test").type(DECIMAL);
        sql("select decimal_test + u_bigint_test from test").type(DECIMAL);
        sql("select decimal_test + decimal_test from test").type(DECIMAL);
        sql("select decimal_test + float_test from test").type(DOUBLE);
        sql("select decimal_test + double_test from test").type(DOUBLE);
        // FLOAT
        sql("select float_test + tinyint_test from test").type(DOUBLE);
        sql("select float_test + u_tinyint_test from test").type(DOUBLE);
        sql("select float_test + smallint_test from test").type(DOUBLE);
        sql("select float_test + u_smallint_test from test").type(DOUBLE);
        sql("select float_test + mediumint_test from test").type(DOUBLE);
        sql("select float_test + u_mediumint_test from test").type(DOUBLE);
        sql("select float_test + integer_test from test").type(DOUBLE);
        sql("select float_test + u_integer_test from test").type(DOUBLE);
        sql("select float_test + bigint_test from test").type(DOUBLE);
        sql("select float_test + u_bigint_test from test").type(DOUBLE);
        sql("select float_test + decimal_test from test").type(DOUBLE);
        sql("select float_test + float_test from test").type(DOUBLE);
        sql("select float_test + double_test from test").type(DOUBLE);
        // DOUBLE
        sql("select double_test + tinyint_test from test").type(DOUBLE);
        sql("select double_test + u_tinyint_test from test").type(DOUBLE);
        sql("select double_test + smallint_test from test").type(DOUBLE);
        sql("select double_test + u_smallint_test from test").type(DOUBLE);
        sql("select double_test + mediumint_test from test").type(DOUBLE);
        sql("select double_test + u_mediumint_test from test").type(DOUBLE);
        sql("select double_test + integer_test from test").type(DOUBLE);
        sql("select double_test + u_integer_test from test").type(DOUBLE);
        sql("select double_test + bigint_test from test").type(DOUBLE);
        sql("select double_test + u_bigint_test from test").type(DOUBLE);
        sql("select double_test + decimal_test from test").type(DOUBLE);
        sql("select double_test + float_test from test").type(DOUBLE);
        sql("select double_test + double_test from test").type(DOUBLE);
        // numeric - type check.
        // TINYINT
        sql("select tinyint_test - tinyint_test from test").type(BIGINT);
        sql("select tinyint_test - u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test - smallint_test from test").type(BIGINT);
        sql("select tinyint_test - u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test - mediumint_test from test").type(BIGINT);
        sql("select tinyint_test - u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test - integer_test from test").type(BIGINT);
        sql("select tinyint_test - u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test - bigint_test from test").type(BIGINT);
        sql("select tinyint_test - u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test - decimal_test from test").type(DECIMAL);
        sql("select tinyint_test - float_test from test").type(DOUBLE);
        sql("select tinyint_test - double_test from test").type(DOUBLE);
        // TINYINT_UNSIGNED
        sql("select u_tinyint_test - tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test - u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test - smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test - u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test - mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test - u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test - integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test - u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test - bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test - u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test - decimal_test from test").type(DECIMAL);
        sql("select u_tinyint_test - float_test from test").type(DOUBLE);
        sql("select u_tinyint_test - double_test from test").type(DOUBLE);
        // SMALLINT
        sql("select smallint_test - tinyint_test from test").type(BIGINT);
        sql("select smallint_test - u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test - smallint_test from test").type(BIGINT);
        sql("select smallint_test - u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test - mediumint_test from test").type(BIGINT);
        sql("select smallint_test - u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test - integer_test from test").type(BIGINT);
        sql("select smallint_test - u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test - bigint_test from test").type(BIGINT);
        sql("select smallint_test - u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test - decimal_test from test").type(DECIMAL);
        sql("select smallint_test - float_test from test").type(DOUBLE);
        sql("select smallint_test - double_test from test").type(DOUBLE);
        // SMALLINT_UNSIGNED
        sql("select u_smallint_test - tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test - u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test - smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test - u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test - mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test - u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test - integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test - u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test - bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test - u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test - decimal_test from test").type(DECIMAL);
        sql("select u_smallint_test - float_test from test").type(DOUBLE);
        sql("select u_smallint_test - double_test from test").type(DOUBLE);
        // MEDIUMINT
        sql("select mediumint_test - tinyint_test from test").type(BIGINT);
        sql("select mediumint_test - u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test - smallint_test from test").type(BIGINT);
        sql("select mediumint_test - u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test - mediumint_test from test").type(BIGINT);
        sql("select mediumint_test - u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test - integer_test from test").type(BIGINT);
        sql("select mediumint_test - u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test - bigint_test from test").type(BIGINT);
        sql("select mediumint_test - u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test - decimal_test from test").type(DECIMAL);
        sql("select mediumint_test - float_test from test").type(DOUBLE);
        sql("select mediumint_test - double_test from test").type(DOUBLE);
        // MEDIUMINT_UNSIGNED
        sql("select u_mediumint_test - tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test - u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test - smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test - u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test - mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test - u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test - integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test - u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test - bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test - u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test - decimal_test from test").type(DECIMAL);
        sql("select u_mediumint_test - float_test from test").type(DOUBLE);
        sql("select u_mediumint_test - double_test from test").type(DOUBLE);
        // INTEGER
        sql("select integer_test - tinyint_test from test").type(BIGINT);
        sql("select integer_test - u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test - smallint_test from test").type(BIGINT);
        sql("select integer_test - u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test - mediumint_test from test").type(BIGINT);
        sql("select integer_test - u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test - integer_test from test").type(BIGINT);
        sql("select integer_test - u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test - bigint_test from test").type(BIGINT);
        sql("select integer_test - u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test - decimal_test from test").type(DECIMAL);
        sql("select integer_test - float_test from test").type(DOUBLE);
        sql("select integer_test - double_test from test").type(DOUBLE);
        // INTEGER_UNSIGNED
        sql("select u_integer_test - tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test - u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test - smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test - u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test - mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test - u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test - integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test - u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test - bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test - u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test - decimal_test from test").type(DECIMAL);
        sql("select u_integer_test - float_test from test").type(DOUBLE);
        sql("select u_integer_test - double_test from test").type(DOUBLE);
        // BIGINT
        sql("select bigint_test - tinyint_test from test").type(BIGINT);
        sql("select bigint_test - u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test - smallint_test from test").type(BIGINT);
        sql("select bigint_test - u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test - mediumint_test from test").type(BIGINT);
        sql("select bigint_test - u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test - integer_test from test").type(BIGINT);
        sql("select bigint_test - u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test - bigint_test from test").type(BIGINT);
        sql("select bigint_test - u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test - decimal_test from test").type(DECIMAL);
        sql("select bigint_test - float_test from test").type(DOUBLE);
        sql("select bigint_test - double_test from test").type(DOUBLE);
        // BIGINT_UNSIGNED
        sql("select u_bigint_test - tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test - u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test - smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test - u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test - mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test - u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test - integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test - u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test - bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test - u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test - decimal_test from test").type(DECIMAL);
        sql("select u_bigint_test - float_test from test").type(DOUBLE);
        sql("select u_bigint_test - double_test from test").type(DOUBLE);
        // DECIMAL
        sql("select decimal_test - tinyint_test from test").type(DECIMAL);
        sql("select decimal_test - u_tinyint_test from test").type(DECIMAL);
        sql("select decimal_test - smallint_test from test").type(DECIMAL);
        sql("select decimal_test - u_smallint_test from test").type(DECIMAL);
        sql("select decimal_test - mediumint_test from test").type(DECIMAL);
        sql("select decimal_test - u_mediumint_test from test").type(DECIMAL);
        sql("select decimal_test - integer_test from test").type(DECIMAL);
        sql("select decimal_test - u_integer_test from test").type(DECIMAL);
        sql("select decimal_test - bigint_test from test").type(DECIMAL);
        sql("select decimal_test - u_bigint_test from test").type(DECIMAL);
        sql("select decimal_test - decimal_test from test").type(DECIMAL);
        sql("select decimal_test - float_test from test").type(DOUBLE);
        sql("select decimal_test - double_test from test").type(DOUBLE);
        // FLOAT
        sql("select float_test - tinyint_test from test").type(DOUBLE);
        sql("select float_test - u_tinyint_test from test").type(DOUBLE);
        sql("select float_test - smallint_test from test").type(DOUBLE);
        sql("select float_test - u_smallint_test from test").type(DOUBLE);
        sql("select float_test - mediumint_test from test").type(DOUBLE);
        sql("select float_test - u_mediumint_test from test").type(DOUBLE);
        sql("select float_test - integer_test from test").type(DOUBLE);
        sql("select float_test - u_integer_test from test").type(DOUBLE);
        sql("select float_test - bigint_test from test").type(DOUBLE);
        sql("select float_test - u_bigint_test from test").type(DOUBLE);
        sql("select float_test - decimal_test from test").type(DOUBLE);
        sql("select float_test - float_test from test").type(DOUBLE);
        sql("select float_test - double_test from test").type(DOUBLE);
        // DOUBLE
        sql("select double_test - tinyint_test from test").type(DOUBLE);
        sql("select double_test - u_tinyint_test from test").type(DOUBLE);
        sql("select double_test - smallint_test from test").type(DOUBLE);
        sql("select double_test - u_smallint_test from test").type(DOUBLE);
        sql("select double_test - mediumint_test from test").type(DOUBLE);
        sql("select double_test - u_mediumint_test from test").type(DOUBLE);
        sql("select double_test - integer_test from test").type(DOUBLE);
        sql("select double_test - u_integer_test from test").type(DOUBLE);
        sql("select double_test - bigint_test from test").type(DOUBLE);
        sql("select double_test - u_bigint_test from test").type(DOUBLE);
        sql("select double_test - decimal_test from test").type(DOUBLE);
        sql("select double_test - float_test from test").type(DOUBLE);
        sql("select double_test - double_test from test").type(DOUBLE);
        // numeric * type check.
        // TINYINT
        sql("select tinyint_test * tinyint_test from test").type(BIGINT);
        sql("select tinyint_test * u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test * smallint_test from test").type(BIGINT);
        sql("select tinyint_test * u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test * mediumint_test from test").type(BIGINT);
        sql("select tinyint_test * u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test * integer_test from test").type(BIGINT);
        sql("select tinyint_test * u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test * bigint_test from test").type(BIGINT);
        sql("select tinyint_test * u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select tinyint_test * decimal_test from test").type(DECIMAL);
        sql("select tinyint_test * float_test from test").type(DOUBLE);
        sql("select tinyint_test * double_test from test").type(DOUBLE);
        // TINYINT_UNSIGNED
        sql("select u_tinyint_test * tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test * u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test * smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test * u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test * mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test * u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test * integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test * u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test * bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test * u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test * decimal_test from test").type(DECIMAL);
        sql("select u_tinyint_test * float_test from test").type(DOUBLE);
        sql("select u_tinyint_test * double_test from test").type(DOUBLE);
        // SMALLINT
        sql("select smallint_test * tinyint_test from test").type(BIGINT);
        sql("select smallint_test * u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test * smallint_test from test").type(BIGINT);
        sql("select smallint_test * u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test * mediumint_test from test").type(BIGINT);
        sql("select smallint_test * u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test * integer_test from test").type(BIGINT);
        sql("select smallint_test * u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test * bigint_test from test").type(BIGINT);
        sql("select smallint_test * u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select smallint_test * decimal_test from test").type(DECIMAL);
        sql("select smallint_test * float_test from test").type(DOUBLE);
        sql("select smallint_test * double_test from test").type(DOUBLE);
        // SMALLINT_UNSIGNED
        sql("select u_smallint_test * tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test * u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test * smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test * u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test * mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test * u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test * integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test * u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test * bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test * u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test * decimal_test from test").type(DECIMAL);
        sql("select u_smallint_test * float_test from test").type(DOUBLE);
        sql("select u_smallint_test * double_test from test").type(DOUBLE);
        // MEDIUMINT
        sql("select mediumint_test * tinyint_test from test").type(BIGINT);
        sql("select mediumint_test * u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test * smallint_test from test").type(BIGINT);
        sql("select mediumint_test * u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test * mediumint_test from test").type(BIGINT);
        sql("select mediumint_test * u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test * integer_test from test").type(BIGINT);
        sql("select mediumint_test * u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test * bigint_test from test").type(BIGINT);
        sql("select mediumint_test * u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select mediumint_test * decimal_test from test").type(DECIMAL);
        sql("select mediumint_test * float_test from test").type(DOUBLE);
        sql("select mediumint_test * double_test from test").type(DOUBLE);
        // MEDIUMINT_UNSIGNED
        sql("select u_mediumint_test * tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test * u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test * smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test * u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test * mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test * u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test * integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test * u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test * bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test * u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test * decimal_test from test").type(DECIMAL);
        sql("select u_mediumint_test * float_test from test").type(DOUBLE);
        sql("select u_mediumint_test * double_test from test").type(DOUBLE);
        // INTEGER
        sql("select integer_test * tinyint_test from test").type(BIGINT);
        sql("select integer_test * u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test * smallint_test from test").type(BIGINT);
        sql("select integer_test * u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test * mediumint_test from test").type(BIGINT);
        sql("select integer_test * u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test * integer_test from test").type(BIGINT);
        sql("select integer_test * u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test * bigint_test from test").type(BIGINT);
        sql("select integer_test * u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select integer_test * decimal_test from test").type(DECIMAL);
        sql("select integer_test * float_test from test").type(DOUBLE);
        sql("select integer_test * double_test from test").type(DOUBLE);
        // INTEGER_UNSIGNED
        sql("select u_integer_test * tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test * u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test * smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test * u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test * mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test * u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test * integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test * u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test * bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test * u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test * decimal_test from test").type(DECIMAL);
        sql("select u_integer_test * float_test from test").type(DOUBLE);
        sql("select u_integer_test * double_test from test").type(DOUBLE);
        // BIGINT
        sql("select bigint_test * tinyint_test from test").type(BIGINT);
        sql("select bigint_test * u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test * smallint_test from test").type(BIGINT);
        sql("select bigint_test * u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test * mediumint_test from test").type(BIGINT);
        sql("select bigint_test * u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test * integer_test from test").type(BIGINT);
        sql("select bigint_test * u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test * bigint_test from test").type(BIGINT);
        sql("select bigint_test * u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select bigint_test * decimal_test from test").type(DECIMAL);
        sql("select bigint_test * float_test from test").type(DOUBLE);
        sql("select bigint_test * double_test from test").type(DOUBLE);
        // BIGINT_UNSIGNED
        sql("select u_bigint_test * tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test * u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test * smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test * u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test * mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test * u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test * integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test * u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test * bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test * u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test * decimal_test from test").type(DECIMAL);
        sql("select u_bigint_test * float_test from test").type(DOUBLE);
        sql("select u_bigint_test * double_test from test").type(DOUBLE);
        // DECIMAL
        sql("select decimal_test * tinyint_test from test").type(DECIMAL);
        sql("select decimal_test * u_tinyint_test from test").type(DECIMAL);
        sql("select decimal_test * smallint_test from test").type(DECIMAL);
        sql("select decimal_test * u_smallint_test from test").type(DECIMAL);
        sql("select decimal_test * mediumint_test from test").type(DECIMAL);
        sql("select decimal_test * u_mediumint_test from test").type(DECIMAL);
        sql("select decimal_test * integer_test from test").type(DECIMAL);
        sql("select decimal_test * u_integer_test from test").type(DECIMAL);
        sql("select decimal_test * bigint_test from test").type(DECIMAL);
        sql("select decimal_test * u_bigint_test from test").type(DECIMAL);
        sql("select decimal_test * decimal_test from test").type(DECIMAL);
        sql("select decimal_test * float_test from test").type(DOUBLE);
        sql("select decimal_test * double_test from test").type(DOUBLE);
        // FLOAT
        sql("select float_test * tinyint_test from test").type(DOUBLE);
        sql("select float_test * u_tinyint_test from test").type(DOUBLE);
        sql("select float_test * smallint_test from test").type(DOUBLE);
        sql("select float_test * u_smallint_test from test").type(DOUBLE);
        sql("select float_test * mediumint_test from test").type(DOUBLE);
        sql("select float_test * u_mediumint_test from test").type(DOUBLE);
        sql("select float_test * integer_test from test").type(DOUBLE);
        sql("select float_test * u_integer_test from test").type(DOUBLE);
        sql("select float_test * bigint_test from test").type(DOUBLE);
        sql("select float_test * u_bigint_test from test").type(DOUBLE);
        sql("select float_test * decimal_test from test").type(DOUBLE);
        sql("select float_test * float_test from test").type(DOUBLE);
        sql("select float_test * double_test from test").type(DOUBLE);
        // DOUBLE
        sql("select double_test * tinyint_test from test").type(DOUBLE);
        sql("select double_test * u_tinyint_test from test").type(DOUBLE);
        sql("select double_test * smallint_test from test").type(DOUBLE);
        sql("select double_test * u_smallint_test from test").type(DOUBLE);
        sql("select double_test * mediumint_test from test").type(DOUBLE);
        sql("select double_test * u_mediumint_test from test").type(DOUBLE);
        sql("select double_test * integer_test from test").type(DOUBLE);
        sql("select double_test * u_integer_test from test").type(DOUBLE);
        sql("select double_test * bigint_test from test").type(DOUBLE);
        sql("select double_test * u_bigint_test from test").type(DOUBLE);
        sql("select double_test * decimal_test from test").type(DOUBLE);
        sql("select double_test * float_test from test").type(DOUBLE);
        sql("select double_test * double_test from test").type(DOUBLE);
        // numeric / type check.
        // TINYINT
        sql("select tinyint_test / tinyint_test from test").type(DECIMAL);
        sql("select tinyint_test / u_tinyint_test from test").type(DECIMAL);
        sql("select tinyint_test / smallint_test from test").type(DECIMAL);
        sql("select tinyint_test / u_smallint_test from test").type(DECIMAL);
        sql("select tinyint_test / mediumint_test from test").type(DECIMAL);
        sql("select tinyint_test / u_mediumint_test from test").type(DECIMAL);
        sql("select tinyint_test / integer_test from test").type(DECIMAL);
        sql("select tinyint_test / u_integer_test from test").type(DECIMAL);
        sql("select tinyint_test / bigint_test from test").type(DECIMAL);
        sql("select tinyint_test / u_bigint_test from test").type(DECIMAL);
        sql("select tinyint_test / decimal_test from test").type(DECIMAL);
        sql("select tinyint_test / float_test from test").type(DOUBLE);
        sql("select tinyint_test / double_test from test").type(DOUBLE);
        // TINYINT_UNSIGNED
        sql("select u_tinyint_test / tinyint_test from test").type(DECIMAL);
        sql("select u_tinyint_test / u_tinyint_test from test").type(DECIMAL);
        sql("select u_tinyint_test / smallint_test from test").type(DECIMAL);
        sql("select u_tinyint_test / u_smallint_test from test").type(DECIMAL);
        sql("select u_tinyint_test / mediumint_test from test").type(DECIMAL);
        sql("select u_tinyint_test / u_mediumint_test from test").type(DECIMAL);
        sql("select u_tinyint_test / integer_test from test").type(DECIMAL);
        sql("select u_tinyint_test / u_integer_test from test").type(DECIMAL);
        sql("select u_tinyint_test / bigint_test from test").type(DECIMAL);
        sql("select u_tinyint_test / u_bigint_test from test").type(DECIMAL);
        sql("select u_tinyint_test / decimal_test from test").type(DECIMAL);
        sql("select u_tinyint_test / float_test from test").type(DOUBLE);
        sql("select u_tinyint_test / double_test from test").type(DOUBLE);
        // SMALLINT
        sql("select smallint_test / tinyint_test from test").type(DECIMAL);
        sql("select smallint_test / u_tinyint_test from test").type(DECIMAL);
        sql("select smallint_test / smallint_test from test").type(DECIMAL);
        sql("select smallint_test / u_smallint_test from test").type(DECIMAL);
        sql("select smallint_test / mediumint_test from test").type(DECIMAL);
        sql("select smallint_test / u_mediumint_test from test").type(DECIMAL);
        sql("select smallint_test / integer_test from test").type(DECIMAL);
        sql("select smallint_test / u_integer_test from test").type(DECIMAL);
        sql("select smallint_test / bigint_test from test").type(DECIMAL);
        sql("select smallint_test / u_bigint_test from test").type(DECIMAL);
        sql("select smallint_test / decimal_test from test").type(DECIMAL);
        sql("select smallint_test / float_test from test").type(DOUBLE);
        sql("select smallint_test / double_test from test").type(DOUBLE);
        // SMALLINT_UNSIGNED
        sql("select u_smallint_test / tinyint_test from test").type(DECIMAL);
        sql("select u_smallint_test / u_tinyint_test from test").type(DECIMAL);
        sql("select u_smallint_test / smallint_test from test").type(DECIMAL);
        sql("select u_smallint_test / u_smallint_test from test").type(DECIMAL);
        sql("select u_smallint_test / mediumint_test from test").type(DECIMAL);
        sql("select u_smallint_test / u_mediumint_test from test").type(DECIMAL);
        sql("select u_smallint_test / integer_test from test").type(DECIMAL);
        sql("select u_smallint_test / u_integer_test from test").type(DECIMAL);
        sql("select u_smallint_test / bigint_test from test").type(DECIMAL);
        sql("select u_smallint_test / u_bigint_test from test").type(DECIMAL);
        sql("select u_smallint_test / decimal_test from test").type(DECIMAL);
        sql("select u_smallint_test / float_test from test").type(DOUBLE);
        sql("select u_smallint_test / double_test from test").type(DOUBLE);
        // MEDIUMINT
        sql("select mediumint_test / tinyint_test from test").type(DECIMAL);
        sql("select mediumint_test / u_tinyint_test from test").type(DECIMAL);
        sql("select mediumint_test / smallint_test from test").type(DECIMAL);
        sql("select mediumint_test / u_smallint_test from test").type(DECIMAL);
        sql("select mediumint_test / mediumint_test from test").type(DECIMAL);
        sql("select mediumint_test / u_mediumint_test from test").type(DECIMAL);
        sql("select mediumint_test / integer_test from test").type(DECIMAL);
        sql("select mediumint_test / u_integer_test from test").type(DECIMAL);
        sql("select mediumint_test / bigint_test from test").type(DECIMAL);
        sql("select mediumint_test / u_bigint_test from test").type(DECIMAL);
        sql("select mediumint_test / decimal_test from test").type(DECIMAL);
        sql("select mediumint_test / float_test from test").type(DOUBLE);
        sql("select mediumint_test / double_test from test").type(DOUBLE);
        // MEDIUMINT_UNSIGNED
        sql("select u_mediumint_test / tinyint_test from test").type(DECIMAL);
        sql("select u_mediumint_test / u_tinyint_test from test").type(DECIMAL);
        sql("select u_mediumint_test / smallint_test from test").type(DECIMAL);
        sql("select u_mediumint_test / u_smallint_test from test").type(DECIMAL);
        sql("select u_mediumint_test / mediumint_test from test").type(DECIMAL);
        sql("select u_mediumint_test / u_mediumint_test from test").type(DECIMAL);
        sql("select u_mediumint_test / integer_test from test").type(DECIMAL);
        sql("select u_mediumint_test / u_integer_test from test").type(DECIMAL);
        sql("select u_mediumint_test / bigint_test from test").type(DECIMAL);
        sql("select u_mediumint_test / u_bigint_test from test").type(DECIMAL);
        sql("select u_mediumint_test / decimal_test from test").type(DECIMAL);
        sql("select u_mediumint_test / float_test from test").type(DOUBLE);
        sql("select u_mediumint_test / double_test from test").type(DOUBLE);
        // INTEGER
        sql("select integer_test / tinyint_test from test").type(DECIMAL);
        sql("select integer_test / u_tinyint_test from test").type(DECIMAL);
        sql("select integer_test / smallint_test from test").type(DECIMAL);
        sql("select integer_test / u_smallint_test from test").type(DECIMAL);
        sql("select integer_test / mediumint_test from test").type(DECIMAL);
        sql("select integer_test / u_mediumint_test from test").type(DECIMAL);
        sql("select integer_test / integer_test from test").type(DECIMAL);
        sql("select integer_test / u_integer_test from test").type(DECIMAL);
        sql("select integer_test / bigint_test from test").type(DECIMAL);
        sql("select integer_test / u_bigint_test from test").type(DECIMAL);
        sql("select integer_test / decimal_test from test").type(DECIMAL);
        sql("select integer_test / float_test from test").type(DOUBLE);
        sql("select integer_test / double_test from test").type(DOUBLE);
        // INTEGER_UNSIGNED
        sql("select u_integer_test / tinyint_test from test").type(DECIMAL);
        sql("select u_integer_test / u_tinyint_test from test").type(DECIMAL);
        sql("select u_integer_test / smallint_test from test").type(DECIMAL);
        sql("select u_integer_test / u_smallint_test from test").type(DECIMAL);
        sql("select u_integer_test / mediumint_test from test").type(DECIMAL);
        sql("select u_integer_test / u_mediumint_test from test").type(DECIMAL);
        sql("select u_integer_test / integer_test from test").type(DECIMAL);
        sql("select u_integer_test / u_integer_test from test").type(DECIMAL);
        sql("select u_integer_test / bigint_test from test").type(DECIMAL);
        sql("select u_integer_test / u_bigint_test from test").type(DECIMAL);
        sql("select u_integer_test / decimal_test from test").type(DECIMAL);
        sql("select u_integer_test / float_test from test").type(DOUBLE);
        sql("select u_integer_test / double_test from test").type(DOUBLE);
        // BIGINT
        sql("select bigint_test / tinyint_test from test").type(DECIMAL);
        sql("select bigint_test / u_tinyint_test from test").type(DECIMAL);
        sql("select bigint_test / smallint_test from test").type(DECIMAL);
        sql("select bigint_test / u_smallint_test from test").type(DECIMAL);
        sql("select bigint_test / mediumint_test from test").type(DECIMAL);
        sql("select bigint_test / u_mediumint_test from test").type(DECIMAL);
        sql("select bigint_test / integer_test from test").type(DECIMAL);
        sql("select bigint_test / u_integer_test from test").type(DECIMAL);
        sql("select bigint_test / bigint_test from test").type(DECIMAL);
        sql("select bigint_test / u_bigint_test from test").type(DECIMAL);
        sql("select bigint_test / decimal_test from test").type(DECIMAL);
        sql("select bigint_test / float_test from test").type(DOUBLE);
        sql("select bigint_test / double_test from test").type(DOUBLE);
        // BIGINT_UNSIGNED
        sql("select u_bigint_test / tinyint_test from test").type(DECIMAL);
        sql("select u_bigint_test / u_tinyint_test from test").type(DECIMAL);
        sql("select u_bigint_test / smallint_test from test").type(DECIMAL);
        sql("select u_bigint_test / u_smallint_test from test").type(DECIMAL);
        sql("select u_bigint_test / mediumint_test from test").type(DECIMAL);
        sql("select u_bigint_test / u_mediumint_test from test").type(DECIMAL);
        sql("select u_bigint_test / integer_test from test").type(DECIMAL);
        sql("select u_bigint_test / u_integer_test from test").type(DECIMAL);
        sql("select u_bigint_test / bigint_test from test").type(DECIMAL);
        sql("select u_bigint_test / u_bigint_test from test").type(DECIMAL);
        sql("select u_bigint_test / decimal_test from test").type(DECIMAL);
        sql("select u_bigint_test / float_test from test").type(DOUBLE);
        sql("select u_bigint_test / double_test from test").type(DOUBLE);
        // DECIMAL
        sql("select decimal_test / tinyint_test from test").type(DECIMAL);
        sql("select decimal_test / u_tinyint_test from test").type(DECIMAL);
        sql("select decimal_test / smallint_test from test").type(DECIMAL);
        sql("select decimal_test / u_smallint_test from test").type(DECIMAL);
        sql("select decimal_test / mediumint_test from test").type(DECIMAL);
        sql("select decimal_test / u_mediumint_test from test").type(DECIMAL);
        sql("select decimal_test / integer_test from test").type(DECIMAL);
        sql("select decimal_test / u_integer_test from test").type(DECIMAL);
        sql("select decimal_test / bigint_test from test").type(DECIMAL);
        sql("select decimal_test / u_bigint_test from test").type(DECIMAL);
        sql("select decimal_test / decimal_test from test").type(DECIMAL);
        sql("select decimal_test / float_test from test").type(DOUBLE);
        sql("select decimal_test / double_test from test").type(DOUBLE);
        // FLOAT
        sql("select float_test / tinyint_test from test").type(DOUBLE);
        sql("select float_test / u_tinyint_test from test").type(DOUBLE);
        sql("select float_test / smallint_test from test").type(DOUBLE);
        sql("select float_test / u_smallint_test from test").type(DOUBLE);
        sql("select float_test / mediumint_test from test").type(DOUBLE);
        sql("select float_test / u_mediumint_test from test").type(DOUBLE);
        sql("select float_test / integer_test from test").type(DOUBLE);
        sql("select float_test / u_integer_test from test").type(DOUBLE);
        sql("select float_test / bigint_test from test").type(DOUBLE);
        sql("select float_test / u_bigint_test from test").type(DOUBLE);
        sql("select float_test / decimal_test from test").type(DOUBLE);
        sql("select float_test / float_test from test").type(DOUBLE);
        sql("select float_test / double_test from test").type(DOUBLE);
        // DOUBLE
        sql("select double_test / tinyint_test from test").type(DOUBLE);
        sql("select double_test / u_tinyint_test from test").type(DOUBLE);
        sql("select double_test / smallint_test from test").type(DOUBLE);
        sql("select double_test / u_smallint_test from test").type(DOUBLE);
        sql("select double_test / mediumint_test from test").type(DOUBLE);
        sql("select double_test / u_mediumint_test from test").type(DOUBLE);
        sql("select double_test / integer_test from test").type(DOUBLE);
        sql("select double_test / u_integer_test from test").type(DOUBLE);
        sql("select double_test / bigint_test from test").type(DOUBLE);
        sql("select double_test / u_bigint_test from test").type(DOUBLE);
        sql("select double_test / decimal_test from test").type(DOUBLE);
        sql("select double_test / float_test from test").type(DOUBLE);
        sql("select double_test / double_test from test").type(DOUBLE);
        // numeric % type check.
        // TINYINT
        sql("select tinyint_test % tinyint_test from test").type(BIGINT);
        sql("select tinyint_test % u_tinyint_test from test").type(BIGINT);
        sql("select tinyint_test % smallint_test from test").type(BIGINT);
        sql("select tinyint_test % u_smallint_test from test").type(BIGINT);
        sql("select tinyint_test % mediumint_test from test").type(BIGINT);
        sql("select tinyint_test % u_mediumint_test from test").type(BIGINT);
        sql("select tinyint_test % integer_test from test").type(BIGINT);
        sql("select tinyint_test % u_integer_test from test").type(BIGINT);
        sql("select tinyint_test % bigint_test from test").type(BIGINT);
        sql("select tinyint_test % u_bigint_test from test").type(BIGINT);
        sql("select tinyint_test % decimal_test from test").type(DECIMAL);
        sql("select tinyint_test % float_test from test").type(DOUBLE);
        sql("select tinyint_test % double_test from test").type(DOUBLE);
        // TINYINT_UNSIGNED
        sql("select u_tinyint_test % tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test % u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test % smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test % u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test % mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test % u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test % integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test % u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test % bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test % u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_tinyint_test % decimal_test from test").type(DECIMAL);
        sql("select u_tinyint_test % float_test from test").type(DOUBLE);
        sql("select u_tinyint_test % double_test from test").type(DOUBLE);
        // SMALLINT
        sql("select smallint_test % tinyint_test from test").type(BIGINT);
        sql("select smallint_test % u_tinyint_test from test").type(BIGINT);
        sql("select smallint_test % smallint_test from test").type(BIGINT);
        sql("select smallint_test % u_smallint_test from test").type(BIGINT);
        sql("select smallint_test % mediumint_test from test").type(BIGINT);
        sql("select smallint_test % u_mediumint_test from test").type(BIGINT);
        sql("select smallint_test % integer_test from test").type(BIGINT);
        sql("select smallint_test % u_integer_test from test").type(BIGINT);
        sql("select smallint_test % bigint_test from test").type(BIGINT);
        sql("select smallint_test % u_bigint_test from test").type(BIGINT);
        sql("select smallint_test % decimal_test from test").type(DECIMAL);
        sql("select smallint_test % float_test from test").type(DOUBLE);
        sql("select smallint_test % double_test from test").type(DOUBLE);
        // SMALLINT_UNSIGNED
        sql("select u_smallint_test % tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test % u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test % smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test % u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test % mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test % u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test % integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test % u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test % bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test % u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_smallint_test % decimal_test from test").type(DECIMAL);
        sql("select u_smallint_test % float_test from test").type(DOUBLE);
        sql("select u_smallint_test % double_test from test").type(DOUBLE);
        // MEDIUMINT
        sql("select mediumint_test % tinyint_test from test").type(BIGINT);
        sql("select mediumint_test % u_tinyint_test from test").type(BIGINT);
        sql("select mediumint_test % smallint_test from test").type(BIGINT);
        sql("select mediumint_test % u_smallint_test from test").type(BIGINT);
        sql("select mediumint_test % mediumint_test from test").type(BIGINT);
        sql("select mediumint_test % u_mediumint_test from test").type(BIGINT);
        sql("select mediumint_test % integer_test from test").type(BIGINT);
        sql("select mediumint_test % u_integer_test from test").type(BIGINT);
        sql("select mediumint_test % bigint_test from test").type(BIGINT);
        sql("select mediumint_test % u_bigint_test from test").type(BIGINT);
        sql("select mediumint_test % decimal_test from test").type(DECIMAL);
        sql("select mediumint_test % float_test from test").type(DOUBLE);
        sql("select mediumint_test % double_test from test").type(DOUBLE);
        // MEDIUMINT_UNSIGNED
        sql("select u_mediumint_test % tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test % u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test % smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test % u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test % mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test % u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test % integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test % u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test % bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test % u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_mediumint_test % decimal_test from test").type(DECIMAL);
        sql("select u_mediumint_test % float_test from test").type(DOUBLE);
        sql("select u_mediumint_test % double_test from test").type(DOUBLE);
        // INTEGER
        sql("select integer_test % tinyint_test from test").type(BIGINT);
        sql("select integer_test % u_tinyint_test from test").type(BIGINT);
        sql("select integer_test % smallint_test from test").type(BIGINT);
        sql("select integer_test % u_smallint_test from test").type(BIGINT);
        sql("select integer_test % mediumint_test from test").type(BIGINT);
        sql("select integer_test % u_mediumint_test from test").type(BIGINT);
        sql("select integer_test % integer_test from test").type(BIGINT);
        sql("select integer_test % u_integer_test from test").type(BIGINT);
        sql("select integer_test % bigint_test from test").type(BIGINT);
        sql("select integer_test % u_bigint_test from test").type(BIGINT);
        sql("select integer_test % decimal_test from test").type(DECIMAL);
        sql("select integer_test % float_test from test").type(DOUBLE);
        sql("select integer_test % double_test from test").type(DOUBLE);
        // INTEGER_UNSIGNED
        sql("select u_integer_test % tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test % u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test % smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test % u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test % mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test % u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test % integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test % u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test % bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test % u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_integer_test % decimal_test from test").type(DECIMAL);
        sql("select u_integer_test % float_test from test").type(DOUBLE);
        sql("select u_integer_test % double_test from test").type(DOUBLE);
        // BIGINT
        sql("select bigint_test % tinyint_test from test").type(BIGINT);
        sql("select bigint_test % u_tinyint_test from test").type(BIGINT);
        sql("select bigint_test % smallint_test from test").type(BIGINT);
        sql("select bigint_test % u_smallint_test from test").type(BIGINT);
        sql("select bigint_test % mediumint_test from test").type(BIGINT);
        sql("select bigint_test % u_mediumint_test from test").type(BIGINT);
        sql("select bigint_test % integer_test from test").type(BIGINT);
        sql("select bigint_test % u_integer_test from test").type(BIGINT);
        sql("select bigint_test % bigint_test from test").type(BIGINT);
        sql("select bigint_test % u_bigint_test from test").type(BIGINT);
        sql("select bigint_test % decimal_test from test").type(DECIMAL);
        sql("select bigint_test % float_test from test").type(DOUBLE);
        sql("select bigint_test % double_test from test").type(DOUBLE);
        // BIGINT_UNSIGNED
        sql("select u_bigint_test % tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test % u_tinyint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test % smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test % u_smallint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test % mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test % u_mediumint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test % integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test % u_integer_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test % bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test % u_bigint_test from test").type(BIGINT_UNSIGNED);
        sql("select u_bigint_test % decimal_test from test").type(DECIMAL);
        sql("select u_bigint_test % float_test from test").type(DOUBLE);
        sql("select u_bigint_test % double_test from test").type(DOUBLE);
        // DECIMAL
        sql("select decimal_test % tinyint_test from test").type(DECIMAL);
        sql("select decimal_test % u_tinyint_test from test").type(DECIMAL);
        sql("select decimal_test % smallint_test from test").type(DECIMAL);
        sql("select decimal_test % u_smallint_test from test").type(DECIMAL);
        sql("select decimal_test % mediumint_test from test").type(DECIMAL);
        sql("select decimal_test % u_mediumint_test from test").type(DECIMAL);
        sql("select decimal_test % integer_test from test").type(DECIMAL);
        sql("select decimal_test % u_integer_test from test").type(DECIMAL);
        sql("select decimal_test % bigint_test from test").type(DECIMAL);
        sql("select decimal_test % u_bigint_test from test").type(DECIMAL);
        sql("select decimal_test % decimal_test from test").type(DECIMAL);
        sql("select decimal_test % float_test from test").type(DOUBLE);
        sql("select decimal_test % double_test from test").type(DOUBLE);
        // FLOAT
        sql("select float_test % tinyint_test from test").type(DOUBLE);
        sql("select float_test % u_tinyint_test from test").type(DOUBLE);
        sql("select float_test % smallint_test from test").type(DOUBLE);
        sql("select float_test % u_smallint_test from test").type(DOUBLE);
        sql("select float_test % mediumint_test from test").type(DOUBLE);
        sql("select float_test % u_mediumint_test from test").type(DOUBLE);
        sql("select float_test % integer_test from test").type(DOUBLE);
        sql("select float_test % u_integer_test from test").type(DOUBLE);
        sql("select float_test % bigint_test from test").type(DOUBLE);
        sql("select float_test % u_bigint_test from test").type(DOUBLE);
        sql("select float_test % decimal_test from test").type(DOUBLE);
        sql("select float_test % float_test from test").type(DOUBLE);
        sql("select float_test % double_test from test").type(DOUBLE);
        // DOUBLE
        sql("select double_test % tinyint_test from test").type(DOUBLE);
        sql("select double_test % u_tinyint_test from test").type(DOUBLE);
        sql("select double_test % smallint_test from test").type(DOUBLE);
        sql("select double_test % u_smallint_test from test").type(DOUBLE);
        sql("select double_test % mediumint_test from test").type(DOUBLE);
        sql("select double_test % u_mediumint_test from test").type(DOUBLE);
        sql("select double_test % integer_test from test").type(DOUBLE);
        sql("select double_test % u_integer_test from test").type(DOUBLE);
        sql("select double_test % bigint_test from test").type(DOUBLE);
        sql("select double_test % u_bigint_test from test").type(DOUBLE);
        sql("select double_test % decimal_test from test").type(DOUBLE);
        sql("select double_test % float_test from test").type(DOUBLE);
        sql("select double_test % double_test from test").type(DOUBLE);

    }

    private String getFiledName(SqlTypeName sqlTypeName) {
        String type = sqlTypeName.toString().toLowerCase();
        int index = type.indexOf("_unsigned");
        if (index != -1) {
            type = "u_" + type.substring(0, index);
        }
        type = type + "_test";
        return type;
    }

    @Ignore
    @Test
    public void testComparison() {
        for (SqlKind cmp : SqlKind.BINARY_COMPARISON) {
            String op = cmp.sql;
            System.out.println("the current op is: " + op);
            // string, string -> comparing as strings
            sql("select varchar_test " + op + " varchar_test from test")
                .type(BIGINT)
                .noCast();

            // numeric, numeric -> comparing as numeric
            sql("select integer_test " + op + " decimal_test, "
                + "decimal_test " + op + " double_test, "
                + "integer_test " + op + " double_test from test")
                .type(BIGINT, BIGINT, BIGINT)
                .noCast();

            // datetime, literal -> comparing as datetime
            sql("select datetime_test " + op + " 'a', "
                + "datetime_test " + op + " 1.0, "
                + "time_test " + op + " 'a', "
                + "time_test " + op + " 1.0, "
                + "date_test " + op + " 'a', "
                + "date_test " + op + " 1.0, "
                + "timestamp_test " + op + " 'a', "
                + "timestamp_test " + op + " 1.0 from test")
                .type(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT)
                .cast(DECIMAL, DATETIME)
                .cast(DECIMAL, TIME)
                .cast(DECIMAL, DATE)
                .cast(DECIMAL, DATE)
                .cast(CHAR, DATETIME)
                .cast(CHAR, TIME)
                .cast(CHAR, DATE)
                .cast(CHAR, DATE);

            // in other case, all operands are compared as double
            sql("select datetime_test " + op + " time_test, "
                + "datetime_test " + op + " varchar_test, "
                + "datetime_test " + op + " integer_test, "
                + "varchar_test " + op + " integer_test, "
                + "varchar_test " + op + " double_test from test")
                .type(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT)
                .cast(TIME, DOUBLE)
                .cast(DATETIME, DOUBLE)
                .cast(VARCHAR, DOUBLE)
                .noCast(INTEGER, DOUBLE)
                .noCast(DOUBLE, DOUBLE);
        }
    }

    @Test
    public void testCaseWhen() {
        // int types -> high level int type, but need no cast
        sql("select case when integer_test then integer_test "
            + "when tinyint_test then tinyint_test "
            + "when smallint_test then smallint_test "
            + "when mediumint_test then mediumint_test "
            + "else bigint_test end from test")
            .type(BIGINT)
            .noCast();

        // int, decimal -> decimal
        sql("select case when integer_test then integer_test "
            + "when tinyint_test then tinyint_test "
            + "when smallint_test then smallint_test "
            + "when mediumint_test then mediumint_test "
            + "when bigint_test then bigint_test "
            + "else decimal_test end from test")
            .type(DECIMAL)
            .cast(TINYINT, DECIMAL)
            .cast(SMALLINT, DECIMAL)
            .cast(MEDIUMINT, DECIMAL)
            .cast(INTEGER, DECIMAL)
            .cast(BIGINT, DECIMAL);

        // exact type -> approx type
        sql("select case when integer_test then integer_test "
            + "when tinyint_test then tinyint_test "
            + "when smallint_test then smallint_test "
            + "when mediumint_test then mediumint_test "
            + "when bigint_test then bigint_test "
            + "when decimal_test then decimal_test "
            + "when float_test then float_test "
            + "else double_test end from test")
            .type(DOUBLE)
            .cast(DECIMAL, DOUBLE)
            .noCast(FLOAT, DOUBLE);

        // among date time types -> DateTime type
        sql("select case when timestamp_test then timestamp_test "
            + "when date_test then date_test "
            + "when time_test then time_test "
            + "else datetime_test end from test")
            .type(DATETIME)
            .cast(TIMESTAMP, DATETIME)
            .cast(DATE, DATETIME)
            .cast(TIME, DATETIME);

        // numeric types, datetime types -> varchar type
        sql("select case when timestamp_test then timestamp_test "
            + "when date_test then date_test "
            + "when time_test then time_test "
            + "when datetime_test then datetime_test "
            + "when integer_test then integer_test "
            + "when tinyint_test then tinyint_test "
            + "when smallint_test then smallint_test "
            + "when mediumint_test then mediumint_test "
            + "when bigint_test then bigint_test "
            + "when decimal_test then decimal_test "
            + "when float_test then float_test "
            + "else double_test end from test")
            .type(VARCHAR)
            .cast(TIMESTAMP, VARCHAR)
            .cast(DATE, VARCHAR)
            .cast(TIME, VARCHAR)
            .cast(DATETIME, VARCHAR)
            .cast(TINYINT, VARCHAR)
            .cast(SMALLINT, VARCHAR)
            .cast(MEDIUMINT, VARCHAR)
            .cast(INTEGER, VARCHAR)
            .cast(BIGINT, VARCHAR)
            .cast(DECIMAL, VARCHAR)
            .cast(FLOAT, VARCHAR)
            .cast(DOUBLE, VARCHAR);

        sql("select case when integer_test > 0 then null else integer_test end from test")
            .type(INTEGER)
            .relType(INTEGER)
            .noCast();
    }

    @Test
    public void testIf() {
        // numeric
        sql("select "
            + "if(0 > 1, integer_test, bigint_test), "
            + "if(0 > 1, bigint_test, decimal_test), "
            + "if(0 > 1, decimal_test, float_test), "
            + "if(0 > 1, decimal_test, double_test) "
            + "from test")
            .type(BIGINT, DECIMAL, DOUBLE, DOUBLE)
            .cast(BIGINT, DECIMAL)
            .cast(DECIMAL, DOUBLE)
            .noCast(INTEGER, BIGINT);

        // datetime
        sql("select "
            + "if(0 > 1, date_test, timestamp_test), "
            + "if(0 > 1, time_test, timestamp_test), "
            + "if(0 > 1, datetime_test, timestamp_test), "
            + "if(0 > 1, date_test, date_test), "
            + "if(0 > 1, time_test, time_test), "
            + "if(0 > 1, datetime_test, datetime_test), "
            + "if(0 > 1, timestamp_test, timestamp_test) "
            + "from test")
            .type(DATETIME, DATETIME, DATETIME, DATE, TIME, DATETIME, TIMESTAMP)
            .cast(DATE, DATETIME)
            .cast(TIME, DATETIME)
            .cast(TIMESTAMP, DATETIME);

        // varchar
        sql("select "
            + "if(0 > 1, date_test, integer_test), "
            + "if(0 > 1, time_test, decimal_test), "
            + "if(0 > 1, varchar_test, timestamp_test), "
            + "if(0 > 1, varchar_test, decimal_test) "
            + "from test")
            .type(VARCHAR, VARCHAR, VARCHAR, VARCHAR)
            .cast(DATE, VARCHAR)
            .cast(TIME, VARCHAR)
            .cast(TIMESTAMP, VARCHAR)
            .cast(INTEGER, VARCHAR)
            .cast(DECIMAL, VARCHAR);

        // null
        sql("select "
            + "if(0 > 1, null, integer_test), "
            + "if(0 > 1, null, decimal_test), "
            + "if(0 > 1, null, timestamp_test), "
            + "if(0 > 1, null, varchar_test) "
            + "from test")
            .type(INTEGER, DECIMAL, TIMESTAMP, VARCHAR)
            .noCast();
    }

    @Test
    public void testIfNull() {
        // numeric
        sql("select "
            + "ifnull(integer_test, bigint_test), "
            + "ifnull(bigint_test, decimal_test), "
            + "ifnull(decimal_test, float_test), "
            + "ifnull(decimal_test, double_test) "
            + "from test")
            .type(BIGINT, DECIMAL, DOUBLE, DOUBLE)
            .cast(BIGINT, DECIMAL)
            .cast(DECIMAL, DOUBLE)
            .noCast(INTEGER, BIGINT);

        // datetime
        sql("select "
            + "ifnull(date_test, timestamp_test), "
            + "ifnull(time_test, timestamp_test), "
            + "ifnull(datetime_test, timestamp_test), "
            + "ifnull(date_test, date_test), "
            + "ifnull(time_test, time_test), "
            + "ifnull(datetime_test, datetime_test), "
            + "ifnull(timestamp_test, timestamp_test) "
            + "from test")
            .type(DATETIME, DATETIME, DATETIME, DATE, TIME, DATETIME, TIMESTAMP)
            .cast(DATE, DATETIME)
            .cast(TIME, DATETIME)
            .cast(TIMESTAMP, DATETIME);

        // varchar
        sql("select "
            + "ifnull(date_test, integer_test), "
            + "ifnull(time_test, decimal_test), "
            + "ifnull(varchar_test, timestamp_test), "
            + "ifnull(varchar_test, decimal_test) "
            + "from test")
            .type(VARCHAR, VARCHAR, VARCHAR, VARCHAR)
            .cast(DATE, VARCHAR)
            .cast(TIME, VARCHAR)
            .cast(TIMESTAMP, VARCHAR)
            .cast(INTEGER, VARCHAR)
            .cast(DECIMAL, VARCHAR);

        // null
        sql("select "
            + "ifnull(null, integer_test), "
            + "ifnull(null, decimal_test), "
            + "ifnull(null, timestamp_test), "
            + "ifnull(null, varchar_test) "
            + "from test")
            .type(INTEGER, DECIMAL, TIMESTAMP, VARCHAR)
            .noCast();
    }

    @Test
    public void testCoalesce() {
        // int types -> high level int type, but need no cast
        sql("select coalesce(integer_test, tinyint_test, smallint_test, mediumint_test, bigint_test) from test")
            .type(BIGINT)
            .noCast();

        // int, decimal -> decimal
        sql("select coalesce(integer_test, tinyint_test, smallint_test, mediumint_test, bigint_test, decimal_test) from test")
            .type(DECIMAL)
            .cast(TINYINT, DECIMAL)
            .cast(SMALLINT, DECIMAL)
            .cast(MEDIUMINT, DECIMAL)
            .cast(INTEGER, DECIMAL)
            .cast(BIGINT, DECIMAL);

        // exact type -> approx type
        sql("select coalesce(integer_test, tinyint_test, smallint_test, mediumint_test, bigint_test, decimal_test, float_test, double_test) from test")
            .type(DOUBLE)
            .cast(DECIMAL, DOUBLE)
            .noCast(FLOAT, DOUBLE);

        // among date time types -> DateTime type
        sql("select coalesce(timestamp_test, date_test, time_test, datetime_test) from test")
            .type(DATETIME)
            .cast(TIMESTAMP, DATETIME)
            .cast(DATE, DATETIME)
            .cast(TIME, DATETIME);

        // numeric types, datetime types -> varchar type
        sql("select coalesce(timestamp_test, date_test, time_test, datetime_test, integer_test, tinyint_test, smallint_test, mediumint_test, bigint_test, decimal_test, float_test, double_test) from test")
            .type(VARCHAR)
            .cast(TIMESTAMP, VARCHAR)
            .cast(DATE, VARCHAR)
            .cast(TIME, VARCHAR)
            .cast(DATETIME, VARCHAR)
            .cast(TINYINT, VARCHAR)
            .cast(SMALLINT, VARCHAR)
            .cast(MEDIUMINT, VARCHAR)
            .cast(INTEGER, VARCHAR)
            .cast(BIGINT, VARCHAR)
            .cast(DECIMAL, VARCHAR)
            .cast(FLOAT, VARCHAR)
            .cast(DOUBLE, VARCHAR);
    }

    @Test
    public void testDynamicParam() {
        sql("select '90.0' + 1")
            .type(DOUBLE)
            .cast(CHAR, DOUBLE);
    }

    @Ignore
    @Test
    public void testNoParameterized() {
        // datetime, literal -> comparing as datetime
        sql("select datetime_test > 'a', "
            + "datetime_test > 1.0, "
            + "time_test > 'a', "
            + "time_test > 1.0, "
            + "date_test > 'a', "
            + "date_test > 1.0, "
            + "timestamp_test > 'a', "
            + "timestamp_test > 1.0 from test", false)
            .type(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT)
            .cast(DECIMAL, DATETIME)
            .cast(DECIMAL, TIME)
            .cast(DECIMAL, DATE)
            .cast(DECIMAL, DATE)
            .cast(CHAR, DATETIME)
            .cast(CHAR, TIME)
            .cast(CHAR, DATE)
            .cast(CHAR, DATE);
    }

    @Test
    public void testCast() {
        sql("select cast(16 as UNSIGNED)")
            .type(BIGINT_UNSIGNED);

        sql("select cast(cast(16 as UNSIGNED) as SIGNED)")
            .type(BIGINT);

        sql("select cast(16 as UNSIGNED) * 16")
            .type(BIGINT_UNSIGNED);

        sql("select coalesce(cast(16 as UNSIGNED), 16)")
            .type(BIGINT_UNSIGNED);

        sql("select cast(null as signed)")
            .type(BIGINT);

        sql("select cast(null as unsigned)")
            .type(BIGINT_UNSIGNED);

        sql("SELECT 37 * COALESCE ( - - COALESCE ( - - ( CASE + + 77 WHEN + - 70 + - + COUNT( * ) THEN 86 END ), + - 92 * - 34 - - 36 * + + MAX( 44 ), 50 * - 87 ), - 26 - + CAST( - SUM( CAST( ( 6 ) AS DECIMAL ) ) + + 47 AS SIGNED ) ) AS col1")
            .type(BIGINT);

        sql("select CAST( - SUM( CAST( ( 6 ) AS DECIMAL ) ) + + 47 AS SIGNED )")
            .type(BIGINT);

        // BIGINT
        sql("select cast(tinyint_test as signed) from test").type(BIGINT);
        sql("select cast(integer_test as signed) from test").type(BIGINT);
        sql("select cast(bigint_test as signed) from test").type(BIGINT);
        sql("select cast(decimal_test as signed) from test").type(BIGINT);
        sql("select cast(float_test as signed) from test").type(BIGINT);
        sql("select cast(double_test as signed) from test").type(BIGINT);
        sql("select cast(varchar_test as signed) from test").type(BIGINT);
        sql("select cast(time_test as signed) from test").type(BIGINT);
        sql("select cast(date_test as signed) from test").type(BIGINT);
        sql("select cast(datetime_test as signed) from test").type(BIGINT);
        sql("select cast(timestamp_test as signed) from test").type(BIGINT);

        // BIGINT_UNSIGNED
        sql("select cast(tinyint_test as unsigned) from test").type(BIGINT_UNSIGNED);
        sql("select cast(integer_test as unsigned) from test").type(BIGINT_UNSIGNED);
        sql("select cast(bigint_test as unsigned) from test").type(BIGINT_UNSIGNED);
        sql("select cast(decimal_test as unsigned) from test").type(BIGINT_UNSIGNED);
        sql("select cast(float_test as unsigned) from test").type(BIGINT_UNSIGNED);
        sql("select cast(double_test as unsigned) from test").type(BIGINT_UNSIGNED);
        sql("select cast(varchar_test as unsigned) from test").type(BIGINT_UNSIGNED);
        sql("select cast(time_test as unsigned) from test").type(BIGINT_UNSIGNED);
        sql("select cast(date_test as unsigned) from test").type(BIGINT_UNSIGNED);
        sql("select cast(datetime_test as unsigned) from test").type(BIGINT_UNSIGNED);
        sql("select cast(timestamp_test as unsigned) from test").type(BIGINT_UNSIGNED);

    }

    @Test
    public void testInferenceWithNullability() {
        sql("SELECT COALESCE ( + + 36, + - 24, - 55 + + COUNT( * ) + 39 + - COUNT( * ) ) * 92")
            .type(BIGINT);
        sql("select (((-8 + MIN((- (+ 16)))) + (- (+ 32))) - (CAST(NULL AS SIGNED) * (- (+ 79))))")
            .type(BIGINT);
        sql("select CAST(NULL AS SIGNED) * (- (+ 79))", false)
            .type(BIGINT);
    }

    @Test
    public void testJson() {
        sql("select cast(true as JSON)").type(JSON);
    }

    @Test
    public void testNullif() {
        sql("select nullif(double_test, date_test) from test").type(DOUBLE);
        sql("select nullif(bigint_test, date_test) from test").type(BIGINT);
        sql("select nullif(decimal_test, date_test) from test").type(DECIMAL);
        sql("select nullif(date_test, date_test) from test").type(VARCHAR);
        sql("select nullif(date_test, '1') from test").type(VARCHAR);
    }

    @Test
    public void testRound() {
        sql("select round(1.0)").type(DECIMAL);
        sql("select round('1.0')").type(DOUBLE);
        sql("select round(1)").type(BIGINT);
        sql("select round(decimal_test) from test").type(DECIMAL);
        sql("select round(double_test) from test").type(DOUBLE);
        sql("select round(bigint_test) from test").type(BIGINT);
        sql("select round(integer_test) from test").type(BIGINT);
        sql("select round(varchar_test) from test").type(DOUBLE);
        sql("select round(time_test) from test").type(DOUBLE);
        sql("select round(timestamp_test) from test").type(DOUBLE);
        sql("select round(datetime_test) from test").type(DOUBLE);
        sql("select round(date_test) from test").type(DOUBLE);

        sql("select round(1.0, 1.0)").type(DECIMAL);
        sql("select round('1.0', 2.0)").type(DOUBLE);
        sql("select round(1, 2.0)").type(BIGINT);
        sql("select round(decimal_test, 2.0) from test").type(DECIMAL);
        sql("select round(double_test, 2.0) from test").type(DOUBLE);
        sql("select round(bigint_test, 2.0) from test").type(BIGINT);
        sql("select round(integer_test, 2.0) from test").type(BIGINT);
        sql("select round(varchar_test, 2.0) from test").type(DOUBLE);
        sql("select round(time_test, 2.0) from test").type(DOUBLE);
        sql("select round(timestamp_test, 2.0) from test").type(DOUBLE);
        sql("select round(datetime_test, 2.0) from test").type(DOUBLE);
        sql("select round(date_test, 2.0) from test").type(DOUBLE);
    }
}

