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

package org.apache.calcite.sql.type;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Standard operand type checker implementation according to MySQL 5.7
 */
public class MySQLStandardOperandTypeCheckers {

    /**
     * For ABS(INT)
     */
    public static final SqlSingleOperandTypeChecker ABS_OPERAND_TYPE_CHECKER_INTEGER =
        new AssertionOperandTypeChecker(
            ImmutableList.of(SqlTypeFamily.INTEGER),
            Predicates.alwaysFalse()
        );

    /**
     * For ABS(STRING)，
     */
    public static final SqlSingleOperandTypeChecker ABS_OPERAND_TYPE_CHECKER_REAL =
        new AssertionOperandTypeChecker(
            ImmutableList.of(SqlTypeFamily.APPROXIMATE_NUMERIC),
            ImmutableList.of(
                // for operand0, use string -> double cast.
                ImmutableMap.of(SqlTypeFamily.STRING, SqlTypeName.DOUBLE)
            ),
            Predicates.alwaysFalse()
        );

    /**
     * For ABS(DECIMAL)
     */
    public static final SqlSingleOperandTypeChecker ABS_OPERAND_TYPE_CHECKER_DECIMAL =
        new AssertionOperandTypeChecker(
            ImmutableList.of(SqlTypeFamily.DECIMAL),
            Predicates.alwaysFalse()
        );

    /**
     * For DIVIDE(NUMERIC, NUMERIC)
     */
    public static final SqlSingleOperandTypeChecker DIVIDE_OPERAND_TYPE_CHECKER =
        new AssertionOperandTypeChecker(
            ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
            ImmutableList.of(
                // for operand0 & operand1, use string -> double cast.
                ImmutableMap.of(SqlTypeFamily.STRING, SqlTypeName.DOUBLE, SqlTypeFamily.DATETIME, SqlTypeName.DECIMAL),
                ImmutableMap.of(SqlTypeFamily.STRING, SqlTypeName.DOUBLE, SqlTypeFamily.DATETIME, SqlTypeName.DECIMAL)
            ),
            Predicates.alwaysFalse()
        );

    /**
     * For MOD(NUMERIC, NUMERIC)
     */
    public static final SqlSingleOperandTypeChecker MOD_OPERAND_TYPE_CHECKER =
        new AssertionOperandTypeChecker(
            ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
            ImmutableList.of(
                // for operand0 & operand1, use string -> double cast.
                ImmutableMap.of(SqlTypeFamily.STRING, SqlTypeName.DOUBLE, SqlTypeFamily.DATETIME, SqlTypeName.BIGINT),
                ImmutableMap.of(SqlTypeFamily.STRING, SqlTypeName.DOUBLE, SqlTypeFamily.DATETIME, SqlTypeName.BIGINT)
            ),
            Predicates.alwaysFalse()
        );
}