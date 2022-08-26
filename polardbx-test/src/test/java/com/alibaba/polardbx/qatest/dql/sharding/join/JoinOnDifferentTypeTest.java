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

package com.alibaba.polardbx.qatest.dql.sharding.join;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author shicai.xsc 2019/2/14 上午12:28
 * @since 5.0.0.0
 */

public class JoinOnDifferentTypeTest extends ReadBaseTestCase {

    private static String TABLE_NAME = "select_hash_join_multi_db_multi_tb";
    private static String SQL_FORMAT = "select a.%s, b.%s from %s a join %s b on a.%s= b.%s order by a.pk";

    /**
     * Number join Number, Date join Date, String join String
     */
    @Test
    public void joinOnSameTypeTest() {
        List<String> joinFields = Arrays.asList("integer_test",
            "varchar_test",
            "char_test",
            "blob_test",
            "tinyint_test",
            "tinyint_1bit_test",
            "smallint_test",
            "mediumint_test",
            "bit_test",
            "bigint_test",
            "float_test",
            "double_test",
            "decimal_test",
            "date_test",
            "time_test",
            "datetime_test",
            "timestamp_test",
            "year_test",
            "mediumtext_test");

        for (String left : joinFields) {
            String sql = String.format(SQL_FORMAT, left, left, TABLE_NAME, TABLE_NAME, left, left);
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);
        }
    }

    /**
     *
     */
    @Test
    public void joinOnDifferntTypeTest_0() {
        List<String> joinFields = Arrays.asList("integer_test",
            "varchar_test",
            "char_test",
//            "blob_test",
            "tinyint_test",
            "tinyint_1bit_test",
            "smallint_test",
            "mediumint_test",
//            "bit_test",
            "bigint_test",
            "float_test",
            "double_test",
            "decimal_test",
            "mediumtext_test"
            // "date_test",
            // "time_test",
            // "datetime_test",
            // "timestamp_test",
            // "year_test",
        );

        for (String left : joinFields) {
            for (String right : joinFields) {
                String sql = String.format(SQL_FORMAT, left, right, TABLE_NAME, TABLE_NAME, left, right);
                selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);
            }
        }
    }

    /**
     *
     */
    @Test
    public void joinOnDifferntTypeTest_1() {
        List<String> joinFields = Arrays.asList("integer_test",
            "varchar_test",
            "char_test",
//            "blob_test",
            "bigint_test",
            "float_test",
            "double_test",
            "decimal_test",
            "year_test",
            "mediumtext_test"
            // "tinyint_test",
            // "tinyint_1bit_test",
            // "smallint_test",
            // "mediumint_test",
            // "bit_test",

            // "date_test",
            // "time_test",
            // "datetime_test",
            // "timestamp_test"
        );

        for (String left : joinFields) {
            for (String right : joinFields) {
                String sql = String.format(SQL_FORMAT, left, right, TABLE_NAME, TABLE_NAME, left, right);
                selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);
            }
        }
    }
}
