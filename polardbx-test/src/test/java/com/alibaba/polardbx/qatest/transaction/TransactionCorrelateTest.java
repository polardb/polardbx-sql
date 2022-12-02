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

package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;

/**
 * @version 1.0
 */
public class TransactionCorrelateTest extends ReadBaseTestCase {

    private String sql =
        " /*TDDL:SEMI_BKA_JOIN((select_base_one_multi_db_one_tb,select_base_two_multi_db_one_tb), select_base_three_multi_db_one_tb)*/  "
            + "select pk from select_base_one_multi_db_one_tb where (integer_test or varchar_test in "
            + "(select varchar_test from select_base_two_multi_db_one_tb)) and "
            + "integer_test in (select integer_test from select_base_three_multi_db_one_tb)   ;";

    @Test
    public void transactionWithCorrelateTest() throws Exception {
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);
        DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);
    }

}
