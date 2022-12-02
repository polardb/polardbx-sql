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

package com.alibaba.polardbx.qatest.dql.auto.explain;

import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.dql.sharding.explain.ExplainTest;
import com.google.common.collect.Table;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

public class ExplainAutoTest extends ReadBaseTestCase {
    public ExplainAutoTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectOneTableMultiRuleMode());
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void explainAllSelectTest() {
        Table<String, String, BiConsumer<ResultSet, String>> testCases = ExplainTest.buildCases();

        // may test more sqls
        List<String> sqls = new ArrayList<>();
        sqls.add("select * from " + baseOneTableName + " a join "
            + baseTwoTableName
            + " b on a.pk = b.pk join " + baseOneTableName + " c on b.integer_test=c.integer_test"
            + " where a.integer_test not in(10,20,30) and b.float_test < 10");
        sqls.add(
            "select * from " + baseOneTableName + " a join " + baseOneTableName + " c on a.integer_test=c.integer_test "
                + "where a.pk in(select pk from "
                + baseTwoTableName + " b  where b.float_test < 10) ");

        for (String sql : sqls) {
            try (Statement statement = tddlConnection.createStatement()) {
                ExplainTest.RunFullTest(testCases, sql, statement, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            try (Statement statement = tddlConnection.createStatement()) {
                ExplainTest.RunFullTest(testCases, sql, statement, false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            //test explain in transaction
            try (Statement statement = tddlConnection.createStatement()) {
                try {
                    statement.execute("begin");
                    ExplainTest.RunFullTest(testCases, sql, statement, true);
                } finally {
                    if (statement != null) {
                        statement.execute("rollback");
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    @FileStoreIgnore
    public void explainAllInsertTest() {
        Table<String, String, BiConsumer<ResultSet, String>> testCases = ExplainTest.buildCases();

        // may test more sqls
        List<String> sqls = new ArrayList<>();
        sqls.add("insert into " + baseOneTableName + " a "
            + "(pk,integer_test,varchar_test) values(10+2,10,\"a\")");

        for (String sql : sqls) {
            try (Statement statement = tddlConnection.createStatement()) {
                ExplainTest.RunExplainInsertTest(testCases, sql, statement);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
