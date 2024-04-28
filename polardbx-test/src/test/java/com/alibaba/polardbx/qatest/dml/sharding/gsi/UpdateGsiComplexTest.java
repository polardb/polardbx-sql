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

package com.alibaba.polardbx.qatest.dml.sharding.gsi;

import com.alibaba.polardbx.qatest.BinlogIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

@BinlogIgnore(ignoreReason = "用例涉及很多主键冲突问题，即不同分区有相同主键，复制到下游Mysql时出现Duplicate Key")
public class UpdateGsiComplexTest extends GsiDMLTest {
    private final static String oneTableName = "update_gsi_complex_test_one";
    private final static String twoTableName = "update_gsi_complex_test_two";

    private final static String oneCreateTable = "CREATE TABLE `" + oneTableName + "` ("
        + "`pk` bigint(11) NOT NULL,"
        + "`id` bigint(11) NOT NULL,"
        + "`col1` bigint(11) NOT NULL,"
        + "`col2` bigint(11) NOT NULL,"
        + "`col3` bigint(11) NOT NULL,"
        + "`col4` bigint(11) NOT NULL,"
        + "`col5` bigint(11) NOT NULL,"
        + "`col6` bigint(11) NOT NULL,"
        + "PRIMARY KEY (`pk`),"
        + "GLOBAL INDEX `" + oneTableName + "_index1` (`col1`) COVERING (`pk`, `col2`) DBPARTITION BY HASH(`col1`)"
        + ") dbpartition by hash(`pk`) tbpartition by hash(`pk`) tbpartitions 4 ";

    private final static String twoCreateTable = "CREATE TABLE `" + twoTableName + "` ("
        + "`pk` bigint(11) NOT NULL,"
        + "`id` bigint(11) NOT NULL,"
        + "`col1` bigint(11) NOT NULL,"
        + "`col2` bigint(11) NOT NULL,"
        + "`col3` bigint(11) NOT NULL,"
        + "`col4` bigint(11) NOT NULL,"
        + "`col5` bigint(11) NOT NULL,"
        + "`col6` bigint(11) NOT NULL,"
        + "PRIMARY KEY (`pk`),"
        + "GLOBAL INDEX `" + twoTableName + "_index1` (`col1`) COVERING (`pk`, `col2`) DBPARTITION BY HASH(`col1`),"
        + "GLOBAL INDEX `" + twoTableName + "_index2` (`col3`) COVERING (`pk`) DBPARTITION BY HASH(`col3`),"
        + "GLOBAL INDEX `" + twoTableName + "_index3` (`col5`,`col6`) COVERING (`pk`) DBPARTITION BY HASH(`col5`)"
        + ") dbpartition by hash(`id`) tbpartition by hash(`id`) tbpartitions 4 ";

    private final static String mysqlCreateTable = "CREATE TABLE `%s` ("
        + "`pk` bigint(11) NOT NULL,"
        + "`id` bigint(11) NOT NULL,"
        + "`col1` bigint(11) NOT NULL,"
        + "`col2` bigint(11) NOT NULL,"
        + "`col3` bigint(11) NOT NULL,"
        + "`col4` bigint(11) NOT NULL,"
        + "`col5` bigint(11) NOT NULL,"
        + "`col6` bigint(11) NOT NULL,"
        + "PRIMARY KEY (`pk`))";

    public UpdateGsiComplexTest() throws Exception {
        super("", null, null);
    }

    @Before
    public void beforeCreateTables() {
        JdbcUtil.dropTable(tddlConnection, oneTableName);
        JdbcUtil.dropTable(tddlConnection, twoTableName);
        JdbcUtil.dropTable(mysqlConnection, oneTableName);
        JdbcUtil.dropTable(mysqlConnection, twoTableName);

        JdbcUtil.executeUpdate(tddlConnection, oneCreateTable);
        JdbcUtil.executeUpdate(tddlConnection, twoCreateTable);
        JdbcUtil.executeUpdate(mysqlConnection, String.format(mysqlCreateTable, oneTableName));
        JdbcUtil.executeUpdate(mysqlConnection, String.format(mysqlCreateTable, twoTableName));

        String sql = "insert into %s (pk,id,col1,col2,col3,col4,col5,col6) values (?,?,?,?,?,?,?,?)";
        List<List<Object>> params = new ArrayList<>();
        for (int i = 1; i < 21; i++) {
            List<Object> param = new ArrayList<>();
            param.add(i);
            param.add(i);
            param.add(i * 2);
            param.add(i * 3);
            param.add(i * 4);
            param.add(i * 5);
            param.add(i * 6);
            param.add(i * 7);

            params.add(param);
        }
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, String.format(sql, oneTableName), params);

        params.clear();
        for (int i = 1; i < 21; i++) {
            List<Object> param = new ArrayList<>();
            param.add(i);
            param.add(i);
            param.add(i + 2);
            param.add(i + 3);
            param.add(i + 4);
            param.add(i + 5);
            param.add(i + 6);
            param.add(i + 7);

            params.add(param);
        }
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, String.format(sql, twoTableName), params);
    }

    @After
    public void afterDropTables() {
        JdbcUtil.dropTable(tddlConnection, oneTableName);
        JdbcUtil.dropTable(tddlConnection, twoTableName);
        JdbcUtil.dropTable(mysqlConnection, oneTableName);
        JdbcUtil.dropTable(mysqlConnection, twoTableName);
    }

    /**
     * 修改gsi的列，不下推
     */
    @Test
    public void updateGsiColTest() {
        String sql = String.format("update %s tb1, %s tb2 set tb1.col2=tb2.col2 where tb1.pk=tb2.id",
            oneTableName,
            twoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, new ArrayList<>(), true);
        sql = "select * from " + oneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        List<String> tables = new ArrayList<>();
        tables.add(oneTableName);
        tables.add(oneTableName + "_index1");
        assertTablesSame(tables);
    }

    /**
     * 修改普通列，名称和第二张表的gsi列相同，下推
     */
    @Test
    public void updateCommonColTest() {
        String sql = String.format("update %s tb1, %s tb2 set tb1.col3=tb2.col2 where tb1.pk=tb2.id",
            oneTableName,
            twoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, new ArrayList<>(), true);
        sql = "select * from " + oneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        List<String> tables = new ArrayList<>();
        tables.add(oneTableName);
        tables.add(oneTableName + "_index1");
        assertTablesSame(tables);
    }

    /**
     * 修改普通列，下推
     */
    @Test
    public void updateCommonCol2Test() {
        String sql = String.format("update %s tb1, %s tb2 set tb1.col4=tb2.col2 where tb1.pk=tb2.id",
            oneTableName,
            twoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, new ArrayList<>(), true);
        sql = "select * from " + oneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        List<String> tables = new ArrayList<>();
        tables.add(oneTableName);
        tables.add(oneTableName + "_index1");
        assertTablesSame(tables);
    }

    /**
     * 修改普通列，名称和第二张表的gsi列相同，下推
     */
    @Test
    public void updateCommonCol3Test() {
        String sql = String.format("update %s tb1, %s tb2 set tb1.col6=tb2.col2, tb1.col3=tb2.col3 where tb1.pk=tb2.id",
            oneTableName,
            twoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, new ArrayList<>(), true);
        sql = "select * from " + oneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        List<String> tables = new ArrayList<>();
        tables.add(oneTableName);
        tables.add(oneTableName + "_index1");
        assertTablesSame(tables);
    }

    /**
     * 换表顺序，修改普通列，下推
     */
    @Test
    public void updateCommonCol4Test() {
        String sql = String.format("update %s tb2, %s tb1 set tb1.col6=tb2.col2 where tb1.pk=tb2.id",
            twoTableName,
            oneTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, new ArrayList<>(), true);
        sql = "select * from " + oneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        List<String> tables = new ArrayList<>();
        tables.add(oneTableName);
        tables.add(oneTableName + "_index1");
        assertTablesSame(tables);
    }

    /**
     * 修改普通列，计算式，下推
     */
    @Test
    public void updateMoreGsiColTest() {
        String sql = String.format("update %s tb1, %s tb2 set tb1.col3=tb2.col2 + tb2.col4 where tb1.pk=tb2.id",
            oneTableName,
            twoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, new ArrayList<>(), true);
        sql = "select * from " + oneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        List<String> tables = new ArrayList<>();
        tables.add(oneTableName);
        tables.add(oneTableName + "_index1");
        assertTablesSame(tables);
    }

}
