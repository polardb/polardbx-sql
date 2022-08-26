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

package com.alibaba.polardbx.qatest.dml.auto.delete;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.ConfigUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * Delete测试
 */

public class DeleteTest extends AutoCrudBasedLockTestCase {

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE));
    }

    public DeleteTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void prepare() throws Exception {
        tableDataPrepare(baseOneTableName, 100,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteAll() throws Exception {
        String sql = String.format("delete from %s", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteOne() throws Exception {
        String sql = String.format("delete from %s where pk = 5", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteWithBetweenAnd() throws Exception {
        String sql = String.format("DELETE FROM %s WHERE pk BETWEEN 2 AND 7", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteWithOrTest() throws Exception {
        String sql = String.format("delete from %s where pk =2 or pk=7", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);

        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteWithInTest() throws Exception {
        String sql = String.format("delete from %s where pk in (2,7,10)", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);

        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteWithInTestWithLimit() throws Exception {
        String sql = String.format("delete from %s where pk in (2,7,10) limit 2", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        if (!ConfigUtil.isMultiTable(baseOneTableName)) {
            sql = String.format("select * from %s", baseOneTableName);
            selectContentSameAssert(sql, null, mysqlConnection,
                tddlConnection);
        }

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deletePkWithOrderByNameLimitTest() throws Exception {
        int limitNum = 2;
        String sql = String.format("delete from %s where pk=? order by varchar_test limit ?", baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(7);
        param.add(limitNum);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deletePkDuplicatedWithOrderByNameLimitTest() throws Exception {
        int limitNum = 2;
        String sql =
            String.format("delete from %s where pk=? and pk=? order by varchar_test limit ?", baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(7);
        param.add(7);
        param.add(limitNum);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        // hit plan cache
        sql = String.format("delete from %s where pk=? and pk=? order by varchar_test limit ?", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);
    }

    @Test
    public void deletePkDuplicatedWithOrderByNameLimitTest1() throws Exception {
        int limitNum = 2;
        String sql =
            String.format("delete from %s where pk=? and pk=? order by varchar_test limit ?", baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(7);
        param.add(6);
        param.add(limitNum);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteNameWithOrderByPkLimitTest() throws Exception {
        int limitNum = 2;
        String sql = String.format("delete from %s where varchar_test =? order by pk limit ?", baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        param.add(limitNum);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteWithNowTest() throws Exception {
        try {
            String sql = String.format("delete from %s where date_test < now()", baseOneTableName);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
                sql, null, true);
            tableDataPrepare(baseOneTableName, 20,
                TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
                tddlConnection, columnDataGenerator);
            sql = String.format("delete from %s where timestamp_test < now()", baseOneTableName);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
                sql, null, true);
            tableDataPrepare(baseOneTableName, 20,
                TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
                tddlConnection, columnDataGenerator);
            sql = String.format("delete from %s where datetime_test > now()", baseOneTableName);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
                sql, null, true);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("delete中暂不支持按照索引进行查询"));
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteNotExistTest() throws Exception {
        int notExist = 111111;
        String sql = String.format("delete from  %s where pk= %s ", baseOneTableName, notExist);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteNotExistTableTest() throws Exception {
        String sql = String.format("delete from norma where pk =1");
        executeErrorAssert(tddlConnection, sql, null, "DOESN'T EXIST");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteNotExistFieldTest() throws Exception {
        String sql = String.format("delete from %s where k =11", baseOneTableName);
        executeErrorAssert(tddlConnection, sql, null, "not found");

    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore
    public void deleteNotExistRecord() throws Exception {
        String sql = String.format("delete from %s where pk ='abc'", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * delete 永假式测试, 1= 2
     */
    @Test
    public void deleteFalseCondFieldTest1() throws Exception {

        String sql = String.format("delete from %s where 1=2", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * delete 永假式测试, !TRUE
     */
    @Test
    public void deleteFalseCondFieldTest2() throws Exception {

        String sql = String.format("delete from %s where !TRUE", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * delete 永假式测试,  NOT TRUE
     */
    @Test
    public void deleteFalseCondFieldTest3() throws Exception {

        String sql = String.format("delete from %s where NOT TRUE", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * delete 永假式测试,  NULL IS NOT NULL
     */
    @Test
    public void deleteFalseCondFieldTest4() throws Exception {

        String sql = String.format("delete from %s where NULL IS NOT NULL", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * delete 永假式测试,  pk != pk
     */
    @Test
    public void deleteFalseCondFieldTest5() throws Exception {

        String sql =
            String.format("delete from %s where %s.pk != %s.pk", baseOneTableName, baseOneTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * delete 永假式测试,  pk != pk with order by and limit
     */
    @Test
    public void deleteFalseCondFieldTest6() throws Exception {

        String sql = String
            .format("delete from %s where %s.pk != %s.pk  order by integer_test limit 10", baseOneTableName,
                baseOneTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * delete 永假式测试, 带子查询(select false from dual)
     */
    @Test
    public void deleteFalseCondFieldSubQueryTest1() throws Exception {

        String sql = String.format("delete from %s where (select false from dual)", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     * delete 永假式测试, 带子查询(select 1 > 2 from dual)
     */
    @Test
    public void deleteFalseCondFieldSubQueryTest2() throws Exception {

        String sql = String.format("delete from %s where (select 1 > 2 from dual)", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     * delete 永假式测试, 带子查询(select 1 > 2 from dual)
     */
    @Test
    public void deleteFalseCondFieldSubQueryTest3() throws Exception {

        String sql = String.format("delete from %s where (select 1 > 2 from dual)", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     * delete 永假式测试, 带子查询((select false) and (select 1>2))
     */
    @Test
    public void deleteFalseCondFieldSubQueryTest4() throws Exception {

        String sql = String.format("delete from %s where ((select false) and (select 1>2))", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     * delete 永假式测试, 带子查询 pk = ANY(select 2 from dual) AND pk <> ANY(select 2 from dual)
     */
    @Test
    public void deleteFalseCondFieldSubQueryTest5() throws Exception {

        String sql = String
            .format("delete from %s where  pk = ANY(select 2 from dual) AND pk <> ANY(select 2 from dual)",
                baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    /**
     * delete 永真式测试 where 1 != 2
     */
    @Test
    public void deleteFalseCondSuccessTest1() throws Exception {

        String sql = String.format("delete from %s where 1 != 2", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
    }

    /**
     * delete 永真式测试 where TRUE
     */
    @Test
    public void deleteFalseCondSuccessTest2() throws Exception {

        String sql = String.format("delete from %s where TRUE", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
    }

    /**
     * delete 永真式测试 where TRUE
     */
    @Test
    public void deleteFalseCondSuccessTest3() throws Exception {

        String sql = String.format("delete from %s where TRUE", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
    }

    /**
     * delete 永真式测试 where NOT FALSE
     */
    @Test
    public void deleteFalseCondSuccessTest4() throws Exception {

        String sql = String.format("delete from %s where NOT FALSE", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
    }

    /**
     * delete 永真式测试 where NULL IS NULL
     */
    @Test
    public void deleteFalseCondSuccessTest5() throws Exception {

        String sql = String.format("delete from %s where NULL IS NULL", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
    }

    /**
     * delete 永真式测试 where %s.pk = %s.pk
     */
    @Test
    public void deleteFalseCondSuccessTest6() throws Exception {

        String sql =
            String.format("delete from %s where %s.pk = %s.pk", baseOneTableName, baseOneTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
    }

    /**
     * delete 永真式测试 where  %s.pk = %s.pk  order by integer_test limit 10
     */
    @Test
    public void deleteFalseCondSuccessTest7() throws Exception {

        String sql = String
            .format("delete from %s where  %s.pk = %s.pk  order by integer_test limit 10", baseOneTableName,
                baseOneTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * delete 永真式测试 (select true from dual)
     */
    @Test
    public void deleteFalseCondSuccessSubQueryTest1() throws Exception {

        String sql = String.format("delete from %s where (select true from dual)", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);

    }

    /**
     * delete 永真式测试 ((select true) and (select 1 <2))
     */
    @Test
    public void deleteFalseCondSuccessSubQueryTest2() throws Exception {

        String sql = String.format("delete from %s where ((select true) and (select 1 <2))", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);

    }

    /**
     * delete 永真式测试 pk = ANY(select 2 from dual) || pk <> ANY(select 2 from dual)
     */
    @Test
    public void deleteFalseCondSuccessSubQueryTest3() throws Exception {

        String sql = String.format("delete from %s where pk = ANY(select 2 from dual) || pk <> ANY(select 2 from dual)",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteNotMacthTypeTest() throws Exception {
        String sql = String.format("delete from %s where pk =  ? %s", baseOneTableName, 11);
        executeErrorAssert(tddlConnection, sql, null, "No value specified for parameter");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteWrongSqlTest() throws Exception {
        String sql = String.format("delete from %s wheer pk =1", baseOneTableName);
        executeErrorAssert(tddlConnection, sql, null, "syntax error");

        sql = String.format("delete form %s wheer pk=1", baseOneTableName);
        executeErrorAssert(tddlConnection, sql, null, "syntax error");
    }

    /**
     * @since 5.1.26
     */
    @Test
    public void deleteWithFunctionTest() throws Exception {
        String sql = String.format("delete from %s where isNull(varchar_test)", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.26
     */
    @Test
    public void deleteWithFunctionTest2() throws Exception {
        String sql = String.format("delete from %s where varchar_test is null", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.26
     */
    @Test
    public void deleteWithUsing() throws Exception {
        String sql = String.format("delete from %s using %s where not_exist = 1", baseOneTableName, baseOneTableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not found");

    }

    /**
     * @since 5.1.26
     */
    @Test
    public void deleteWithFunctionTest3() throws Exception {
        String sql =
            String.format("delete from %s where integer_test > 100 and varchar_test is null", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.26
     */
    @Test
    public void deleteWithSelfJoin() {
        String sql = String.format("delete %s from %s , %s as t2 where %s.char_test = t2.char_test and %s.pk > t2.pk",
            baseOneTableName, baseOneTableName, baseOneTableName, baseOneTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    @Test
    public void deleteWithAlias() {
        String sql = String.format("delete `4.t1` from %s as `4.t1` where `4.t1`.pk = 5", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    @Test
    public void deleteWithAlias2() {
        String sql = String.format("delete FROM `4.t1` USING %s as `4.t1` where `4.t1`.pk = 5;", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    @Ignore("疑似bug")
    @Test
    public void deleteWithVariables() {
        String sql = String.format("delete from %s where (@a:= pk) order by pk limit 1", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT @a";
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    @Test
    public void deleteWithOrderByTwoColumn() {
        String sql =
            String.format("DELETE FROM %s ORDER BY date_test ASC, time_test ASC, pk LIMIT 1", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT  * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    @Test
    public void deleteWhenDateIsNull() {
        String sql = String.format("update  %s set date_test='2020-12-21' where pk > 20", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        sql = String.format("DELETE FROM %s where date_test is null", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    @Test
    public void deleteOrderByNotExistColumn() {
        String sql = String.format("DELETE FROM %s order by not_exist ", baseOneTableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not found");

    }

    @Test
    public void deleteOrderByNotExistColumn2() {
        String sql = String.format("DELETE FROM %s order by t2.pk ", baseOneTableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not found");

    }

    @Test
    public void deleteSubQuery() {
        String sql = String.format("DELETE FROM %s ORDER BY (SELECT 1) ", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);

    }

    @Ignore("疑似bug")
    @Test
    public void deleteWithFunctionInOrderBy() {
        String sql = String.format("DELETE FROM %s ORDER BY (hello(10)) LIMIT 1; ", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

    }

    @Test
    public void deleteWithSqlBufferIs1() {
        String sql = "SET SESSION SQL_BUFFER_RESULT=1";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        sql = String.format(" DELETE %s FROM (SELECT SUM(pk) a FROM %s) x, %s", baseOneTableName, baseOneTableName,
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        sql = "SET SESSION SQL_BUFFER_RESULT=DEFAULT";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
    }

    @Test
    public void deleteWithNormalCondition() {
        String sql = String.format("DELETE FROM %s USING %s WHERE pk = 1", baseOneTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);

    }

    @Test
    public void deleteWithUsing3() {
        String sql = String.format("DELETE FROM %s a USING %s  a WHERE pk = 1", baseOneTableName, baseOneTableName);
        executeErrorAssert(tddlConnection, sql, null, "You have an error in your SQL syntax");
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);

    }

    @Test
    public void deleteWithOrderByAndLimit() {
        String sql = String.format("DELETE FROM %s  where smallint_test= 10 or mediumint_test = 20 order by pk limit 1",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);

    }

    /**
     * @since 5.1.26
     */
    @Test
    public void deleteWithLowerProperties() {
        String sql = String.format("DELETE LOW_PRIORITY from %s where pk=1;", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.26
     */
    @Test
    public void deleteWithDbNameNotExist() {
        String sql = String.format("DELETE  from %s where pk=1;", "not_exist_db." + baseOneTableName);
        executeErrorAssert(tddlConnection, sql, null, "");
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.3.12
     */
    @Test
    public void deleteShardingKeyEqualNullTest() throws Exception {
        String sql =
            String.format("explain sharding delete from update_delete_base_two_multi_db_multi_tb where pk = null");
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            com.alibaba.polardbx.common.utils.Assert.assertTrue(1 == rs.getInt("SHARD_COUNT"));
            rs.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.12
     */
    @Test
    public void deleteShardingKeyIsNullTest() throws Exception {
        String sql =
            String.format("explain sharding delete from update_delete_base_two_multi_db_multi_tb where pk is null");
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            com.alibaba.polardbx.common.utils.Assert.assertTrue(1 == rs.getInt("SHARD_COUNT"));
            rs.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.12
     */
    @Test
    public void deleteTwoTableShardingKeyIsNullTest() throws Exception {
        String sql = String.format("explain sharding delete t2, t3 " +
            "from update_delete_base_two_multi_db_multi_tb t2, update_delete_base_three_multi_db_multi_tb t3" +
            " where t2.pk = t3.pk and t2.pk is null");
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            com.alibaba.polardbx.common.utils.Assert.assertTrue(1 == rs.getInt("SHARD_COUNT"));
            rs.next();
            com.alibaba.polardbx.common.utils.Assert.assertTrue(1 == rs.getInt("SHARD_COUNT"));
            rs.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.12
     */
    @Test
    public void deleteTwoTableShardingKeyEqualNullTest() throws Exception {
        String sql = String.format("explain sharding delete t2, t3 " +
            "from update_delete_base_two_multi_db_multi_tb t2, update_delete_base_three_multi_db_multi_tb t3" +
            " where t2.pk = t3.pk and t2.pk = null");
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            com.alibaba.polardbx.common.utils.Assert.assertTrue(1 == rs.getInt("SHARD_COUNT"));
            rs.next();
            com.alibaba.polardbx.common.utils.Assert.assertTrue(1 == rs.getInt("SHARD_COUNT"));
            rs.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void deleteWithReferCondition() {
        String sql = String.format("DELETE FROM %s WHERE %s.pk = 9 AND %s.integer_test = 1",
            baseOneTableName, baseOneTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void deleteWithReferConditionAndOrderBy() {
        String sql = String.format("DELETE FROM %s WHERE %s.pk = 13 ORDER BY %s.integer_test LIMIT 1",
            baseOneTableName, baseOneTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void deleteFromWithReferCondition() {
        String sql = String.format("DELETE %s FROM %s WHERE %s.pk = 14 AND %s.integer_test = 1",
            baseOneTableName, baseOneTableName, baseOneTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void deleteUsingWithReferCondition() {
        String sql = String.format("DELETE FROM %s USING %s WHERE %s.pk = 15 AND %s.integer_test = 1",
            baseOneTableName, baseOneTableName, baseOneTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void deleteFromAliasWithReferCondition() {
        String sql = String.format("DELETE tb FROM %s tb WHERE tb.pk = 16 AND tb.integer_test = 1",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void deleteUsingAliasWithReferCondition() {
        String sql = String.format("DELETE FROM tb USING %s tb WHERE tb.pk = 17 AND tb.integer_test = 1",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void deleteMysqlPartitionTest() throws Exception {
        String sql = String.format("delete from %s partition (p123) where pk = 5", baseOneTableName);
        executeErrorAssert(tddlConnection, sql, null, "Do not support table with mysql partition");
    }

    @Test
    public void deleteSingleTableWithAlias() throws Exception {
        String mysqlSql = String.format("delete from %s where pk =2", baseOneTableName);
        String tddlSql = String.format("delete from %s as a where a.pk =2", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlSql, tddlSql, null, true);

        String sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}

