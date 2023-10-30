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

package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 字段名为关键字
 *
 * @author chenhui
 * @since 5.1.9
 */

public class KeyWordTest extends AutoCrudBasedLockTestCase {

    public KeyWordTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Parameterized.Parameters(name = "{index}:table1={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeOneTable(ExecuteTableName.KEY_WORD_TEST));
    }

    @Before
    public void prepare() throws Exception {
        String sql = String.format("delete from %s", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    /**
     * @since 5.1.9
     */
    @Test
    public void testSelectAllData() throws Exception {
        prepareData(5);
        String sql = String.format("select * from %s  ", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.9
     */
    @Test
    public void testSelectWithWhere() throws Exception {
        prepareData(10);
        String sql = String.format("select * from %s where `primary`>5 ", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.9
     */
    @Test
    public void testSelectWithColumn() throws Exception {
        prepareData(10);
        String sql = String.format("select `create`,`table`,`database` from %s where `desc`>5 ", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.9
     */
    @Test
    @Ignore
    public void testSelectColumnWithNormalMark() throws Exception {
        prepareData(10);
        String sql = String.format("select 'create',\"table\",'database' from %s where 'desc'>5 ", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.1.9
     */
    @Test
    public void testSelectColumnWithTableName() throws Exception {
        prepareData(10);
        String sql = String.format("select %s.`create`,%s.`table`,%s.`database` from %s where `desc`>5 ",
            baseOneTableName,
            baseOneTableName,
            baseOneTableName,
            baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.9
     */
    @Test
    public void testSelectWithAliasKeyWord() throws Exception {
        prepareData(10);
        String sql = String
            .format("select `table`.`create`,`table`.`table`,`table`.`database` from %s as `table` where `desc`>5 ",
                baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.9
     */
    @Test
    public void testSelectWithOrderByKeyWord() throws Exception {
        prepareData(10);
        String sql = String.format(
            "select `table`.`create`,`table`.`table`,`table`.`database` from %s as `table` where `desc`>5 order by `table`.`table` ",
            baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.9
     */
    @Test
    @Ignore
    public void testSelectWithGroupByKeyWord() throws Exception {
        prepareData(10);
        String sql = String.format(
            "select `table`.`create`,`table`.`table`,`table`.`database` from %s as `table` where `desc`>5 group by `table`.`table` ",
            baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.9
     */
    @Test
    public void testSelectCountKeyWord() throws Exception {
        prepareData(10);
        String sql = String.format("select count(`table`.`create`),count(`table`) from %s as `table`  where `desc`>5 ",
            baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.9
     */
    @Test
    public void testSelectSumKeyWord() throws Exception {
        prepareData(10);
        String sql = String.format("select sum(`table`.`table`) from %s as `table`  where `desc`>5 ", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.9
     */
    @Test
    @Ignore
    public void testSelectMinusKeyWord() throws Exception {
        prepareData(10);
        String sql = String.format("select - `table` from %s where `desc`>5 ", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void testUpdateKeyWord() throws Exception {
        prepareData(10);
        String sql = String.format("update %s set `table`=10 where `key`>2", baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = String.format("select * from %s ", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void testInsertDuplicateKeyWord() throws Exception {
        prepareData(10);
        String sql =
            String.format("insert into %s(`desc`,`key`,`group`) values(1,1,100) on duplicate key update `table`=100;",
                baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = String.format("select * from %s ", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    public void prepareData(int num) throws Exception {
        for (int i = 0; i < num; i++) {
            String sql = String
                .format("insert into %s(`create`,`table`,`database`,`by`,`desc`,`int`,`group`,`order`,`primary`,`key`) "
                        + "values(%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)",
                    baseOneTableName,
                    i,
                    i,
                    i,
                    i,
                    i,
                    i,
                    i,
                    i,
                    i,
                    i);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        }
    }

    @Test
    public void testJsonWord() {
        String sql = String.format("select 1 as json  ");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
