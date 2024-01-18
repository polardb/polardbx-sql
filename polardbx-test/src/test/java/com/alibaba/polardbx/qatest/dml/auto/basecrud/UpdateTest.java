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

package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataOperator;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXAutoDBName1;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssertWithDiffSql;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.updateErrorAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * Update测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class UpdateTest extends AutoCrudBasedLockTestCase {
    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();

    private static String TABLE_FIRST_COLUMN_NOT_NUMERIC =
        "CREATE TABLE IF NOT EXISTS `bi_business_analyse_index_day` (\n"
            + "  `cycle_id` date NOT NULL COMMENT '统计日期',\n"
            + "  `node_id` int(11) NOT NULL COMMENT '节点ID(同统计日期：格式yyyyMMdd)Partition(hash())',\n"
            + "  `swjg_dm` varchar(11) NOT NULL COMMENT '税务机关代码',\n"
            + "  `business_type` tinyint(4) NOT NULL COMMENT '业务类型(1:消息,2:采集,3:核实)',\n"
            + "  PRIMARY KEY (`cycle_id`,`swjg_dm`,`business_type`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='大数据_业务数据分析数据日表'";

    static {
        TABLE_FIRST_COLUMN_NOT_NUMERIC += " partition by hash(`node_id`) partitions 8";
    }

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeWithStringRuleOneTable(ExecuteTableName.UPDATE_DELETE_BASE));

    }

    public UpdateTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = "bi_business_analyse_index_day";
    }

    @Before
    public void initData() throws Exception {
        tableDataPrepare(baseOneTableName, 20,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateAll() throws Exception {

        String sql = "UPDATE "
            + baseOneTableName
            + " SET  char_test=?,blob_test=?,integer_test=?,tinyint_test=?,tinyint_1bit_test=?,smallint_test=?,"
            + "mediumint_test=?,bit_test=?,bigint_test=?,float_test=?,double_test=?,decimal_test=?,date_test=?,time_test=?,datetime_test=?"
            + ",timestamp_test=?,year_test=?";

        List<List<Object>> params = new ArrayList<List<Object>>();
        List<Object> param = new ArrayList<Object>();
        columnDataGenerator.getAllColumnValueExceptPkAndVarchar_Test(param);
        params.add(param);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName);
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateOne() throws Exception {
        long pk = 5;
        String sql = "UPDATE "
            + baseOneTableName
            + " SET char_test=?,blob_test=?,integer_test=?,tinyint_test=?,tinyint_1bit_test=?,smallint_test=?,"
            + "mediumint_test=?,bit_test=?,bigint_test=?,float_test=?,double_test=?,decimal_test=?,date_test=?,time_test=?,datetime_test=?  "
            + ",timestamp_test=?,year_test=? WHERE pk=?";

        List<Object> param = new ArrayList<Object>();
        columnDataGenerator.getAllColumnValueExceptPkAndVarchar_Test(param);
        param.add(pk);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName);
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateOneWithAlias() throws Exception {
        long pk = 5;
        String sql = "UPDATE "
            + baseOneTableName
            + " tt "
            + " SET tt.char_test=?,tt.blob_test=?,tt.integer_test=?,tinyint_test=?,tinyint_1bit_test=?,smallint_test=?,"
            + "mediumint_test=?,bit_test=?,bigint_test=?,float_test=?,double_test=?,decimal_test=?,date_test=?,time_test=?,datetime_test=?  "
            + ",timestamp_test=?,year_test=? WHERE pk=?";
        List<Object> param = new ArrayList<Object>();
        columnDataGenerator.getAllColumnValueExceptPkAndVarchar_Test(param);
        param.add(pk);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName);
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateOneWithAlias1() throws Exception {
        long pk = 5;
        String sql = "UPDATE "
            + baseOneTableName
            + " `tt` "
            + " SET tt.char_test=?,`tt`.blob_test=?,tt.integer_test=?,tinyint_test=?,tinyint_1bit_test=?,smallint_test=?,"
            + "mediumint_test=?,bit_test=?,bigint_test=?,float_test=?,double_test=?,decimal_test=?,date_test=?,time_test=?,datetime_test=?  "
            + ",timestamp_test=?,year_test=? WHERE pk=?";
        List<Object> param = new ArrayList<Object>();
        columnDataGenerator.getAllColumnValueExceptPkAndVarchar_Test(param);
        param.add(pk);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName);
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateOneWithAliasWrong() throws Exception {
        long pk = 5;
        String sql = "UPDATE "
            + baseOneTableName
            + " tt "
            + " SET tt.char_test=?,tt.blob_test=?,aa.integer_test=?,tinyint_test=?,tinyint_1bit_test=?,smallint_test=?,"
            + "mediumint_test=?,bit_test=?,bigint_test=?,float_test=?,double_test=?,decimal_test=?,date_test=?,time_test=?,datetime_test=?  "
            + ",timestamp_test=?,year_test=? WHERE pk=?";
        List<Object> param = new ArrayList<Object>();
        columnDataGenerator.getAllColumnValueExceptPkAndVarchar_Test(param);
        param.add(pk);
        updateErrorAssert(sql, param, tddlConnection, "Table 'aa' not found");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome() throws Exception {

        String sql = "UPDATE " + baseOneTableName
            + " SET float_test=?,double_test=? WHERE  pk BETWEEN 3 AND 7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "SELECT * FROM " + baseOneTableName
            + "  WHERE pk BETWEEN 3 AND 7";
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName)
            + "  WHERE pk BETWEEN 3 AND 7";
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome1() throws Exception {

        String sql = "UPDATE " + baseOneTableName
            + " SET float_test=?,double_test=? WHERE pk > 7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);
        sql = "SELECT * FROM " + baseOneTableName + "  WHERE pk > 7";
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName) + "  WHERE pk > 7";
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome2() throws Exception {

        String sql = "UPDATE " + baseOneTableName
            + " SET float_test=?,double_test=? WHERE pk < 7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "SELECT * FROM " + baseOneTableName + "  WHERE pk < 7 order by pk";
        selectOrderAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName) + "  WHERE pk < 7 order by pk";
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome3() throws Exception {

        String sql = "UPDATE " + baseOneTableName
            + " SET float_test=?,double_test=? WHERE pk >= 7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "SELECT * FROM " + baseOneTableName + "  WHERE pk >= 7";
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName) + "  WHERE pk >= 7";
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome4() throws Exception {

        String sql = "UPDATE " + baseOneTableName
            + " SET float_test= ? , double_test= ? WHERE  pk <=7";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "SELECT * FROM " + baseOneTableName + "  WHERE pk <= 7";
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName) + "  WHERE pk <= 7";
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateSome5() throws Exception {
        if (baseOneTableName.contains("string")) {
            return;
        }
        String sql = "UPDATE "
            + baseOneTableName
            + " SET float_test=?,double_test=? WHERE pk = 7 order by pk limit 1";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);
        sql = "SELECT * FROM " + baseOneTableName
            + "  WHERE pk = 7 order by pk limit 1";
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName)
            + "  WHERE pk = 7 order by pk limit 1";
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void nowTest() throws Exception {
        String sql = "update " + baseOneTableName
            + " SET date_test= now(),datetime_test=now()  "
            + ",timestamp_test=now()  where pk=1";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);

        sql = "select * from " + baseOneTableName + " where pk = 1";
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName) + " where pk = 1";
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void whereWithComplexTest() throws Exception {

        String sql = "update "
            + baseOneTableName
            + " set float_test= ? where varchar_test= ? or  ((?-integer_test>100)||(?<integer_test && ?-integer_test >200))";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(400);
        param.add(200);
        param.add(800);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName);
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void setWithIncrementTest() throws Exception {

        String sql = "update "
            + baseOneTableName
            + " set float_test= float_test+ ? where integer_test =? and varchar_test =?";
        List<Object> param = new ArrayList<Object>();
        param.add(2);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName);
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateNotExistDataTest() throws Exception {

        long pk = -1l;
        String sql = "UPDATE " + baseOneTableName
            + "  SET integer_test=? WHERE pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(pk);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName);
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateNotExistFielddTest() throws Exception {

        String sql = "UPDATE " + baseOneTableName + "  SET nothisfield =1";
        executeErrorAssert(tddlConnection, sql, null,
            "Unknown target column 'nothisfield'");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateNotExistTableTest() throws Exception {

        String sql = "UPDATE nor SET pk = ?";
        List<Object> param = new ArrayList<Object>();
        param.add(1);

        executeErrorAssert(tddlConnection, sql, param, "TABLE 'NOR' DOESN'T EXIST");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateShardingKey() {
        if (baseOneTableName.endsWith("one_db_one_tb")
            || baseOneTableName.contains("string")
            || baseOneTableName.contains("broadcast")) {
            return;
        }
        String sql = "UPDATE " + baseOneTableName + "  SET pk = pk + 100";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);

        sql = "select pk from " + baseOneTableName + " order by pk";
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateMultiColumnIncludeShardingKey() {
        if (baseOneTableName.endsWith("one_db_one_tb")
            || baseOneTableName.contains("string")
            || baseOneTableName.contains("broadcast")) {
            return;
        }
        String sql = "UPDATE " + baseOneTableName
            + "  SET integer_test = integer_test + 1, pk = pk + 100";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);

        sql = "select pk,integer_test  from " + baseOneTableName + " order by pk";
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * update 永假式处理
     */
    @Test
    public void updateWihtFalseCond() throws Exception {
        String sql = String.format("update  %s  SET integer_test =1 where 1=2",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        // try {
        // int tddlUpdateData = tddlUpdateData(
        // String.format("update update_test set name='x' where 1=2",
        // "delete_test"), null);
        // int mysqlUpdateData = mysqlUpdateData(
        // String.format("update  %s  SET pk = pk + 100 where 1=2",
        // baseOneTableName), null);
        // Assert.assertEquals(tddlUpdateData, mysqlUpdateData);
        // } catch (Exception ex) {
        // Assert.fail();
        // }

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName);
        assertBrocastTableSame(sql, tddlSql);

    }

    @Test
    public void updateWithSetTwice() throws Exception {
        String sql = String.format("update  %s  SET integer_test=1, integer_test=integer_test+1 where integer_test=?",
            baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(18);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        sql = String.format("update  %s  SET integer_test=1, integer_test=integer_test+1, float_test=?, "
                + "float_test=?+1 where integer_test=?",
            baseOneTableName);
        param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Ignore("does not support UDF")
    public void testUpdateWithUDF() throws Exception {
        //忽略string规则的表
        if (baseOneTableName.contains("string")) {
            return;
        }
        String sql = String
            .format("update %s set varchar_test = hello(varchar_test) where pk >1 and pk < 20",
                baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = String
            .format("select hello(varchar_test) as newname from %s where pk > 1 and pk < 20",
                baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String tddlSql = String
            .format("select hello(varchar_test) as newname from %s where pk > 1 and pk < 20",
                toPhyTableName(baseOneTableName));
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testUpdateWithAlwaysFalse1() throws Exception {
//		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String.format("UPDATE  %s  SET integer_test=? WHERE pk < 0 AND pk > 0 ", baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName);
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.1.24
     */
    @Test
    @Ignore("do not support subquery")
    public void testUpdateWithAlwaysFalse2() throws Exception {
//		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql =
            String.format("UPDATE  %s  SET integer_test=? WHERE (pk IN ('a', 'b', 'c')) IS TRUE", baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName);
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testUpdateWithAlwaysTrue1() throws Exception {
//		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String.format("UPDATE  %s  SET integer_test=? WHERE NOT (pk < 0 AND pk > 0) ", baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName);
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     * @since 5.1.24
     */
    @Test
    @Ignore("do not support subquery")
    public void testUpdateWithAlwaysTRUE2() throws Exception {
//		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql =
            String.format("UPDATE  %s  SET integer_test=? WHERE (pk IN ('a', 'b', 'c')) IS FALSE", baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "select * from " + toPhyTableName(baseOneTableName);
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     *
     */
    @Test
    public void updateWithTableName() throws Exception {

        String sql = "UPDATE %s SET %s.float_test=?, %s.double_test=? WHERE  pk BETWEEN 3 AND 7";
        sql = String.format(sql, baseOneTableName, baseOneTableName, baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.float_testValue);
        param.add(columnDataGenerator.double_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "SELECT * FROM " + baseOneTableName
            + "  WHERE pk BETWEEN 3 AND 7";
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        String tddlSql = "SELECT * FROM " + toPhyTableName(baseOneTableName)
            + "  WHERE pk BETWEEN 3 AND 7";
        assertBrocastTableSame(sql, tddlSql);
    }

    /**
     *
     */
    @Test
    public void updateWithWrongTableName() throws Exception {

        String sql = "UPDATE %s SET %s.float_test=0, %s.double_test=0 WHERE  pk BETWEEN 3 AND 7";
        sql = String.format(sql, baseOneTableName, baseOneTableName, "wrongName");

        executeErrorAssert(tddlConnection, sql, null, "not found");
    }

    /**
     *
     */
    @Test
    public void updateWithSubquery() throws Exception {

        String sql = "UPDATE (select * from %s) t SET %s.float_test=0, %s.double_test=0 WHERE  pk BETWEEN 3 AND 7";
        sql = String.format(sql, baseOneTableName, baseOneTableName, baseOneTableName);

        executeErrorAssert(tddlConnection, sql, null, "The target table 't' of the UPDATE is not updatable");

        sql = "UPDATE %s a,(select * from %s) t SET a.float_test=0, a.double_test=0 WHERE  a.pk BETWEEN 3 AND 7";
        sql = String.format(sql, baseOneTableName, baseOneTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    @Test
    public void updateWithReferCondition() {
        String sql = String.format("UPDATE %s SET %s.integer_test = 1 WHERE %s.pk = 9 AND %s.varchar_test = '1'",
            baseOneTableName, baseOneTableName, baseOneTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = "SELECT * FROM " + baseOneTableName + " WHERE pk = 9";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void updateWithReferConditionAndSchema() {
        final String qualifiedTable = String.join(".", polardbXAutoDBName1(), baseOneTableName);
        final String tddlSql = String.format(
            "UPDATE %s SET %s.integer_test = 1 WHERE %s.pk = 9 AND %s.varchar_test = 'adaabcwer'",
            qualifiedTable, qualifiedTable, baseOneTableName, qualifiedTable);
        final String mysqlSql = String.format(
            "UPDATE %s SET %s.integer_test = 1 WHERE %s.pk = 9 AND %s.varchar_test = 'adaabcwer'",
            baseOneTableName, baseOneTableName, baseOneTableName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlSql, tddlSql, null, true);
        final String sql = "SELECT * FROM " + baseOneTableName + " WHERE pk = 9";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void updateAliasWithReferCondition() {
        String sql = String.format("UPDATE %s tb SET tb.integer_test = 1 WHERE tb.pk = 11 AND tb.varchar_test = '1'",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = "SELECT * FROM " + baseOneTableName + " WHERE pk = 11";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    public void assertBrocastTableSame(String sql, String tddlsql) {
        //not support route hint now.
        // 判断下广播表是否能够广播更新到每张表
        if (baseOneTableName.contains("broadcast")) {
            for (int i = 0; i < 2; i++) {
                String hint = String.format("/*TDDL:node=%s*/", i);
                selectContentSameAssertWithDiffSql(
                    hint + tddlsql,
                    sql,
                    null,
                    mysqlConnection,
                    tddlConnection,
                    true,
                    false,
                    true
                );
            }
        }
    }

    /**
     * @since 5.3.12
     */
    @Test
    public void updateShardingKeyEqualNullTest() throws Exception {
        String sql = String.format(
            "explain sharding update update_delete_base_two_multi_db_multi_tb set integer_test = 1 where pk = null");
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
    public void updateShardingKeyIsNullTest() throws Exception {
        String sql = String.format(
            "explain sharding update update_delete_base_two_multi_db_multi_tb set integer_test = 1 where pk is null");
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
    public void updateTwoShardingKeyIsNullTest() throws Exception {
        String sql = String.format(
            "explain sharding update update_delete_base_two_multi_db_multi_tb t2, update_delete_base_three_multi_db_multi_tb t3 "
                +
                " set t2.integer_test = 1 where t2.pk = t3.pk and t2.pk is null");
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
    public void updateTwoShardingKeyEqualNullTest() throws Exception {
        String sql = String.format(
            "explain sharding update update_delete_base_two_multi_db_multi_tb t2, update_delete_base_three_multi_db_multi_tb t3 "
                +
                " set t2.integer_test = 1 where t2.pk = t3.pk and t2.pk = null");
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
    public void updateWithFirstColumnNotNumeric() throws Exception {
        JdbcUtil.executeSuccess(tddlConnection, TABLE_FIRST_COLUMN_NOT_NUMERIC);

        final String sql =
            String.format("update bi_business_analyse_index_day set swjg_dm = '13306920000' where swjg_dm = "
                + "'13306020000'");
        final int affectedRows = JdbcUtil.updateData(tddlConnection, sql, null);
        Assert.assertEquals(sql, 0, affectedRows);
    }

    /**
     * @since 5.3.12
     */
    @Test
    public void updateWithFirstColumnNotNumeric2() throws Exception {
        JdbcUtil.executeSuccess(tddlConnection, TABLE_FIRST_COLUMN_NOT_NUMERIC);

        final String sql = String.format("/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ update "
            + "bi_business_analyse_index_day set swjg_dm = '13306920000' where swjg_dm = '13306020000' limit 10");
        final int affectedRows = JdbcUtil.updateData(tddlConnection, sql, null);
        Assert.assertEquals(sql, 0, affectedRows);
    }

    private String toPhyTableName(String logicalTableName) {
        ResultSet resultSet =
            JdbcUtil.executeQuerySuccess(tddlConnection, "show topology from " + logicalTableName);
        String physicalTableName = logicalTableName;
        try {
            resultSet.next();
            physicalTableName = (String) JdbcUtil.getObject(resultSet, 3);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return physicalTableName;
    }

    @Test
    public void updateWithLimitOffset() {
        int checkNum = 17345239;
        String updateSql = String.format("update %s set integer_test = %d limit 0,2", baseOneTableName, checkNum);
        int updateNum = JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, updateSql);
        Assert.assertEquals(2, updateNum);

        String countSql = String.format("select count(1) from %s where integer_test = %d", baseOneTableName, checkNum);
        int count = Integer.parseInt(
            JdbcUtil.getAllResult(JdbcUtil.executeQuery(countSql, tddlConnection)).get(0).get(0).toString());
        Assert.assertEquals(2, count);

        String updateSqlErr = String.format("update %s set integer_test = %d limit 2,2", baseOneTableName, checkNum);
        JdbcUtil.executeUpdateFailed(tddlConnection, updateSqlErr, "UPDATE/DELETE statement");

        updateSqlErr = String.format("update %s set integer_test = %d limit 1,1", baseOneTableName, checkNum);
        JdbcUtil.executeUpdateFailed(tddlConnection, updateSqlErr, "UPDATE/DELETE statement");

        checkNum = 1695978;
        updateSql = String.format(
            "/*+TDDL:CMD_EXTRA(ENABLE_MODIFY_LIMIT_OFFSET_NOT_ZERO=true)*/ update %s set integer_test = %d limit 2,2",
            baseOneTableName, checkNum);
        updateNum = JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, updateSql);
        Assert.assertEquals(2, updateNum);
        countSql = String.format("select count(1) from %s where integer_test = %d", baseOneTableName, checkNum);
        count = Integer.parseInt(
            JdbcUtil.getAllResult(JdbcUtil.executeQuery(countSql, tddlConnection)).get(0).get(0).toString());
        Assert.assertEquals(2, count);
    }

    /**
     * 修改拆分键, 走LogicalRelocate
     */
    @Test
    public void updateWithLimitOffsetWhenRelocate() {
        String updateSql =
            String.format("update %s set pk = pk + 10000 where pk in (4,5,6) limit 0,2", baseOneTableName);
        int updateNum = JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, updateSql);
        Assert.assertEquals(2, updateNum);

        String countSql = String.format("select count(1) from %s where pk > 10000", baseOneTableName);
        int count = Integer.parseInt(
            JdbcUtil.getAllResult(JdbcUtil.executeQuery(countSql, tddlConnection)).get(0).get(0).toString());
        Assert.assertEquals(2, count);

        String updateSqlErr =
            String.format("update %s set pk = pk + 10000 where pk in (7,8,9) limit 2,2", baseOneTableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, updateSqlErr, "UPDATE/DELETE statement");

        updateSqlErr = String.format("update %s set pk = pk + 10000 where pk in (0,0,0) limit 1,1", baseOneTableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, updateSqlErr, "UPDATE/DELETE statement");

        updateSql = String.format(
            "/*+TDDL:CMD_EXTRA(ENABLE_MODIFY_LIMIT_OFFSET_NOT_ZERO=true)*/ update %s set pk = pk + 10000 where pk in (7,8,9) limit 1,2",
            baseOneTableName);
        updateNum = JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, updateSql);
        Assert.assertEquals(2, updateNum);
        count = Integer.parseInt(
            JdbcUtil.getAllResult(JdbcUtil.executeQuery(countSql, tddlConnection)).get(0).get(0).toString());
        Assert.assertEquals(4, count);

        updateSql = String.format(
            "/*+TDDL:CMD_EXTRA(ENABLE_MODIFY_LIMIT_OFFSET_NOT_ZERO=true)*/ update %s set pk = pk + 10000 order by pk limit 1,2",
            baseOneTableName);
        updateNum = JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, updateSql);
        Assert.assertEquals(2, updateNum);
        count = Integer.parseInt(
            JdbcUtil.getAllResult(JdbcUtil.executeQuery(countSql, tddlConnection)).get(0).get(0).toString());
        Assert.assertEquals(6, count);
    }

    @Test
    public void updateWithOrderAndLimitOffset() {
        int checkNum = 17345239;
        String mysqlSql =
            String.format("update %s set integer_test = %d order by pk limit 2", baseOneTableName, checkNum);
        String tddlSql =
            String.format("update %s set integer_test = %d order by pk limit 0,2", baseOneTableName, checkNum);
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlSql, tddlSql, null, true);

        String sql = "SELECT * FROM " + baseOneTableName;
        DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        checkNum = 1695978;
        mysqlSql = String.format("update %s set integer_test = %d order by pk limit 2", baseOneTableName, checkNum);
        tddlSql = String.format("update %s set integer_test = %d order by pk limit 0,2", baseOneTableName, checkNum);
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlSql, tddlSql, null, true);
        sql = "SELECT * FROM " + baseOneTableName;
        DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        String updateSqlErr =
            String.format("update %s set integer_test = %d order by pk limit 2,2", baseOneTableName, checkNum);
        JdbcUtil.executeUpdateFailed(tddlConnection, updateSqlErr, "UPDATE/DELETE statement");

        checkNum = 1893072;
        tddlSql = String.format(
            "/*+TDDL:CMD_EXTRA(ENABLE_MODIFY_LIMIT_OFFSET_NOT_ZERO=true)*/ update %s set integer_test = %d order by pk limit 2,2",
            baseOneTableName, checkNum);
        int count = JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, tddlSql);
        Assert.assertEquals(2, count);

        sql = String.format("select count(1) from %s where integer_test = %d", baseOneTableName, checkNum);
        count = Integer.parseInt(
            JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlConnection)).get(0).get(0).toString());
        Assert.assertEquals(2, count);
    }

    @Test
    public void updateWithDualAndSubQuery() {
        String sql = String.format(
            "update %s t1 inner join ( select 1 pk from dual ) t2 on t1.pk = t2.pk set t1.integer_test = 123456 where 1=1",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = "SELECT integer_test FROM " + baseOneTableName + " WHERE pk = 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void updateWithView() {
        final String viewName = "update_with_view_test_view";

        // Recreate view
        String sql = "drop view " + viewName;
        JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, sql, ImmutableSet.of("Unknown view"));
        JdbcUtil.executeUpdateSuccessIgnoreErr(mysqlConnection, sql, ImmutableSet.of("Unknown table"));

        sql = String.format("create view %s as\n"
            + "(\n"
            + "    select integer_test, varchar_test from %s as a where a.pk < 11 \n"
            + ")\n", viewName, baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        // Execute update
        sql =
            String.format("update %s a, %s v set a.bigint_test = v.integer_test where a.varchar_test = v.varchar_test",
                baseOneTableName, viewName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        // Check update result
        sql = "SELECT bigint_test FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        // Check error message
        sql =
            String.format("update %s a, %s v set v.integer_test = a.bigint_test where a.varchar_test = v.varchar_test",
                baseOneTableName, viewName);
        executeErrorAssert(tddlConnection, sql, null,
            MessageFormat.format("{0}'' of the {1} is not updatable", viewName, "UPDATE"));
    }
}

