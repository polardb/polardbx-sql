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

package com.alibaba.polardbx.qatest.dql.auto.function;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConfigUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.isMySQL80;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.assertContentLengthSame;
import static com.alibaba.polardbx.qatest.validator.DataValidator.assertShardCount;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainResultMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectErrorAssert;
import static com.google.common.truth.Truth.assertWithMessage;

public class SelectGroupConcatTest extends AutoCrudBasedLockTestCase {
    private static final Log log = LogFactory.getLog(
        SelectGroupConcatTest.class);

    protected String broadcastTableName;
    protected String whereCondition;
    protected String baseOneTableName1;
    protected String baseTwoTableName2;
    protected String baseThreeTableName3;
    protected String baseFourTableName4;

    @Parameters(name = "{index}:table0={0}, table1={1}, table2={2}, table3={3}, table4={4}, table5={5}, hint={6}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableSelect.selectWithMultiTableHint());
    }

    public SelectGroupConcatTest(String baseOneTableName1,
                                 String baseTwoTableName2,
                                 String baseThreeTableName3,
                                 String baseFourTableName4,
                                 String broadcastTableName,
                                 String updateDeleteTableName,
                                 String hint) {
        this.baseOneTableName1 = baseOneTableName1;
        this.baseTwoTableName2 = baseTwoTableName2;
        this.baseThreeTableName3 = baseThreeTableName3;
        this.baseFourTableName4 = baseFourTableName4;
        this.broadcastTableName = broadcastTableName;
        this.baseFiveTableName = updateDeleteTableName;
        this.whereCondition = "";
    }

    @Before
    public void prepare() throws Exception {
        normaltblPrepare(1, 51, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testNormalGroupConcat() throws Exception {
        String sql = String
            .format(
                "select integer_test, group_concat(varchar_test order by pk) as groupname from %s %s group by integer_test order by integer_test",
                baseOneTableName1,
                whereCondition);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatNull() throws Exception {
        String sql = String
            .format("select group_concat(NULL order by pk)," +
                    " group_concat(NULL,'a' order by pk), " +
                    " group_concat(NULL) from %s ",
                baseOneTableName1);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testMultiGroupConcat() throws Exception {
        String sql = String
            .format("select  " +
                    "group_concat(pk order by pk)," +
                    "group_concat(varchar_test order by pk)," +
                    "group_concat(integer_test order by pk)," +
                    "group_concat(char_test order by pk)," +
                    "group_concat(blob_test order by pk)," +
                    "group_concat(tinyint_test order by pk)," +
                    "group_concat(tinyint_1bit_test order by pk)," +
                    "group_concat(smallint_test order by pk)," +
                    "group_concat(mediumint_test order by pk)," +
                    "group_concat(bit_test order by pk)," +
                    "group_concat(bigint_test order by pk)," +
                    "group_concat(float_test order by pk)," +
                    "group_concat(double_test order by pk)," +
                    "group_concat(decimal_test order by pk)," +
                    "group_concat(year_test order by pk), " +
                    "group_concat(pk, blob_test order by pk)," +
                    "group_concat(varchar_test, blob_test order by pk)," +
                    "group_concat(integer_test, blob_test order by pk)," +
                    "group_concat(char_test, blob_test order by pk)," +
                    "group_concat(blob_test, blob_test order by pk)," +
                    "group_concat(tinyint_test, bit_test order by pk)," +
                    "group_concat(tinyint_1bit_test, bit_test order by pk)," +
                    "group_concat(smallint_test, bit_test order by pk)," +
                    "group_concat(mediumint_test, bit_test order by pk)," +
                    "group_concat(bit_test, bit_test order by pk)," +
                    "group_concat(bigint_test, bit_test order by pk)," +
                    "group_concat(float_test, bit_test order by pk)," +
                    "group_concat(double_test, bit_test order by pk)," +
                    "group_concat(decimal_test, bit_test, blob_test order by pk)," +
                    "group_concat(year_test, bit_test, blob_test order by pk), " +
                    "group_concat(pk, convert(blob_test using 'utf8') order by pk)," +
                    "group_concat(varchar_test, convert(blob_test using 'utf8') order by pk)," +
                    "group_concat(integer_test, convert(blob_test using 'utf8') order by pk)" +
                    " from %s %s group by integer_test ",
                baseOneTableName1, whereCondition);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithOrderBy() throws Exception {
        String sql = String
            .format(
                "select group_concat(integer_test order by integer_test asc) as groupid, varchar_test from %s %s group by varchar_test order by varchar_test",
                baseOneTableName1,
                whereCondition);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithOrderBySameColumn() throws Exception {
        String sql = String
            .format(
                "select group_concat(varchar_test order by varchar_test asc) as groupid, varchar_test from %s %s group by varchar_test order by varchar_test",
                baseOneTableName1,
                whereCondition);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithOrderByPk() throws Exception {
        String sql = String
            .format(
                "select group_concat(varchar_test order by pk asc) as groupid, varchar_test from %s %s group by varchar_test order by varchar_test",
                baseOneTableName1,
                whereCondition);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithDistinctDiff() throws Exception {
        String sql = String.format(
            "select group_concat(distinct integer_test order by pk) as groupid, varchar_test from %s %s group by varchar_test order by varchar_test",
            baseOneTableName1, whereCondition);
        String[] columnParam = new String[] {"GROUPID"};
        assertContentLengthSame(sql, null, columnParam, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithDistinctSame() throws Exception {
        String sql = String
            .format(
                "select group_concat(distinct varchar_test, integer_test order by pk) as groupid, varchar_test from %s %s group by varchar_test",
                baseOneTableName1, whereCondition);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithSeparator() {
        String sql = String.format(
            "select group_concat(integer_test order by pk SEPARATOR ',') as groupid, varchar_test from %s %s group by varchar_test order by varchar_test",
            baseOneTableName1, whereCondition);
        String[] columnParam = new String[] {"GROUPID"};
        assertContentLengthSame(sql, null, columnParam, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithDistinctAndOrderBy() throws Exception {
        String sql = String
            .format(
                "select group_concat(distinct integer_test order by integer_test desc) as groupid, varchar_test from %s  %s group by varchar_test",
                baseOneTableName1, whereCondition);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithDistinctOrderbyAndSeparator()
        throws Exception {
        String sql = String
            .format(
                "select group_concat(distinct integer_test order by integer_test desc SEPARATOR '|') as groupid, varchar_test from %s  %s group by varchar_test order by varchar_test",
                baseOneTableName1,
                whereCondition);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithResultNull() throws Exception {
        // 准备带有null值的数据
//		normaltblPrepare(1, 51, mysqlConnection, tddlConnection,true);
        String sql = String
            .format(
                "select group_concat(varchar_test order by pk) as groupname, integer_test from %s  %s group by integer_test order by integer_test",
                baseOneTableName1,
                whereCondition);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithPartlyResultNull() throws
        Exception {
        // 准备带有null值得数据
//		normaltblPrepare(1, 51, mysqlConnection, tddlConnection,true);
        String sql = String
            .format(
                "select group_concat(length(integer_test), integer_test, varchar_test order by pk) as groupname, max(integer_test) as mid from %s %s group by float_test",
                baseOneTableName1, whereCondition);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithBlob() throws Exception {
        // blob字段全部为空
        String sql = String
            .format(
                "select group_concat(blob_test order by pk ) as groupbc, max(integer_test)  from %s %s group by varchar_test",
                baseOneTableName1, whereCondition);
        String[] columnParam = new String[] {"groupbc", "max(integer_test)"};
//		selectBlobContentSameAssert(sql,null, columnParam, mysqlConnection, tddlConnection);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithBlobNotNull() throws Exception {
//		normaltblPrepare(1, 51, mysqlConnection, tddlConnection,true);
        String sql = String
            .format(
                "select group_concat(blob_test, varchar_test separator '|') as groupbc, max(integer_test) from %s %s group by varchar_test",
                baseFiveTableName, whereCondition);

        // blob字段部分为空
        String insertsql = String
            .format("insert into %s(pk, integer_test,varchar_test, blob_test) values(51, 1900,\"bcTest\", 0101010101);",
                baseFiveTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertsql, null);
        insertsql = String
            .format("insert into %s(pk, integer_test,varchar_test, blob_test) values(52, 1900,\"bcTest\", 0101010101);",
                baseFiveTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertsql, null);
        insertsql = String
            .format("insert into %s(pk, integer_test,varchar_test, blob_test) values(53, 1900,\"bcTest\", 0101010101);",
                baseFiveTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertsql, null);
        String[] columnParam = new String[] {"groupbc", "max(integer_test)"};

//		selectBlobContentSameAssert(sql,null,columnParam, mysqlConnection, tddlConnection);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithBlobOnlyConcatBlob() throws
        Exception {
//		normaltblPrepare(1, 51, mysqlConnection, tddlConnection,true);
        String sql = String
            .format("select group_concat(blob_test) as groupbc, max(integer_test) from %s %s group by varchar_test",
                baseFiveTableName, whereCondition);

        // blob字段部分为空
        String insertsql = String
            .format("insert into %s(pk, integer_test,varchar_test, blob_test) values(51, 1900,\"bcTest\", 0101010101);",
                baseFiveTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertsql, null);
        insertsql = String
            .format("insert into %s(pk, integer_test,varchar_test, blob_test) values(52, 1900,\"bcTest\", 0101010101);",
                baseFiveTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertsql, null);
        insertsql = String
            .format("insert into %s(pk, integer_test,varchar_test, blob_test) values(53, 1900,\"bcTest\", 0101010101);",
                baseFiveTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertsql, null);

        String[] columnParam = new String[] {"groupbc", "max(integer_test)"};
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
//		selectBlobContentSameAssert(sql,null,columnParam, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithSepcialSepartor() throws Exception {
        // String[] separators = { "`",
        // "/*+TDDL({\"extra\":{\"ALLOW_FULL_TABLE_SCAN\":\"TRUE\"}})*/",
        // "\"", "\\\'" };
        String[] separators = {"/*+TDDL({\"extra\":{\"ALLOW_FULL_TABLE_SCAN\":\"TRUE\"}})*/"};
        for (String separator : separators) {
            String sql = String
                .format(
                    "select group_concat(integer_test order by pk SEPARATOR '%s') as groupid, varchar_test from %s %s group by varchar_test",
                    separator, baseOneTableName1, whereCondition);
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatLong() throws Exception {
        String sql = String.format("select * from  %s", baseOneTableName1);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = String.format("select * from  %s", baseTwoTableName2);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = String
            .format(
                "select group_concat(varchar_test order by pk SEPARATOR '%s') as groupname, varchar_test from %s  %s group by varchar_test order by varchar_test",
                RandomUtils.getStringBetween(512, 512),
                baseOneTableName1,
                whereCondition);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithLongerThan1024WithBlob() throws Exception {
        String sql = String
            .format("select group_concat(blob_test separator '%s') as groupbc from %s %s group by varchar_test",
                RandomUtils.getStringBetween(400, 400),
                baseFiveTableName, whereCondition);

        // blob字段部分为空
        String insertsql = String
            .format("insert into %s(pk, integer_test,varchar_test, blob_test) values(51, 1900,\"bcTest\", 0101010101);",
                baseFiveTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertsql, null);
        insertsql = String
            .format("insert into %s(pk, integer_test,varchar_test, blob_test) values(52, 1900,\"bcTest\", 0101010101);",
                baseFiveTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertsql, null);
        insertsql = String
            .format("insert into %s(pk, integer_test,varchar_test, blob_test) values(53, 1900,\"bcTest\", 0101010101);",
                baseFiveTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertsql, null);

        String[] columnParam = new String[] {"groupbc"};
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
//		selectBlobContentSameAssert(sql,null,columnParam, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithOrderByInSelect() throws Exception {
        String sql = String
            .format(
                "select group_concat(integer_test, varchar_test order by pk SEPARATOR ',') as groupname, varchar_test from %s %s where pk < 100 group by varchar_test order by integer_test desc",
                baseOneTableName1, whereCondition);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithOrderByInSelectOrderBy() throws Exception {
        String sql = String
            .format(
                "select group_concat(integer_test, varchar_test order by integer_test desc SEPARATOR '#') as groupname, varchar_test from %s %s group by varchar_test order by integer_test desc",
                baseOneTableName1, whereCondition);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithAlias() throws Exception {
        String sql = String
            .format(
                "select group_concat(integer_test, varchar_test order by pk SEPARATOR ',') as groupname, varchar_test from %s a %s group by a.varchar_test",
                baseOneTableName1, whereCondition);
        String[] columnParam = new String[] {"GROUPNAME"};
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithLimitInSelect() throws Exception {
        String sql = String
            .format(
                "select group_concat(integer_test, varchar_test order by pk SEPARATOR ',') as groupname, varchar_test from %s %s group by varchar_test order by varchar_test limit 2",
                baseOneTableName1, whereCondition);
        String[] columnParam = new String[] {"GROUPNAME", "varchar_test"};
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithLimitWithCountOrderBy() throws Exception {
        String sql = String
            .format(
                "select group_concat(integer_test, varchar_test order by varchar_test, integer_test SEPARATOR ',') as groupname, count(integer_test) from %s %s group by varchar_test",
                baseOneTableName1, whereCondition);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Ignore("group_concat")
    public void testGroupConcatWithLimitWithCountDistinct() throws Exception {
        String sql = String
            .format(
                "select group_concat(integer_test, varchar_test order by pk SEPARATOR ',') as groupname, count(distinct(integer_test)) from %s %s group by varchar_test",
                baseOneTableName1, whereCondition);
        String[] columnParam = new String[] {
            "GROUPNAME",
            "count(distinct(integer_test))"};
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithLimitWithDistinct() throws Exception {
        String sql = String
            .format(
                "select group_concat(distinct integer_test, varchar_test order by pk SEPARATOR ',') as groupname from %s %s group by varchar_test order by varchar_test limit 2",
                baseOneTableName1, whereCondition);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithLimitWithDistinctOrderBy() throws Exception {
        String sql = String
            .format(
                "select group_concat(distinct integer_test, varchar_test order by varchar_test,integer_test SEPARATOR '|!@') as groupname from %s %s group by varchar_test order by varchar_test limit 2",
                baseOneTableName1, whereCondition);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithDistinctOrderByNumIdx() throws Exception {
        String sql = String
            .format(
                "select group_concat(distinct integer_test, varchar_test order by 2,integer_test SEPARATOR '|!@') as groupname from %s %s group by varchar_test",
                baseOneTableName1, whereCondition);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithDistinctOrderByNumIdxV2() throws Exception {
        String sql = String
            .format(
                "select group_concat(distinct integer_test, ':', varchar_test order by 3,2,1 SEPARATOR '|!@') as groupname from %s %s group by varchar_test",
                baseOneTableName1, whereCondition);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithDistinctOrderByNumIdxError() throws Exception {
        String sql = String
            .format(
                "select group_concat(distinct integer_test, ':', varchar_test order by 4,2,1 SEPARATOR '|!@') as groupname from %s %s group by varchar_test",
                baseOneTableName1, whereCondition);

        selectErrorAssert(sql, null, tddlConnection, "");
    }

    @Test
    public void testGroupConcatWithEmptyString() throws Exception {
        String sql = String
            .format("select group_concat('') as groupname from %s t1, %s t2 where t1.pk = t2.pk group by t1.pk",
                baseOneTableName1, baseTwoTableName2);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testGroupConcatWithEmptyStringOrderBy() throws Exception {
        String sql = String
            .format(
                "select group_concat('' order by 1) as groupname from %s t1, %s t2 where t1.pk = t2.pk group by t1.pk",
                baseOneTableName1, baseTwoTableName2);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithDistinctOrderByNumIdxErrorV2() throws Exception {
        String sql = String
            .format(
                "select group_concat(distinct integer_test, ':', varchar_test order by 3,0,1 SEPARATOR '|!@') as groupname from %s %s group by varchar_test",
                baseOneTableName1, whereCondition);

        selectErrorAssert(sql, null, tddlConnection, "");
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithSelectConstant() throws Exception {
        String sql = String
            .format(
                "select group_concat(integer_test-1000, 'abc' order by pk SEPARATOR ',') as groupname from %s %s group by varchar_test",
                baseOneTableName1, whereCondition);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithSelectConstantOrderBy() throws Exception {
        String sql = String
            .format(
                "select group_concat(integer_test-1000, 'abc' order by integer_test SEPARATOR ',') as groupname from %s %s group by varchar_test",
                baseOneTableName1, whereCondition);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatGroupByPk() throws Exception {
        String sql = String
            .format("select group_concat(varchar_test) as groupname from %s group by pk",
                baseOneTableName1);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatWithSelectConstantOrderBy20Byte() throws Exception {
        Connection newTddlConnection = null;
        Connection newMysqlConnection = null;
        Statement tddlStatement = null;
        Statement mysqlStatement = null;
        ResultSet tddlRs = null;
        ResultSet mysqlRs = null;
        try {
            newTddlConnection = getPolardbxConnection();
            newMysqlConnection = getMysqlConnection();
            tddlStatement = newTddlConnection.createStatement();
            mysqlStatement = newMysqlConnection.createStatement();

            String sql = String
                .format(
                    "select group_concat(integer_test-1000, 'abc' order by integer_test SEPARATOR ',') as groupname from %s %s group by varchar_test",
                    baseOneTableName1, whereCondition);
            tddlRs = tddlStatement.executeQuery(sql);
            mysqlRs = mysqlStatement.executeQuery(sql);

            List<List<Object>> mysqlResults = JdbcUtil.getAllResult(mysqlRs);
            List<List<Object>> tddlResults = JdbcUtil.getAllResult(tddlRs);
            // 不允许为空结果集合
            Assert.assertTrue("sql语句:" + sql + " 查询的结果集为空，请修改sql语句，保证有结果集",
                mysqlResults.size() != 0);
            assertWithMessage(" 非顺序情况下：mysql 返回结果与tddl 返回结果不一致 \n sql 语句为：" + sql).that(
                    mysqlResults)
                .containsExactlyElementsIn(tddlResults);

        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            tddlRs.close();
            mysqlRs.close();
            tddlStatement.close();
            mysqlStatement.close();
            newTddlConnection.close();
            newMysqlConnection.close();
        }

    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatBroadcast() throws Exception {
        String sql = String
            .format(
                "explain select group_concat(distinct integer_test order by pk separator ':') as groupname from %s group by varchar_test",
                broadcastTableName);
        String pattern = "LogicalView\\(tables.*SELECT GROUP_CONCAT\\(DISTINCT.*ORDER BY.*SEPARATOR.*FROM.*GROUP BY.*";
        explainResultMatchAssert(sql, null, tddlConnection, pattern);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatJoinTwoWithOrderAscAndDesc() throws Exception {
        /**
         * to see:https://yuque.antfin-inc.com/coronadb/design/lryp8u
         */
        if (isMySQL80()) {
            return;
        }
        String sql = String.format("select * from  %s", baseOneTableName1);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = String.format("select * from  %s", baseTwoTableName2);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = String
            .format(
                "select group_concat(distinct t1.integer_test, ' - ', t1.pk, ' = ', t2.pk, t2.pk order by t1.pk asc ,t2.pk desc separator ':') as groupname "
                    +
                    "from %s t1,%s t2 where t1.pk < 100 group by t1.varchar_test",
                baseOneTableName1, baseTwoTableName2);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatJoinTwoWithOrderAscAndDescAndInteger() throws Exception {
        String sql = String.format("select * from  %s", baseOneTableName1);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = String.format("select * from  %s", baseTwoTableName2);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = String
            .format(
                "select group_concat(distinct t1.integer_test, ' - ', t1.pk, ' = ', t2.pk order by 5 asc ,3 desc, t1.integer_test separator ':') as groupname "
                    +
                    "from %s t1,%s t2 where t1.pk < 100 group by t1.varchar_test",
                baseOneTableName1, baseTwoTableName2);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatJoinTwo() throws Exception {
        String sql = String
            .format("select group_concat(distinct t1.integer_test order by t1.pk separator ':') as groupname " +
                    "from %s t1,%s t2 where t1.pk = t2.pk and t1.pk = 2 group by t1.varchar_test",
                baseOneTableName1, baseTwoTableName2);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatJoinThree() throws Exception {
        String sql = String
            .format("select group_concat(distinct t1.integer_test order by t1.pk separator ':') as groupname "
                    + "from %s t1,%s t2, %s t3 where t1.pk = t2.pk and t1.pk = 2 and t3.integer_test > 1 group by t1.varchar_test",
                baseOneTableName1,
                baseTwoTableName2,
                baseThreeTableName3);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatBroadcastJoinTwo() throws Exception {
        String sql = String
            .format("select group_concat(distinct t1.integer_test order by t1.pk separator ':') as groupname " +
                    "from %s t1,%s t2 where t1.pk = t2.pk and t1.pk = 2 group by t1.varchar_test",
                broadcastTableName, baseOneTableName1);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testGroupConcatBroadcastJoinThree() throws Exception {
        String sql = String
            .format("select group_concat(distinct t1.integer_test order by t1.pk separator ':') as groupname " +
                    "from %s t1,%s t2, %s t3 where t1.pk = t2.pk and t1.pk = 2 and t1.pk = t3.pk group by t1.varchar_test",
                broadcastTableName, baseOneTableName1, broadcastTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.4.9-SNAPSHOT
     */
    @Test
    public void testGroupConcatPush() throws Exception {
        if (!baseFourTableName4.equalsIgnoreCase("select_base_four_multi_db_multi_tb")) {
            return;
        }
        String sql = "select b.* from (select id,MD5(GROUP_CONCAT(identifier ORDER BY identifier)) AS identifier"
            + " FROM (SELECT tinyint_test id , MD5(CONCAT(integer_test, GREATEST(date_test, '2020-12-16'), timestamp_test)) AS identifier "
            + "         FROM select_base_four_multi_db_multi_tb where pk = 1) a group by id) a "
            + "inner join (select id,MD5(GROUP_CONCAT(identifier ORDER BY identifier)) AS identifier "
            + " FROM (SELECT tinyint_test id , MD5(CONCAT(integer_test, GREATEST(date_test, '2020-12-16'), timestamp_test)) AS identifier "
            + "         FROM select_base_four_multi_db_multi_tb where pk = 2) b group by id) b on a.identifier = b.identifier and a.id != b.id;";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testExplainGroupConcatBroadcastJoinTwo() throws Exception {
        if (usingNewPartDb()) {
            return;
        }
        String sql = String
            .format("explain select group_concat(distinct t1.integer_test order by t1.pk separator ':') as groupname " +
                    "from %s t1,%s t2 where t1.pk = t2.pk and t1.pk = 2 group by t1.varchar_test",
                broadcastTableName, baseOneTableName1);
        String pattern =
            "(LogicalView\\(tables.*SELECT.*GROUP_CONCAT\\(DISTINCT.*ORDER BY.*SEPARATOR.*FROM.*INNER JOIN.*GROUP BY.*)"
                +
                "|(LogicalView\\(tables.*SELECT GROUP_CONCAT\\(DISTINCT.*ORDER BY.*SEPARATOR.*FROM.*WHERE.*AND.*GROUP BY.*)";
        explainResultMatchAssert(sql, null, tddlConnection, pattern);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void testExplainGroupConcatBroadcastJoinThree() throws Exception {
        String sql = String
            .format("select group_concat(distinct t1.integer_test order by t1.pk separator ':') as groupname " +
                    "from %s t1,%s t2, %s t3 where t1.pk = t2.pk and t1.pk = 2 and t1.pk = t3.pk group by t1.varchar_test",
                broadcastTableName, baseOneTableName1, broadcastTableName);

        Set<String> tableNames = new HashSet<>();
        tableNames.add(broadcastTableName);
        tableNames.add(baseOneTableName1);
        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * 只有跨库的情况是失败的，其余情况都成功
     */
    public void assertRunOkExceptionShard(String sql, List<Object> columnParam,
                                          String errorMessage) {
        if (ConfigUtil.isMultiTable(baseOneTableName1) && whereCondition.equals("")) {
            selectErrorAssert(sql, null, tddlConnection, errorMessage);
        } else {
            selectContentSameAssert(sql, columnParam, mysqlConnection, tddlConnection);
        }
    }

    /**
     * 只有跨库的情况是失败的，其余情况都成功
     */
    public void assertContentSameExceptNoWhereShard(String sql,
                                                    String[] columnParam) throws Exception {
        if (ConfigUtil.isMultiTable(baseOneTableName1) && whereCondition.equals("")) {
            assertContentLengthSame(sql, null, columnParam, mysqlConnection, tddlConnection);
        } else {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * normaltbl表数据的准备 start为插入起始数据，end为插入结束数据
     */
    public void normaltblPrepare(int start, int end, Connection mysqlConnection, Connection tddlConnection) {
        normaltblPrepare(start, end, mysqlConnection, tddlConnection, false);
    }

    /**
     * normaltbl表数据的准备 start为插入起始数据，end为插入结束数据
     */
    public void normaltblPrepare(int start, int end, Connection mysqlConnection, Connection tddlConnection,
                                 boolean containNull) {
        String name = "zhuoxue";
        String name1 = "zhuoxue_yll";
        String newName = "zhuoxue.yll";
        Date gmtDayBefore = new Date(1150300800000l);
        Date gmtDayNext = new Date(1550246400000l);
        Date gmtNext = new Date(1550304585000l);
        Date gmtBefore = new Date(1150304585000l);
        String sql = "delete from  " + baseFiveTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null);

        sql = "INSERT INTO "
            + baseFiveTableName
            + " (pk,integer_test,varchar_test,float_test) VALUES(?,?,?,?)";
        try {
            mysqlConnection.setAutoCommit(false);
        } catch (SQLException e) {
            String errorMs = "[connection set auto commit ] failed ";
            Assert.fail(errorMs + " \n " + e);
        }

        PreparedStatement mysqlPreparedStatement = JdbcUtil.preparedStatement(
            sql, mysqlConnection);
        PreparedStatement tddlPreparedStatement = JdbcUtil.preparedStatement(
            sql, tddlConnection);

        try {
            mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
            tddlPreparedStatement = tddlConnection.prepareStatement(sql);
        } catch (SQLException e) {
            String errorMs = "[prepareStatement  ] failed ";
            Assert.fail(errorMs + " \n " + e);
            JdbcUtil.close(mysqlPreparedStatement);
            JdbcUtil.close(tddlPreparedStatement);
        }
        try {
            for (int i = start; i < end / 2; i++) {

                tddlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                tddlPreparedStatement.setObject(2, i % 4 * 100);
                mysqlPreparedStatement.setObject(2, i % 4 * 100);

                // if (i % 4 == 0) {
//			tddlPreparedStatement.setObject(3, "0000-00-00 00:00:00");
//			mysqlPreparedStatement.setObject(3, "0000-00-00 00:00:00");
//			tddlPreparedStatement.setObject(4, "0000-00-00 00:00:00");
//			mysqlPreparedStatement.setObject(4, "0000-00-00 00:00:00");
//			tddlPreparedStatement.setObject(5, "0000-00-00 00:00:00");
//			mysqlPreparedStatement.setObject(5, "0000-00-00 00:00:00");
                tddlPreparedStatement.setObject(3, name);
                mysqlPreparedStatement.setObject(3, name);
                tddlPreparedStatement.setObject(4, 1.1);
                mysqlPreparedStatement.setObject(4, 1.1);
                tddlPreparedStatement.executeUpdate();
                mysqlPreparedStatement.executeUpdate();
                // tddlPreparedStatement.addBatch();
                // mysqlPreparedStatement.addBatch();
            }

            for (int i = end / 2; i < end - 1; i++) {
                tddlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                tddlPreparedStatement.setObject(2, i * 100);
                mysqlPreparedStatement.setObject(2, i * 100);
//			tddlPreparedStatement.setObject(3, gmtDayNext);
//			mysqlPreparedStatement.setObject(3, gmtDayNext);
//			tddlPreparedStatement.setObject(4, gmtNext);
//			mysqlPreparedStatement.setObject(4, gmtNext);
//			tddlPreparedStatement.setObject(5, gmtNext);
//			mysqlPreparedStatement.setObject(5, gmtNext);
                if (containNull) {
                    tddlPreparedStatement.setObject(3, null);
                    mysqlPreparedStatement.setObject(3, null);
                } else {
                    tddlPreparedStatement.setObject(3, newName);
                    mysqlPreparedStatement.setObject(3, newName);
                }
                tddlPreparedStatement.setObject(4, 1.1);
                mysqlPreparedStatement.setObject(4, 1.1);
                // tddlPreparedStatement.addBatch();
                // mysqlPreparedStatement.addBatch();
                tddlPreparedStatement.executeUpdate();
                mysqlPreparedStatement.executeUpdate();
            }

            for (int i = end - 1; i < end; i++) {
                tddlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                tddlPreparedStatement.setObject(2, i * 100);
                mysqlPreparedStatement.setObject(2, i * 100);
//			tddlPreparedStatement.setObject(3, gmtDayBefore);
//			mysqlPreparedStatement.setObject(3, gmtDayBefore);
//			tddlPreparedStatement.setObject(4, gmtBefore);
//			mysqlPreparedStatement.setObject(4, gmtBefore);
//			tddlPreparedStatement.setObject(5, gmtBefore);
//			mysqlPreparedStatement.setObject(5, gmtBefore);
                tddlPreparedStatement.setObject(3, name1);
                mysqlPreparedStatement.setObject(3, name1);
                tddlPreparedStatement.setObject(4, (float) (i * 0.01));
                mysqlPreparedStatement.setObject(4, (float) (i * 0.01));
                // tddlPreparedStatement.addBatch();
                // mysqlPreparedStatement.addBatch();
                tddlPreparedStatement.executeUpdate();
                mysqlPreparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            String errorMs = "[prepareStatement set ] failed ";
            Assert.fail(errorMs + " \n " + e);
            JdbcUtil.close(mysqlPreparedStatement);
            JdbcUtil.close(tddlPreparedStatement);
        }
        // try {
        // mysqlPreparedStatement.executeBatch();
        // tddlPreparedStatement.executeBatch();
        // } catch (SQLException e) {
        // String errorMs = "[prepareStatement executeBatch ] failed ";
        // Assert.fail(errorMs + " \n " + e);
        // JdbcUtil.close(mysqlPreparedStatement);
        // JdbcUtil.close(tddlPreparedStatement);
        // }
        try {
            mysqlConnection.commit();
            mysqlConnection.setAutoCommit(true);
        } catch (SQLException e) {
            String errorMs = "[connection commit ] failed ";
            Assert.fail(errorMs + " \n " + e);
            JdbcUtil.close(mysqlPreparedStatement);
            JdbcUtil.close(tddlPreparedStatement);
        } finally {
            JdbcUtil.close(mysqlPreparedStatement);
            JdbcUtil.close(tddlPreparedStatement);
        }
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
