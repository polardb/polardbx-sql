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

package com.alibaba.polardbx.qatest.ddl.auto.autoNewPartition;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

@CdcIgnore(ignoreReason = "cdc未支持insert overwrite")
public class AutoPartitionInsertOverwriteTest extends BaseAutoPartitionNewPartition {
    private final static String testTableName = "insert_overwrite_test";
    private final static String testGsiTableName = "insert_overwrite_test_index";
    private final static String testGsiTableName2 = "insert_overwrite_test_index2";
    private final static String createTableTemp = "create table %s ("
        + " `pk` int(11) NOT NULL AUTO_INCREMENT,"
        + " `c1` int(11) NOT NULL DEFAULT '1',"
        + " `c2` int(11) NOT NULL DEFAULT '2',"
        + " `pad` varchar(20) NOT NULL DEFAULT 'abc',"
        + " %s "
        + " PRIMARY KEY (`pk`)"
        + " ) %s";
    private final static List<String[]> tableDefines = Arrays.asList(
        new String[][] {
            {"", ""}, //新分区表
            {
                "global index " + testGsiTableName + " (`c1`),",
                ""
            }, //包含gsi
            {
                "global index " + testGsiTableName + " (`c1`),"
                    + "global index " + testGsiTableName2 + "(`c2`),",
                ""
            }, //包含两个gsi
            {
                "global index " + testGsiTableName + " (`c1`),"
                    + "clustered index " + testGsiTableName2 + "(`c2`),",
                ""
            }, //包含两个gsi，其中一个聚簇索引
            {
                "clustered index " + testGsiTableName + " (`c1`),"
                    + "clustered index " + testGsiTableName2 + "(`c2`),",
                ""
            }, //包含两个聚簇索引
        }
    );

    private final String[] tableDefine;

    @Parameterized.Parameters()
    public static List<Object[]> initParameters() {
        List<Object[]> allTest = new ArrayList<>();
        tableDefines.forEach(strings -> allTest.add(new Object[] {strings}));
        return allTest;
    }

    public AutoPartitionInsertOverwriteTest(String[] tableDefine) {
        this.tableDefine = tableDefine;
    }

    @Before
    public void before() throws SQLException {

        dropTableWithGsi(testTableName, ImmutableList.of(testGsiTableName, testGsiTableName2));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(createTableTemp, testTableName, tableDefine[0], tableDefine[1]));
    }

    /**
     * 普通insert overwrite 功能验证
     */
    @Test
    public void insertOverwriteTest() {

        String tableName = testTableName;
        String insertSql = "insert into " + tableName + "(pk,c1) values(1,1), (2,2), (3,3), (4,4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        String overwrite = "insert overwrite " + tableName + "(pk,c1) values(5,5), (6,6), (7,7)";

        int affectRows = JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, overwrite);
        Assert.assertEquals(3, affectRows);

        List<Long> pks = JdbcUtil.selectIds("select pk from " + tableName, "pk", tddlConnection);
        List<Long> expect = ImmutableList.of(5L, 6L, 7L);
        assertThat(pks).containsExactlyElementsIn(expect);

    }

    /**
     * 采用PreparedStatement多次执行
     */
    @Test
    public void insertOverwritePreparedStatementTest() throws Exception {
        String tableName = testTableName;
        String insertSql = "insert into " + tableName + "(pk,c1) values(1,1), (2,2), (3,3), (4,4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        String overwrite = "insert overwrite " + tableName + "(pk) values(?), (?)";
        PreparedStatement ps = JdbcUtil.preparedStatement(overwrite, tddlConnection);
        ps.setLong(1, 6L);
        ps.setLong(2, 7L);
        int affect = ps.executeUpdate();
        Assert.assertEquals(2, affect);

        Connection otherConnect = getPolardbxConnection();
        List<Long> pks = JdbcUtil.selectIds("select pk from " + tableName, "pk", otherConnect);
        assertThat(pks).containsExactlyElementsIn(ImmutableList.of(6L, 7L));

        ps.setLong(1, 8L);
        ps.setLong(2, 9L);
        affect = ps.executeUpdate();
        Assert.assertEquals(2, affect);

        pks = JdbcUtil.selectIds("select pk from " + tableName, "pk", otherConnect);
        assertThat(pks).containsExactlyElementsIn(ImmutableList.of(8L, 9L));

        ps.setLong(1, 3L);
        ps.setLong(2, 4L);
        affect = ps.executeUpdate();
        Assert.assertEquals(2, affect);

        pks = JdbcUtil.selectIds("select pk from " + tableName, "pk", otherConnect);
        assertThat(pks).containsExactlyElementsIn(ImmutableList.of(3L, 4L));

        ps.close();
    }

    /**
     * insert overwrite into
     */
    @Test
    public void insertOverwriteIntoTest() {

        String tableName = testTableName;
        String insertSql = "insert into " + tableName + "(pk,c1) values(1,1), (2,2), (3,3), (4,4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        String overwrite = "insert overwrite into " + tableName + "(pk,c1) values(5,5), (6,6), (7,7)";

        int affectRows = JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, overwrite);
        Assert.assertEquals(3, affectRows);

        List<Long> pks = JdbcUtil.selectIds("select pk from " + tableName, "pk", tddlConnection);
        List<Long> expect = ImmutableList.of(5L, 6L, 7L);
        assertThat(pks).containsExactlyElementsIn(expect);

    }

    /**
     * insert语句解析出错，不会truncate table
     */
    @Test
    public void insertOverwriteErrorParserTest() {

        String tableName = testTableName;
        String insertSql = "insert into " + tableName + "(pk,c1) values(1,1), (2,2), (3,3), (4,4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        String overwrite = "insert overwrite " + tableName + "(pk,c1) values(8), (6,6,6)";

        JdbcUtil.executeUpdateFailed(tddlConnection, overwrite, "ERR_PARSER");

        List<Long> pks = JdbcUtil.selectIds("select pk from " + tableName, "pk", tddlConnection);
        List<Long> expect = ImmutableList.of(1L, 2L, 3L, 4L);
        assertThat(pks).containsExactlyElementsIn(expect);

    }

    /**
     * insert语句validate出错，不会truncate table
     */
    @Test
    public void insertOverwriteErrorValidateTest() {

        String tableName = testTableName;
        String insertSql = "insert into " + tableName + "(pk,c1) values(1,1), (2,2), (3,3), (4,4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        String overwrite = "insert overwrite " + tableName + "(pk,c99) values(5,5), (6,6)";

        JdbcUtil.executeUpdateFailed(tddlConnection, overwrite, "ERR_VALIDATE");

        List<Long> pks = JdbcUtil.selectIds("select pk from " + tableName, "pk", tddlConnection);
        List<Long> expect = ImmutableList.of(1L, 2L, 3L, 4L);
        assertThat(pks).containsExactlyElementsIn(expect);

    }

    /**
     * insert语句执行insert出错，不会truncate table
     */
    @Test
    public void insertOverwriteErrorInsertTest() {

        String tableName = testTableName;
        String insertSql = "insert into " + tableName + "(pk,c1) values(1,1), (2,2), (3,3), (4,4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        String overwrite = "insert overwrite " + tableName + "(pk) values(5), (5)";

        JdbcUtil.executeUpdateFailed(tddlConnection, overwrite, "ERR_EXECUTE_ON_MYSQL");

        List<Long> pks = JdbcUtil.selectIds("select pk from " + tableName, "pk", tddlConnection);
        List<Long> expect = ImmutableList.of(1L, 2L, 3L, 4L);
        assertThat(pks).containsExactlyElementsIn(expect);
    }

    /**
     * 测试 insert overwrite ... select
     */
    @Test
    public void insertOverwriteSelectTest() {

        String tableName = testTableName;
        String insertSql = "insert into " + tableName + "(pk,c1) values(1,1), (2,2), (3,3), (4,4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        String tableName2 = testTableName + "_2";
        dropTableIfExists(tddlConnection, tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTableTemp, tableName2, "", ""));
        insertSql = "insert into " + tableName2 + "(pk,c1) values(6,6), (7,7), (8,8)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        String overwrite = "insert overwrite " + tableName + "(pk,c1) select pk,c1 from " + tableName2;

        int affectRows = JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, overwrite);
        Assert.assertEquals(3, affectRows);

        List<Long> pks = JdbcUtil.selectIds("select pk from " + tableName, "pk", tddlConnection);
        List<Long> expect = ImmutableList.of(6L, 7L, 8L);
        assertThat(pks).containsExactlyElementsIn(expect);

    }

    /**
     * 测试 insert overwrite ... select Self; select自身表
     */
    @Test
    public void insertOverwriteSelectSelfTest() {

        String tableName = testTableName;
        String insertSql = "insert into " + tableName + "(pk,c1) values(1,1), (2,2), (3,3), (4,4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        String overwrite = "insert overwrite " + tableName + "(pk,c1) select pk,c1 from " + tableName;

        int affectRows = JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, overwrite);
        Assert.assertEquals(0, affectRows);

        List<Long> pks = JdbcUtil.selectIds("select pk from " + tableName, "pk", tddlConnection);
        List<Long> expect = ImmutableList.of();
        assertThat(pks).containsExactlyElementsIn(expect);

    }

    /**
     * insert overwrite ... on duplicate key
     */
    @Test
    public void insertOverwriteOnDuplicateKeyTest() {

        String tableName = testTableName;
        String insertSql = "insert into " + tableName + "(pk,c1) values(1,1), (2,2), (3,3), (4,4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        String overwrite = "insert overwrite " + tableName
            + "(pk,c1) values(5,5), (6,6), (6,6) on duplicate key update pk = pk + 1, c1 = c1 + 1";

        int affectRows = JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, overwrite);
        Assert.assertEquals(4, affectRows);

        List<Long> pks = JdbcUtil.selectIds("select pk from " + tableName, "pk", tddlConnection);
        List<Long> expect = ImmutableList.of(5L, 7L);
        assertThat(pks).containsExactlyElementsIn(expect);

    }
}
