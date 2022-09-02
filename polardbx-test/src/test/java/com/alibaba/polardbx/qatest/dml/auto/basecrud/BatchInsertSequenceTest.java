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
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BatchInsertSequenceTest extends AutoCrudBasedLockTestCase {

    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();

    /**
     * 这里特别注意，应该首先添加自定义的sequence在com.taobao.tddl.sequence.${appname}中如：
     * 参考com.taobao.tddl.sequence.corona_qatest的设置 <bean
     * id="AUTO_SEQ_batch_sequence_test_tbl"
     * class="GroupSequence"
     * init-method="init"> <property name="sequenceDao" ref="sequenceDao" />
     * <property name="name" value="batch_sequence_test_tbl" /> </bean>
     */
    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allMultiTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE_AUTONIC));
    }

    public BatchInsertSequenceTest(String tableName) {
        this.baseOneTableName = tableName;
    }

    private static int PERMIT_BATCH_SIZE = 10;

    @Before
    public void initData() throws Exception {
        String sql = "delete from  " + baseOneTableName;
        JdbcUtil.updateDataTddl(tddlConnection, sql, null);
    }

    /**
     * 这里测试的允许值是在<property name="innerStep" value="10" />中设置的
     */
    @Test
    public void insertBatchSequenceTest_tddl() throws Exception {
        String sql = "insert into " + baseOneTableName + " (integer_test,varchar_test) values(?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        List<String> keys = new ArrayList<String>();
        for (int i = 0; i < PERMIT_BATCH_SIZE; i++) {
            List<Object> param = new ArrayList<Object>();
            String pk = RandomStringUtils.randomNumeric(8);
            keys.add(pk);
            param.add(Long.valueOf(pk)); // integer_test
            param.add(String.format("index %3d", i)); // name
            params.add(param);
        }
        PreparedStatement ps = JdbcUtil.preparedStatementBatch(sql, tddlConnection);
        JdbcUtil.updateDataBatch(ps, sql, params);
        ResultSet rs = ps.getGeneratedKeys();
        // 需要保证id递增
        long firstId = -1;
        long lastId = -1;
        List<String> ids = new ArrayList<String>();
        for (int i = 0; i < PERMIT_BATCH_SIZE; i++) {
            Assert.assertEquals(true, rs.next());
            long id = rs.getLong(1);
            Assert.assertTrue(id > 0);
            if (firstId < 0) {
                firstId = id;
            }

            if (lastId > 0) {
                Assert.assertEquals(lastId + 1, id);
            }

            lastId = id;
            ids.add(String.valueOf(id));
        }
        JdbcUtil.close(ps);
        JdbcUtil.close(rs);

        sql = "select count(*) as cnt from " + baseOneTableName + " where pk in (" + StringUtils.join(ids, ',') + ")";
        ps = JdbcUtil.preparedStatement(sql, tddlConnection);
        rs = JdbcUtil.executeQuery(sql, ps);
        Assert.assertEquals(true, rs.next());
        Assert.assertEquals(PERMIT_BATCH_SIZE, rs.getLong("cnt"));

        JdbcUtil.close(ps);
        JdbcUtil.close(rs);
    }

    /**
     * 以下从BatchInsertTest 类迁移过来 迁移开始===
     */
    @Test
    public void insertMultiTableSequenceTest() throws Exception {
        String sql = "insert/*+TDDL({'extra':{'PROCESS_AUTO_INCREMENT_BY_SEQUENCE':'TRUE'}})*/ into "
            + baseOneTableName + "  (pk,varchar_test) values(null,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add("test" + i);
            params.add(param);
        }

        PreparedStatement ps = JdbcUtil.preparedStatementBatch(sql, tddlConnection);
        int[] nn = JdbcUtil.updateDataBatch(ps, sql, params);

        int affect = 0;
        for (int n : nn) {
            affect += n;
        }

        Assert.assertTrue(100 == affect || java.sql.Statement.SUCCESS_NO_INFO * 100 == affect);
        ResultSet rs = ps.getGeneratedKeys();
        Assert.assertEquals(true, rs.next());
        Assert.assertTrue(rs.getLong(1) > 0);

        assertIdentityEqualLastInsertId(tddlConnection);
        JdbcUtil.close(ps);
        JdbcUtil.close(rs);
    }

    @Test
    public void insertMultiTableZeorNumberSequenceTest() throws Exception {
        String sql = "insert/*+TDDL({'extra':{'PROCESS_AUTO_INCREMENT_BY_SEQUENCE':'TRUE'}})*/ into "
            + baseOneTableName + "  (pk,varchar_test) values(?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(0);
            param.add("test" + i);
            params.add(param);
        }

        PreparedStatement ps = JdbcUtil.preparedStatementBatch(sql, tddlConnection);
        int[] nn = JdbcUtil.updateDataBatch(ps, sql, params);
        int affect = 0;
        for (int n : nn) {
            affect += n;
        }

        Assert.assertTrue(100 == affect || java.sql.Statement.SUCCESS_NO_INFO * 100 == affect);
        ResultSet rs = ps.getGeneratedKeys();
        Assert.assertEquals(true, rs.next());
        Assert.assertTrue(rs.getLong(1) > 0);

        assertIdentityEqualLastInsertId(tddlConnection);
        JdbcUtil.close(ps);
        JdbcUtil.close(rs);
    }

    @Ignore("Can't use default for sharding key")
    @Test
    public void insertMultiTableDefaultSequenceBatchTest() throws Exception {
        String sql = "/*+TDDL({'extra':{'PROCESS_AUTO_INCREMENT_BY_SEQUENCE':'TRUE'}})*/insert into "
            + baseOneTableName + "  (pk,varchar_test) values(default,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add("test" + i);
            params.add(param);
        }

        PreparedStatement ps = JdbcUtil.preparedStatementBatch(sql, tddlConnection);
        int[] nn = JdbcUtil.updateDataBatch(ps, sql, params);
        int affect = 0;
        for (int n : nn) {
            affect += n;
        }

        Assert.assertTrue(100 == affect || java.sql.Statement.SUCCESS_NO_INFO * 100 == affect);
        ResultSet rs = ps.getGeneratedKeys();
        Assert.assertEquals(true, rs.next());
        Assert.assertTrue(rs.getLong(1) > 0);

        assertIdentityEqualLastInsertId(tddlConnection);
        JdbcUtil.close(ps);
        JdbcUtil.close(rs);
    }

    /**
     * 以上从BatchInsertTest 类迁移过来 迁移结束===
     */

    /**
     * 以下从InsertSequenceTest 类迁移过来 迁移开始===
     */

    /**
     * @since 5.0.1
     */
    @Test
    public void insertLastInsertIdTest() throws Exception {
        String sql = "insert into  " + baseOneTableName + "   (varchar_test) values(?)";
        List<Object> param = new ArrayList<Object>();
        param.add("test");
        PreparedStatement tddlPreparedStatement = JdbcUtil.preparedStatementBatch(sql, tddlConnection);
        int affect = JdbcUtil.updateData(tddlPreparedStatement, sql, param);
        Assert.assertEquals(1, affect);
        ResultSet rs = tddlPreparedStatement.getGeneratedKeys();
        Assert.assertEquals(true, rs.next());
        Assert.assertTrue(rs.getLong(1) > 0);

        assertIdentityEqualLastInsertId(tddlConnection);
        JdbcUtil.close(tddlPreparedStatement);
        JdbcUtil.close(rs);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void insertIdentityTest() throws Exception {
        String sql = "insert into  " + baseOneTableName + "   (varchar_test) values(?)";
        List<Object> param = new ArrayList<Object>();
        param.add("test");

        PreparedStatement tddlPreparedStatement = JdbcUtil.preparedStatementBatch(sql, tddlConnection);
        int affect = JdbcUtil.updateData(tddlPreparedStatement, sql, param);
        Assert.assertEquals(1, affect);
        ResultSet rs = tddlPreparedStatement.getGeneratedKeys();
        Assert.assertEquals(true, rs.next());
        Assert.assertTrue(rs.getLong(1) > 0);

        assertIdentityEqualLastInsertId(tddlConnection);
        JdbcUtil.close(tddlPreparedStatement);
        JdbcUtil.close(rs);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertLastInsertIdOneGroupMultiTableTest() throws Exception {
        String sql = "/*+TDDL({'extra':{'PROCESS_AUTO_INCREMENT_BY_SEQUENCE':'TRUE'}})*/insert into "
            + baseOneTableName + "  (varchar_test) values(?)";
        List<Object> param = new ArrayList<Object>();
        param.add("test");

        PreparedStatement tddlPreparedStatement = JdbcUtil.preparedStatementBatch(sql, tddlConnection);
        int affect = JdbcUtil.updateData(tddlPreparedStatement, sql, param);
        Assert.assertEquals(1, affect);
        ResultSet rs = tddlPreparedStatement.getGeneratedKeys();
        Assert.assertEquals(true, rs.next());
        Assert.assertTrue(rs.getLong(1) > 0);

        assertIdentityEqualLastInsertId(tddlConnection);
        JdbcUtil.close(tddlPreparedStatement);
        JdbcUtil.close(rs);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void insertIdentityOneGroupMultiTableTest() throws Exception {
        String sql = "/*+TDDL({'extra':{'PROCESS_AUTO_INCREMENT_BY_SEQUENCE':'TRUE'}})*/insert into "
            + baseOneTableName + "  (varchar_test) values(?)";
        List<Object> param = new ArrayList<Object>();
        param.add("test");

        PreparedStatement tddlPreparedStatement = JdbcUtil.preparedStatementBatch(sql, tddlConnection);
        int affect = JdbcUtil.updateData(tddlPreparedStatement, sql, param);
        Assert.assertEquals(1, affect);
        ResultSet rs = tddlPreparedStatement.getGeneratedKeys();
        Assert.assertEquals(true, rs.next());
        Assert.assertTrue(rs.getLong(1) > 0);

        assertIdentityEqualLastInsertId(tddlConnection);
        JdbcUtil.close(tddlPreparedStatement);
        JdbcUtil.close(rs);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertOneGroupMultiTableNotProcessSequenceTest() throws Exception {
        String sql = "/*+TDDL({'extra':{'PROCESS_AUTO_INCREMENT_BY_SEQUENCE':'TRUE'}})*/insert into "
            + baseOneTableName + "  (pk,varchar_test) values(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(10);
        param.add("test");

        PreparedStatement tddlPreparedStatement = JdbcUtil.preparedStatementBatch(sql, tddlConnection);
        int affect = JdbcUtil.updateData(tddlPreparedStatement, sql, param);
        Assert.assertEquals(1, affect);
        ResultSet rs = tddlPreparedStatement.getGeneratedKeys();
        Assert.assertEquals(true, rs.next());
        Assert.assertEquals(10, rs.getLong(1));
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void insertIdentifyOneGroupMultiTableNotProcessSequenceTest() throws Exception {
        String sql = "/*+TDDL({'extra':{'PROCESS_AUTO_INCREMENT_BY_SEQUENCE':'TRUE'}})*/insert into "
            + baseOneTableName + "  (pk,varchar_test) values(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(10);
        param.add("test");

        PreparedStatement tddlPreparedStatement = JdbcUtil.preparedStatementBatch(sql, tddlConnection);
        int affect = JdbcUtil.updateData(tddlPreparedStatement, sql, param);
        Assert.assertEquals(1, affect);
        ResultSet rs = tddlPreparedStatement.getGeneratedKeys();
        Assert.assertEquals(true, rs.next());
        Assert.assertEquals(10, rs.getLong(1));
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void insertMultiTableZeroSequenceTest() throws Exception {
        String sql = "/*+TDDL({'extra':{'PROCESS_AUTO_INCREMENT_BY_SEQUENCE':'TRUE'}})*/insert into "
            + baseOneTableName + "  (pk,varchar_test) values(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(0);
        param.add("test");

        PreparedStatement tddlPreparedStatement = JdbcUtil.preparedStatementBatch(sql, tddlConnection);
        int affect = JdbcUtil.updateData(tddlPreparedStatement, sql, param);
        Assert.assertEquals(1, affect);
        ResultSet rs = tddlPreparedStatement.getGeneratedKeys();
        Assert.assertEquals(true, rs.next());
        Assert.assertTrue(rs.getLong(1) > 0);

        assertIdentityEqualLastInsertId(tddlConnection);
        JdbcUtil.close(tddlPreparedStatement);
        JdbcUtil.close(rs);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void insertIdentityMultiTableZeroSequenceTest() throws Exception {
        String sql = "/*+TDDL({'extra':{'PROCESS_AUTO_INCREMENT_BY_SEQUENCE':'TRUE'}})*/insert into "
            + baseOneTableName + "  (pk,varchar_test) values(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(0);
        param.add("test");

        PreparedStatement tddlPreparedStatement = JdbcUtil.preparedStatementBatch(sql, tddlConnection);
        int affect = JdbcUtil.updateData(tddlPreparedStatement, sql, param);
        Assert.assertEquals(1, affect);
        ResultSet rs = tddlPreparedStatement.getGeneratedKeys();
        Assert.assertEquals(true, rs.next());
        Assert.assertTrue(rs.getLong(1) > 0);

        assertIdentityEqualLastInsertId(tddlConnection);
        JdbcUtil.close(tddlPreparedStatement);
        JdbcUtil.close(rs);
    }

    /**
     * @since 5.0.1
     */
    @Ignore("Can't use default for sharding key")
    @Test
    public void insertMultiTableDefaultSequenceTest() throws Exception {
        String sql = "/*+TDDL({'extra':{'PROCESS_AUTO_INCREMENT_BY_SEQUENCE':'TRUE'}})*/insert into "
            + baseOneTableName + "  (pk,varchar_test) values(default,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("test");

        PreparedStatement tddlPreparedStatement = JdbcUtil.preparedStatementBatch(sql, tddlConnection);
        int affect = JdbcUtil.updateData(tddlPreparedStatement, sql, param);
        Assert.assertEquals(1, affect);
        ResultSet rs = tddlPreparedStatement.getGeneratedKeys();
        Assert.assertEquals(true, rs.next());
        Assert.assertTrue(rs.getLong(1) > 0);

        assertIdentityEqualLastInsertId(tddlConnection);
        JdbcUtil.close(tddlPreparedStatement);
        JdbcUtil.close(rs);
    }

    /**
     * @since 5.1.19
     */
    @Ignore("Can't use default for sharding key")
    @Test
    public void insertIdentityMultiTableDefaultSequenceTest() throws Exception {
        String sql = "/*+TDDL({'extra':{'PROCESS_AUTO_INCREMENT_BY_SEQUENCE':'TRUE'}})*/insert into "
            + baseOneTableName + "  (pk,varchar_test) values(default,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("test");

        PreparedStatement tddlPreparedStatement = JdbcUtil.preparedStatementBatch(sql, tddlConnection);
        int affect = JdbcUtil.updateData(tddlPreparedStatement, sql, param);
        Assert.assertEquals(1, affect);
        ResultSet rs = tddlPreparedStatement.getGeneratedKeys();
        Assert.assertEquals(true, rs.next());
        Assert.assertTrue(rs.getLong(1) > 0);

        assertIdentityEqualLastInsertId(tddlConnection);
    }

    private void assertIdentityEqualLastInsertId(Connection tddlConnection) {
        PreparedStatement tddlPreparedStatement = null;
        ResultSet rs = null;
        try {
            String sql = "select @@identity a";
            long identityValue;
            tddlPreparedStatement = tddlConnection.prepareStatement(sql);
            rs = tddlPreparedStatement.executeQuery();
            Assert.assertEquals(true, rs.next());
            identityValue = rs.getLong(1);
            Assert.assertTrue(identityValue > 0);
            JdbcUtil.close(tddlPreparedStatement);
            JdbcUtil.close(rs);
            sql = "select last_insert_id() a";
            long lastInsertIdValue;
            tddlPreparedStatement = tddlConnection.prepareStatement(sql);
            rs = tddlPreparedStatement.executeQuery();
            Assert.assertEquals(true, rs.next());
            lastInsertIdValue = rs.getLong(1);
            Assert.assertTrue(identityValue > 0);
            Assert.assertEquals(identityValue, lastInsertIdValue);
        } catch (Exception e) {
            throw new RuntimeException("assertIdentityEqualLastInsertId error");
        } finally {
            JdbcUtil.close(tddlPreparedStatement);
            JdbcUtil.close(rs);
        }
    }
}
