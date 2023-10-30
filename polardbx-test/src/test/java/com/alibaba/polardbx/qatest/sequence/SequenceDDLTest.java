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

package com.alibaba.polardbx.qatest.sequence;

import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

/**
 * Sequence DDL测试
 *
 * @author chenhui
 * @since 5.1.0
 */

public class SequenceDDLTest extends BaseSequenceTestCase {

    private String seqName1;
    private String seqName2;
    private String seqSimpleName1;

    public SequenceDDLTest(String schema) {
        this.schema = schema;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
        this.seqSimpleName1 = randomTableName("chenhui", 4);
        this.seqName1 = schemaPrefix + seqSimpleName1;
        this.seqName2 = schemaPrefix + randomTableName("simple_seq_test_xiaoying", 4);
    }

    @Parameterized.Parameters(name = "{index}:schema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {{""}, {PropertiesUtil.polardbXShardingDBName2()}});
    }

    @Before
    public void initData() throws Exception {
        dropSequence(seqName1);
        dropSequence(seqName2);
    }

    @After
    public void afterData() throws Exception {
        dropSequence(seqName1);
        dropSequence(seqName2);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void testCreateSequence() {
        String sql = String.format("create sequence %s start with 100", seqName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // New Sequence by default
        assertExistsSequence(seqName1, 100);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void testDeleteSequence() {

        String sql = String.format("create group sequence %s start with 0", seqName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertExistsSequence(seqName1, 0);

        String schemaName = PropertiesUtil.polardbXShardingDBName1();
        if (!StringUtils.isBlank(schema)) {
            schemaName = schema;
        }

        sql = String.format("%s delete from sequence where schema_name='%s' and name like '" + seqSimpleName1 + "'",
            BaseSequenceTestCase.META_DB_HINT, schemaName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertNotExistsSequence(seqName1);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void testSequenceValue() {
        String sql = String.format("create sequence %s start with 10", seqName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // New Sequence by default
        assertExistsSequence(seqName1, 10);

        try {
            sql = String.format("select %s.nextval from dual", seqName1);
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            Assert.assertTrue(rs.next());
            long seqVal = rs.getLong(seqName1 + ".nextVal");

            rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getLong(seqName1 + ".nextVal"), seqVal + 1);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void testSimpleMaxValueSequenceValue() {
        String sql = String.format("create sequence %s start with 99999999996 maxvalue 99999999999", seqName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertExistsSequence(seqName2, 99999999996L);

        try {
            sql = String.format("select %s.nextval  from dual", seqName2);
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            Assert.assertTrue(rs.next());
            long seqVal = rs.getLong(seqName2 + ".nextVal");

            rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getLong(seqName2 + ".nextVal"), seqVal + 1);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    public void assertExistsSequence(String name, long num) {
        String simpleSeqName = getSimpleTableName(name);

        String sql = String.format("show sequences where name = '%s'", simpleSeqName);

        boolean existName = false;
        boolean valueRight = false;

        Connection connection = StringUtils.isBlank(schemaPrefix) ? tddlConnection : tddlConnection2;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(connection, sql)) {
            if (rs.next()) {
                existName = true;
                if (rs.getLong("value") == num) {
                    valueRight = true;
                }
            }
            Assert.assertTrue(existName);
            Assert.assertTrue(valueRight);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void assertNotExistsSequence(String name) {
        String sql = "show sequences";
        boolean existName = false;
        try {
            Connection connection = StringUtils.isBlank(schemaPrefix) ? tddlConnection : tddlConnection2;
            ResultSet rs = JdbcUtil.executeQuerySuccess(connection, sql);
            while (rs.next()) {
                if (rs.getString("name").equals(getSimpleTableName(name))) {
                    if (!rs.getString("schema_name").equalsIgnoreCase(this.schema)) {
                        // polarx为实例级别
                        continue;
                    }
                    existName = true;
                }
            }
            Assert.assertFalse(existName);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
