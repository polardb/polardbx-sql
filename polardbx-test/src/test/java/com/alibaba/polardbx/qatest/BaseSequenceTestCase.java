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

package com.alibaba.polardbx.qatest;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.entity.TestSequence;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

public class BaseSequenceTestCase extends BaseTestCase {
    protected static final Logger logger = LoggerFactory.getLogger(BaseSequenceTestCase.class);

    protected static final String HINT_CREATE_GSI =
        "/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false, ALLOW_ADD_GSI=true)*/ ";

    protected static String META_DB_HINT = "/*TDDL:NODE='__META_DB__'*/";

    protected Connection tddlConnection;
    protected Connection mysqlConnection;
    protected Connection tddlConnection2;
    protected Connection infomationSchemaDbConnection;

    protected String schema;
    protected String schemaPrefix;

    @Before
    public void checkOtherConnectionNotNull() {
        tddlConnection = getPolardbxConnection();
        tddlConnection2 = getPolardbxConnection2();
        mysqlConnection = getMysqlConnection();
        infomationSchemaDbConnection = getMysqlConnection("information_schema");
    }

    public void enableFailPoint(String key, String value) {
        String sql = String.format("set @%s='%s'", key, value);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public void dropTableIfExists(String tableName) {
        dropTableIfExists(tddlConnection, tableName);
    }

    public void dropTableIfExists(Connection conn, String tableName) {
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(conn, sql);
    }

    public void dropTableWithGsi(String primary, List<String> indexNames) {
        final String finalPrimary = quoteSpecialName(primary);
        try (final Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + finalPrimary);

            for (String gsi : Optional.ofNullable(indexNames).orElse(ImmutableList.of())) {
                stmt.execute("DROP TABLE IF EXISTS " + quoteSpecialName(gsi));
            }
            return;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static String quoteSpecialName(String primary) {
        if (!TStringUtil.contains(primary, ".")) {
            if (primary.contains("`")) {
                primary = "`" + primary.replaceAll("`", "``") + "`";
            } else {
                primary = "`" + primary + "`";
            }
        }
        return primary;
    }

    public void dropTable(String tableName) {
        String sql = "drop table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public long getLastInsertId(Connection conn) throws SQLException {
        String sql = "select last_insert_id() as  a";
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            assertThat(rs.next()).isTrue();
            return rs.getLong("a");
        } finally {
            JdbcUtil.close(rs);
        }

    }

    public long getIdentity(Connection conn) throws SQLException {
        String sql = "select @@identity a";
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            assertThat(rs.next()).isTrue();
            return rs.getLong("a");
        } finally {
            JdbcUtil.close(rs);
        }

    }

    /**
     * assert表不存在
     */
    public static void assertNotExistsTable(String tableName, Connection conn) {
        String sql = String.format("select * from `%s` limit 1", tableName);
        ResultSet rs = null;
        PreparedStatement prepareStatement = null;
        try {
            prepareStatement = conn.prepareStatement(sql);
            rs = prepareStatement.executeQuery();
            Assert.fail("table exist : " + tableName);
        } catch (Exception ex) {
            // 因为表不存在，所以要报错
            Assert.assertTrue(ex != null);
        } finally {
            try {
                if (prepareStatement != null) {
                    prepareStatement.close();
                }
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                logger.error("rs close error", e);
            }

        }
    }

    public String showCreateTable(Connection conn, String tbName) {
        String sql = "show create table " + tbName;

        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            assertThat(rs.next()).isTrue();
            return rs.getString("Create Table");
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            JdbcUtil.close(rs);
        }
        return null;
    }

    public TestSequence showSequence(String seqName) {
        TestSequence testSequence = null;
        ResultSet rs;
        String curSchema = null;

        if (seqName.contains(".")) {
            curSchema = seqName.split("\\.")[0];
        }

        seqName = getSimpleTableName(seqName);

        String sql = String.format("show sequences where name='%s'", seqName);
        if (curSchema != null && curSchema.equalsIgnoreCase(PropertiesUtil.polardbXDBName2(usingNewPartDb()))) {
            rs = JdbcUtil.executeQuerySuccess(tddlConnection2, sql);
        } else {
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        }

        try {
            if (rs.next()) {
                testSequence = new TestSequence();
                testSequence.setName(rs.getString("name"));
                testSequence.setValue(parseLongWithNA(rs.getString("value")));
                testSequence.setIncrementBy(parseLongWithNA(getSeqAttrWithoutEx(rs, "increment_by")));
                testSequence.setStartWith(parseLongWithNA(getSeqAttrWithoutEx(rs, "start_with")));
                testSequence.setMaxValue(parseLongWithNA(getSeqAttrWithoutEx(rs, "max_value")));
                testSequence.setCycle(getSeqAttrWithoutEx(rs, "cycle"));
                testSequence.setUnitCount(parseLongWithNA(getSeqAttrWithoutEx(rs, "unit_count")));
                testSequence.setUnitIndex(parseLongWithNA(getSeqAttrWithoutEx(rs, "unit_index")));
                testSequence.setInnerStep(parseLongWithNA(getSeqAttrWithoutEx(rs, "inner_step")));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail(e.getMessage());
            testSequence = null;
        } finally {
            JdbcUtil.close(rs);
        }

        return testSequence;
    }

    private String getSeqAttrWithoutEx(ResultSet rs, String colName) {
        String result = SequenceAttribute.STR_NA;
        try {
            result = rs.getString(colName);
        } catch (SQLException e) {
            // Ignore and use default value.
        }
        return result.trim();
    }

    private long parseLongWithNA(String longWithNA) {
        if (longWithNA.equalsIgnoreCase(SequenceAttribute.STR_NA)) {
            return 0L;
        }
        return Long.parseLong(longWithNA);
    }

    public long getSequenceNextVal(String seqName) throws Exception {
        long nextVal = -1;
        String sql = "select " + seqName + ".nextval";
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            while (rs.next()) {
                nextVal = rs.getLong(seqName + ".nextval");
            }
        } finally {
            JdbcUtil.close(rs);
        }
        return nextVal;
    }

    public void dropSequence(String seqName) {
        String sql = "drop sequence " + seqName;
        JdbcUtil.executeUpdate(tddlConnection, sql);
    }

    public void createOldSequence(String seqName) {
        Connection conn = seqName.contains(".") ? tddlConnection2 : tddlConnection;
        seqName = getSimpleTableName(seqName);
        //String sql = "insert into sequence (name, value, gmt_modified) values('" + seqName + "', 100000, now())";
        String sql = "create group sequence " + seqName;
        // 默认库是第三个
        JdbcUtil.executeUpdateSuccess(conn, sql);
    }

    public void createOldSimpleSequence(String seqName) {
        // 默认库是第三个
        Connection conn = seqName.contains(".") ? tddlConnection2 : tddlConnection;
        seqName = getSimpleTableName(seqName);
        //String sql =
        //    "insert into sequence_opt (name, value, increment_by, start_with, max_value, cycle, gmt_created, gmt_modified) "
        //        + "values('" + seqName + "', 1, 1, 1, 9223372036854775807, 0, now(), now())";
        String sql = "create simple sequence " + seqName;
        JdbcUtil.executeUpdateSuccess(conn, sql);
    }

    /**
     * 判断sequence或者sequence_opt标有无这条记录
     */
    public boolean isExistInSequence(String seqName, String sequenceTable) {
        Connection conn = seqName.contains(".") ? tddlConnection2 : tddlConnection;

        String schemaName = PropertiesUtil.polardbXDBName1(usingNewPartDb());
        String simpleSeqName = seqName;
        if (seqName.contains(".")) {
            schemaName = TStringUtil.remove(seqName.split("\\.")[0], "`");
            simpleSeqName = TStringUtil.remove(seqName.split("\\.")[1], "`");
        }

        String sql = String.format(
            "%s select * from %s where schema_name='%s' and name='%s'", META_DB_HINT, sequenceTable,
            schemaName, simpleSeqName);

        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);

        try {
            return rs.next();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        return false;
    }

    /**
     * 正常的非边界值的情况下对不同类型的sequence做粗粒度的检测
     */
    public void simpleCheckSequence(String seqName, String seqType) throws Exception {
        TestSequence sequence = showSequence(seqName);

        if (seqType.equalsIgnoreCase("simple") || seqType.equalsIgnoreCase("by simple")) {
            // 先判断表结构
            assertThat(isExistInSequence(seqName, "sequence")).isFalse();
            assertThat(isExistInSequence(seqName, "sequence_opt")).isTrue();

            // 粗判断show sequence结果
            assertThat(sequence.getStartWith()).isNotEqualTo(0);
            assertThat(sequence.getMaxValue()).isNotEqualTo(0);
            assertThat(sequence.getIncrementBy()).isNotEqualTo(0);
            assertThat(sequence.getValue()).isAtLeast(sequence.getStartWith());
            assertThat(sequence.getCycle()).isAnyOf(SequenceAttribute.STR_YES, SequenceAttribute.STR_NO);

            // 取下一个值,判断值正常变化
            long nextVal = getSequenceNextVal(seqName);
            assertThat(sequence.getValue()).isEqualTo(nextVal);
            nextVal = getSequenceNextVal(seqName);
            assertThat(nextVal).isEqualTo(sequence.getValue() + sequence.getIncrementBy());
            sequence = showSequence(seqName);
            assertThat(sequence.getValue()).isEqualTo(nextVal + sequence.getIncrementBy());

        } else if (seqType.contains("group") || (!usingNewPartDb() && seqType.equalsIgnoreCase(""))) {
            assertThat(isExistInSequence(seqName, "sequence")).isTrue();
            assertThat(isExistInSequence(seqName, "sequence_opt")).isFalse();

            // 粗判断sequence
            assertThat(sequence.getStartWith()).isEqualTo(0);
            assertThat(sequence.getMaxValue()).isEqualTo(0);
            assertThat(sequence.getIncrementBy()).isEqualTo(0);

            // 取下一个值,再判断sequence
            getSequenceNextVal(seqName);
            sequence = showSequence(seqName);
            assertThat(sequence.getStartWith()).isEqualTo(0);
            assertThat(sequence.getCycle()).isEqualTo(SequenceAttribute.STR_NA);
            assertThat(sequence.getValue()).isAtLeast(100000L);

        } else if (seqType.contains("new") || (usingNewPartDb() && seqType.equalsIgnoreCase(""))) {
            // New Sequence
            assertThat(isExistInSequence(seqName, "sequence")).isFalse();
            assertThat(isExistInSequence(seqName, "sequence_opt")).isTrue();

            // 粗判断sequence
            assertThat(sequence.getStartWith()).isNotEqualTo(0);
            assertThat(sequence.getMaxValue()).isNotEqualTo(0);
            assertThat(sequence.getIncrementBy()).isNotEqualTo(0);
            assertThat(sequence.getValue()).isAtLeast(sequence.getStartWith());
            assertThat(sequence.getCycle()).isAnyOf(SequenceAttribute.STR_YES, SequenceAttribute.STR_NO);

        } else if (seqType.contains("time")) {
            assertThat(isExistInSequence(seqName, "sequence")).isFalse();
            assertThat(isExistInSequence(seqName, "sequence_opt")).isTrue();
            //assertNotExistsTable(schemaPrefix + "sequence_opt_mem_" + simpleSeqName, tddlConnection);

            assertThat(sequence.getStartWith()).isEqualTo(0);
            assertThat(sequence.getMaxValue()).isEqualTo(0);
            assertThat(sequence.getIncrementBy()).isEqualTo(0);
            assertThat(sequence.getCycle()).isEqualTo(SequenceAttribute.STR_NA);

            long nextVal1 = getSequenceNextVal(seqName);
            long nextVal2 = getSequenceNextVal(seqName);
            assertThat(nextVal1).isLessThan(nextVal2);

        } else {

            assertWithMessage("sequence 模式不正确,无法判断").fail();
        }

    }

    public boolean isSpecialSequence(String seqType) {
        return seqType.toLowerCase().contains("time") || seqType.toLowerCase().contains("group")
            || seqType.trim().isEmpty();
    }

    public boolean isSpecialSequencePart(String seqType) {
        return seqType.toLowerCase().contains("time") || seqType.toLowerCase().contains("group");
    }

    /**
     * 用于循环执行sql, 用户指定次数, 只执行update语句
     */
    public class SQLRunner implements Runnable {

        private String sql;
        private int count;
        private Connection conn;

        public SQLRunner(String sql, int count, Connection conn) {
            this.sql = sql;
            this.count = count;
            this.conn = conn;
        }

        @Override
        public void run() {
            for (int i = 0; i < count; i++) {
                PreparedStatement ps = null;
                try {
                    ps = conn.prepareStatement(sql);
                    ps.executeUpdate();
                } catch (Exception e) {
                    throw GeneralUtil.nestedException(e);
                } finally {
                    try {
                        if (ps != null) {
                            ps.close();
                        }

                    } catch (SQLException e) {

                    }
                }

            }

        }
    }

    public static String getSimpleTableName(String tableName) {
        if (tableName.contains(".")) {
            return tableName.split("\\.")[1];
        }
        return tableName;
    }

    /**
     * Assert that all selected data are routed correctly in this table.
     * Approach: select by its sharding keys and primary keys, if it has a
     * result, then it's right.
     *
     * @param tableName may be the base table or the index
     * @param selectedData result of "select *"
     * @param columnNames column names corresponding to selectedData
     */
    protected void assertRouteCorrectness(String tableName, List<List<Object>> selectedData, List<String> columnNames,
                                          List<String> shardingKeys) throws Exception {
        List<Integer> shardingColumnIndexes = shardingKeys.stream()
            .map(columnNames::indexOf)
            .collect(Collectors.toList());
        List<List<Object>> shardingValues = new ArrayList<>(selectedData.size());
        for (List<Object> row : selectedData) {
            List<Object> shardingValuesInRow = new ArrayList<>(shardingColumnIndexes.size());
            for (int index : shardingColumnIndexes) {
                Object value = row.get(index);
                if (value instanceof JdbcUtil.MyDate) {
                    value = ((JdbcUtil.MyDate) value).getDate();
                } else if (value instanceof JdbcUtil.MyNumber) {
                    value = ((JdbcUtil.MyNumber) value).getNumber();
                }
                shardingValuesInRow.add(value);
            }
            shardingValues.add(shardingValuesInRow);
        }

        String sql = String.format(Optional.ofNullable(hint).orElse("") + "select * from %s where ", tableName);
        for (int i = 0; i < shardingKeys.size(); i++) {
            String shardingKeyName = shardingKeys.get(i);
            if (i == shardingKeys.size() - 1) {
                sql += shardingKeyName + "=?";
            } else {
                sql += shardingKeyName + "=? and ";
            }
        }

        for (List<Object> row : shardingValues) {
            PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, row, tddlConnection);
            ResultSet tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
            Assert.assertTrue(tddlRs.next());
        }
    }
}
