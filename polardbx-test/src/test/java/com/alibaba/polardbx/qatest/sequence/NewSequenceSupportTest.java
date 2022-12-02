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

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class NewSequenceSupportTest extends BaseSequenceTestCase {

    private static final String CREATE_SEQ = "create %s sequence %s";
    private static final String DROP_SEQ = "drop sequence %s";

    private static final String SHOW_SEQ = "show sequences where name='%s'";

    private static final String CREATE_TABLE_SHARDING =
        "create table %s (id int not null primary key auto_increment %s, age int) dbpartition by hash(id)";
    private static final String CREATE_TABLE_AUTO =
        "create table %s (id int not null primary key auto_increment %s, age int)";
    private static final String CREATE_TABLE_PARTITION =
        "create table %s (id int not null primary key auto_increment %s, age int) partition by key(id)";

    private Connection drdsDbConn;
    private Connection autoDbConn;

    private final String seqName;
    private final String tableName;
    private final String tableSeqName;

    @Before
    public void init() {
        this.drdsDbConn = getPolardbxDirectConnection(PropertiesUtil.polardbXShardingDBName1());
        this.autoDbConn = getPolardbxDirectConnection(PropertiesUtil.polardbXAutoDBName1());
    }

    public NewSequenceSupportTest() {
        this.seqName = "new_seq_support";
        this.tableName = randomTableName("test_new_seq_support", 4);
        this.tableSeqName = "AUTO_SEQ_" + this.tableName;
    }

    @Test
    public void testDrdsDb() throws Exception {
        createSequence(drdsDbConn, "");
        checkSeqType(drdsDbConn, "GROUP");
        dropSequence(drdsDbConn);

        final String errMsg = "New Sequence is only supported in a database with the AUTO mode";

        createSequenceFailed(drdsDbConn, "NEW", errMsg);
        dropSequence(drdsDbConn);

        createShardingTable(drdsDbConn, "", null);
        checkTableSeqType(drdsDbConn, "GROUP");
        dropTable(drdsDbConn);

        createShardingTable(drdsDbConn, "BY NEW", errMsg);
        dropTable(drdsDbConn);
    }

    @Test
    public void testAutoDb() throws Exception {
        createSequence(autoDbConn, "");
        checkSeqType(autoDbConn, "NEW");
        dropSequence(autoDbConn);

        createSequence(autoDbConn, "NEW");
        checkSeqType(autoDbConn, "NEW");
        dropSequence(autoDbConn);

        createAutoTable(autoDbConn, "");
        checkTableSeqType(autoDbConn, "NEW");
        dropTable(autoDbConn);

        createAutoTable(autoDbConn, "BY NEW");
        checkTableSeqType(autoDbConn, "NEW");
        dropTable(autoDbConn);

        createPartitionTable(autoDbConn, "");
        checkTableSeqType(autoDbConn, "NEW");
        dropTable(autoDbConn);

        createPartitionTable(autoDbConn, "BY NEW");
        checkTableSeqType(autoDbConn, "NEW");
        dropTable(autoDbConn);
    }

    private void createSequence(Connection conn, String seqType) {
        String sql = String.format(CREATE_SEQ, seqType, seqName);
        JdbcUtil.executeUpdateSuccess(conn, sql);
    }

    private void createSequenceFailed(Connection conn, String seqType, String expectedErrMsg) {
        String sql = String.format(CREATE_SEQ, seqType, seqName);
        JdbcUtil.executeUpdateFailed(conn, sql, expectedErrMsg);
    }

    private void dropSequence(Connection conn) {
        String sql = String.format(DROP_SEQ, seqName);
        JdbcUtil.executeUpdateSuccess(conn, sql);
    }

    private void createShardingTable(Connection conn, String seqType, String expectedErrMsg) {
        createTable(conn, CREATE_TABLE_SHARDING, seqType, expectedErrMsg);
    }

    private void createAutoTable(Connection conn, String seqType) {
        createTable(conn, CREATE_TABLE_AUTO, seqType, null);
    }

    private void createPartitionTable(Connection conn, String seqType) {
        createTable(conn, CREATE_TABLE_PARTITION, seqType, null);
    }

    private void createTable(Connection conn, String createSql, String seqType, String expectedErrMsg) {
        String sql = String.format(createSql, tableName, seqType);
        if (TStringUtil.isNotBlank(expectedErrMsg)) {
            JdbcUtil.executeUpdateFailed(conn, sql, expectedErrMsg);
        } else {
            JdbcUtil.executeUpdateSuccess(conn, sql);
        }
    }

    private void dropTable(Connection conn) {
        dropTableIfExists(conn, tableName);
    }

    private void checkSeqType(Connection conn, String expectedSeqType) throws SQLException {
        checkType(conn, seqName, expectedSeqType);
    }

    private void checkTableSeqType(Connection conn, String expectedSeqType) throws SQLException {
        checkType(conn, tableSeqName, expectedSeqType);
    }

    private void checkType(Connection conn, String seqName, String expectedSeqType) throws SQLException {
        String actualSeqType = "";
        String sql = String.format(SHOW_SEQ, seqName);
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                actualSeqType = rs.getString("TYPE");
            }
        }
        Assert.assertTrue(TStringUtil.equals(expectedSeqType, actualSeqType),
            "Expected Type: " + expectedSeqType + ", Actual Type: " + actualSeqType);
    }

    @After
    public void destroy() {
        dropSequence(drdsDbConn);
        dropSequence(autoDbConn);

        dropTable(drdsDbConn);
        dropTable(autoDbConn);

        if (drdsDbConn != null) {
            try {
                drdsDbConn.close();
            } catch (SQLException ignored) {
            }
        }

        if (autoDbConn != null) {
            try {
                autoDbConn.close();
            } catch (SQLException ignored) {
            }
        }
    }

}
