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
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author jichen 2017-11-28 15:49
 */
public class ShowTableStatusTest extends BaseSequenceTestCase {

    @Test
    public void testShowTableStatus() throws SQLException {
        Statement stmt = null;
        try {
            // Show table status works with various Sequences including time-based one.
            createVariousSeqs();

            stmt = tddlConnection.createStatement();

            // Just check if the command can be executed without any exception.
            stmt.executeQuery("show table status like 'showTableStatusWith%'");
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                }
            }
            dropVariousSeqs();
        }
    }

    private void createVariousSeqs() throws SQLException {
        String sql = "create table if not exists showTableStatusWith%s"
            + "(c1 bigint not null auto_increment %s, c2 int, primary key(c1)) dbpartition by hash(c1)";
        Statement stmt = null;
        try {
            stmt = tddlConnection.createStatement();
            stmt.execute(String.format(sql, "GroupSeq", ""));
            stmt.execute(String.format(sql, "SimpleSeq", "by simple"));
            stmt.execute(String.format(sql, "TimeSeq", "by time"));
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    private void dropVariousSeqs() throws SQLException {
        String sql = "drop table if exists showTableStatusWith%s";
        Statement stmt = null;
        try {
            stmt = tddlConnection.createStatement();
            stmt.execute(String.format(sql, "GroupSeq"));
            stmt.execute(String.format(sql, "SimpleSeq"));
            stmt.execute(String.format(sql, "TimeSeq"));
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                }
            }
        }
    }

}
