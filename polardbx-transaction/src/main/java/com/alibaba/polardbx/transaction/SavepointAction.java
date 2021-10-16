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

package com.alibaba.polardbx.transaction;

import com.alibaba.polardbx.executor.utils.ExecUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

public interface SavepointAction {

    void doAction(Connection conn) throws SQLException;

    public static class SetSavepoint implements SavepointAction {

        private Savepoint savepoint;

        public SetSavepoint(Savepoint savepoint) {
            this.savepoint = savepoint;
        }

        @Override
        public void doAction(Connection conn) throws SQLException {

            Statement stmt = null;

            StringBuffer savePointQuery = new StringBuffer("SAVEPOINT ");
            savePointQuery.append('`');
            savePointQuery.append(savepoint.getSavepointName());
            savePointQuery.append('`');

            try {
                stmt = conn.createStatement();
                stmt.executeUpdate(savePointQuery.toString());
            } finally {
                ExecUtils.closeStatement(stmt);
            }

        }
    }

    public static class ReleaseSavepoint implements SavepointAction {

        private Savepoint savepoint;

        public ReleaseSavepoint(Savepoint savepoint) {
            this.savepoint = savepoint;
        }

        @Override
        public void doAction(Connection conn) throws SQLException {

            Statement stmt = null;

            StringBuffer savePointQuery = new StringBuffer("RELEASE SAVEPOINT ");
            savePointQuery.append('`');
            savePointQuery.append(savepoint.getSavepointName());
            savePointQuery.append('`');

            try {
                stmt = conn.createStatement();
                stmt.executeUpdate(savePointQuery.toString());
            } finally {
                ExecUtils.closeStatement(stmt);
            }
        }

    }

    public static class RollbackTo implements SavepointAction {

        private Savepoint savepoint;

        public RollbackTo(Savepoint savepoint) {
            this.savepoint = savepoint;
        }

        @Override
        public void doAction(Connection conn) throws SQLException {

            Statement stmt = null;

            StringBuffer savePointQuery = new StringBuffer("ROLLBACK TO SAVEPOINT ");
            savePointQuery.append('`');
            savePointQuery.append(savepoint.getSavepointName());
            savePointQuery.append('`');

            try {
                stmt = conn.createStatement();
                stmt.executeUpdate(savePointQuery.toString());
            } finally {
                ExecUtils.closeStatement(stmt);
            }

        }

    }

}
