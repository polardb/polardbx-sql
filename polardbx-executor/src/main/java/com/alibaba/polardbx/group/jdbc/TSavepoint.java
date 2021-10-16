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

package com.alibaba.polardbx.group.jdbc;

import com.mysql.jdbc.SQLError;

import java.sql.SQLException;
import java.sql.Savepoint;

public class TSavepoint implements Savepoint {

    private final String savepointName;

    public TSavepoint(String savepoint) {
        this.savepointName = savepoint;
    }

    @Override
    public int getSavepointId() throws SQLException {
        throw SQLError.createSQLException("Only named savepoints are supported.",
            SQLError.SQL_STATE_DRIVER_NOT_CAPABLE,
            null);

    }

    @Override
    public String getSavepointName() throws SQLException {
        return this.savepointName;

    }

}
