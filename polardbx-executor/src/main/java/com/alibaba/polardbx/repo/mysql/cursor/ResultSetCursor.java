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

package com.alibaba.polardbx.repo.mysql.cursor;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.repo.mysql.spi.MyJdbcHandler;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResultSetCursor extends ResultCursor {

    private ResultSet rs;
    private MyJdbcHandler jdbcHandler = null;
    private List<ColumnMeta> returnColumns;

    public ResultSetCursor(ResultSet rs, MyJdbcHandler jdbcHandler) {
        super();
        this.rs = rs;

        this.jdbcHandler = jdbcHandler;
    }

    public ResultSet getResultSet() {
        return this.rs;
    }

    @Override
    public Row doNext() {
        try {
            return jdbcHandler.next();
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e);
        }
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exs) {
        if (exs == null) {
            exs = new ArrayList<>();
        }
        try {
            rs.close();
        } catch (Throwable e) {
            exs.add(new TddlNestableRuntimeException(e));
        }

        return exs;
    }

}
