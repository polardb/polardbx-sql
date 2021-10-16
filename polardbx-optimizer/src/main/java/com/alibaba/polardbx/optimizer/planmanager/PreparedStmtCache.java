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

package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.common.model.SqlType;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;

public class PreparedStmtCache {
    private final Statement stmt;
    private SqlParameterized sqlParameterized;
    private SqlType sqlType;
    private ExecutorMode executorMode;

    public PreparedStmtCache(Statement stmt, SqlParameterized sqlParameterized,
                             SqlType sqlType) {
        this.stmt = stmt;
        this.sqlParameterized = sqlParameterized;
        this.sqlType = sqlType;
    }

    public PreparedStmtCache(Statement stmt) {
        this.stmt = stmt;
    }

    public Statement getStmt() {
        return stmt;
    }

    public SqlParameterized getSqlParameterized() {
        return sqlParameterized;
    }

    public void setSqlParameterized(SqlParameterized sqlParameterized) {
        this.sqlParameterized = sqlParameterized;
    }

    public ExecutorMode getExecutorMode() {
        return executorMode;
    }

    public void setExecutorMode(ExecutorMode executorMode) {
        this.executorMode = executorMode;
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    public void setSqlType(SqlType sqlType) {
        this.sqlType = sqlType;
    }
}