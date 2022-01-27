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

package com.alibaba.polardbx.optimizer.parse.bean;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLoadXmlStatement;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlTableNameCollector;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Set;

/**
 * Created by hongxi.chx on 2017/12/1.
 */
public class SqlParameterized {

    /**
     * origin sql = parameterized sql + parameters
     */
    private ByteString originSql;

    /**
     * parameterized sql
     */
    private final String sql;

    /**
     * parameters
     */
    private final List<Object> parameters;

    /**
     * non-parameterized sql ast
     * DO NOT use for execute
     * just for verify usage
     */
    private final SQLStatement stmt;
    private final Set<Pair<String, String>> tables;

    /**
     * Type info digest for parameters.
     */
    private Long digest = null;

    public SqlParameterized(ByteString originSql, String sql, List<Object> parameters, SQLStatement stmt) {
        this.originSql = originSql;
        this.sql = sql;
        this.parameters = parameters;
        this.stmt = stmt;

        FastSqlTableNameCollector collector = new FastSqlTableNameCollector();
        stmt.accept(collector);
        this.tables = collector.getTables();
    }

    public SQLStatement getAst() {
        return this.stmt;
    }

    public ByteString getOriginSql() {
        return originSql;
    }

    public String getSql() {
        return sql;
    }

    public SQLStatement getStmt() {
        return stmt;
    }

    public List<Object> getParameters() {
        return parameters;
    }

    public Set<Pair<String, String>> getTables() {
        return this.tables;
    }

    public boolean needCache() {
        return SqlParameterizeUtils.needCache(stmt);
    }

    public boolean isUpdateDelete() {
        return stmt instanceof SQLUpdateStatement
            || stmt instanceof SQLDeleteStatement;
    }

    public boolean isDML() {
        return stmt instanceof SQLInsertStatement
            || stmt instanceof SQLReplaceStatement
            || stmt instanceof SQLUpdateStatement
            || stmt instanceof SQLDeleteStatement
            || stmt instanceof MySqlLoadDataInFileStatement
            || stmt instanceof MySqlLoadXmlStatement;
    }

    public long getDigest() {
        if (digest == null) {
            digest = doComputeDigest();
        }
        return digest;
    }

    private long doComputeDigest() {
        long digest = 0L;
        for (Object value : parameters) {
            int typeCode = getTypeCode(value);
            digest = 31 * digest + typeCode;
        }
        return digest;
    }

    private static int getTypeCode(Object param) {
        if (param == null) {
            // NULL
            return 0;
        } else if (param instanceof Integer || param instanceof Long) {
            // BIGINT
            return 1 << 1;
        } else if (param instanceof BigInteger) {
            // BIGINT_INTEGER
            return 1 << 2;
        } else if (param instanceof BigDecimal) {
            // DECIMAL
            return 1 << 3;
        } else {
            // VARCHAR
            return 1 << 4;
        }
    }
}
