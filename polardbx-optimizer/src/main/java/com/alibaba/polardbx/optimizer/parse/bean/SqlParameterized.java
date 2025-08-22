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
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLoadXmlStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlTableNameCollector;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by hongxi.chx on 2017/12/1.
 */
public class SqlParameterized {

    public static final int INVALID_CODE = -1;
    public static final int NULL_CODE = 0;
    public static final int BIGINT_CODE = 1;
    public static final int BIGINT_UNSIGNED_CODE = 1 << 1;
    public static final int DECIMAL_CODE = 1 << 2;
    public static final int VARCHAR_CODE = 1 << 3;
    public static final int VARCHAR_BYTE_CODE = 1 << 4;
    public static final int END_OF_LIST_CODE = 1 << 5;
    /**
     * prevent collision when switch strict mode
     */
    public static final long SEED_FOR_NO_STRICT_MODE = 0X8D21E8DDC1CC38C1L;
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
    /**
     * SqlParameterized 不一定经过了参数化
     * 用该字段来标记
     */
    private final boolean unparameterized;

    private boolean forPrepare;

    private int paraMemory = 0;

    public SqlParameterized(ByteString originSql, String sql, List<Object> parameters, SQLStatement stmt,
                            boolean unparameterized) {
        this.originSql = originSql;
        this.sql = sql;
        this.parameters = parameters;
        this.stmt = stmt;
        this.unparameterized = unparameterized;
        FastSqlTableNameCollector collector = new FastSqlTableNameCollector();
        stmt.accept(collector);
        this.tables = collector.getTables();
    }

    public SqlParameterized(String sql, List<Object> parameters) {
        this.originSql = null;
        this.sql = sql;
        this.parameters = parameters;
        this.unparameterized = false;
        this.stmt = null;
        this.tables = null;
    }

    public SqlParameterized(String parameterSql, Map<Integer, ParameterContext> currentParameter) {
        this.sql = parameterSql;
        this.parameters = Lists.newLinkedList();
        for (int i = 1; i <= currentParameter.size(); i++) {
            parameters.add(currentParameter.get(i));
        }
        this.unparameterized = false;
        this.stmt = null;
        this.tables = null;
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
            digest = doComputeDigest(DynamicConfig.getInstance().enablePlanTypeDigestStrictMode());
        }
        return digest;
    }

    // Used for test two mode
    public long getDigest(boolean enableStrictMode) {
        if (digest == null) {
            digest = doComputeDigest(enableStrictMode);
        }
        return digest;
    }

    private long doComputeDigest(boolean enableStrictMode) {
        long digest = 0L;
        if (insertWithoutParamsInFunction(stmt)) {
            return digest;
        }
        if (DynamicConfig.getInstance().enablePlanTypeDigest()) {
            if (enableStrictMode) {
                for (Object value : parameters) {
                    if (value instanceof List) {
                        for (Object v : (List) value) {
                            int typeCode = getTypeCode(v);
                            digest = 63 * digest + typeCode;
                        }
                    }
                    int typeCode = getTypeCode(value);
                    digest = 63 * digest + typeCode;
                }
            } else {
                digest += SEED_FOR_NO_STRICT_MODE;
                for (Object value : parameters) {
                    if (value instanceof List) {
                        int lastTypeCodeInList = INVALID_CODE;
                        for (Object v : (List) value) {
                            int typeCode = getTypeCode(v);
                            // omit continuous param of the same type
                            // such that "key in (1, 2, 3)" and "key in (1)" will get same digest
                            // notice that "key in (1, 2, 3, '4')" and "key in (1, '2')" will also get same digest
                            if (lastTypeCodeInList == typeCode) {
                                continue;
                            }
                            lastTypeCodeInList = typeCode;
                            digest = 63 * digest + typeCode;
                        }
                    }
                    int typeCode = getTypeCode(value);
                    digest = 63 * digest + typeCode;
                }
            }
        }
        return digest;
    }

    public static boolean insertWithoutParamsInFunction(SQLStatement stmt) {
        if (stmt instanceof MySqlInsertStatement) {
            return !((MySqlInsertStatement) stmt).isHasArgsInFunction();
        }
        if (stmt instanceof SQLReplaceStatement) {
            return !((SQLReplaceStatement) stmt).isHasArgsInFunction();
        }
        return false;
    }

    public static int getTypeCode(Object param) {
        if (param == null) {
            // NULL
            return NULL_CODE;
        } else if (param instanceof Integer || param instanceof Long) {
            // BIGINT
            return BIGINT_CODE;
        } else if (param instanceof BigInteger) {
            // BIGINT_UNSIGNED
            return BIGINT_UNSIGNED_CODE;
        } else if (param instanceof BigDecimal) {
            // DECIMAL
            return DECIMAL_CODE;
        } else if (param instanceof List) {
            // END OF LIST
            // To distinguish "a in (1, 2) and b in (3)" with "a in (1) and b in (2, 3)"
            return END_OF_LIST_CODE;
        } else {
            if (param instanceof byte[]) {
                // VARCHAR
                return VARCHAR_BYTE_CODE;
            } else {
                // VARCHAR
                return VARCHAR_CODE;
            }
        }
    }

    public boolean isUnparameterized() {
        return unparameterized;
    }

    /**
     * @return -1 if the memory consumption is too big
     */
    public int getParaMemory() {
        if (paraMemory == 0) {
            if (parameters.size() > 100) {
                return -1;
            }
            for (Object value : parameters) {
                paraMemory += getTypeLen(value);
                if (paraMemory > 500) {
                    paraMemory = -1;
                    return -1;
                }
            }
        }
        return paraMemory;
    }

    /**
     * get the estimated length of parameter
     *
     * @param param the parameter to be estimated
     * @return an estimated length
     */
    private static int getTypeLen(Object param) {
        if (param instanceof List) {
            int sum = 0;
            for (Object o : (List) param) {
                sum += getTypeLen(o);
            }
            return sum;
        }
        if (param == null) {
            // NULL
            return 0;
        } else if (param instanceof Integer || param instanceof Long) {
            // BIGINT
            return 4;
        } else if (param instanceof BigInteger || param instanceof BigDecimal) {
            // BIGINT_UNSIGNED
            // DECIMAL
            return 20;
        }
        if (param instanceof byte[]) {
            // VARCHAR
            return ((byte[]) param).length;
        }
        if (param instanceof String) {
            // VARCHAR
            return ((String) param).length();
        } else {
            return 1000;
        }
    }

    public boolean getForPrepare() {
        return this.forPrepare;
    }

    public void setForPrepare(boolean forPrepare) {
        this.forPrepare = forPrepare;
    }
}
