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

package com.alibaba.polardbx.optimizer.biv;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mysql.jdbc.ByteArrayRow;
import com.mysql.jdbc.ResultSetRow;
import com.mysql.jdbc.StringUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDesc;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.TDDLSqlSelect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MockUtils {

    public static boolean fastMode = ConfigDataMode.isFastMock();
    public static final String RULE_STACK = "RuleSchemaManager";
    public static final String SCHEDUAL_STACK = "java.util.concurrent.ScheduledThreadPoolExecutor";

    public static final ThreadLocal<Map<Integer, Object>> threadLocal = new ThreadLocal<Map<Integer, Object>>();

    private static final LoadingCache<String, CursorMeta> cacheMeta = CacheBuilder.newBuilder()
        .expireAfterWrite(3, TimeUnit.SECONDS)
        .build(new CacheLoader<String, CursorMeta>() {
            @Override
            public CursorMeta load(String schemaSql) throws Exception {
                String schema = schemaSql.split("[|]", 2)[0];
                String sql = schemaSql.split("[|]", 2)[1];
                return findMeta(sql, schema);
            }
        });

    public static Set<String> fastsqls = Sets.newHashSet();

    static {
//        fastsqls.add("select @@version");
//        fastsqls.add("xa recover");
//        fastsqls.add("show variables like 'version'");
//        fastsqls.add("select @@tx_isolation as `@@session.tx_isolation`");
//        fastsqls.add(
//            "select @@auto_increment_increment as `auto_increment_increment`, @@character_set_client as `character_set_client`, @@character_set_connection as `character_set_connection`, @@character_set_results as `character_set_results`, @@character_set_server as `character_set_server`, @@collation_server as `collation_server`, @@init_connect as `init_connect`, @@interactive_timeout as `interactive_timeout`, @@license as `license`, @@lower_case_table_names as `lower_case_table_names`, @@max_allowed_packet as `max_allowed_packet`, @@net_buffer_length as `net_buffer_length`, @@net_write_timeout as `net_write_timeout`, @@query_cache_size as `query_cache_size`, @@query_cache_type as `query_cache_type`, @@sql_mode as `sql_mode`, @@system_time_zone as `system_time_zone`, @@time_zone as `time_zone`, @@tx_isolation as `transaction_isolation`, @@wait_timeout as `wait_timeout`");
    }

    public static boolean isSqlNeedTransparent(String sql, String schema,
                                               Map<Integer, Object> params) {
        if (fastMode) {
            int index = sql.indexOf("*/");
            if (index > 0) {
                sql = sql.substring(index + 2);
            }
            if (fastsqls.contains(sql.toLowerCase())) {
                return true;
            }
            if (sql.startsWith("XA")) {
                return false;
            }
            if (!isMetaBuildCapable(sql, schema)) {
                return true;
            }

        }

        if (checkStack(RULE_STACK)) {
            return true;
        }
        sql = sql.toUpperCase();
        if (sql.toUpperCase().contains("INFORMATION_SCHEMA")) {
            return true;
        }
        if (sql.contains("SEQUENCE")) {
            return false;
        }
        if (sql.contains("TDDL_RULE")) {
            return true;
        }
        if (sql.contains("__DRDS_")) {
            return true;
        }
        if (buildMeta(sql, schema, params) == null) {
            return true;
        }
        return false;
    }

    public static String stripSql(String sql, String schema, Map<Integer, Object> params) {
        if (fastsqls.contains(sql.toLowerCase())) {
            return sql;
        }
        // TODO remove this trick code
        if (sql.contains("begin;")) {
            sql = sql.substring(sql.indexOf("begin;") + 6);
        }
        if (sql.contains("XA START")) {
            sql = sql.split(";")[1];
        }
        if (sql.equalsIgnoreCase("commit")) {
            return sql;
        }
        if (sql.startsWith("XA ")) {
            return sql;
        }

        if (sql.toLowerCase().trim().startsWith("explain")) {
            sql = sql.toLowerCase().trim().substring("explain".length());
        }
        try {
            SqlNode sqlNode = new FastsqlParser().parse(sql, paramsMapToList(params)).get(0);
            if (sqlNode instanceof SqlShow) {
                if (((SqlShow) sqlNode).getDbName() != null && ((SqlShow) sqlNode).getDbName()
                    .toString()
                    .equalsIgnoreCase("information_schema")) {
                } else {
                    ((SqlShow) sqlNode).setTableName("test");
                }
                if (!(sqlNode instanceof SqlDesc)) {
                    sqlNode = ((SqlShow) sqlNode).removeLWOL(SqlParserPos.ZERO);
                }
            }
            return sqlNode.toSqlString(MysqlSqlDialect.DEFAULT).getSql();
        } catch (Exception e) {
            throw new RuntimeException("Could not parse physical sql in mock mode :" + sql, e);
        }
    }

    private static List<?> paramsMapToList(Map<Integer, Object> params) {
        if (params == null) {
            return null;
        }
        List<Object> p = Lists.newArrayList();
        for (int i = 1; i <= params.size(); i++) {
            p.add(params.get(i));
        }
        return p;
    }

    private static boolean isMetaBuildCapable(String sql, String schema) {
        try {
            SqlNode sqlNode = new FastsqlParser().parse(sql).get(0);
            if (sqlNode instanceof SqlShow) {
                return false;
            }
            if (sqlNode instanceof TDDLSqlSelect && ((TDDLSqlSelect) sqlNode).getFrom() == null) {
                ExecutionContext ec = new ExecutionContext();
                ec.setSchemaName(schema);
                SqlConverter sqlConverter = SqlConverter.getInstance(ec);
                SqlNode validatedNode = sqlConverter.validate(sqlNode);
                sqlConverter.toRel(validatedNode);
            }
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    public static boolean checkStack(String stack) {
        for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
            if (stackTraceElement.getClassName().equals(stack)) {
                return true;
            }
        }
        return false;
    }

    public static CursorMeta buildMeta(String sql, String schema,
                                       Map<Integer, Object> params) {
        try {
            threadLocal.set(params);
            return cacheMeta.get(schema + "|" + sql);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static CursorMeta findMeta(String sql, String schema) throws ExecutionException {
        RelNode rel;
        try {
            SqlNode sqlNode = new FastsqlParser().parse(sql, paramsMapToList(threadLocal.get())).get(0);
            ExecutionContext ec = new ExecutionContext();
            ec.setSchemaName(schema);
            SqlConverter sqlConverter = SqlConverter.getInstance(schema, ec);
            SqlNode validatedNode = sqlConverter.validate(sqlNode);
            rel = sqlConverter.toRel(validatedNode);
        } catch (Throwable e) {
            throw new NotSupportException("meta mock failed :" + sql + ", " + e.getMessage());
        }
        CursorMeta meta = CursorMeta.build(CalciteUtils.buildColumnMeta(rel, "__fake_data__"));

        if (meta == null) {
            throw new NotSupportException("mock not support :" + sql);
        }
        return meta;
    }

    public static final List<Integer> numSqlTypes = Lists.newArrayList(Types.INTEGER,
        Types.SMALLINT,
        Types.BIGINT,
        Types.TINYINT,
        Types.DOUBLE,
        Types.DECIMAL,
        Types.FLOAT,
        Types.NUMERIC,
        Types.BIT,
        Types.BOOLEAN);

    public static final List<Integer> dateSqlTypes = Lists.newArrayList(Types.TIMESTAMP, Types.TIME, Types.DATE);

    public static final List<Integer> charSqlTypes = Lists.newArrayList(Types.VARCHAR,
        Types.CHAR,
        Types.NCHAR,
        Types.NVARCHAR);

    public static boolean isSqlTypeNum(int sqlType) {
        if (numSqlTypes.contains(sqlType)) {
            return true;
        }
        return false;
    }

    public static boolean isSqlTypeDate(int sqlType) {
        if (dateSqlTypes.contains(sqlType)) {
            return true;
        }
        return false;
    }

    public static boolean isSqlTypeChar(int sqlType) {
        if (charSqlTypes.contains(sqlType)) {
            return true;
        }
        return false;
    }

    public static void replaceParams(String sql, Map<Integer, Object> params, StringBuilder sb) {
        sb.append(sql);
        return;
    }

    private static final char[] HEX_CHARS = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E',
        'F'};

    private static void toHexString(byte[] bytes, StringBuilder out) {
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            out.append(HEX_CHARS[v / 16]);
            out.append(HEX_CHARS[v % 16]);
        }
    }

    public static Matrix getMatrix(String appname) {
        List<Group> groups = new LinkedList<>();
        groups.add(fakeGroup(appname + "_0000"));
        groups.add(fakeGroup(appname + "_0001"));
        groups.add(fakeGroup(appname + "_0002"));
        groups.add(fakeGroup(appname + "_0003"));

        Matrix matrix = new Matrix();
        matrix.setGroups(groups);

        return matrix;
    }

    private static Group fakeGroup(String name) {
        Group g = new Group();
        g.setName(name);
        return g;
    }

    public static String mockDsWeightCommaStr(String dbGroupKey) {
        return dbGroupKey + "_db:r10w10";
    }

    public static String stripStraceId(String s) {
        String[] v = s.split("/");
        if (v.length > 3) {
            return v[3];
        }
        return "";
    }

    public static Object mockVariable(String key) {
        if ("transaction_isolation".equalsIgnoreCase(key)) {
            return "REPEATABLE-READ";
        } else if ("auto_increment_increment".equalsIgnoreCase(key)) {
            return '1';
        } else if ("max_allowed_packet".equalsIgnoreCase(key)) {
            return -1;
        } else if ("net_buffer_length".equalsIgnoreCase(key)) {
            return 16384;
        } else if ("tx_isolation".equalsIgnoreCase(key)) {
            return "REPEATABLE-READ";
        }
        return "";
    }

    public static ResultSet buildGenerateKeyRs(int num) {
        ArrayList<ResultSetRow> rowSet = new ArrayList();
        long beginAt = num;
        byte[][] row = new byte[1][];
        row[0] = StringUtils.getBytes(Long.toString(beginAt));
        rowSet.add(new ByteArrayRow(row, null));

        ColumnMeta columnMeta = new ColumnMeta("GenerateKey",
            "GENERATED_KEY",
            null,
            new Field(DataTypes.IntegerType));
        return new MockResultSet((MockStatement) null, CursorMeta.build(Lists.newArrayList(columnMeta)), true);
    }

}
