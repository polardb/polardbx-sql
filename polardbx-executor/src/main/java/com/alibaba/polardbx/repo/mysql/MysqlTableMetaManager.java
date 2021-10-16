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

package com.alibaba.polardbx.repo.mysql;

import com.alibaba.druid.pool.GetConnectionTimeoutException;
import com.alibaba.druid.proxy.jdbc.ResultSetMetaDataProxy;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.extension.Activate;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.executor.spi.IDataSourceGetter;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexType;
import com.alibaba.polardbx.optimizer.config.table.Relationship;
import com.alibaba.polardbx.optimizer.config.table.RepoSchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.repo.mysql.spi.MyDataSourceGetter;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.compatible.XResultSet;
import com.alibaba.polardbx.rpc.compatible.XResultSetMetaData;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.result.XResult;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import com.mysql.jdbc.StringUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.commons.lang.exception.ExceptionUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

@Activate(name = "MYSQL_JDBC", order = 2)
public class MysqlTableMetaManager extends RepoSchemaManager {

    private final static Logger logger = LoggerFactory.getLogger(MysqlTableMetaManager.class);

    private static final java.lang.reflect.Field MYSQL_RSMD_FIELDS;

    static {
        try {
            MYSQL_RSMD_FIELDS = com.mysql.jdbc.ResultSetMetaData.class.getDeclaredField("fields");
            MYSQL_RSMD_FIELDS.setAccessible(true);
        } catch (SecurityException | NoSuchFieldException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private IDataSourceGetter mysqlDsGetter = null;

    public MysqlTableMetaManager() {
        super(null);
    }

    @Override
    protected void doInit() {
        super.doInit();
        this.schemaName = this.getGroup().getSchemaName();
        mysqlDsGetter = new MyDataSourceGetter(getSchemaName());
    }

    protected IDataSourceGetter getDatasourceGetter() {
        return this.mysqlDsGetter;
    }

    /**
     * 需要各Repo来实现
     */
    @Override
    protected TableMeta getTable0(String logicalTableName, String actualTableName) {
        if (null != logicalTableName) {
            logicalTableName = logicalTableName.toLowerCase();
        }
        TableMeta ts = fetchSchema(logicalTableName, actualTableName);
        return ts;
    }

    /**
     * 20160429 方物 增加获取表collation meta信息
     */
    private Map<String, String> fetchCollationType(Connection conn, String actualTableName) {
        Map<String, String> collationType = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();
            // 物理库 collation info 会自动按照 列 > 表 > 库 > server 的顺序获取
            rs = stmt.executeQuery("SHOW FULL COLUMNS FROM `" + actualTableName + "`");
            while (rs.next()) {
                String field = rs.getString("Field");
                String collation = rs.getString("Collation");
                collationType.put(field, collation);
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                logger.warn("", e);
            }
        }

        return collationType;
    }

    private Map<String, String> fetchColumnExtraAndDefault(Connection conn, String actualTableName,
                                                           Map<String, String> defaultInfo) {
        Map<String, String> columnExtra = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("DESC `" + actualTableName + "`");
            while (rs.next()) {
                String field = rs.getString("Field");
                String extra = rs.getString("Extra");
                String defalutStr = rs.getString("Default");
                if (extra != null) {
                    columnExtra.put(field, extra);
                }
                if (defalutStr != null) {
                    defaultInfo.put(field, defalutStr);
                }
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                logger.warn("", e);
            }
        }

        return columnExtra;
    }

    private Map<String, String> fetchColumnType(Connection conn, String actualTableName) {
        Map<String, String> specialType = new TreeMap(String.CASE_INSENSITIVE_ORDER);
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("desc `" + actualTableName + "`");
            while (rs.next()) {
                String field = rs.getString("Field");
                String type = rs.getString("Type");

                if (TStringUtil.startsWithIgnoreCase(type, "enum(")) {
                    specialType.put(field, type);
                }
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                logger.warn("", e);
            }
        }

        return specialType;
    }

    private TableMeta fetchTableMeta(Connection conn, String actualTableName, String logicalTableName,
                                     Map<String, String> collationType, Map<String, String> specialType,
                                     Map<String, String> extraInfo, Map<String, String> defaultInfo) {
        Statement stmt = null;
        ResultSet rs = null;
        TableMeta meta = null;

        try {
            try {
                stmt = conn.createStatement();
                rs = stmt.executeQuery("select * from `" + actualTableName + "` where 1 = 2");
                ResultSetMetaData rsmd = rs.getMetaData();
                DatabaseMetaData dbmd = conn.isWrapperFor(XConnection.class) ? null : conn.getMetaData();
                meta = resultSetMetaToSchema(rsmd,
                    dbmd,
                    specialType,
                    collationType,
                    extraInfo,
                    defaultInfo,
                    logicalTableName,
                    actualTableName);
            } catch (Exception e) {
                if (e instanceof SQLException) {
                    if ("42000".equals(((SQLException) e).getSQLState())) {
                        try {
                            rs = stmt.executeQuery("select * from `" + actualTableName + "` where rownum<=2");
                            ResultSetMetaData rsmd = rs.getMetaData();
                            DatabaseMetaData dbmd = conn.isWrapperFor(XConnection.class) ? null : conn.getMetaData();
                            return resultSetMetaToSchema(rsmd,
                                dbmd,
                                specialType,
                                collationType,
                                extraInfo,
                                defaultInfo,
                                logicalTableName,
                                actualTableName);
                        } catch (SQLException e1) {
                            logger.warn(e);
                        }
                    }
                }
                logger.error("schema of " + logicalTableName + " cannot be fetched", e);
                Throwables.propagate(e);
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                    if (stmt != null) {
                        stmt.close();
                    }
                } catch (SQLException e) {
                    logger.warn("", e);
                }
            }
        } finally {
            //ignore
        }

        return meta;
    }

    private TableMeta fetchSchema(String logicalTableName, String actualTableName) {
        if (actualTableName == null) {
            throw new IllegalArgumentException("table " + logicalTableName
                + " cannot fetched without a actual tableName");
        }

        DataSource ds = getDatasourceGetter().getDataSource(getSchemaName(), this.getGroup().getName());
        if (ds == null) {
            logger.error("schema of " + logicalTableName + " cannot be fetched, datasource is null, group name is "
                + this.getGroup().getName());
            return null;
        }

        if (TStringUtil.startsWithIgnoreCase(logicalTableName, "information_schema.")) {
            logicalTableName = TStringUtil.substringAfter(logicalTableName, "information_schema.");
        } else if (TStringUtil.startsWithIgnoreCase(logicalTableName, "performance_schema.")) {
            logicalTableName = TStringUtil.substringAfter(logicalTableName, "performance_schema.");
        } else if (TStringUtil.startsWithIgnoreCase(logicalTableName, "mysql.")) {
            logicalTableName = TStringUtil.substringAfter(logicalTableName, "mysql.");
        }

        // 在mysql中，对含有特殊字符如`的表名进行查询时，需要使用转义方式，
        // 这里是对 目标物理表名的特殊字符进行转义操作
        // 如将'`'替换为'``'
        actualTableName = TStringUtil.escape(actualTableName, '`', '`');

        Connection conn = null;
        try {
            conn = ds.getConnection();
        } catch (Exception e) {
            propagateIfGetConnectionFailed(e);
            Throwables.propagate(e);
        }

        try {
            Map<String, String> collationTypes = fetchCollationType(conn, actualTableName);
            Map<String, String> specialTypes = fetchColumnType(conn, actualTableName);
            Map<String, String> defaultInfo = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            Map<String, String> extraInfo = fetchColumnExtraAndDefault(conn, actualTableName, defaultInfo);
            return fetchTableMeta(conn, actualTableName, logicalTableName, collationTypes, specialTypes, extraInfo,
                defaultInfo);
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.warn("", e);
                }
            }
        }
        return null;
    }

    @Override
    public TableMeta getTableMetaFromConnection(String tableName, Connection conn) {
        try {
            Map<String, String> collationTypes = fetchCollationType(conn, tableName);
            Map<String, String> specialTypes = fetchColumnType(conn, tableName);
            Map<String, String> defaultInfo = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            Map<String, String> extraInfo = fetchColumnExtraAndDefault(conn, tableName, defaultInfo);
            return fetchTableMeta(conn, tableName, tableName, collationTypes, specialTypes, extraInfo, defaultInfo);
        } catch (Exception e) {
            return null;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.warn("", e);
                }
            }
        }
    }

    public static TableMeta resultSetMetaToSchema(ResultSetMetaData rsmd, DatabaseMetaData dbmd,
                                                  Map<String, String> specialType, Map<String, String> collationType,
                                                  Map<String, String> extraInfo, Map<String, String> defaultInfo,
                                                  String logicalTableName, String actualTableName) {

        return resultSetMetaToTableMeta(rsmd,
            dbmd,
            specialType,
            collationType,
            extraInfo,
            defaultInfo,
            logicalTableName,
            actualTableName);
    }

    private static TableMeta resultSetMetaToTableMeta(ResultSetMetaData rsmd, DatabaseMetaData dbmd,
                                                      Map<String, String> specialType,
                                                      Map<String, String> collationType,
                                                      Map<String, String> extraInfo,
                                                      Map<String, String> defaultInfo,
                                                      String tableName, String actualTableName) {

        List<ColumnMeta> allColumnsOrderByDefined = new ArrayList<>();
        List<IndexMeta> secondaryIndexMetas = new ArrayList<>();
        Map<String, ColumnMeta> columnMetaMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        boolean hasPrimaryKey;
        List<String> primaryKeys = new ArrayList<>();

        try {
            com.mysql.jdbc.Field[] fields = null;
            if (rsmd instanceof ResultSetMetaDataProxy) {
                rsmd = rsmd.unwrap(com.mysql.jdbc.ResultSetMetaData.class);
            }
            if (rsmd instanceof com.mysql.jdbc.ResultSetMetaData) {
                fields = (com.mysql.jdbc.Field[]) MYSQL_RSMD_FIELDS.get(rsmd);
            }

            if (rsmd instanceof XResultSetMetaData) {
                // It's impossible to step into this code block.
                // X connection.

                // Column.
                for (int i = 0; i < rsmd.getColumnCount(); ++i) {
                    final PolarxResultset.ColumnMetaData metaData =
                        ((XResultSetMetaData) rsmd).getResult().getMetaData().get(i);
                    String extra = extraInfo.get(rsmd.getColumnName(i + 1));
                    String defaultStr = defaultInfo.get(rsmd.getColumnName(i + 1));
                    ColumnMeta columnMeta = TableMetaParser.buildColumnMeta(metaData,
                        XSession.toJavaEncoding(
                            ((XResultSetMetaData) rsmd).getResult().getSession()
                                .getResultMetaEncodingMySQL()),
                        extra, defaultStr);
                    allColumnsOrderByDefined.add(columnMeta);
                    columnMetaMap.put(columnMeta.getName(), columnMeta);
                }

                // PK and index.
                try {
                    if (TStringUtil.startsWithIgnoreCase(actualTableName, "information_schema.")) {
                        hasPrimaryKey = true;
                        primaryKeys.add(rsmd.getColumnName(1));
                    } else {
                        final XResult result = ((XResultSetMetaData) rsmd).getResult();
                        final XConnection connection = result.getConnection();

                        // Consume all request before send new one.
                        while (result.next() != null) {
                            ;
                        }

                        final XResult keyResult = connection.execQuery("SHOW KEYS FROM `" + actualTableName + '`');
                        final XResultSet pkrs = new XResultSet(keyResult);
                        TreeMap<Integer, String> treeMap = new TreeMap<>();
                        while (pkrs.next()) {
                            if (pkrs.getString("Key_name").equalsIgnoreCase("PRIMARY")) {
                                treeMap.put(pkrs.getInt("Seq_in_index"), pkrs.getString("Column_name"));
                            }
                        }

                        for (String v : treeMap.values()) {
                            primaryKeys.add(v);
                        }

                        if (primaryKeys.size() == 0) {
                            primaryKeys.add(rsmd.getColumnName(1));
                            hasPrimaryKey = false;
                        } else {
                            hasPrimaryKey = true;
                        }

                        final XResult indexResult = connection.execQuery("SHOW INDEX FROM `" + actualTableName + '`');
                        final XResultSet sirs = new XResultSet(indexResult);
                        Map<String, SecondaryIndexMeta> secondaryIndexMetaMap = new HashMap<>();
                        while (sirs.next()) {
                            String indexName = sirs.getString("Key_name");
                            if (indexName.equalsIgnoreCase("PRIMARY")) {
                                continue;
                            }
                            SecondaryIndexMeta meta;
                            if ((meta = secondaryIndexMetaMap.get(indexName)) == null) {
                                meta = new SecondaryIndexMeta();
                                meta.name = indexName;
                                meta.keys = new ArrayList<>();
                                meta.values = primaryKeys;
                                meta.unique = sirs.getInt("Non_unique") == 0;
                                secondaryIndexMetaMap.put(indexName, meta);
                            }
                            meta.keys.add(sirs.getString("Column_name"));
                        }
                        for (SecondaryIndexMeta meta : secondaryIndexMetaMap.values()) {
                            secondaryIndexMetas
                                .add(convertFromSecondaryIndexMeta(meta, columnMetaMap, tableName, true));
                        }

                    }
                } catch (Exception ex) {
                    propagateIfGetConnectionFailed(ex);
                    throw ex;
                }
            } else {
                // Legacy mysql driver.
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String columnName = rsmd.getColumnName(i);

                    String collation = collationType.get(columnName);
                    String extra = extraInfo.get(columnName);
                    String defaultStr = defaultInfo.get(rsmd.getColumnName(i));

                    long length = fields != null ? fields[i - 1].getLength() : Field.DEFAULT_COLUMN_SIZE;
                    int precision = rsmd.getPrecision(i);
                    int scale = rsmd.getScale(i);

                    String typeSpec;
                    if (specialType.containsKey(columnName)) {
                        typeSpec = specialType.get(columnName);
                    } else {
                        typeSpec = rsmd.getColumnTypeName(i);
                    }

                    // TODO: nullable not supported yet
                    boolean nullable = rsmd.isNullable(i) > 0;

                    // for datetime / timestamp / time
                    SqlTypeName typeName = SqlTypeName.getNameForJdbcType(rsmd.getColumnType(i));
                    if (typeName != null && SqlTypeName.DATETIME_TYPES.contains(typeName)) {
                        precision = TddlRelDataTypeSystemImpl.getInstance().getMaxPrecision(typeName);
                        // adapt to JDBC behavior.
                        if (typeName == SqlTypeName.TIME || typeName == SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE) {
                            scale = MySQLTimeTypeUtil.getScaleOfTime(length);
                        } else {
                            scale = MySQLTimeTypeUtil.getScaleOfDatetime(length);
                        }
                    }

                    RelDataType calciteDataType = DataTypeUtil.jdbcTypeToRelDataType(rsmd.getColumnType(i),
                        typeSpec, precision, scale, length, nullable);

                    if (SqlTypeUtil.isCharacter(calciteDataType)) {
                        // If the type is a kind of character and the collation is null, it must be in binary character set.
                        String jdbcCollation = Optional.ofNullable(collation).orElseGet(
                            () -> CollationName.BINARY.name()
                        );
                        String jdbcCharset = Optional.of(jdbcCollation)
                            .map(CollationName::getCharsetOf)
                            .map(Enum::name)
                            .orElse(
                                CharsetName.DEFAULT_CHARACTER_SET
                            );
                        // keep consistency of charset and collation.
                        if (jdbcCharset == CharsetName.DEFAULT_CHARACTER_SET) {
                            jdbcCollation = CharsetName.DEFAULT_COLLATION;
                        }
                        calciteDataType = DataTypeUtil
                            .getCharacterTypeWithCharsetAndCollation(calciteDataType, jdbcCharset, jdbcCollation);
                    }

                    boolean autoIncrement = rsmd.isAutoIncrement(i);
                    if (extra != null && !autoIncrement) {
                        autoIncrement = TStringUtil.equalsIgnoreCase(extra, "auto_increment");
                    }

                    Field field = new Field(tableName, columnName, collation, extra, defaultStr, calciteDataType,
                        autoIncrement, false);
                    ColumnMeta columnMeta = new ColumnMeta(tableName, columnName, null, field);

                    allColumnsOrderByDefined.add(columnMeta);
                    columnMetaMap.put(columnMeta.getName(), columnMeta);
                }

                try {
                    if (TStringUtil.startsWithIgnoreCase(actualTableName, "information_schema.")) {
                        hasPrimaryKey = true;
                        primaryKeys.add(rsmd.getColumnName(1));
                    } else {

                        ResultSet pkrs = dbmd.getPrimaryKeys(null, null, quoteForJdbc(actualTableName));
                        TreeMap<Integer, String> treeMap = new TreeMap<>();
                        while (pkrs.next()) {
                            treeMap.put(pkrs.getInt("KEY_SEQ"), pkrs.getString("COLUMN_NAME"));
                        }

                        for (String v : treeMap.values()) {
                            primaryKeys.add(v);
                        }

                        if (primaryKeys.size() == 0) {
                            primaryKeys.add(rsmd.getColumnName(1));
                            hasPrimaryKey = false;
                        } else {
                            hasPrimaryKey = true;
                        }

                        ResultSet sirs = dbmd.getIndexInfo(null, null, quoteForJdbc(actualTableName), false, true);
                        Map<String, SecondaryIndexMeta> secondaryIndexMetaMap = new HashMap<>();
                        while (sirs.next()) {
                            String indexName = sirs.getString("INDEX_NAME");
                            if (indexName.equalsIgnoreCase("PRIMARY")) {
                                continue;
                            }
                            SecondaryIndexMeta meta;
                            if ((meta = secondaryIndexMetaMap.get(indexName)) == null) {
                                meta = new SecondaryIndexMeta();
                                meta.name = indexName;
                                meta.keys = new ArrayList<>();
                                meta.values = primaryKeys;
                                meta.unique = !sirs.getBoolean("NON_UNIQUE");
                                secondaryIndexMetaMap.put(indexName, meta);
                            }
                            meta.keys.add(sirs.getString("COLUMN_NAME"));
                        }
                        for (SecondaryIndexMeta meta : secondaryIndexMetaMap.values()) {
                            secondaryIndexMetas
                                .add(convertFromSecondaryIndexMeta(meta, columnMetaMap, tableName, true));
                        }

                    }
                } catch (Exception ex) {
                    propagateIfGetConnectionFailed(ex);
                    throw ex;
                }
            }
        } catch (Exception ex) {
            logger.error("fetch schema error", ex);
            return null;
        }

        List<String> primaryValues = new ArrayList<String>(allColumnsOrderByDefined.size() - primaryKeys.size());
        for (ColumnMeta column : allColumnsOrderByDefined) {
            boolean c = false;
            for (String s : primaryKeys) {
                if (column.getName().equalsIgnoreCase(s)) {
                    c = true;
                    break;
                }
            }
            if (!c) {
                primaryValues.add(column.getName());
            }
        }
        IndexMeta primaryKeyMeta = buildPrimaryIndexMeta(tableName,
            columnMetaMap,
            true,
            primaryKeys,
            primaryValues);

        TableMeta tableMeta = new TableMeta(tableName,
            allColumnsOrderByDefined,
            primaryKeyMeta,
            secondaryIndexMetas,
            hasPrimaryKey, TableStatus.PUBLIC, 0);

        return tableMeta;

    }

    /**
     * Compatible with {@link com.mysql.jdbc.StringUtils#quoteIdentifier(java.lang.String, java.lang.String, boolean)}
     * called in {@link com.mysql.jdbc.DatabaseMetaData#getPrimaryKeys(java.lang.String, java.lang.String, java.lang.String)}
     */
    private static String quoteForJdbc(String actualTableName) {
        if (actualTableName.startsWith("`") && actualTableName.endsWith("`")) {
            return "`" + actualTableName + "`";
        } else {
            return StringUtils.unQuoteIdentifier("`" + actualTableName + "`", "`");
        }
    }

    private static void propagateIfGetConnectionFailed(Throwable t) {
        String message = null;
        List<Throwable> ths = ExceptionUtils.getThrowableList(t);
        for (int i = ths.size() - 1; i >= 0; i--) {
            Throwable e = ths.get(i);
            if (e instanceof GetConnectionTimeoutException) {
                if (e.getCause() != null) {
                    message = e.getCause().getMessage();
                } else {
                    message = e.getMessage();
                }
                throw new TddlRuntimeException(ErrorCode.ERR_ATOM_GET_CONNECTION_FAILED_UNKNOWN_REASON, e, message);
            }
        }
    }

    private static class SecondaryIndexMeta {
        String name;
        Boolean unique;
        List<String> keys;
        List<String> values;
    }

    private static IndexMeta convertFromSecondaryIndexMeta(SecondaryIndexMeta secondaryIndexMeta,
                                                           Map<String, ColumnMeta> columnMetas, String tableName,
                                                           boolean strongConsistent) {

        return new IndexMeta(tableName,
            toColumnMeta(secondaryIndexMeta.keys, columnMetas, tableName),
            toColumnMeta(secondaryIndexMeta.values, columnMetas, tableName),
            IndexType.BTREE,
            Relationship.NONE,
            strongConsistent,
            false,
            secondaryIndexMeta.unique,
            secondaryIndexMeta.name);
    }

    private static IndexMeta buildPrimaryIndexMeta(String tableName, Map<String, ColumnMeta> columnMetas,
                                                   boolean strongConsistent, List<String> primaryKeys,
                                                   List<String> primaryValues) {
        if (primaryKeys == null) {
            primaryKeys = new ArrayList<>();
        }
        if (primaryValues == null) {
            primaryValues = new ArrayList<>();
        }

        return new IndexMeta(tableName,
            toColumnMeta(primaryKeys, columnMetas, tableName),
            toColumnMeta(primaryValues, columnMetas, tableName),
            IndexType.BTREE,
            Relationship.NONE,
            strongConsistent,
            true,
            true,
            "PRIMARY");
    }

    private static List<ColumnMeta> toColumnMeta(List<String> columns, Map<String, ColumnMeta> columnMetas,
                                                 String tableName) {
        List<ColumnMeta> metas = Lists.newArrayList();
        for (String cname : columns) {
            if (!columnMetas.containsKey(cname)) {
                throw new RuntimeException("column " + cname + " is not a column of table " + tableName);
            }
            metas.add(columnMetas.get(cname));
        }
        return metas;
    }

    @Override
    public GsiMetaManager getGsiMetaManager() {
        DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
        return new GsiMetaManager(dataSource, getSchemaName());
    }
}
