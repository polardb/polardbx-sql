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

package com.alibaba.polardbx.executor.statistic.entity;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.statistic.ndv.HyperLogLogUtil.HLL_REGBYTES;
import static com.alibaba.polardbx.executor.statistic.ndv.HyperLogLogUtil.bitToInt;

/**
 * @author fangwu
 */
public class PolarDbXSystemTableNDVSketchStatistic implements SystemTableNDVSketchStatistic {

    private static final Logger logger = LoggerFactory.getLogger(PolarDbXSystemTableNDVSketchStatistic.class);

    public static final String TABLE_NAME = GmsSystemTables.NDV_SKETCH_STATISTICS;

    private static final String CREATE_TABLE_IF_NOT_EXIST_SQL = "create table if not exists `" + TABLE_NAME + "` (\n"
        + "  `schema_name` varchar(64) not null default '',\n"
        + "  `table_name` varchar(64) not null,\n"
        + "  `column_names` varchar(128) not null,\n"
        + "  `shard_part` varchar(255) not null,\n"
        + "  `dn_cardinality` bigint(20) not null,\n"
        + "  `composite_cardinality` bigint(20) not null,\n"
        + "  `sketch_bytes` varbinary(12288) not null,\n"
        + "  `sketch_type` varchar(16) not null,\n"
        + "  `compress_type` varchar(16) not null default 'NA',\n"
        + "  `gmt_created` timestamp default current_timestamp,\n"
        + "  `gmt_modified` timestamp default current_timestamp on update current_timestamp,\n"
        + "   PRIMARY KEY `logical_table_column` (`schema_name`, `table_name`, `column_names`, `shard_part`)\n"
        + ") engine=innodb default charset=utf8;";

    private static final String UPDATE_TABLE_NAME_SQL =
        "UPDATE `" + TABLE_NAME + "` SET TABLE_NAME = ? WHERE TABLE_NAME ="
            + " ? AND SCHEMA_NAME = ?";

    private static final String DELETED_BY_TABLENAME_COLUMNS_SQL =
        "DELETE FROM `" + TABLE_NAME + "` WHERE SCHEMA_NAME = ? AND "
            + "TABLE_NAME = ? AND COLUMN_NAMES =? AND SHARD_PART !='UNKNOWN'";

    private static final String DELETED_BY_TABLENAME_SQL = "DELETE FROM `" + TABLE_NAME + "` WHERE SCHEMA_NAME = ? AND "
        + "TABLE_NAME = ? ";

    private static final String DELETED_BY_SCHEMA_SQL = "DELETE FROM `" + TABLE_NAME + "` WHERE SCHEMA_NAME = ? ";

    private static final String MARK_TIMEOUT_SQL = "REPLACE INTO `" + TABLE_NAME +
        "` (`SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAMES`, `SHARD_PART`, `TIMEOUT_FLAG`, `DN_CARDINALITY`, `COMPOSITE_CARDINALITY`, `SKETCH_BYTES`, `SKETCH_TYPE`) "
        + "VALUES (?, ?, ?, 'UNKNOWN', 1, -1, -1, '', '')";

    private static final String CHECK_TIMEOUT_SQL =
        "SELECT timeout_flag FROM `" + TABLE_NAME + "` WHERE SCHEMA_NAME = ? AND "
            + "TABLE_NAME = ? AND COLUMN_NAMES =? AND TIMEOUT_FLAG=1";

    private static final String LOAD_ALL_SQL =
        "SELECT `SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAMES`, `SHARD_PART`, `INDEX_NAME`, `DN_CARDINALITY`, `COMPOSITE_CARDINALITY`, `SKETCH_TYPE`, `GMT_MODIFIED`, `GMT_CREATED` FROM "
            + TABLE_NAME + " WHERE TIMEOUT_FLAG != 1";

    private static final String LOAD_BY_TABLE_NAME_SQL =
        "SELECT `SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAMES`, `SHARD_PART`, `INDEX_NAME`, `DN_CARDINALITY`, `COMPOSITE_CARDINALITY`, `SKETCH_TYPE`, `GMT_MODIFIED`, `GMT_CREATED` FROM `"
            + TABLE_NAME + "` WHERE SCHEMA_NAME = ? AND TABLE_NAME = ? AND TIMEOUT_FLAG != 1";

    private static final String LOAD_BY_TABLE_NAME_AND_COLUMN_NAME_SQL =
        "SELECT `SHARD_PART`, `SKETCH_BYTES` FROM `" + TABLE_NAME
            + "` WHERE SCHEMA_NAME = ? AND TABLE_NAME = ? AND COLUMN_NAMES = ? AND TIMEOUT_FLAG != 1";

    /**
     * select table rows sql, need to concat with values
     */
    private static final String REPLACE_SQL = "REPLACE INTO `" + TABLE_NAME +
        "` (`SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAMES`, `SHARD_PART`, `INDEX_NAME`, `DN_CARDINALITY`, `COMPOSITE_CARDINALITY`, `SKETCH_BYTES`, `SKETCH_TYPE`) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    /**
     * update composite cardinality
     */
    private static final String UPDATE_SQL = "UPDATE `" + TABLE_NAME
        + "` SET `COMPOSITE_CARDINALITY` = ? WHERE  `SCHEMA_NAME` = ? AND `TABLE_NAME` = ? AND `COLUMN_NAMES` = ?";

    private static PolarDbXSystemTableNDVSketchStatistic polarDbXSystemTableNDVSketchStatistic =
        new PolarDbXSystemTableNDVSketchStatistic();

    public static PolarDbXSystemTableNDVSketchStatistic getInstance() {
        return polarDbXSystemTableNDVSketchStatistic;
    }

    @Override
    public void createTableIfNotExist() {
        if (!ConfigDataMode.isMasterMode()) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        PreparedStatement psAlter = null;
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement(CREATE_TABLE_IF_NOT_EXIST_SQL);
            ps.executeUpdate();
        } catch (Exception e) {
            logger.error("create " + TABLE_NAME + " if not exist error", e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(psAlter);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public void updateTableName(String schemaName, String oldLogicalTableName, String newLogicalTableName) {
        if (!ConfigDataMode.isMasterMode()) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement(UPDATE_TABLE_NAME_SQL);
            ps.setString(1, newLogicalTableName.toLowerCase());
            ps.setString(2, oldLogicalTableName.toLowerCase());
            ps.setString(3, schemaName.toLowerCase());
            ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("renameTable " + TABLE_NAME + " error, sql = " + UPDATE_TABLE_NAME_SQL, e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    public void deleteByTableNameAndColumnNames(String schemaName, String logicalTableName, String columnNames) {
        if (!ConfigDataMode.isMasterMode()) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement(DELETED_BY_TABLENAME_COLUMNS_SQL);
            ps.setString(1, schemaName.toLowerCase());
            ps.setString(2, logicalTableName.toLowerCase());
            ps.setString(3, columnNames.toLowerCase());
            ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("delete " + TABLE_NAME + " error, sql = " + DELETED_BY_TABLENAME_COLUMNS_SQL, e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public void deleteByTableName(String schemaName, String logicalTableName) {
        if (!ConfigDataMode.isMasterMode()) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement(DELETED_BY_TABLENAME_SQL);
            ps.setString(1, schemaName.toLowerCase());
            ps.setString(2, logicalTableName.toLowerCase());
            ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("delete " + TABLE_NAME + " error, sql = " + DELETED_BY_TABLENAME_SQL, e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public void deleteBySchemaName(String schemaName) {
        if (!ConfigDataMode.isMasterMode()) {
            return;
        }
        PreparedStatement ps = null;
        try (Connection conn = MetaDbDataSource.getInstance().getDataSource().getConnection()) {
            ps = conn.prepareStatement(DELETED_BY_SCHEMA_SQL);
            ps.setString(1, schemaName.toLowerCase());
            ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("delete " + TABLE_NAME + " error, sql = " + DELETED_BY_SCHEMA_SQL, e);
        } finally {
            JdbcUtils.close(ps);
        }
    }

    @Override
    public SketchRow[] loadAll() {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<SketchRow> rows = Lists.newLinkedList();
        try {
            conn = MetaDbUtil.getConnection();
            ps = conn.prepareStatement(LOAD_ALL_SQL);
            rs = ps.executeQuery();

            while (rs.next()) {
                try {
                    SketchRow row = new SketchRow(
                        rs.getString("SCHEMA_NAME"),
                        rs.getString("TABLE_NAME"),
                        rs.getString("COLUMN_NAMES"),
                        rs.getString("SHARD_PART"),
                        rs.getString("INDEX_NAME"),
                        rs.getLong("DN_CARDINALITY"),
                        rs.getLong("COMPOSITE_CARDINALITY"),
                        rs.getString("SKETCH_TYPE"),
                        rs.getTimestamp("GMT_CREATED").getTime(),
                        rs.getTimestamp("GMT_MODIFIED").getTime()
                    );
                    rows.add(row);
                } catch (Exception e) {
                    logger.error("parse row of " + TABLE_NAME + " error", e);
                }
            }
        } catch (Exception e) {
            logger.error("select " + TABLE_NAME + " error", e);
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
        return rows.toArray(new SketchRow[0]);
    }

    public Map<String, Set<String>> loadAllSchemaAndTableName() {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, Set<String>> schemaMap = Maps.newHashMap();
        try {
            conn = MetaDbUtil.getConnection();
            ps = conn.prepareStatement(LOAD_ALL_SQL);
            rs = ps.executeQuery();

            while (rs.next()) {
                String schema = rs.getString("SCHEMA_NAME").toLowerCase();
                String table = rs.getString("TABLE_NAME").toLowerCase();
                if (schemaMap.containsKey(schema)) {
                    schemaMap.get(schema).add(table);
                } else {
                    schemaMap.put(schema, Sets.newHashSet(table));
                }
            }
        } catch (Exception e) {
            logger.error("loadAllSchemaAndTableName error", e);
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
        return schemaMap;
    }

    @Override
    public SketchRow[] loadByTableName(String schemaName, String tableName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<SketchRow> rows = Lists.newLinkedList();
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement(LOAD_BY_TABLE_NAME_SQL);
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            rs = ps.executeQuery();

            while (rs.next()) {
                try {
                    SketchRow row = new SketchRow(
                        rs.getString("SCHEMA_NAME"),
                        rs.getString("TABLE_NAME"),
                        rs.getString("COLUMN_NAMES"),
                        rs.getString("SHARD_PART"),
                        rs.getString("INDEX_NAME"),
                        rs.getLong("DN_CARDINALITY"),
                        rs.getLong("COMPOSITE_CARDINALITY"),
                        rs.getString("SKETCH_TYPE"),
                        rs.getTimestamp("GMT_CREATED").getTime(),
                        rs.getTimestamp("GMT_MODIFIED").getTime()
                    );
                    rows.add(row);
                } catch (Exception e) {
                    logger.error("parse row of " + TABLE_NAME + " error", e);
                }
            }
        } catch (Exception e) {
            logger.error("select " + TABLE_NAME + " error", e);
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
        return rows.toArray(new SketchRow[0]);
    }

    @Override
    public void batchReplace(SketchRow[] sketchRows) throws SQLException {
        if (FailPoint.isKeyEnable(FailPointKey.FP_INJECT_IGNORE_PERSIST_NDV_STATISTIC)) {
            throw new SQLException("ignore persist ndv statistic");
        }

        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement(REPLACE_SQL);
            for (SketchRow sketchRow : sketchRows) {
                //`SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAMES`, `SHARD_PART`, `DN_CARDINALITY`, `SKETCH_BYTES`, `SKETCH_TYPE`
                ps.setString(1, sketchRow.getSchemaName());
                ps.setString(2, sketchRow.getTableName());
                ps.setString(3, sketchRow.getColumnNames());
                ps.setString(4, sketchRow.getShardPart());
                ps.setString(5, sketchRow.getIndexName());
                ps.setLong(6, sketchRow.getDnCardinality());
                ps.setLong(7, sketchRow.getCompositeCardinality());
                ps.setBytes(8, sketchRow.getSketchBytes());
                ps.setString(9, sketchRow.getSketchType());
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            logger.error("select " + TABLE_NAME + " error", e);
            throw e;
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public void updateCompositeCardinality(String schemaName, String tableName, String columnNames,
                                           long compositeCardinality) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement(UPDATE_SQL);
            //`SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAMES`, `SHARD_PART`, `DN_CARDINALITY`, `SKETCH_BYTES`, `SKETCH_TYPE`
            ps.setLong(1, compositeCardinality);
            ps.setString(2, schemaName);
            ps.setString(3, tableName);
            ps.setString(4, columnNames);

            ps.execute();
        } catch (SQLException e) {
            logger.error("select " + TABLE_NAME + " error", e);
            throw e;
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public boolean loadByTableNameAndColumnName(String schemaName, String tableName, String columnName,
                                                Map<String, byte[]> shardParts, int[] registers) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement(LOAD_BY_TABLE_NAME_AND_COLUMN_NAME_SQL);
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            ps.setString(3, columnName);
            rs = ps.executeQuery();

            while (rs.next()) {
                try {
                    byte[] oneRow = null;
                    String shardPart = rs.getString("SHARD_PART");
                    if (shardParts.containsKey(shardPart)) {
                        oneRow = shardParts.get(shardPart);
                    } else {
                        oneRow = rs.getBytes("SKETCH_BYTES");
                    }
                    if (oneRow == null || oneRow.length == 0) {
                        throw new IllegalArgumentException("sketch bytes not ready yet");
                    }
                    if (oneRow.length == 1) {
                        throw new IllegalArgumentException("sketch bytes from columnar");
                    }

                    BitSet bitSet = BitSet.valueOf(oneRow);
                    for (int j = 0; j * 6 < HLL_REGBYTES * 8; j++) {// cal the reciprocal
                        int v = bitToInt(bitSet, j * 6);
                        if (registers[j] < v) {
                            registers[j] = v;
                        }
                    }

                } catch (IllegalArgumentException e) {
                    throw e;
                } catch (Exception e) {
                    logger.error("parse row of " + TABLE_NAME + " error", e);
                }
            }
        } catch (Exception e) {
            logger.error("select " + TABLE_NAME + " error", e);
            return false;
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
        return true;
    }

    public boolean deleteByColumn(String schemaName, String tableName, String columns) {
        return deleteByColumn(schemaName, tableName, columns, MetaDbDataSource.getInstance().getDataSource());
    }

    /**
     * Deletes records based on the specified schema name, table name, and column names.
     *
     * @param schemaName The schema name to operate on
     * @param tableName The table name to operate on
     * @param columns The list of column names
     */
    public boolean deleteByColumn(String schemaName, String tableName, String columns, DataSource dataSource) {
        // Return early if not in master mode
        if (!ConfigDataMode.isMasterMode() || dataSource == null) {
            return false;
        }

        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(DELETED_BY_TABLENAME_COLUMNS_SQL);

            // Set parameter values (converted to lowercase)
            ps.setString(1, schemaName.toLowerCase());
            ps.setString(2, tableName.toLowerCase());
            ps.setString(3, columns.toLowerCase());

            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            // Log error message
            logger.error("Delete " + TABLE_NAME + " error, SQL statement: " + DELETED_BY_TABLENAME_COLUMNS_SQL
                + ", parameters: " + schemaName + "," + tableName + "," + columns, e);
            return false;
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    public boolean markTimeout(String schemaName, String tableName, String columns) {
        return markTimeout(schemaName, tableName, columns, MetaDbDataSource.getInstance().getDataSource());
    }

    public boolean markTimeout(String schemaName, String tableName, String columns, DataSource dataSource) {
        // Return early if not in master mode
        if (!ConfigDataMode.isMasterMode() || dataSource == null) {
            return false;
        }

        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(MARK_TIMEOUT_SQL);

            // Set parameter values (converted to lowercase)
            ps.setString(1, schemaName.toLowerCase());
            ps.setString(2, tableName.toLowerCase());
            ps.setString(3, columns.toLowerCase());

            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            // Log error message
            logger.error("REPLACE " + TABLE_NAME + " error, SQL statement: " + MARK_TIMEOUT_SQL
                + ", parameters: " + schemaName + "," + tableName + "," + columns, e);
            return false;
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    public boolean isTimeoutMarked(String schemaName, String tableName, String columns, DataSource dataSource) {
        if (dataSource == null) {
            return false;
        }

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(CHECK_TIMEOUT_SQL);

            // Set parameter values (converted to lowercase)
            ps.setString(1, schemaName.toLowerCase());
            ps.setString(2, tableName.toLowerCase());
            ps.setString(3, columns.toLowerCase());

            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt("timeout_flag") == 1;
            } else {
                return false;
            }
        } catch (SQLException e) {
            // Log error message
            logger.error("UPDATE " + TABLE_NAME + " error, SQL statement: " + MARK_TIMEOUT_SQL
                + ", parameters: " + schemaName + "," + tableName + "," + columns, e);
            return false;
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
            JdbcUtils.close(rs);
        }
    }
}

