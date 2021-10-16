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
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

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
            + "TABLE_NAME = ? AND COLUMN_NAMES =? ";

    private static final String DELETED_BY_TABLENAME_SQL = "DELETE FROM `" + TABLE_NAME + "` WHERE SCHEMA_NAME = ? AND "
        + "TABLE_NAME = ? ";

    private static final String LOAD_ALL_SQL =
        "SELECT `SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAMES`, `SHARD_PART`, `DN_CARDINALITY`, `COMPOSITE_CARDINALITY`, `SKETCH_BYTES`, `SKETCH_TYPE`, `GMT_MODIFIED`, `GMT_CREATED` FROM `"
            + TABLE_NAME + "` WHERE SCHEMA_NAME = ? ";

    private static final String LOAD_BY_TABLE_NAME_SQL =
        "SELECT `SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAMES`, `SHARD_PART`, `DN_CARDINALITY`, `COMPOSITE_CARDINALITY`, `SKETCH_BYTES`, `SKETCH_TYPE`, `GMT_MODIFIED`, `GMT_CREATED` FROM `"
            + TABLE_NAME + "` WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?";

    private static final String LOAD_BY_TABLE_NAME_AND_COLUMN_NAME_SQL =
        "SELECT `SHARD_PART`, `SKETCH_BYTES` FROM `" + TABLE_NAME
            + "` WHERE SCHEMA_NAME = ? AND TABLE_NAME = ? AND COLUMN_NAMES = ?";

    /**
     * select table rows sql, need to concat with values
     */
    private static final String REPLACE_SQL = "REPLACE INTO `" + TABLE_NAME +
        "` (`SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAMES`, `SHARD_PART`, `DN_CARDINALITY`, `COMPOSITE_CARDINALITY`, `SKETCH_BYTES`, `SKETCH_TYPE`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

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

    @Override
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
    public SketchRow[] loadAll(String schemaName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<SketchRow> rows = Lists.newLinkedList();
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement(LOAD_ALL_SQL);
            ps.setString(1, schemaName);
            rs = ps.executeQuery();

            while (rs.next()) {
                try {
                    SketchRow row = new SketchRow(
                        rs.getString("SCHEMA_NAME"),
                        rs.getString("TABLE_NAME"),
                        rs.getString("COLUMN_NAMES"),
                        rs.getString("SHARD_PART"),
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
    public void batchReplace(SketchRow[] sketchRows) {
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
                ps.setLong(5, sketchRow.getDnCardinality());
                ps.setLong(6, sketchRow.getCompositeCardinality());
                ps.setBytes(7, sketchRow.getSketchBytes());
                ps.setString(8, sketchRow.getSketchType());
                ps.addBatch();
            }

            try {
                ps.executeBatch();
            } catch (Exception e) {
                logger.error("parse row of " + TABLE_NAME + " error", e);
            }
        } catch (Exception e) {
            logger.error("select " + TABLE_NAME + " error", e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public void updateCompositeCardinality(String schemaName, String tableName, String columnNames,
                                           long compositeCardinality) {
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

            try {
                ps.execute();
            } catch (Exception e) {
                logger.error("parse row of " + TABLE_NAME + " error", e);
            }
        } catch (Exception e) {
            logger.error("select " + TABLE_NAME + " error", e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public Map<String, byte[]> loadByTableNameAndColumnName(String schemaName, String tableName, String columnName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, byte[]> rowMap = Maps.newHashMap();
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement(LOAD_BY_TABLE_NAME_AND_COLUMN_NAME_SQL);
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            ps.setString(3, columnName);
            rs = ps.executeQuery();

            while (rs.next()) {
                try {
                    byte[] oneRow = rs.getBytes("SKETCH_BYTES");
                    String shardPart = rs.getString("SHARD_PART");
                    rowMap.put(shardPart, oneRow);
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
        return rowMap;
    }
}

