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
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.TopN;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;

/**
 * @author dylan
 */
public class PolarDbXSystemTableColumnStatistic implements SystemTableColumnStatistic {

    private static final Logger logger = LoggerFactory.getLogger(PolarDbXSystemTableColumnStatistic.class);

    public static final String TABLE_NAME = GmsSystemTables.COLUMN_STATISTICS;

    private static final String CREATE_TABLE_IF_NOT_EXIST_SQL = "create table if not exists `" + TABLE_NAME + "` (\n"
        + "  `gmt_modified` timestamp default current_timestamp on update current_timestamp,\n"
        + "  `schema_name` varchar(64) not null default '',\n"
        + "  `table_name` varchar(64) not null,\n"
        + "  `column_name` varchar(64) not null,\n"
        + "  `cardinality` bigint(20) not null,\n"
        + "  `cmsketch` longtext not null,\n"
        + "  `histogram` longtext not null,\n"
        + "  `topn` longtext,\n"
        + "  `null_count` bigint(20) not null,\n"
        + "  `sample_rate` float not null,\n"
        + "  `extend_field` longtext default null comment 'json string extend field',\n"
        + "  primary key `logical_table_column` (`schema_name`, `table_name`, `column_name`)\n"
        + ") engine=innodb default charset=utf8mb4;";

    private static final String ALTER_TABLE_TOPN = "ALTER TABLE `" + TABLE_NAME + "` ADD COLUMN `TOPN` LONGTEXT";

    private static final String ALTER_TABLE_UTF8MB4 = "ALTER TABLE `" + TABLE_NAME + "` CHARACTER SET = UTF8MB4";

    private static final String SELECT_SQL =
        "SELECT SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, CARDINALITY, CMSKETCH, HISTOGRAM, TOPN, NULL_COUNT, SAMPLE_RATE, UNIX_TIMESTAMP(GMT_MODIFIED) AS UNIX_TIME FROM `"
            + TABLE_NAME + "` ";

    private static final String DELETE_TABLE_SQL = "DELETE FROM `" + TABLE_NAME + "` WHERE SCHEMA_NAME = '%s' AND "
        + "TABLE_NAME IN ";

    private static final String DELETE_ALL_SQL = "DELETE FROM `" + TABLE_NAME + "` WHERE SCHEMA_NAME = ?";

    private static final String DELETE_TABLE_COLUMN_SQL = "DELETE FROM `" + TABLE_NAME + "` WHERE SCHEMA_NAME = '%s' "
        + "AND ";

    private static final String RENAME_TABLE_SQL = "UPDATE `" + TABLE_NAME + "` SET TABLE_NAME = ? WHERE TABLE_NAME ="
        + " ? AND SCHEMA_NAME = ?";

    /**
     * select table rows sql, need to concat with values
     */
    private static final String REPLACE_SQL = "REPLACE INTO `" + TABLE_NAME + "` (`SCHEMA_NAME`, `TABLE_NAME`, "
        + "`COLUMN_NAME`, `CARDINALITY`, `CMSKETCH`, `HISTOGRAM`, `TOPN`, `NULL_COUNT`, `SAMPLE_RATE`) VALUES ";

    private static final int batchSize = 30;

    @Override
    public void createTableIfNotExist() {
        if (!canWrite()) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        PreparedStatement psAlterTopN = null;
        PreparedStatement psAlterCharSet = null;
        try {
            conn = MetaDbUtil.getConnection();
            ps = conn.prepareStatement(CREATE_TABLE_IF_NOT_EXIST_SQL);
            ps.executeUpdate();
            psAlterCharSet = conn.prepareStatement(ALTER_TABLE_UTF8MB4);
            psAlterCharSet.executeUpdate();
            psAlterTopN = conn.prepareStatement(ALTER_TABLE_TOPN);
            psAlterTopN.executeUpdate();
        } catch (Exception e) {
            if (!e.getMessage().contains("Duplicate column name")) {
                logger.error("create " + TABLE_NAME + " if not exist error", e);
            }
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(psAlterTopN);
            JdbcUtils.close(psAlterCharSet);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public void renameTable(String schema, String oldLogicalTableName, String newLogicalTableName) {
        if (!canWrite()) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        String sql = "";
        try {
            conn = MetaDbUtil.getConnection();
            ps = conn.prepareStatement(RENAME_TABLE_SQL);
            ps.setString(1, newLogicalTableName.toLowerCase());
            ps.setString(2, oldLogicalTableName.toLowerCase());
            ps.setString(3, schema.toLowerCase());
            ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("renameTable " + TABLE_NAME + " error, sql = " + sql, e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public void removeLogicalTableColumnList(String schema, String logicalTableName, List<String> columnNameList) {
        if (!canWrite()) {
            return;
        }
        if (logicalTableName == null) {
            return;
        }
        if (columnNameList == null || columnNameList.isEmpty()) {
            return;
        }
        Connection conn = null;
        Statement ps = null;
        String sql = "";
        try {
            conn = MetaDbUtil.getConnection();
            StringBuilder sqlBuilder =
                new StringBuilder(String.format(DELETE_TABLE_COLUMN_SQL, schema.toLowerCase().replace("'", "\\'")));
            sqlBuilder.append("TABLE_NAME = ");
            sqlBuilder.append("'");
            sqlBuilder.append(logicalTableName.toLowerCase().replace("'", "\\'"));
            sqlBuilder.append("' AND COLUMN_NAME IN ");
            boolean first = false;
            sqlBuilder.append("(");
            for (String columnName : columnNameList) {
                if (!first) {
                    first = true;
                } else {
                    sqlBuilder.append(",");
                }
                sqlBuilder.append("'");
                sqlBuilder.append(columnName.toLowerCase().replace("'", "\\'"));
                sqlBuilder.append("'");
            }
            sqlBuilder.append(")");
            ps = conn.createStatement();
            ps.executeUpdate(sql = sqlBuilder.toString());
        } catch (SQLException e) {
            logger.error("removeLogicalTableColumnList " + TABLE_NAME + " error, sql = " + sql, e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public void removeLogicalTableList(String schema, List<String> logicalTableNameList) {
        if (!canWrite()) {
            return;
        }
        if (logicalTableNameList == null) {
            return;
        }
        if (logicalTableNameList.isEmpty()) {
            return;
        }
        Connection conn = null;
        Statement ps = null;
        String sql = "";
        try {
            conn = MetaDbUtil.getConnection();
            StringBuilder sqlBuilder =
                new StringBuilder(String.format(DELETE_TABLE_SQL, schema.toLowerCase().replace("'", "\\'")));
            boolean first = false;
            sqlBuilder.append("(");
            for (String LogicalTableName : logicalTableNameList) {
                if (!first) {
                    first = true;
                } else {
                    sqlBuilder.append(",");
                }
                sqlBuilder.append("'");
                sqlBuilder.append(LogicalTableName.toLowerCase().replace("'", "\\'"));
                sqlBuilder.append("'");
            }
            sqlBuilder.append(")");
            ps = conn.createStatement();
            ps.executeUpdate(sql = sqlBuilder.toString());
        } catch (SQLException e) {
            logger.error("removeLogicalTableList " + TABLE_NAME + " error, sql = " + sql, e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public boolean deleteAll(String schema, Connection conn) {
        if (!canWrite()) {
            return false;
        }
        PreparedStatement ps = null;
        String sql = "";
        try {
            ps = conn.prepareStatement(DELETE_ALL_SQL);
            ps.setString(1, schema.toLowerCase());
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            logger.error("delete all " + TABLE_NAME + " error, sql = " + sql, e);
            throw new TddlNestableRuntimeException(e);
        } finally {
            JdbcUtils.close(ps);
        }
    }

    @Override
    public Collection<Row> selectAll(long sinceTime) {
        Collection<Row> rows = Lists.newLinkedList();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = MetaDbUtil.getConnection();
            ps = conn.prepareStatement(SELECT_SQL + " WHERE UNIX_TIMESTAMP"
                + "(GMT_MODIFIED)"
                + " >"
                + " " + sinceTime);
            rs = ps.executeQuery();
            while (rs.next()) {
                try {
                    SystemTableColumnStatistic.Row row = new SystemTableColumnStatistic.Row(
                        rs.getString("SCHEMA_NAME"),
                        rs.getString("TABLE_NAME"),
                        rs.getString("COLUMN_NAME"),
                        rs.getLong("CARDINALITY"),
                        Histogram.deserializeFromJson(rs.getString("HISTOGRAM")),
                        TopN.deserializeFromJson(rs.getString("TOPN")),
                        rs.getLong("NULL_COUNT"),
                        rs.getFloat("SAMPLE_RATE"),
                        rs.getLong("UNIX_TIME"));

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
        return rows;
    }

    @Override
    public void batchReplace(final List<SystemTableColumnStatistic.Row> rowList) {
        if (!innerBatchReplace(rowList)) {
            createTableIfNotExist();
            innerBatchReplace(rowList);
        }
    }

    /**
     * @param rowList the row list that need to replace
     * @return return false when COLUMN_STATISTICS doesn't exist, otherwise return true
     */
    private boolean innerBatchReplace(final List<SystemTableColumnStatistic.Row> rowList) {
        if (!canWrite()) {
            return false;
        }
        if (rowList == null || rowList.isEmpty()) {
            return true;
        }
        Connection conn = null;
        PreparedStatement pps = null;
        String sql = "";
        try {
            conn = MetaDbUtil.getConnection();
            int index = 0;
            while (index < rowList.size()) {
                StringBuilder sqlBuilder = new StringBuilder(REPLACE_SQL);
                boolean first = false;
                int batchCount = 0;
                for (; index < rowList.size() && batchCount < batchSize; index++, batchCount++) {
                    if (!first) {
                        first = true;
                    } else {
                        sqlBuilder.append(",");
                    }
                    sqlBuilder.append("(?,?,?,?,?,?,?,?,?)");
                }
                pps = conn.prepareStatement(sql = sqlBuilder.toString());
                for (int k = 0; k < batchCount; k++) {
                    SystemTableColumnStatistic.Row row = rowList.get(k + index - batchCount);
                    pps.setString(k * 9 + 1, row.getSchema().toLowerCase());
                    pps.setString(k * 9 + 2, row.getTableName().toLowerCase());
                    pps.setString(k * 9 + 3, row.getColumnName().toLowerCase());
                    pps.setLong(k * 9 + 4, row.getCardinality());
                    String cmSketchString =
                        Base64.encodeBase64String(CountMinSketch.serialize(new CountMinSketch(1, 1, 1)));
                    pps.setString(k * 9 + 5, cmSketchString);
                    String histogramString;
                    if (row.getHistogram() != null) {
                        histogramString = Histogram.serializeToJson(row.getHistogram());
                    } else {
                        histogramString = Histogram.serializeToJson(new Histogram(1, DataTypes.IntegerType, 1));
                    }
                    pps.setString(k * 9 + 6, histogramString);

                    String topN = null;
                    if (row.getTopN() != null) {
                        topN = TopN.serializeToJson(row.getTopN());
                    }
                    pps.setString(k * 9 + 7, topN);
                    pps.setLong(k * 9 + 8, row.getNullCount());
                    pps.setFloat(k * 9 + 9, row.getSampleRate());
                }
                pps.executeUpdate();
                pps.close();
            }
            logger.debug("batchReplace with " + ((rowList.size() - 1) / batchSize + 1) + " sql");
            return true;
        } catch (SQLException e) {
            if (e.getErrorCode() == 1146) {
                logger.error("batch replace " + TABLE_NAME + " error, we will try again", e);
                return false;
            } else {
                logger.error("batch replace " + TABLE_NAME + " error, sql = " + sql, e);
                return true;
            }
        } catch (Exception e) {
            logger.error("batch replace " + TABLE_NAME + " error, sql = " + sql, e);
            return true;
        } finally {
            JdbcUtils.close(pps);
            JdbcUtils.close(conn);
        }
    }

    private boolean canWrite() {
        return ConfigDataMode.isMasterMode();
    }
}
