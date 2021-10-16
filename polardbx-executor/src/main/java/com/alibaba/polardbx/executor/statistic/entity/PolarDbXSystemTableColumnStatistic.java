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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.util.JdbcUtils;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.TopN;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.apache.commons.codec.binary.Base64;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
        + ") engine=innodb default charset=utf8;";

    private static final String ALTER_TABLE_TOPN = "ALTER TABLE `" + TABLE_NAME + "` ADD COLUMN `TOPN` LONGTEXT";

    private static final String SELECT_SQL =
        "SELECT TABLE_NAME, COLUMN_NAME, CARDINALITY, CMSKETCH, HISTOGRAM, TOPN, NULL_COUNT, SAMPLE_RATE, UNIX_TIMESTAMP(GMT_MODIFIED) AS UNIX_TIME FROM `"
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

    private String schemaName;

    private boolean checkTableFromCache() {
        try {
            return SystemTableColumnStatistic.APPNAME_TABLE_COLUMN_ENABLED.get(schemaName, this::checkTable);
        } catch (ExecutionException e) {
            logger.error("APPNAME_TABLE_COLUMN_ENABLED.get error", e);
            return false;
        }
    }

    public PolarDbXSystemTableColumnStatistic(String appName) {
        if (appName == null) {
            logger.error("PolarDbXSystemTableColumnStatistic schemaName is null");
        }
        this.schemaName = appName;
    }

    @Override
    public void createTableIfNotExist() {
        if (!canWrite()) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        PreparedStatement psAlter = null;
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement(CREATE_TABLE_IF_NOT_EXIST_SQL);
            ps.executeUpdate();
            psAlter = conn.prepareStatement(ALTER_TABLE_TOPN);
            psAlter.executeUpdate();
        } catch (Exception e) {
            if (!e.getMessage().contains("Duplicate column name")) {
                logger.error("create " + TABLE_NAME + " if not exist error", e);
            }
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(psAlter);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public void renameTable(String oldLogicalTableName, String newLogicalTableName) {
        if (!canWrite()) {
            return;
        }
        if (!checkTableFromCache()) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        String sql = "";
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement(RENAME_TABLE_SQL);
            ps.setString(1, newLogicalTableName.toLowerCase());
            ps.setString(2, oldLogicalTableName.toLowerCase());
            ps.setString(3, schemaName.toLowerCase());
            ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("renameTable " + TABLE_NAME + " error, sql = " + sql, e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public void removeLogicalTableColumnList(String logicalTableName, List<String> columnNameList) {
        if (!canWrite()) {
            return;
        }
        if (!checkTableFromCache()) {
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
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            StringBuilder sqlBuilder =
                new StringBuilder(String.format(DELETE_TABLE_COLUMN_SQL, schemaName.toLowerCase().replace("'", "\\'")));
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
    public void removeLogicalTableList(List<String> logicalTableNameList) {
        if (!canWrite()) {
            return;
        }
        if (!checkTableFromCache()) {
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
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            StringBuilder sqlBuilder =
                new StringBuilder(String.format(DELETE_TABLE_SQL, schemaName.toLowerCase().replace("'", "\\'")));
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

    public static boolean deleteAll(String schemaName, Connection conn) {
        PreparedStatement ps = null;
        String sql = "";
        try {
            ps = conn.prepareStatement(DELETE_ALL_SQL);
            ps.setString(1, schemaName.toLowerCase());
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
    public boolean deleteAll(Connection conn) {
        if (!canWrite()) {
            return false;
        }
        if (!checkTableFromCache()) {
            return false;
        }
        return deleteAll(schemaName, conn);
    }

    @Override
    public void selectAll(StatisticManager statisticManager, long sinceTime) {
        if (!canRead()) {
            return;
        }
        if (!checkTableFromCache()) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps =
                conn.prepareStatement(SELECT_SQL + " WHERE SCHEMA_NAME = '" +
                    schemaName.toLowerCase().replace("'", "\\'") + "' AND UNIX_TIMESTAMP"
                    + "(GMT_MODIFIED)"
                    + " >"
                    + " " + sinceTime);
            rs = ps.executeQuery();
            while (rs.next()) {
                try {
                    SystemTableColumnStatistic.Row row = new SystemTableColumnStatistic.Row(
                        rs.getString("TABLE_NAME"),
                        rs.getString("COLUMN_NAME"),
                        rs.getLong("CARDINALITY"),
                        CountMinSketch.deserialize(Base64.decodeBase64(rs.getString("CMSKETCH"))),
                        Histogram.deserializeFromJson(rs.getString("HISTOGRAM")),
                        TopN.deserializeFromJson(rs.getString("TOPN")),
                        rs.getLong("NULL_COUNT"),
                        rs.getFloat("SAMPLE_RATE"),
                        rs.getLong("UNIX_TIME"));

                    StatisticManager.CacheLine cacheLine = statisticManager.getCacheLine(row.getTableName(), true);
                    cacheLine.setCardinality(row.getColumnName(), row.getCardinality());
                    cacheLine.setCountMinSketch(row.getColumnName(), row.getCountMinSketch());
                    cacheLine.setHistogram(row.getColumnName(), row.getHistogram());
                    cacheLine.setNullCount(row.getColumnName(), row.getNullCount());
                    cacheLine.setSampleRate(row.getSampleRate());
                    cacheLine.setTopN(row.getColumnName(), row.getTopN());
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
        if (!checkTableFromCache()) {
            return true;
        }
        if (rowList == null || rowList.isEmpty()) {
            return true;
        }
        Connection conn = null;
        PreparedStatement pps = null;
        String sql = "";
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            int index = 0;
            while (index < rowList.size()) {
                StringBuilder sqlBuilder = new StringBuilder(REPLACE_SQL);
                boolean first = false;
                int batchCount = 0;
                for (; index < rowList.size() && batchCount < batchSize; index++, batchCount++) {
                    SystemTableColumnStatistic.Row row = rowList.get(index);
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
                    pps.setString(k * 9 + 1, schemaName.toLowerCase());
                    pps.setString(k * 9 + 2, row.getTableName().toLowerCase());
                    pps.setString(k * 9 + 3, row.getColumnName().toLowerCase());
                    pps.setLong(k * 9 + 4, row.getCardinality());
                    String cmSketchString;
                    if (row.getCountMinSketch() != null) {
                        cmSketchString = Base64.encodeBase64String(CountMinSketch.serialize(row.getCountMinSketch()));
                    } else {
                        cmSketchString =
                            Base64.encodeBase64String(CountMinSketch.serialize(new CountMinSketch(1, 1, 1)));
                    }
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

    private boolean canRead() {
        return MetaDbDataSource.getInstance().getDataSource() != null;
    }

    private boolean canWrite() {
        return ConfigDataMode.isMasterMode()
            && MetaDbDataSource.getInstance().getDataSource() != null;
    }

    private boolean checkTable() {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement("show tables like '" + TABLE_NAME + "'");
            ps.executeQuery();
            rs = ps.executeQuery();
            if (rs.next()) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            logger.error("check " + TABLE_NAME + " exist error", e);
            return false;
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
            JdbcUtils.close(rs);
        }
    }

}
