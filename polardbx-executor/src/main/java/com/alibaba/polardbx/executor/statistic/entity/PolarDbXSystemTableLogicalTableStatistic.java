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
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author dylan
 */
public class PolarDbXSystemTableLogicalTableStatistic implements SystemTableTableStatistic {

    private static final Logger logger = LoggerFactory.getLogger(PolarDbXSystemTableLogicalTableStatistic.class);

    private static final String TABLE_NAME = GmsSystemTables.TABLE_STATISTICS;

    private static final String CREATE_TABLE_IF_NOT_EXIST_SQL = "create table if not exists `" + TABLE_NAME + "` (\n"
        + "  `gmt_modified` timestamp default current_timestamp on update current_timestamp,\n"
        + "  `schema_name` varchar(64) NOT NULL DEFAULT '',\n"
        + "  `table_name` varchar(64) not null,\n"
        + "  `row_count` bigint(20) not null,\n"
        + "  `extend_field` longtext default null comment 'json string extend field',\n"
        + "  primary key `logical_table_name` (`schema_name`,`table_name`)\n"
        + ") engine=innodb default charset=utf8;";

    private static final String SELECT_SQL = "SELECT TABLE_NAME, ROW_COUNT, UNIX_TIMESTAMP(GMT_MODIFIED) AS UNIX_TIME"
        + " FROM `" + TABLE_NAME + "` ";

    private static final String DELETE_TABLE_SQL = "DELETE FROM `" + TABLE_NAME + "` WHERE SCHEMA_NAME = '%s' AND "
        + "TABLE_NAME IN ";

    private static final String RENAME_TABLE_SQL = "UPDATE `" + TABLE_NAME + "` SET TABLE_NAME = ? WHERE TABLE_NAME ="
        + " ? AND SCHEMA_NAME = ?";

    private static final String DELETE_ALL_SQL = "DELETE FROM `" + TABLE_NAME + "` WHERE SCHEMA_NAME = ?";

    /**
     * select table rows sql, need to concat with values
     */
    private static final String REPLACE_SQL = "REPLACE INTO `" + TABLE_NAME + "` (`SCHEMA_NAME`, `TABLE_NAME`, "
        + "`ROW_COUNT`) VALUES ";

    private static final int batchSize = 200;

    private String schemaName;

    private boolean checkTableFromCache() {
        try {
            return SystemTableTableStatistic.APPNAME_TABLE_STATISTIC_ENABLED.get(schemaName, this::checkTable);
        } catch (ExecutionException e) {
            logger.error("APPNAME_TABLE_STATISTIC_ENABLED.get error", e);
            return false;
        }
    }

    public PolarDbXSystemTableLogicalTableStatistic(String schemaName) {
        if (schemaName == null) {
            logger.error("PolarDbXSystemTableLogicalTableStatistic schemaName is null");
        }

        this.schemaName = schemaName;
    }

    @Override
    public void createTableIfNotExist() {
        if (!canWrite()) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement(CREATE_TABLE_IF_NOT_EXIST_SQL);
            ps.executeUpdate();
        } catch (Exception e) {
            logger.error("create " + TABLE_NAME + " if not exist error", e);
        } finally {
            JdbcUtils.close(ps);
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
    public void removeLogicalTableList(List<String> logicalTableNameList) {
        if (!canWrite()) {
            return;
        }
        if (!checkTableFromCache()) {
            return;
        }
        Connection conn = null;
        Statement ps = null;
        String sql = "";
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            StringBuilder sqlBuilder = new StringBuilder(String.format(DELETE_TABLE_SQL,
                schemaName.toLowerCase().replace("'", "\\'")));
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
    public Collection<Row> selectAll(long sinceTime) {
        ArrayList<Row> result = new ArrayList<>();

        if (!canRead()) {
            return result;
        }
        if (!checkTableFromCache()) {
            return result;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        logger.debug("[debug] selectAll");
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
                SystemTableTableStatistic.Row
                    row = new SystemTableTableStatistic.Row(rs.getString("TABLE_NAME"), rs.getLong("ROW_COUNT"),
                    rs.getLong("UNIX_TIME"));

                result.add(row);
            }
        } catch (Exception e) {
            logger.error("select " + TABLE_NAME + " error", e);
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
        return result;
    }

    @Override
    public void batchReplace(final List<SystemTableTableStatistic.Row> rowList) {
        if (!innerBatchReplace(rowList)) {
            createTableIfNotExist();
            innerBatchReplace(rowList);
        }
    }

    /**
     * @param rowList the row list that need to replace
     * @return return false when table_statistics doesn't exist, otherwise return true
     */
    private boolean innerBatchReplace(final List<SystemTableTableStatistic.Row> rowList) {
        if (!canWrite()) {
            return true;
        }
        if (!checkTableFromCache()) {
            return true;
        }
        if (rowList == null || rowList.isEmpty()) {
            return true;
        }
        Connection conn = null;
        Statement ps = null;
        String sql = "";
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            int index = 0;
            while (index < rowList.size()) {
                StringBuilder sqlBuilder = new StringBuilder(REPLACE_SQL);
                boolean first = false;
                int batchCount = 0;
                for (; index < rowList.size(); index++) {
                    SystemTableTableStatistic.Row row = rowList.get(index);
                    if (!first) {
                        first = true;
                    } else {
                        sqlBuilder.append(",");
                    }
                    sqlBuilder.append("('");
                    sqlBuilder.append(schemaName.toLowerCase().replace("'", "\\'"));
                    sqlBuilder.append("','");
                    sqlBuilder.append(row.getTableName().toLowerCase().replace("'", "\\'"));
                    sqlBuilder.append("',");
                    sqlBuilder.append(row.getRowCount());
                    sqlBuilder.append(")");
                    batchCount++;
                    if (batchCount >= batchSize) {
                        break;
                    }
                }
                ps = conn.createStatement();
                ps.executeUpdate(sql = sqlBuilder.toString());
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
            JdbcUtils.close(ps);
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
                logger.debug("[debug] check table = true");
                return true;
            } else {
                logger.debug("[debug] check table = false");
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

