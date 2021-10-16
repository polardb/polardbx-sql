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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.util.JdbcUtils;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

/**
 * @author dylan
 */
public class PolarDbXSystemTablePlanInfo implements SystemTablePlanInfo {
    private static final Logger logger = LoggerFactory.getLogger(PolarDbXSystemTablePlanInfo.class);

    public static final String TABLE_NAME = GmsSystemTables.PLAN_INFO;

    private static final String CREATE_TABLE_IF_NOT_EXIST_SQL = "create table if not exists `" + TABLE_NAME + "` (\n"
        + "  `id` bigint(20) not null,\n"
        + "  `schema_name` varchar(64) not null,\n"
        + "  `baseline_id` bigint(20) not null,\n"
        + "  `gmt_modified` timestamp default current_timestamp on update current_timestamp,\n"
        + "  `gmt_created` timestamp default current_timestamp,\n"
        + "  `last_execute_time` timestamp null default null,\n"
        + "  `plan` longtext not null,\n"
        + "  `plan_type` varchar(255) null,\n"
        + "  `plan_error` longtext null,\n"
        + "  `choose_count` bigint(20) not null,\n"
        + "  `cost` double not null,\n"
        + "  `estimate_execution_time` double not null,\n"
        + "  `accepted` tinyint(4) not null,\n"
        + "  `fixed` tinyint(4) not null,\n"
        + "  `trace_id` varchar(255) not null,\n"
        + "  `origin` varchar(255) default null,\n"
        + "  `estimate_optimize_time` double default null,\n"
        + "  `cpu` double default null,\n"
        + "  `memory` double default null,\n"
        + "  `io` double default null,\n"
        + "  `net` double default null,\n"
        + "  `tables_hashcode` bigint not null,\n"
        + "  `extend_field` longtext default null comment 'json string extend field',\n"
        + "  primary key `primary_key` (`schema_name`, `id`, `baseline_id`)\n,"
        + "  key `baseline_id_key` (`schema_name`, `baseline_id`)\n"
        + ") engine=innodb default charset=utf8;";

    private static final String ALTER_TABLE_ADD_TABLES_HASHCODE =
        "ALTER TABLE " + TABLE_NAME + " ADD COLUMN `TABLES_HASHCODE` BIGINT NOT NULL";

    public static final String REPLACE_SQL = "REPLACE INTO `" + TABLE_NAME + "` " +
        "(`SCHEMA_NAME`, `ID`, `BASELINE_ID`, `LAST_EXECUTE_TIME`, `PLAN`, `CHOOSE_COUNT`, `COST`, "
        + "`ESTIMATE_EXECUTION_TIME`, "
        + "`ACCEPTED`, `FIXED`, `TRACE_ID`, `ORIGIN`, `TABLES_HASHCODE`)" +
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public static final String DELETE_SQL = "DELETE FROM " + TABLE_NAME +
        " WHERE SCHEMA_NAME = ? AND BASELINE_ID = ? AND ID = ?";

    public static final String DELETE_ALL_SQL = "DELETE FROM " + TABLE_NAME +
        " WHERE SCHEMA_NAME = ?";

    private DataSource dataSource;

    private String schemaName;

    public boolean checkTableFromCache() {
        try {
            return APPNAME_PLAN_INFO_ENABLED.get(schemaName, this::checkTable);
        } catch (ExecutionException e) {
            logger.error("APPNAME_PLAN_INFO_ENABLED.get error", e);
            return false;
        }
    }

    public PolarDbXSystemTablePlanInfo(DataSource dataSource, String schemaName) {
        if (dataSource == null) {
            logger.error("PolarDbXSystemTablePlanInfo dataSource is null");
        }
        if (schemaName == null) {
            logger.error("PolarDbXSystemTablePlanInfo schemaName is null");
        }
        this.dataSource = dataSource;
        this.schemaName = schemaName;
    }

    @Override
    public void resetDataSource(DataSource dataSource) {
        if (dataSource == null) {
            logger.error("resetDataSource dataSource is null");
        }
        this.dataSource = dataSource;
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
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(CREATE_TABLE_IF_NOT_EXIST_SQL);
            ps.executeUpdate();
            psAlter = conn.prepareStatement(ALTER_TABLE_ADD_TABLES_HASHCODE);
            psAlter.executeUpdate();
        } catch (Exception e) {
            if (e instanceof SQLException && e.getMessage().contains("Duplicate column name")) {
                // ignore duplicate column error
            } else {
                logger.error("create " + TABLE_NAME + " if not exist error", e);
            }
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(psAlter);
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

    public boolean canRead() {
        return dataSource != null;
    }

    public boolean canWrite() {
        return ConfigDataMode.isMasterMode() && dataSource != null;
    }

    private boolean checkTable() {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
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
