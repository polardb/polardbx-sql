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

package com.alibaba.polardbx.optimizer.view;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author dylan
 */
public class PolarDbXSystemTableView implements SystemTableView {

    private static final Logger logger = LoggerFactory.getLogger(PolarDbXSystemTableView.class);

    public static final String TABLE_NAME = GmsSystemTables.VIEWS;

    private static final String CREATE_TABLE_IF_NOT_EXIST_SQL = "create table if not exists `" + TABLE_NAME + "` (\n"
        + "  `catalog_name` varchar(512) null,\n"
        + "  `schema_name` varchar(64) not null,\n"
        + "  `view_name` varchar(64) not null,\n"
        + "  `column_list` mediumtext null,\n"
        + "  `view_definition` longtext not null,\n"
        + "  `plan` longtext null,\n"
        + "  `plan_type` varchar(255) null,\n"
        + "  `plan_error` longtext null,\n"
        + "  `materialization`  varchar(255) null,\n"
        + "  `check_option` varchar(8) null,\n"
        + "  `is_updatable` varchar(3) null,\n"
        + "  `definer` varchar(93) null,\n"
        + "  `security_type` varchar(7) null,\n"
        + "  `character_set_client` varchar(32) null,\n"
        + "  `collation_connection` varchar(32) null,\n"
        + "  `gmt_modified` timestamp default current_timestamp on update current_timestamp,\n"
        + "  `gmt_created` timestamp default current_timestamp,\n"
        + "  `extend_field` longtext default null comment 'json string extend field',\n"
        + "  primary key `id_key` (`schema_name`, `view_name`)\n"
        + ") engine=innodb default charset=utf8;";

    private static final String SELECT_SQL =
        "SELECT `SCHEMA_NAME`, `VIEW_NAME`, `COLUMN_LIST`, `VIEW_DEFINITION`, `PLAN`, `PLAN_TYPE`, `PLAN_ERROR`"
            + " FROM `" + TABLE_NAME + "` "
            + " WHERE `SCHEMA_NAME` = ? AND `VIEW_NAME` = ?";

    private static final String INSERT_SQL = "INSERT INTO `" + TABLE_NAME
        + "` (`SCHEMA_NAME`, `VIEW_NAME`, `COLUMN_LIST`, `VIEW_DEFINITION`, `DEFINER`, `PLAN`, `PLAN_TYPE`) VALUES (?, ?, ?, ?, ?, ?, ?) ";

    private static final String REPLACE_SQL = "REPLACE INTO `" + TABLE_NAME
        + "` (`SCHEMA_NAME`, `VIEW_NAME`, `COLUMN_LIST`, `VIEW_DEFINITION`, `DEFINER`, `PLAN`, `PLAN_TYPE`) VALUES (?, ?, ?, ?, ?, ?, ?) ";

    private static final String DELETE_SQL =
        "DELETE FROM `" + TABLE_NAME + "` WHERE `SCHEMA_NAME` = ? AND `VIEW_NAME` = ?";

    private static final String DELETE_ALL_SQL =
        "DELETE FROM `" + TABLE_NAME + "` WHERE `SCHEMA_NAME` = ?";

    private static final String RECORD_PLAN_ERROR_SQL =
        "UPDATE `" + TABLE_NAME + "` SET `PLAN_ERROR` = ? WHERE `SCHEMA_NAME` = ? AND `VIEW_NAME` = ?";

    private static final String COUNT_SQL = "SELECT COUNT(*) FROM `" + TABLE_NAME + "` WHERE `SCHEMA_NAME` = ?";

    private DataSource dataSource;

    private String schemaName;

    private boolean checkTableFromCache() {
        try {
            return APPNAME_VIEW_ENABLED.get(schemaName, this::checkTable);
        } catch (ExecutionException e) {
            logger.error("APPNAME_VIEW_ENABLED.get error", e);
            return false;
        }
    }

    public static void invalidateCache() {
        logger.debug("invalidateCache");
        APPNAME_VIEW_ENABLED.invalidateAll();
    }

    public PolarDbXSystemTableView(DataSource dataSource, String schemaName) {
        if (dataSource == null) {
            logger.error("PolarDbXSystemTableView dataSource is null");
        }
        if (schemaName == null) {
            logger.error("PolarDbXSystemTableView schemaName is null");
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
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(CREATE_TABLE_IF_NOT_EXIST_SQL);
            ps.executeUpdate();
        } catch (Exception e) {
            logger.error("create " + TABLE_NAME + " if not exist error", e);
        } finally {
            GeneralUtil.close(ps);
            GeneralUtil.close(conn);
        }
    }

    @Override
    public SystemTableView.Row select(String viewName) {
        if (!canRead()) {
            return null;
        }
        if (!checkTableFromCache()) {
            return null;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(SELECT_SQL);
            ps.setString(1, schemaName);
            ps.setString(2, viewName);
            rs = ps.executeQuery();
            if (rs.next()) {
                String columnListString = rs.getString("COLUMN_LIST");
                List<String> columnList = null;
                if (columnListString != null) {
                    columnList = JSON.parseArray(columnListString, String.class);
                }
                String planError = rs.getString("PLAN_ERROR");
                SystemTableView.Row row = new SystemTableView.Row(
                    rs.getString("SCHEMA_NAME"),
                    rs.getString("VIEW_NAME"), columnList,
                    rs.getString("VIEW_DEFINITION"),
                    planError == null ? rs.getString("PLAN") : null,
                    planError == null ? rs.getString("PLAN_TYPE") : null);
                return row;
            } else {
                return null;
            }
        } catch (Exception e) {
            logger.error("select " + TABLE_NAME + " error with viewName = " + viewName, e);
            throw new TddlNestableRuntimeException(e);
        } finally {
            GeneralUtil.close(rs);
            GeneralUtil.close(ps);
            GeneralUtil.close(conn);
        }
    }

    @Override
    public boolean insert(String viewName, List<String> columnList, String viewDefinition, String definer,
                          String planString, String planType) {
        return insertOrReplace(viewName, columnList, viewDefinition, definer, planString, planType, INSERT_SQL);
    }

    @Override
    public boolean replace(String viewName, List<String> columnList, String viewDefinition, String definer,
                           String planString, String planType) {
        return insertOrReplace(viewName, columnList, viewDefinition, definer, planString, planType, REPLACE_SQL);
    }

    public boolean insertOrReplace(String viewName, List<String> columnList, String viewDefinition, String definer,
                                   String planString, String planType, String sql) {
        if (!canWrite()) {
            return false;
        }
        if (!checkTableFromCache()) {
            return false;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setString(1, schemaName);
            ps.setString(2, viewName);
            ps.setString(3, columnList == null ? null : JSON.toJSONString(columnList));
            ps.setString(4, viewDefinition);
            ps.setString(5, definer);
            ps.setString(6, planString);
            ps.setString(7, planType);
            ps.executeUpdate();
            return true;
        } catch (Exception e) {
            logger.error(
                "insert " + TABLE_NAME + " error with viewName = " + viewName + ", viewDefinition = " + viewDefinition,
                e);
            throw new TddlNestableRuntimeException(e);
        } finally {
            GeneralUtil.close(rs);
            GeneralUtil.close(ps);
            GeneralUtil.close(conn);
        }
    }

    @Override
    public boolean delete(String viewName) {
        if (!canWrite()) {
            return false;
        }
        if (!checkTableFromCache()) {
            return false;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(DELETE_SQL);
            ps.setString(1, schemaName);
            ps.setString(2, viewName);
            ps.executeUpdate();
            return true;
        } catch (Exception e) {
            logger.error("delete " + TABLE_NAME + " error with viewName = " + viewName, e);
            throw new TddlNestableRuntimeException(e);
        } finally {
            GeneralUtil.close(rs);
            GeneralUtil.close(ps);
            GeneralUtil.close(conn);
        }
    }

    public static boolean deleteAll(String schemaName, Connection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(DELETE_ALL_SQL);
            ps.setString(1, schemaName);
            ps.executeUpdate();
            return true;
        } catch (Exception e) {
            logger.error("delete all" + TABLE_NAME + " error", e);
            throw new TddlNestableRuntimeException(e);
        } finally {
            GeneralUtil.close(rs);
            GeneralUtil.close(ps);
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
    public boolean recordPlanError(String schemaName, String viewName, String planError) {
        if (!canWrite()) {
            return false;
        }
        if (!checkTableFromCache()) {
            return false;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(RECORD_PLAN_ERROR_SQL);
            ps.setString(1, planError);
            ps.setString(2, schemaName);
            ps.setString(3, viewName);
            ps.executeUpdate();
            return true;
        } catch (Exception e) {
            logger.error("record plan error " + TABLE_NAME + " error with viewName = " + viewName, e);
            throw new TddlNestableRuntimeException(e);
        } finally {
            GeneralUtil.close(rs);
            GeneralUtil.close(ps);
            GeneralUtil.close(conn);
        }
    }

    @Override
    public int count(String schemaName) {
        if (!canRead()) {
            return -1;
        }
        if (!checkTableFromCache()) {
            return -1;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(COUNT_SQL);
            ps.setString(1, schemaName);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                return -1;
            }
        } catch (Exception e) {
            logger.error("count " + TABLE_NAME + " error ", e);
            throw new TddlNestableRuntimeException(e);
        } finally {
            GeneralUtil.close(rs);
            GeneralUtil.close(ps);
            GeneralUtil.close(conn);
        }
    }

    private boolean canRead() {
        return dataSource != null;
    }

    private boolean canWrite() {
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
            GeneralUtil.close(ps);
            GeneralUtil.close(conn);
            GeneralUtil.close(rs);
        }
    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

}

