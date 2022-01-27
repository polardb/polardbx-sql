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
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.Pair;
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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author dylan
 */
public class PolarDbXSystemTableBaselineInfo implements SystemTableBaselineInfo {
    private static final Logger logger = LoggerFactory.getLogger(PolarDbXSystemTableBaselineInfo.class);

    public static final String TABLE_NAME = GmsSystemTables.BASELINE_INFO;

    private static final String CREATE_TABLE_IF_NOT_EXIST_SQL = "create table if not exists `" + TABLE_NAME + "` (\n"
        + "  `id` bigint not null,\n"
        + "  `schema_name` varchar(64) not null default '',\n"
        + "  `gmt_modified` timestamp default current_timestamp on update current_timestamp,\n"
        + "  `gmt_created` timestamp default current_timestamp,\n"
        + "  `sql` mediumtext not null,\n"
        + "  `table_set` text not null,\n"
        + "  `extend_field` longtext default null comment 'json string extend field',\n"
        + "  primary key `id_key` (`schema_name`, `id`)\n"
        + ") engine=innodb default charset=utf8;";

    private static final String DROP_TABLES_HASHCODE_COLUMN =
        "ALTER TABLE " + TABLE_NAME + " DROP COLUMN `tables_hashcode`";

    private static final String LOAD_DATA_SQL =
        "SELECT BASELINE_INFO.ID, BASELINE_INFO.SQL, BASELINE_INFO.TABLE_SET, BASELINE_INFO.EXTEND_FIELD, PLAN_INFO.TABLES_HASHCODE, "
            +
            "PLAN_INFO.ID, PLAN_INFO.PLAN, UNIX_TIMESTAMP(PLAN_INFO.LAST_EXECUTE_TIME), PLAN_INFO.CHOOSE_COUNT, PLAN_INFO.COST, PLAN_INFO.ESTIMATE_EXECUTION_TIME, "
            +
            "PLAN_INFO.ACCEPTED, PLAN_INFO.FIXED, PLAN_INFO.TRACE_ID, PLAN_INFO.ORIGIN, PLAN_INFO.EXTEND_FIELD AS PLAN_EXTEND, UNIX_TIMESTAMP(PLAN_INFO"
            + ".GMT_MODIFIED), UNIX_TIMESTAMP(PLAN_INFO.GMT_CREATED) FROM "
            +
            "`" + TABLE_NAME + "` AS BASELINE_INFO INNER JOIN `" + PolarDbXSystemTablePlanInfo.TABLE_NAME
            + "` AS PLAN_INFO " +
            "ON BASELINE_INFO.SCHEMA_NAME = PLAN_INFO.SCHEMA_NAME AND BASELINE_INFO.ID = PLAN_INFO.BASELINE_ID " +
            "WHERE BASELINE_INFO.SCHEMA_NAME = ? AND UNIX_TIMESTAMP(BASELINE_INFO"
            + ".GMT_MODIFIED) > ? AND "
            + "UNIX_TIMESTAMP"
            + "(PLAN_INFO"
            + ".GMT_MODIFIED) > ?";

    private static final String LOAD_DATA_SQL_WITH_ID = LOAD_DATA_SQL + " AND BASELINE_INFO.ID = ?";

    private static final String SELECT_SQL =
        "SELECT `ID`, `SQL`, `TABLE_SET` FROM `" + TABLE_NAME + "` WHERE SCHEMA_NAME = ? AND ID ="
            + " ? "
            + "FOR UPDATE ";

    private static final String INSERT_SQL = "INSERT INTO `" + TABLE_NAME + "` (`SCHEMA_NAME`, `ID`, `SQL`, "
        + "`TABLE_SET`, `EXTEND_FIELD`) VALUES (?, ?, ?, ?, ?)";

    private static final String UPDATE_SQL =
        "UPDATE `" + TABLE_NAME + "` SET `EXTEND_FIELD` = ? WHERE `SCHEMA_NAME`= ? AND `ID` = ?";

    private static final String DELETE_SQL = "DELETE BASELINE_INFO, PLAN_INFO FROM " +
        "`" + TABLE_NAME + "` AS BASELINE_INFO LEFT JOIN `" + PolarDbXSystemTablePlanInfo.TABLE_NAME + "` AS PLAN_INFO "
        +
        "ON BASELINE_INFO.SCHEMA_NAME = PLAN_INFO.SCHEMA_NAME AND BASELINE_INFO.ID = PLAN_INFO.BASELINE_ID " +
        "WHERE BASELINE_INFO.SCHEMA_NAME  = ? AND BASELINE_INFO.ID = ?";

    public static final String DELETE_ALL_SQL = "DELETE FROM " + TABLE_NAME +
        " WHERE SCHEMA_NAME = ?";

    private DataSource dataSource;

    private String schemaName;

    private PolarDbXSystemTablePlanInfo polarDbXSystemTablePlanInfo;

    private boolean checkTableFromCache() {
        try {
            return APPNAME_BASELINE_INFO_ENABLED.get(schemaName, this::checkTable);
        } catch (ExecutionException e) {
            logger.error("APPNAME_BASELINE_INFO_ENABLED.get error", e);
            return false;
        }
    }

    public PolarDbXSystemTableBaselineInfo(DataSource dataSource, String schemaName,
                                           PolarDbXSystemTablePlanInfo polarDbXSystemTablePlanInfo) {
        if (dataSource == null) {
            logger.error("PolarDbXSystemTableBaselineInfo dataSource is null");
        }
        if (schemaName == null) {
            logger.error("PolarDbXSystemTableBaselineInfo schemaName is null");
        }
        this.dataSource = dataSource;
        this.schemaName = schemaName;
        this.polarDbXSystemTablePlanInfo = polarDbXSystemTablePlanInfo;
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
        PreparedStatement psDropColumn = null;
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(CREATE_TABLE_IF_NOT_EXIST_SQL);
            ps.executeUpdate();
            psDropColumn = conn.prepareStatement(DROP_TABLES_HASHCODE_COLUMN);
            psDropColumn.executeUpdate();
        } catch (Exception e) {
            if (e instanceof SQLException && e.getMessage().contains("Can't DROP ")) {
                // ignore drop column error
            } else {
                logger.error("create " + TABLE_NAME + " if not exist error", e);
            }
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(psDropColumn);
            JdbcUtils.close(conn);
        }
    }

    /**
     * @param sinceTime unix time
     */
    @Override
    public void loadData(PlanManager planManager, long sinceTime) {
        loadData(planManager, sinceTime, null);
    }

    @Override
    public void loadData(PlanManager planManager, long sinceTime, Integer searchBaselineId) {
        if (!canRead()) {
            return;
        }
        if (!checkTableFromCache()) {
            return;
        }
        if (!polarDbXSystemTablePlanInfo.canRead()) {
            return;
        }
        if (!polarDbXSystemTablePlanInfo.checkTableFromCache()) {
            return;
        }
        Set<Integer> invalidBaselineInfoIdSet = new HashSet<>();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            final int maxBaselineSize = planManager.getParamManager().getInt(ConnectionParams.SPM_MAX_BASELINE_SIZE);
            final int maxBaselineInfoSqlLength =
                planManager.getParamManager().getInt(ConnectionParams.SPM_MAX_BASELINE_INFO_SQL_LENGTH);
            final int maxPlanInfoSqlLength =
                planManager.getParamManager().getInt(ConnectionParams.SPM_MAX_PLAN_INFO_PLAN_LENGTH);
            conn = dataSource.getConnection();
            if (searchBaselineId != null) {
                ps = conn.prepareStatement(LOAD_DATA_SQL_WITH_ID);
                ps.setString(1, schemaName.toLowerCase());
                ps.setLong(2, sinceTime);
                ps.setLong(3, sinceTime);
                ps.setLong(4, searchBaselineId);
            } else {
                ps = conn.prepareStatement(LOAD_DATA_SQL);
                ps.setString(1, schemaName.toLowerCase());
                ps.setLong(2, sinceTime);
                ps.setLong(3, sinceTime);
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                try {
                    int baselineId = rs.getInt("BASELINE_INFO.ID");
                    String parameterSql = rs.getString("BASELINE_INFO.SQL");
                    Set<Pair<String, String>> tableSet =
                        BaselineInfo.deserializeTableSet(rs.getString("BASELINE_INFO.TABLE_SET"));
                    int tablesHashCode = PlanInfo.INVAILD_HASH_CODE;
                    try {
                        tablesHashCode = rs.getInt("PLAN_INFO.TABLES_HASHCODE");
                    } catch (Throwable t) {
                    }

                    int planId = rs.getInt("PLAN_INFO.ID");
                    String planString = rs.getString("PLAN_INFO.PLAN");
                    Long lastExecuteTime = rs.getLong("UNIX_TIMESTAMP(PLAN_INFO.LAST_EXECUTE_TIME)");
                    if (rs.wasNull()) {
                        lastExecuteTime = null;
                    }
                    int chooseCount = rs.getInt("PLAN_INFO.CHOOSE_COUNT");
                    double cost = rs.getDouble("PLAN_INFO.COST");
                    double estimateExecutionTime = rs.getDouble("PLAN_INFO.ESTIMATE_EXECUTION_TIME");
                    boolean accepted = rs.getBoolean("PLAN_INFO.ACCEPTED");
                    boolean fixed = rs.getBoolean("PLAN_INFO.FIXED");
                    String traceId = rs.getString("PLAN_INFO.TRACE_ID");
                    long createTime = rs.getLong("UNIX_TIMESTAMP(PLAN_INFO.GMT_CREATED)");
                    String origin = rs.getString("PLAN_INFO.ORIGIN");
                    String planExtendField = rs.getString("PLAN_EXTEND");
                    String extendField = rs.getString("BASELINE_INFO.EXTEND_FIELD");

                    if (parameterSql.length() > maxBaselineInfoSqlLength
                        || planString.length() > maxPlanInfoSqlLength) {
                        continue;
                    }

                    BaselineInfo baselineInfo = planManager.getBaselineMap().get(parameterSql);
                    if (baselineInfo == null) {
                        baselineInfo = new BaselineInfo(parameterSql, tableSet);
                        assert baselineInfo.getId() == baselineId;
                        if (planManager.getBaselineMap().size() > maxBaselineSize) {
                            continue;
                        }
                        planManager.getBaselineMap().put(parameterSql, baselineInfo);
                    }
                    baselineInfo.setExtend(extendField);
                    PlanInfo planInfo =
                        new PlanInfo(baselineId, planString, createTime, lastExecuteTime, chooseCount, cost,
                            estimateExecutionTime, accepted, fixed, traceId, origin, planExtendField, tablesHashCode);

                    assert planInfo.getId() == planId;
                    if (planInfo.isAccepted()) {
                        baselineInfo.addAcceptedPlan(planInfo);
                    } else {
                        baselineInfo.addUnacceptedPlan(planInfo);
                    }

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
        /* try to delete invalid baseline */
        deleteBaselineList(new ArrayList<>(invalidBaselineInfoIdSet));
    }

    @Override
    public void deletePlan(int baselineInfoId, int planInfoId) {
        if (!canWrite()) {
            return;
        }
        if (!checkTableFromCache()) {
            return;
        }
        if (!polarDbXSystemTablePlanInfo.canWrite()) {
            return;
        }
        if (!polarDbXSystemTablePlanInfo.checkTableFromCache()) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(PolarDbXSystemTablePlanInfo.DELETE_SQL);
            ps.setString(1, schemaName.toLowerCase());
            ps.setLong(2, baselineInfoId);
            ps.setLong(3, planInfoId);
            ps.execute();
        } catch (SQLException e) {
            logger.error("delete baselineInfo " + TABLE_NAME + " error", e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public void updatePlan(BaselineInfo baselineInfo, PlanInfo updatePlanInfo, int originPlanId) {
        if (!canWrite()) {
            return;
        }
        if (!checkTableFromCache()) {
            return;
        }
        if (!polarDbXSystemTablePlanInfo.canWrite()) {
            return;
        }
        if (!polarDbXSystemTablePlanInfo.checkTableFromCache()) {
            return;
        }

        deletePlan(baselineInfo.getId(), originPlanId);

        Connection conn = null;
        PreparedStatement pps = null;
        try {
            conn = dataSource.getConnection();
            pps = conn.prepareStatement(PolarDbXSystemTablePlanInfo.REPLACE_SQL);
            if (updatePlanInfo != null) {
                pps.setString(1, schemaName.toLowerCase());
                pps.setInt(2, updatePlanInfo.getId());
                pps.setInt(3, updatePlanInfo.getBaselineId());
                if (updatePlanInfo.getLastExecuteTime() != null) {
                    pps.setTimestamp(4, new Timestamp(updatePlanInfo.getLastExecuteTime() * 1000));
                } else {
                    pps.setTimestamp(4, null);
                }
                pps.setString(5, updatePlanInfo.getPlanJsonString());
                pps.setInt(6, updatePlanInfo.getChooseCount());
                pps.setDouble(7, updatePlanInfo.getCost());
                pps.setDouble(8, updatePlanInfo.getEstimateExecutionTime());
                pps.setBoolean(9, updatePlanInfo.isAccepted());
                pps.setBoolean(10, updatePlanInfo.isFixed());
                pps.setString(11, updatePlanInfo.getTraceId());
                pps.setString(12, updatePlanInfo.getOrigin());
                pps.setInt(13, updatePlanInfo.getTablesHashCode());
                pps.setString(14, updatePlanInfo.encodeExtend());
                pps.execute();
            } else {
                logger.warn("Don't exist the planInfo " + updatePlanInfo);
            }
        } catch (SQLException e) {
            logger.error("Replace planInfo failed for " + updatePlanInfo, e);
        } finally {
            JdbcUtils.close(pps);
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
    public void delete(int baselineInfoId) {
        deleteBaselineList(Arrays.asList(baselineInfoId));
    }

    @Override
    public void deleteBaselineList(List<Integer> baselineInfoIdList) {
        if (!canWrite()) {
            return;
        }
        if (!checkTableFromCache()) {
            return;
        }
        if (!polarDbXSystemTablePlanInfo.canWrite()) {
            return;
        }
        if (!polarDbXSystemTablePlanInfo.checkTableFromCache()) {
            return;
        }
        if (baselineInfoIdList == null) {
            return;
        }
        if (baselineInfoIdList.isEmpty()) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(DELETE_SQL);
            int batchSize = 32;
            int listSize = baselineInfoIdList.size();
            for (int i = 0; i < listSize; i++) {
                ps.setString(1, schemaName.toLowerCase());
                ps.setInt(2, baselineInfoIdList.get(i));
                ps.addBatch();
                if (i % batchSize == batchSize - 1) {
                    ps.executeBatch();
                }
            }
            if (listSize % batchSize != 0) {
                ps.executeBatch();
            }
            if (logger.isDebugEnabled()) {
                String baselineInfoIdListString = baselineInfoIdList.stream()
                    .map(Objects::toString)
                    .collect(Collectors.joining(", ", "[", "]"));
                logger.debug("delete baselineInfoIdList = " + baselineInfoIdListString);
            }
            LoggerUtil.logSpm(schemaName, "baseline delete:" + baselineInfoIdList);
        } catch (SQLException e) {
            logger.error("delete baselineInfo " + TABLE_NAME + " error", e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public SystemTableBaselineInfo.PersistResult persist(BaselineInfo baselineInfo) {
        SystemTableBaselineInfo.PersistResult persistResult = innerPersist(baselineInfo);
        if (persistResult == SystemTableBaselineInfo.PersistResult.TABLE_MISS) {
            createTableIfNotExist();
            polarDbXSystemTablePlanInfo.createTableIfNotExist();
            return innerPersist(baselineInfo);
        } else {
            return persistResult;
        }
    }

    private SystemTableBaselineInfo.PersistResult innerPersist(BaselineInfo baselineInfo) {
        if (!canWrite()) {
            return SystemTableBaselineInfo.PersistResult.DO_NOTHING;
        }
        if (!checkTableFromCache()) {
            return SystemTableBaselineInfo.PersistResult.DO_NOTHING;
        }
        if (!polarDbXSystemTablePlanInfo.canWrite()) {
            return SystemTableBaselineInfo.PersistResult.DO_NOTHING;
        }
        if (!polarDbXSystemTablePlanInfo.checkTableFromCache()) {
            return SystemTableBaselineInfo.PersistResult.DO_NOTHING;
        }
        if (baselineInfo == null) {
            return SystemTableBaselineInfo.PersistResult.DO_NOTHING;
        }
        Connection conn = null;
        PreparedStatement pps = null;
        ResultSet resultSet = null;
        SystemTableBaselineInfo.PersistResult persistResult = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            pps = conn.prepareStatement(SELECT_SQL);
            pps.setString(1, schemaName.toLowerCase());
            pps.setInt(2, baselineInfo.getId());
            resultSet = pps.executeQuery();
            if (resultSet.next()) {
                String parameterSql = resultSet.getString("SQL");
                if (!baselineInfo.getParameterSql().equals(parameterSql)) {
                    logger.error("sql1 : " + parameterSql + "\n" + "sql2 : " + baselineInfo.getParameterSql()
                        + "\n has the same hashcode!");
                    return SystemTableBaselineInfo.PersistResult.CONFLICT;
                }

                /** UPDATE */
                resultSet.close();
                pps.close();
                pps = conn.prepareStatement(UPDATE_SQL);
                pps.setString(1, baselineInfo.getExtend());
                pps.setString(2, schemaName.toLowerCase());
                pps.setInt(3, baselineInfo.getId());
                pps.executeUpdate();
                pps.close();
                persistResult = SystemTableBaselineInfo.PersistResult.INSERT;
                LoggerUtil.logSpm(schemaName, "baseline update:" + BaselineInfo.serializeBaseInfoToJson(baselineInfo));
            } else {
                /** insert */
                resultSet.close();
                pps.close();
                pps = conn.prepareStatement(INSERT_SQL);
                pps.setString(1, schemaName.toLowerCase());
                pps.setInt(2, baselineInfo.getId());
                pps.setString(3, baselineInfo.getParameterSql());
                pps.setString(4, BaselineInfo.serializeTableSet(baselineInfo.getTableSet()));
                pps.setString(5, baselineInfo.getExtend());
                pps.executeUpdate();
                pps.close();
                persistResult = SystemTableBaselineInfo.PersistResult.INSERT;
                // TODO log baseline create
                LoggerUtil.logSpm(schemaName, "baseline create:" + BaselineInfo.serializeBaseInfoToJson(baselineInfo));
            }

            int acceptedPlanCount = 0;
            pps = conn.prepareStatement(PolarDbXSystemTablePlanInfo.REPLACE_SQL);
            int[] plansInfoIds = new int[baselineInfo.getAcceptedPlans().size()];
            for (PlanInfo planInfo : baselineInfo.getAcceptedPlans().values()) {
                plansInfoIds[acceptedPlanCount] = planInfo.getId();
                pps.setString(1, schemaName.toLowerCase());
                pps.setInt(2, planInfo.getId());
                pps.setInt(3, planInfo.getBaselineId());
                if (planInfo.getLastExecuteTime() != null) {
                    pps.setTimestamp(4, new Timestamp(planInfo.getLastExecuteTime() * 1000));
                } else {
                    pps.setTimestamp(4, null);
                }
                pps.setString(5, planInfo.getPlanJsonString());
                pps.setInt(6, planInfo.getChooseCount());
                pps.setDouble(7, planInfo.getCost());
                pps.setDouble(8, planInfo.getEstimateExecutionTime());
                pps.setBoolean(9, planInfo.isAccepted());
                pps.setBoolean(10, planInfo.isFixed());
                pps.setString(11, planInfo.getTraceId());
                pps.setString(12, planInfo.getOrigin());
                pps.setInt(13, planInfo.getTablesHashCode());
                pps.setString(14, planInfo.encodeExtend());
                pps.addBatch();
                acceptedPlanCount++;
            }
            if (acceptedPlanCount == 0) {
                logger.error("baselineInfo without accepted plan baselineInfoId = " + baselineInfo.getId());
                conn.rollback();
                return SystemTableBaselineInfo.PersistResult.ERROR;
            }
            int[] results = pps.executeBatch();

            Stream.iterate(0, i -> i + 1)
                .limit(results.length)
                // filter out update plan, result==1 means insert,
                // which demonstrate a new plan that needs to be logging
                .filter(i -> results[i] == 1)
                .forEach(i -> LoggerUtil.logSpm(schemaName,
                    "plan create:" + PlanInfo.serializeToJson(baselineInfo.getAcceptedPlans()
                        .get(plansInfoIds[i]))));

            // TODO should we persist unAccepted ExecutionPlan ?
            pps.close();
            conn.commit();
            logger.debug("persist baselineInfoId = " + baselineInfo.getId());
            return persistResult;
        } catch (SQLException e) {
            if (e.getErrorCode() == 1146) {
                logger.error("persist " + TABLE_NAME + " error, we will try again", e);
                return SystemTableBaselineInfo.PersistResult.TABLE_MISS;
            } else {
                logger.error("persist " + TABLE_NAME + " error, BASELINE_IN = " + baselineInfo.getId(), e);
                return SystemTableBaselineInfo.PersistResult.ERROR;
            }
        } catch (Exception e) {
            logger.error("persist " + TABLE_NAME + " error, BASELINE_IN = " + baselineInfo.getId(), e);
            return SystemTableBaselineInfo.PersistResult.ERROR;
        } finally {
            JdbcUtils.close(resultSet);
            JdbcUtils.close(pps);
            JdbcUtils.close(conn);
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
            JdbcUtils.close(ps);
            JdbcUtils.close(conn);
            JdbcUtils.close(rs);
        }
    }
}
