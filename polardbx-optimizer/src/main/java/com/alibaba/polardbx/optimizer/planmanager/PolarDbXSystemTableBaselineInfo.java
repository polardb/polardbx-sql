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

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.druid.util.StringUtils;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author dylan
 */
public class PolarDbXSystemTableBaselineInfo {
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
        "SELECT BASELINE_INFO.SCHEMA_NAME, BASELINE_INFO.ID, BASELINE_INFO.SQL, BASELINE_INFO.TABLE_SET, BASELINE_INFO.EXTEND_FIELD, PLAN_INFO.TABLES_HASHCODE, "
            +
            "PLAN_INFO.ID, PLAN_INFO.PLAN, UNIX_TIMESTAMP(PLAN_INFO.LAST_EXECUTE_TIME), PLAN_INFO.CHOOSE_COUNT, PLAN_INFO.COST, PLAN_INFO.ESTIMATE_EXECUTION_TIME, "
            +
            "PLAN_INFO.ACCEPTED, PLAN_INFO.FIXED, PLAN_INFO.TRACE_ID, PLAN_INFO.ORIGIN, PLAN_INFO.EXTEND_FIELD AS PLAN_EXTEND, UNIX_TIMESTAMP(PLAN_INFO"
            + ".GMT_MODIFIED), UNIX_TIMESTAMP(PLAN_INFO.GMT_CREATED) FROM "
            +
            "`" + TABLE_NAME + "` AS BASELINE_INFO LEFT JOIN `" + PolarDbXSystemTablePlanInfo.TABLE_NAME
            + "` AS PLAN_INFO " +
            "ON BASELINE_INFO.SCHEMA_NAME = PLAN_INFO.SCHEMA_NAME AND BASELINE_INFO.ID = PLAN_INFO.BASELINE_ID " +
            "WHERE UNIX_TIMESTAMP(BASELINE_INFO.GMT_MODIFIED) > ? AND "
            + "(PLAN_INFO.GMT_MODIFIED IS NULL OR UNIX_TIMESTAMP(PLAN_INFO.GMT_MODIFIED) > ?)";

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

    public static void createTableIfNotExist() {
        if (cannotWrite()) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        PreparedStatement psDropColumn = null;
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
            ps = conn.prepareStatement(CREATE_TABLE_IF_NOT_EXIST_SQL);
            ps.executeUpdate();
            psDropColumn = conn.prepareStatement(DROP_TABLES_HASHCODE_COLUMN);
            psDropColumn.executeUpdate();
        } catch (Exception e) {
            if (e instanceof SQLException && e.getMessage().contains("Can't DROP ")) {
                // ignore drop column error
                logger.debug("create " + TABLE_NAME + " if not exist error", e);
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
    public static Map<String, Map<String, BaselineInfo>> loadData(long sinceTime,
                                                                  int maxBaselineSize, int maxSqlLength,
                                                                  int maxPlanLength) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, Map<String, BaselineInfo>> baselineMap = Maps.newConcurrentMap();
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();

            ps = conn.prepareStatement(LOAD_DATA_SQL);
            ps.setLong(1, sinceTime);
            ps.setLong(2, sinceTime);
            int count = 0;
            rs = ps.executeQuery();
            while (rs.next()) {
                try {
                    int baselineId = rs.getInt("BASELINE_INFO.ID");
                    String schema = rs.getString("BASELINE_INFO.SCHEMA_NAME");
                    if (StringUtils.isEmpty(schema)) {
                        continue;
                    }
                    schema = schema.toLowerCase(Locale.ROOT);
                    String parameterSql = rs.getString("BASELINE_INFO.SQL");
                    Set<Pair<String, String>> tableSet =
                        BaselineInfo.deserializeTableSet(rs.getString("BASELINE_INFO.TABLE_SET"));
                    int tablesHashCode = rs.getInt("PLAN_INFO.TABLES_HASHCODE");

                    int planId = rs.getInt("PLAN_INFO.ID");
                    String planString = rs.getString("PLAN_INFO.PLAN");
                    Long lastExecuteTime = rs.getLong("UNIX_TIMESTAMP(PLAN_INFO.LAST_EXECUTE_TIME)");
                    if (rs.wasNull()) {
                        lastExecuteTime = null;
                    }
                    int chooseCount = rs.getInt("PLAN_INFO.CHOOSE_COUNT");
                    double cost = rs.getDouble("PLAN_INFO.COST");
                    double estimateExecutionTime = rs.getDouble("PLAN_INFO.ESTIMATE_EXECUTION_TIME");
                    boolean fixed = rs.getBoolean("PLAN_INFO.FIXED");
                    String traceId = rs.getString("PLAN_INFO.TRACE_ID");
                    long createTime = rs.getLong("UNIX_TIMESTAMP(PLAN_INFO.GMT_CREATED)");
                    String origin = rs.getString("PLAN_INFO.ORIGIN");
                    String planExtendField = rs.getString("PLAN_EXTEND");
                    String extendField = rs.getString("BASELINE_INFO.EXTEND_FIELD");

                    // planString might be null if baseline is rebuildAtLoad
                    if (parameterSql.length() > maxSqlLength
                        || (planString != null && planString.length() > maxPlanLength)) {
                        continue;
                    }

                    BaselineInfo baselineInfo;
                    if (!baselineMap.containsKey(schema)) {
                        Map<String, BaselineInfo> newSchemaMap = Maps.newConcurrentMap();
                        baselineMap.put(schema, newSchemaMap);
                    }

                    if (baselineMap.get(schema).containsKey(parameterSql)) {
                        baselineInfo = baselineMap.get(schema).get(parameterSql);
                        if (baselineInfo.isRebuildAtLoad()) {
                            continue;
                        }
                    } else {
                        if (count > maxBaselineSize) {
                            continue;
                        }
                        // sql -> baseline
                        baselineInfo = new BaselineInfo(parameterSql, tableSet);
                        baselineMap.get(schema).put(baselineInfo.getParameterSql(), baselineInfo);
                    }
                    assert baselineInfo.getId() == baselineId;

                    baselineInfo.setExtend(extendField);
                    if (baselineInfo.isRebuildAtLoad()) {
                        continue;
                    }

                    PlanInfo planInfo =
                        new PlanInfo(baselineId, planString, createTime, lastExecuteTime, chooseCount, cost,
                            estimateExecutionTime, true, fixed, traceId, origin, planExtendField, tablesHashCode);

                    assert planInfo.getId() == planId;
                    baselineInfo.addAcceptedPlan(planInfo);

                    count++;
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
        return baselineMap;
    }

    public static void deletePlan(String schemaName, int baselineInfoId, int planInfoId) {
        if (cannotWrite()) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
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

    public static boolean deleteAll(String schemaName) {
        if (cannotWrite()) {
            return false;
        }

        PreparedStatement ps = null;
        String sql = "";
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            ps = metaDbConn.prepareStatement(DELETE_ALL_SQL);
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

    public static void delete(String schemaName, int baselineInfoId) {
        deleteBaselineList(schemaName, Collections.singletonList(baselineInfoId));
    }

    public static void deleteBaselineList(String schemaName, List<Integer> baselineInfoIdList) {
        if (cannotWrite()) {
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
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
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

    public static SystemTableBaselineInfo.PersistResult persist(String schemaName, BaselineInfo baselineInfo) {
        SystemTableBaselineInfo.PersistResult persistResult = innerPersist(schemaName, baselineInfo);
        if (persistResult == SystemTableBaselineInfo.PersistResult.TABLE_MISS) {
            logger.error("persist error :" + persistResult);
        }
        return persistResult;
    }

    private static SystemTableBaselineInfo.PersistResult innerPersist(String schemaName, BaselineInfo baselineInfo) {
        if (cannotWrite()) {
            return SystemTableBaselineInfo.PersistResult.DO_NOTHING;
        }
        if (baselineInfo == null) {
            return SystemTableBaselineInfo.PersistResult.DO_NOTHING;
        }
        Connection conn = null;
        PreparedStatement pps = null;
        ResultSet resultSet = null;
        SystemTableBaselineInfo.PersistResult persistResult;
        try {
            conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
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

                // UPDATE
                resultSet.close();
                pps.close();
                pps = conn.prepareStatement(UPDATE_SQL);
                pps.setString(1, baselineInfo.getExtend());
                pps.setString(2, schemaName.toLowerCase());
                pps.setInt(3, baselineInfo.getId());
                pps.executeUpdate();
                pps.close();
                persistResult = SystemTableBaselineInfo.PersistResult.UPDATE;
                LoggerUtil.logSpm(schemaName, "baseline update:" + BaselineInfo.serializeBaseInfoToJson(baselineInfo));
            } else {
                // insert
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

            // delete plan
            pps = conn.prepareStatement(PolarDbXSystemTablePlanInfo.DELETE_BY_BASELINE_SQL);
            pps.setString(1, schemaName.toLowerCase());
            pps.setInt(2, baselineInfo.getId());
            pps.executeUpdate();
            pps.close();

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
                pps.setBoolean(9, true);
                pps.setBoolean(10, planInfo.isFixed());
                pps.setString(11, planInfo.getTraceId());
                pps.setString(12, planInfo.getOrigin());
                pps.setInt(13, planInfo.getTablesHashCode());
                pps.setString(14, planInfo.encodeExtend());
                pps.addBatch();
                acceptedPlanCount++;
            }
            // rebuild at load baseline does not cache plan
            if (acceptedPlanCount == 0 && !baselineInfo.isRebuildAtLoad()) {
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

    private static boolean cannotWrite() {
        boolean canWrite = ConfigDataMode.isMasterMode() && LeaderStatusBridge.getInstance().hasLeadership();
        return !canWrite;
    }
}
