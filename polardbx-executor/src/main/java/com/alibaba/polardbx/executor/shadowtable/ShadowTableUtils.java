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

package com.alibaba.polardbx.executor.shadowtable;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.utils.StringUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.alibaba.polardbx.druid.sql.SQLUtils.parseStatementsWithDefaultFeatures;
import static com.alibaba.polardbx.gms.metadb.limit.Limits.MAX_LENGTH_OF_IDENTIFIER_NAME;

public class ShadowTableUtils {
    private final static Logger LOG = SQLRecorderLogger.ddlMetaLogger;

    static String taskName = "ShadowTableCheckBeforeExecute";

    public static String generateShadowTableName(String logicalTableName, Long id) {
        // physical table name generated is t1_m7yv_00074, in which case we have shadow table name like
        // __t1_12345678, where the shadow table name is no longer than the physical table name.
        final int suffixLength = "__t1_12345678".length() - "t1".length();
        final int maxLogicalTableNameLength = MAX_LENGTH_OF_IDENTIFIER_NAME - suffixLength;
        if (logicalTableName.length() > maxLogicalTableNameLength) {
            logicalTableName = logicalTableName.substring(0, maxLogicalTableNameLength);
        }
        String shadowTableName = String.format("__%s_%08d", logicalTableName, id % 100_000_000L);
        return shadowTableName;
    }

    private static String randomString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            char ch = (char) (ThreadLocalRandom.current().nextInt('x' - 'a') + 'a');
            sb.append(ch);
        }
        return sb.toString();
    }

    /**
     * change set opt for logical table
     */
    public static void createShadowTable(ExecutionContext originEc, String schemaName, String logicalTableName,
                                         String groupName,
                                         String phyTableName, String shadowTableName) {
        String createTableSql =
            generateCreateTableSql(originEc, schemaName, logicalTableName, groupName, phyTableName, shadowTableName);
        TwoPhaseDdlUtils.executePhyDdlBypassConnPool(originEc, -1L, schemaName, groupName, createTableSql, "",
            shadowTableName);
    }

    public static String showCreateShadowTable(ExecutionContext originEc, String schemaName, String logicalTableName,
                                               String groupName,
                                               String phyTableName, String shadowTableName) {
        String showCreateTableSql =
            String.format("show create table %s", SqlIdentifier.surroundWithBacktick(shadowTableName));
        List<List<Object>> results =
            TwoPhaseDdlUtils.queryGroup(originEc, -1L, taskName, schemaName, logicalTableName, groupName,
                showCreateTableSql);
        return results.get(0).get(1).toString();
    }

    // TODO: what if CN HA after alter shadow table
    public static void alterShadowTable(ExecutionContext originEc, String schemaName, String logicalTableName,
                                        String groupName,
                                        String shadowTableName, String alterStmt) {
//        if (!createShadowTableSql.equalsIgnoreCase(createTableSql)) {
        String sql = alterStmt;
        TwoPhaseDdlUtils.executePhyDdlBypassConnPool(originEc, -1L, schemaName, groupName, sql, "", shadowTableName);
//            shadowTableAltered = true;
    }

    public static void clearShadowTable(ExecutionContext originEc, String schemaName, String logicalTableName,
                                        String groupName,
                                        String shadowTableName) {
        String dropTableStmt = "DROP TABLE IF EXISTS %s";
        String sql = String.format(dropTableStmt, SqlIdentifier.surroundWithBacktick(shadowTableName));
        TwoPhaseDdlUtils.executePhyDdlBypassConnPool(originEc, -1L, schemaName, groupName, sql, "", shadowTableName);
    }

    public static void initTraceShadowTable(ExecutionContext originEc, String schemaName, String logicalTableName,
                                            String groupName,
                                            String shadowTableName, Long id) {
        String sql =
            String.format(TwoPhaseDdlUtils.SQL_INIT_TWO_PHASE_DDL, schemaName, String.valueOf(id),
                TStringUtil.quote(shadowTableName));
        TwoPhaseDdlUtils.updateGroup(originEc, -1L, schemaName, groupName, sql);
        sql = String.format(TwoPhaseDdlUtils.SQL_TRACE_TWO_PHASE_DDL, schemaName, String.valueOf(id));
        TwoPhaseDdlUtils.updateGroup(originEc, -1L, schemaName, groupName, sql);
//            shadowTableAltered = true;
    }

    public static void finishTraceShadowTable(ExecutionContext originEc, String schemaName, String logicalTableName,
                                              String groupName,
                                              String shadowTableName, Long id) {
        String sql = String.format(TwoPhaseDdlUtils.SQL_FINISH_TWO_PHASE_DDL, schemaName, String.valueOf(id));
        TwoPhaseDdlUtils.updateGroup(originEc, -1L, schemaName, groupName, sql);
//            shadowTableAltered = true;
    }

    public static Pair<Boolean, Boolean> fetchTraceTableDdl(ExecutionContext originEc, String schemaName,
                                                            String logicalTableName,
                                                            String groupName,
                                                            String shadowTableName, Long id) {
        String sql = String.format(TwoPhaseDdlUtils.SQL_PROF_TWO_PHASE_DDL, schemaName, String.valueOf(id));
        Map<String, Object> result =
            TwoPhaseDdlUtils.queryGroupBypassConnPool(originEc, -1L, "", schemaName, logicalTableName, groupName, sql)
                .get(0);
        Boolean prepareMoment = !result.get("REACHED_PREPARED_MOMENT").toString().startsWith("1970");
        Boolean commitMoment = !result.get("REACHED_COMMIT_MOMENT").toString().startsWith("1970");
        return Pair.of(prepareMoment, commitMoment);
//            shadowTableAltered = true;
    }

    public static String generateCreateTableSql(ExecutionContext originEc, String schemaName, String logicalTableName,
                                                String groupName,
                                                String originalTableName, String targetTableName) {
        String showCreateTableStmt = "SHOW CREATE TABLE  %s";
        String sql = String.format(showCreateTableStmt, SqlIdentifier.surroundWithBacktick(originalTableName));
        String createTableSql =
            TwoPhaseDdlUtils.queryGroup(originEc, -1L, taskName, schemaName, logicalTableName, groupName, sql).get(0)
                .get(1)
                .toString();
        final MySqlCreateTableStatement
            createTableStmt =
            (MySqlCreateTableStatement) parseStatementsWithDefaultFeatures(createTableSql, JdbcConstants.MYSQL).get(0)
                .clone();
        createTableStmt.setTableName(SqlIdentifier.surroundWithBacktick(targetTableName));
        createTableStmt.setIfNotExiists(true);
        return createTableStmt.toString();
    }

//    public CompareResult compareShadowTable(ExecutionContext originEc) {
//        String createShadowTableStmt = generateCreateTableSql(originEc, shadowTableName, phyTableName);
//        String createPhyTableStmt = generateCreateTableSql(originEc, phyTableName, phyTableName);
//        CompareResult compareResult = new CompareResult(createShadowTableStmt, createPhyTableStmt);
//        return compareResult;
//    }

    //    public static void updateGroup(ExecutionContext ec, String schema, String groupName, String sql, List<String> params) {
//
//        ExecutorContext executorContext = ExecutorContext.getContext(schema);
//        IGroupExecutor ge = executorContext.getTopologyExecutor().getGroupExecutor(groupName);
//
//
//        try (Connection conn = ge.getDataSource().getConnection()) {
//            PreparedStatement stmt = conn.prepareStatement(sql);
//            for(int i = 0; i < params.size(); i++){
//                stmt.set(i + 1, params.get(i));
//            }
//            stmt.executeUpdate();
//        } catch (SQLException e) {
//            throw GeneralUtil.nestedException(
//                String.format("failed to execute on group(%s): %s , Caused by: %s", groupName, sql, e.getMessage()), e);
//        }
//    }

}
