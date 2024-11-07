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

package com.alibaba.polardbx.executor.columnar.checker;

import com.alibaba.polardbx.common.IInnerConnection;
import com.alibaba.polardbx.common.IInnerConnectionManager;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * @author yaozhili
 */
public class CciChecker implements ICciChecker {
    private static final Logger logger = LoggerFactory.getLogger(CciChecker.class);
    private final String schemaName;
    protected final String tableName;
    protected final String indexName;
    private long primaryHashCode = -1;
    private long columnarHashCode = -1;
    private long primaryCount = -1;
    private long columnarCount = -1;
    private long primaryPkHashCode = -1;
    private long columnarPkHashCode = -1;

    private String columnarCheckSql;
    private String primaryCheckSql;
    private static final String CALCULATE_PRIMARY_HASH =
        "select count(0) as count, check_sum_v2(%s) as pk_checksum, check_sum_v2(*) as checksum "
            + "from %s force index(primary)";
    private static final String CALCULATE_COLUMNAR_HASH =
        "select count(0) as count, check_sum_v2(%s) as pk_checksum, check_sum_v2(*) as checksum "
            + "from %s force index(%s)";
    protected static final String PRIMARY_HINT =
        "/*+TDDL:WORKLOAD_TYPE=AP "
            + "SOCKET_TIMEOUT=259200000 MPP_TASK_MAX_RUN_TIME=259200000 %s */";
    protected static final String COLUMNAR_HINT =
        "/*+TDDL:WORKLOAD_TYPE=AP ENABLE_COLUMNAR_OPTIMIZER=true "
            + "OPTIMIZER_TYPE='columnar' ENABLE_HTAP=true SOCKET_TIMEOUT=259200000 "
            + "MPP_TASK_MAX_RUN_TIME=259200000 %s */";

    public CciChecker(String schemaName, String tableName, String indexName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.indexName = indexName;
    }

    @Override
    public void check(ExecutionContext baseEc, Runnable recoverChangedConfigs) throws Throwable {
        Pair<Long, Long> tso = getCheckTso();
        IInnerConnectionManager manager = ExecutorContext.getContext(schemaName).getInnerConnectionManager();
        logger.warn("Check cci using innodb tso " + tso.getKey() + ", columnar tso " + tso.getValue());

        // Hold this trx object to prevent purge.
        ITransaction trx = ExecUtils.createColumnarTransaction(schemaName, baseEc, tso.getValue());
        try {
            // Calculate primary table checksum.
            try (Connection conn = manager.getConnection(schemaName);
                Statement stmt = conn.createStatement()) {
                long start = System.nanoTime();
                calPrimaryHashCode(stmt, baseEc, tso.getKey());
                SQLRecorderLogger.ddlLogger.info("[Naive checker] Primary checksum calculated, costing "
                    + ((System.nanoTime() - start) / 1_000_000) + " ms");
            } catch (Throwable t) {
                SQLRecorderLogger.ddlLogger.error(
                    String.format("Error occurs when checking primary index %s.%s", tableName, indexName), t);
                throw t;
            } finally {
                if (null != recoverChangedConfigs) {
                    recoverChangedConfigs.run();
                }
            }

            // Calculate columnar table checksum.
            try (IInnerConnection conn = manager.getConnection(schemaName);
                Statement stmt = conn.createStatement()) {
                conn.addExecutionContextInjectHook(
                    // To see non-PUBLIC CCI.
                    (ec) -> ((ExecutionContext) ec).setCheckingCci(true));
                long start = System.nanoTime();
                calColumnarHashCode(stmt, baseEc, tso.getValue());
                SQLRecorderLogger.ddlLogger.info("[Naive checker] Columnar checksum calculated, costing "
                    + ((System.nanoTime() - start) / 1_000_000) + " ms");
                conn.clearExecutionContextInjectHooks();

            } catch (Throwable t) {
                SQLRecorderLogger.ddlLogger.error(
                    String.format("Error occurs when checking columnar index %s.%s", tableName, indexName), t);
                throw t;
            }

        } finally {
            trx.close();
        }

    }

    protected Pair<Long, Long> getCheckTso() {
        return ColumnarTransactionUtils.getLatestOrcCheckpointTsoFromGms();
    }

    @Override
    public boolean getCheckReports(Collection<String> reports) {
        boolean success = true;
        if (-1 == primaryCount || primaryCount != columnarCount) {
            // Check fail.
            reports.add("Inconsistency detected: primary count: " + primaryCount
                + ", columnar count: " + columnarCount);
            success = false;
        }

        if (-1 == primaryPkHashCode || primaryPkHashCode != columnarPkHashCode) {
            // Check fail.
            reports.add("Inconsistency detected: primary pk hash: " + primaryPkHashCode
                + ", columnar pk hash: " + columnarPkHashCode);
            success = false;
        }

        if (-1 == primaryHashCode || primaryHashCode != columnarHashCode) {
            // Check fail.
            reports.add("Inconsistency detected: primary hash: " + primaryHashCode
                + ", columnar hash: " + columnarHashCode);
            success = false;
        }

        if (!success) {
            reports.add("Primary table check sql: " + primaryCheckSql
                + "\nColumnar table check sql: " + columnarCheckSql);
        }

        return success;
    }

    private void calPrimaryHashCode(Statement stmt, ExecutionContext ec, long tso) throws SQLException {
        // Build pk list.
        String pkList = ec
            .getSchemaManager(schemaName)
            .getTable(tableName)
            .getPrimaryKey()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.joining(","));
        String sql = getPrimarySql(ec, tso, pkList);
        logger.warn("Check CCI primary sql: " + sql);
        primaryCheckSql = sql;
        ResultSet explainRs = stmt.executeQuery("explain " + sql);
        StringBuilder explainResult = new StringBuilder();
        while (explainRs.next()) {
            explainResult.append(explainRs.getString(1)).append("\n");
        }
        logger.warn("Check CCI primary sql plan: \n" + explainResult);
        if (!explainResult.toString().contains("LogicalView")) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Check cci plan does not contain LogicalView");
        }

        ResultSet rs = stmt.executeQuery(sql);
        if (rs.next()) {
            primaryCount = rs.getLong("count");
            primaryPkHashCode = rs.getLong("pk_checksum");
            primaryHashCode = rs.getLong("checksum");
        }
    }

    private void calColumnarHashCode(Statement stmt, ExecutionContext ec, long tso) throws SQLException {
        // Build pk list.
        String pkList = ec
            .getSchemaManager(schemaName)
            .getTable(tableName)
            .getPrimaryKey()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.joining(","));
        String sql = getColumnarSql(ec, tso, pkList);
        logger.warn("Check CCI columnar sql: " + sql);
        columnarCheckSql = sql;
        ResultSet explainRs = stmt.executeQuery("explain " + sql);
        StringBuilder explainResult = new StringBuilder();
        while (explainRs.next()) {
            explainResult.append(explainRs.getString(1)).append("\n");
        }
        logger.warn("Check CCI columnar sql plan: \n" + explainResult);
        if (!explainResult.toString().contains("OSSTableScan")) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Check cci plan does not contain OSSTableScan");
        }

        ResultSet rs = stmt.executeQuery(sql);
        if (rs.next()) {
            columnarCount = rs.getLong("count");
            columnarPkHashCode = rs.getLong("pk_checksum");
            columnarHashCode = rs.getLong("checksum");
        }
    }

    protected String getColumnarSql(ExecutionContext ec, long tso, String pkList) {
        // Build hint.
        StringBuilder sb = new StringBuilder();
        ICciChecker.setBasicHint(ec, sb);
        sb.append(" SNAPSHOT_TS=")
            .append(tso)
            .append(" ");
        String hint = String.format(COLUMNAR_HINT, sb);

        String sql = String.format(CALCULATE_COLUMNAR_HASH, pkList, tableName, indexName);

        return hint + sql;
    }

    protected String getPrimarySql(ExecutionContext ec, long tso, String pkList) {
        // Build hint.
        StringBuilder sb = new StringBuilder();
        ICciChecker.setBasicHint(ec, sb);
        sb.append(" SNAPSHOT_TS=")
            .append(tso)
            .append(" ");
        sb.append(" TRANSACTION_POLICY=TSO");
        String hint = String.format(PRIMARY_HINT, sb);

        String sql = String.format(CALCULATE_PRIMARY_HASH, pkList, tableName);

        return hint + sql;
    }
}
