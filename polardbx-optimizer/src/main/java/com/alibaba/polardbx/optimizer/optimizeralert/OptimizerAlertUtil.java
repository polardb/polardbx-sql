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

package com.alibaba.polardbx.optimizer.optimizeralert;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.rpc.result.XResult;
import org.eclipse.jetty.util.StringUtil;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogPattern.CHECK_FAIL;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.STATISTIC_CHECK;

public class OptimizerAlertUtil {

    public static final long XPLAN_ALERT_THRESHOLD = 300L;
    public static final long TP_ALERT_THRESHOLD = 500L;
    public static final long BKA_ALERT_THRESHOLD = 300L;
    public static final long PLAN_CACHE_ALERT_THRESHOLD = 500L;

    public static void bkaAlert(ExecutionContext ec, LogicalView logicalView, long phySqlCount) {
        if (!DynamicConfig.getInstance().optimizerAlert()) {
            return;
        }
        try {
            if (bkaShouldAlert(ec, logicalView, phySqlCount)) {
                OptimizerAlertManager.getInstance().log(OptimizerAlertType.BKA_TOO_MUCH, ec);
            }
        } catch (Exception e) {
            // ignore
        }
    }

    public static void tpAlert(ExecutionContext ec, double executeTimeMs) {
        if (!DynamicConfig.getInstance().optimizerAlert()) {
            return;
        }
        try {
            if (tpShouldAlert(ec, executeTimeMs)) {
                OptimizerAlertManager.getInstance().log(OptimizerAlertType.TP_SLOW, ec);
            }
        } catch (Exception e) {
            // ignore
        }
    }

    public static void xplanAlert(ExecutionContext ec, XResult xResult) {
        if (!DynamicConfig.getInstance().optimizerAlert()) {
            return;
        }
        try {
            if (xplanShouldAlert(ec, xResult)) {
                OptimizerAlertManager.getInstance().log(OptimizerAlertType.XPLAN_SLOW, ec);
            }
        } catch (Exception e) {
            // ignore
        }
    }

    public static void plancacheAlert(ExecutionContext ec, long currentCapacity) {
        if (!DynamicConfig.getInstance().optimizerAlert()) {
            return;
        }
        try {
            if (plancacheShouldAlert(ec, currentCapacity)) {
                OptimizerAlertManager.getInstance().log(OptimizerAlertType.PLAN_CACHE_FULL, ec);
            }
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * check if any statistic info(both topn&histogram) missing
     */
    public static void statisticsAlert(String schema, String table, StatisticManager.CacheLine c) {
        if (!DynamicConfig.getInstance().optimizerAlert()) {
            return;
        }
        if (SystemDbHelper.isDBBuildIn(schema)) {
            return;
        }
        // if table row count == 0 ,skip check
        if (c.getRowCount() == 0L) {
            return;
        }
        List<ColumnMeta> columnMetas = StatisticUtils.getColumnMetas(false, schema, table);
        if (columnMetas == null || columnMetas.isEmpty()) {
            return;
        }
        for (ColumnMeta cm : columnMetas) {
            String col = cm.getName().toLowerCase();
            if (c.getAllNullCols().contains(col)) {
                continue;
            }
            // check if any statistic missing
            boolean topNNull = c.getTopN(col) == null;
            Histogram histogram = c.getHistogramMap() == null ? null : c.getHistogramMap().get(col);
            boolean histogramNull = false;
            if (histogram == null || histogram.getBuckets().size() == 0) {
                histogramNull = true;
            }

            if (topNNull && histogramNull) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.STATISTICS,
                        CHECK_FAIL,
                        new String[] {
                            STATISTIC_CHECK.name(),
                            schema + "," + table + "," + col
                        },
                        NORMAL);
                OptimizerAlertManager.getInstance().log(OptimizerAlertType.STATISTIC_MISS, null);
            }
        }
    }

    private static boolean bkaShouldAlert(ExecutionContext ec, LogicalView logicalView, long phySqlCount) {
        if (ec.getParamManager().getBoolean(ConnectionParams.ENABLE_ALERT_TEST)) {
            return true;
        }
        if (SystemDbHelper.isDBBuildIn(ec.getSchemaName())) {
            return false;
        }
        // fast path to avoid check
        if (phySqlCount < BKA_ALERT_THRESHOLD) {
            return false;
        }
        Map<String, List<List<String>>> targetTables = logicalView.getTargetTables(ec);
        long shardCount = targetTables.values().stream().mapToInt(List::size).sum();
        if (phySqlCount > shardCount * ec.getParamManager()
            .getInt(ConnectionParams.ALERT_BKA_BASE)) {
            return true;
        }
        return false;
    }

    private static boolean tpShouldAlert(ExecutionContext ec, double executeTimeMs) {
        // only alert select sql
        if (!SqlType.SELECT.equals(ec.getSqlType())) {
            return false;
        }
        if (SystemDbHelper.isDBBuildIn(ec.getSchemaName())) {
            return false;
        }
        // alert in test mode
        if (ec.getParamManager().getBoolean(ConnectionParams.ENABLE_ALERT_TEST)) {
            return true;
        }
        // fast path to avoid check
        if (executeTimeMs < TP_ALERT_THRESHOLD) {
            return false;
        }
        if (executeTimeMs >= ec.getParamManager()
            .getLong(ConnectionParams.SLOW_SQL_TIME) * ec.getParamManager().getInt(ConnectionParams.ALERT_TP_BASE)) {
            return true;
        }
        return false;
    }

    private static boolean xplanShouldAlert(ExecutionContext ec, XResult xResult) {
        if (xResult == null) {
            return false;
        }
        // make sure the sql is xplan
        if (xResult.getRequestType() != XResult.RequestType.PLAN_QUERY) {
            return false;
        }
        if (SystemDbHelper.isDBBuildIn(ec.getSchemaName())) {
            return false;
        }
        // alert if alert_test enabled
        if (ec.getParamManager().getBoolean(ConnectionParams.ENABLE_ALERT_TEST)) {
            return true;
        }
        // time in millisecond
        long time = xResult.getFinishNanos() / 1000000;
        // fast pass to avoid alert
        if (time <= XPLAN_ALERT_THRESHOLD) {
            return false;
        }

        long threshold = ec.getPhysicalRecorder().getSlowSqlTime();
        // Use slow sql time of db level first
        if (ec.getExtraCmds().containsKey(ConnectionProperties.SLOW_SQL_TIME)) {
            threshold = ec.getParamManager().getLong(ConnectionParams.SLOW_SQL_TIME);
        }
        if (time > threshold &&
            (xResult.getExaminedRowCount() > xResult.getRowsAffected() * 100)) {
            return true;
        }
        return false;
    }

    private static boolean plancacheShouldAlert(ExecutionContext ec, long currentCapacity) {
        // alert if alert_test enabled
        if (ec.getParamManager().getBoolean(ConnectionParams.ENABLE_ALERT_TEST)) {
            return true;
        }
        if (SystemDbHelper.isDBBuildIn(ec.getSchemaName())) {
            return false;
        }
        if (PlanCache.getInstance().getCache().size() <= PLAN_CACHE_ALERT_THRESHOLD) {
            return false;
        }

        if (PlanCache.getInstance().getCache().size() >= currentCapacity - 20) {
            return true;
        }
        return false;
    }
}
