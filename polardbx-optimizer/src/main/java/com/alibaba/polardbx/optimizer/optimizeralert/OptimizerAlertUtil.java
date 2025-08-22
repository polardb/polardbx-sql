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
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.ExceptionUtils;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.statis.XplanStat;

import java.util.HashMap;
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

    public static void selectivityAlert(ExecutionContext ec, Throwable throwable) {
        if (!DynamicConfig.getInstance().optimizerAlert()) {
            return;
        }
        try {
            OptimizerAlertManager.getInstance().log(OptimizerAlertType.SELECTIVITY_ERR, ec, throwable);
        } catch (Exception e) {
            // ignore
        }
    }

    public static void spmAlert(ExecutionContext ec, Throwable throwable) {
        if (!DynamicConfig.getInstance().optimizerAlert()) {
            return;
        }
        try {
            OptimizerAlertManager.getInstance().log(OptimizerAlertType.SPM_ERR, ec, throwable);
        } catch (Exception e) {
            // ignore
        }
    }

    public static void statisticsAlert(String schema, String table, OptimizerAlertType type, ExecutionContext ec, Object obj) {
        Map<String, Object> extraMap = new HashMap<>();
        extraMap.put("schema", schema);
        extraMap.put("table", table);
        if (obj instanceof Throwable){
            obj = ExceptionUtils.exceptionStackTrace((Throwable) obj);
        }
        OptimizerAlertManager.getInstance().log(type, ec, obj, extraMap);
    }

    /**
     * check if any statistic info(both topn&histogram) missing
     */
    public static void checkStatisticsMiss(String schema, String table, StatisticManager.CacheLine c, int sampleRowSize) {
        if (SystemDbHelper.isDBBuildIn(schema)) {
            return;
        }
        // if table row count == 0 ,skip check
        if (c.getRowCount() == 0L ||
                (c.getRowCount() < InstConfUtil.getInt(ConnectionParams.STATISTICS_MISS_MIN_ROWCOUNT) && sampleRowSize == 0)) {
            return;
        }
        List<ColumnMeta> columnMetas = StatisticUtils.getColumnMetas(false, schema, table);
        if (columnMetas == null || columnMetas.isEmpty()) {
            return;
        }
        for (ColumnMeta cm : columnMetas) {
            String col = cm.getName().toLowerCase();
            DataType dataType = cm.getDataType();
            if (c.getAllNullCols().contains(col)) {
                continue;
            }
            // check if any statistic missing
            boolean topNNull = c.getTopN(col) == null;
            Histogram histogram = c.getHistogramMap() == null ? null : c.getHistogramMap().get(col);
            boolean histogramNull = false;
            if (!DataTypeUtil.isChar(dataType) && (histogram == null || histogram.getBuckets().size() == 0)) {
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
                OptimizerAlertUtil.statisticsAlert(schema, table, OptimizerAlertType.STATISTIC_MISS, null, null);
            }
        }
    }

    /**
     * statistic job interrupted by error
     */
    public static void statisticErrorAlert() {
        OptimizerAlertManager.getInstance().log(OptimizerAlertType.STATISTIC_JOB_INTERRUPT, null);
    }

    /**
     * Inconsistent statistical information data among nodes/metadb.
     */
    public static void statisticInconsistentAlert() {
        //OptimizerAlertManager.getInstance().log(OptimizerAlertType.STATISTIC_INCONSISTENT, null);
    }

    private static boolean bkaShouldAlert(ExecutionContext ec, LogicalView logicalView, long phySqlCount) {
        // if ENABLE_ALERT_TEST_DEFAULT is false, only alert according to ENABLE_ALERT_TEST
        if (!ec.getParamManager().getBoolean(ConnectionParams.ENABLE_ALERT_TEST_DEFAULT)) {
            return ec.getParamManager().getBoolean(ConnectionParams.ENABLE_ALERT_TEST);
        }
        if (SystemDbHelper.isDBBuildIn(ec.getSchemaName())) {
            return false;
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

        if (SystemDbHelper.isDBBuildIn(ec.getSchemaName())) {
            return false;
        }

        // don't alert with node hint or direct plan
        if ((ec.getFinalPlan() != null) && (!ec.getFinalPlan().isCheckTpSlow())) {
            return false;
        }

        // if ENABLE_ALERT_TEST_DEFAULT is false, only alert according to ENABLE_ALERT_TEST
        if (!ec.getParamManager().getBoolean(ConnectionParams.ENABLE_ALERT_TEST_DEFAULT)) {
            return ec.getParamManager().getBoolean(ConnectionParams.ENABLE_ALERT_TEST);
        }

        if (!ec.getParamManager().getBoolean(ConnectionParams.ENABLE_TP_SLOW_ALERT)) {
            return false;
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

    private static boolean xplanShouldAlert(ExecutionContext ec, double executeTimeMs, long lastAffectedRows) {
        XplanStat xplanStat = ec.getXplanStat();
        if (xplanStat == null) {
            return false;
        }
        if (XplanStat.getXplanIndex(xplanStat) == null) {
            return false;
        }
        if (SystemDbHelper.isDBBuildIn(ec.getSchemaName())) {
            return false;
        }
        if (SystemDbHelper.isDBBuildIn(ec.getSchemaName())) {
            return false;
        }
        // if ENABLE_ALERT_TEST_DEFAULT is false, only alert according to ENABLE_ALERT_TEST
        if (!ec.getParamManager().getBoolean(ConnectionParams.ENABLE_ALERT_TEST_DEFAULT)) {
            return ec.getParamManager().getBoolean(ConnectionParams.ENABLE_ALERT_TEST);
        }

        // fast pass to avoid alert
        if (executeTimeMs <= XPLAN_ALERT_THRESHOLD) {
            return false;
        }

        // don't alert if failed to execute
        if (lastAffectedRows < 0) {
            return false;
        }
        long threshold = ec.getParamManager().getLong(ConnectionParams.SLOW_SQL_TIME);
        if (executeTimeMs > threshold &&
            (XplanStat.getExaminedRowCount(xplanStat) > lastAffectedRows * 100)) {
            return true;
        }
        return false;
    }

    private static boolean planCacheShouldAlert(ExecutionContext ec) {
        // if ENABLE_ALERT_TEST_DEFAULT is false, only alert according to ENABLE_ALERT_TEST
        if (!ec.getParamManager().getBoolean(ConnectionParams.ENABLE_ALERT_TEST_DEFAULT)) {
            return ec.getParamManager().getBoolean(ConnectionParams.ENABLE_ALERT_TEST);
        }
        if (SystemDbHelper.isDBBuildIn(ec.getSchemaName())) {
            return false;
        }
        if (PlanCache.getInstance().getCache().size() <= PLAN_CACHE_ALERT_THRESHOLD) {
            return false;
        }

        if (PlanCache.getInstance().getCache().size() >= PlanCache.getInstance().getCurrentCapacity() - 20) {
            return true;
        }
        return false;
    }
}
