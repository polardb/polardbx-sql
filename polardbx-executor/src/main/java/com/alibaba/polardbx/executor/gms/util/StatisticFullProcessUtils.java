package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.gms.module.StatisticModuleLogUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.alibaba.polardbx.stats.metric.FeatureStats;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_START;
import static com.alibaba.polardbx.stats.metric.FeatureStatsItem.SAMPLE_TASK_FAIL;
import static com.alibaba.polardbx.stats.metric.FeatureStatsItem.SAMPLE_TASK_SUCC;

/**
 * 统计信息整体任务: 1. collect statistic 2. analyze table 3. 定时统计信息任务
 *
 * 一般由统计信息子任务调用组成，虽然所有的子任务调用失败都进行了statistic alert，但是统计信息整体任务的代码中也可能出现非预期异常
 * 目前对于手动执行的collect statistic 、 analyze table都没有进行拦截报警
 * 但是对于定时任务重的统计信息都需要报警
 */
public class StatisticFullProcessUtils {

    /**
     * 1. rowcount
     * 2. sample
     * 3. sketch (hll, persist, sync)
     * 4. persist
     * 5. sync
     * @param schema
     * @param logicalTableName
     * @param errMsg
     * @param ec
     * @return
     */
    public static boolean forceAnalyzeColumnsDdl(String schema, String logicalTableName, List<String> errMsg,
                                                 ExecutionContext ec) {
        try {
            long startNanos = System.nanoTime();
            // to lower case
            schema = schema.toLowerCase();
            logicalTableName = logicalTableName.toLowerCase();

            // check table if exists
            if (OptimizerContext.getContext(schema).getLatestSchemaManager()
                .getTableWithNull(logicalTableName) == null) {
                errMsg.add("FAIL skip tables that not exists:" + schema + "," + logicalTableName);
                return false;
            }

            StatisticSubProcessUtils.collectRowCount(schema, logicalTableName, ec);
            long endNanos = System.nanoTime();
            StatisticUtils.logger.info(String.format("Collecting row count of %s.%s consumed %.2fs",
                schema, logicalTableName, (endNanos - startNanos) / 1_000_000_000D));

            startNanos = endNanos;
            StatisticSubProcessUtils.sampleTableDdl(schema, logicalTableName, ec);
            endNanos = System.nanoTime();
            StatisticUtils.logger.info(String.format("Sampling %s.%s consumed %.2fs",
                schema, logicalTableName, (endNanos - startNanos) / 1_000_000_000D));

            startNanos = endNanos;

            StatisticSubProcessUtils.sketchTableDdl(schema, logicalTableName,
                !ec.getParamManager().getBoolean(ConnectionParams.ANALYZE_TEST_UPDATE), ec);
            endNanos = System.nanoTime();
            StatisticUtils.logger.info(String.format("HLL sketch of %s.%s consumed %.2fs",
                schema, logicalTableName, (endNanos - startNanos) / 1_000_000_000D));

            /** persist */
            StatisticSubProcessUtils.persistStatistic(schema, logicalTableName, true, ec);
            /** sync other nodes */
            StatisticSubProcessUtils.syncUpdateStatistic(schema, logicalTableName, StatisticManager.getInstance().getCacheLine(schema, logicalTableName), ec);

        } catch (Throwable e) {
            StatisticUtils.logger.error(e);
            errMsg.add("FAIL " + e.getMessage());
            return false;
        }
        errMsg.add("OK");
        return true;
    }

    /**
     * 1. rowcount
     * 2. sample
     * 3. sketch (hll, persist, sync)
     * 4. persist
     * 5. sync
     * @param schema
     * @param logicalTableName
     * @param enableHll
     * @param ec
     * @return
     */
    public static boolean collectStatistic(String schema, String logicalTableName, boolean enableHll,
                                           ExecutionContext ec) {
        try {
            long startNanos = System.nanoTime();
            // check table if exists
            if (OptimizerContext.getContext(schema).getLatestSchemaManager()
                .getTableWithNull(logicalTableName) == null) {
                StatisticUtils.logger.error("FAIL skip tables that not exists:" + schema + "," + logicalTableName);
                return false;
            }

            StatisticSubProcessUtils.collectRowCount(schema, logicalTableName, ec);
            long endNanos = System.nanoTime();
            StatisticUtils.logger.info(String.format("Collecting row count of %s.%s consumed %.2fs",
                schema, logicalTableName, (endNanos - startNanos) / 1_000_000_000D));

            startNanos = endNanos;
            StatisticSubProcessUtils.sampleTableDdl(schema, logicalTableName, ec);
            endNanos = System.nanoTime();
            StatisticUtils.logger.info(String.format("Sampling %s.%s consumed %.2fs",
                schema, logicalTableName, (endNanos - startNanos) / 1_000_000_000D));

            if (enableHll) {
                startNanos = endNanos;
                StatisticSubProcessUtils.sketchTableDdl(schema, logicalTableName,
                    !ec.getParamManager().getBoolean(ConnectionParams.ANALYZE_TEST_UPDATE), ec);
                endNanos = System.nanoTime();
                StatisticUtils.logger.info(String.format("HLL sketch of %s.%s consumed %.2fs",
                    schema, logicalTableName, (endNanos - startNanos) / 1_000_000_000D));
            }

            /** persist */
            StatisticSubProcessUtils.persistStatistic(schema, logicalTableName, true, ec);
            /** sync other nodes */
            StatisticSubProcessUtils.syncUpdateStatistic(schema, logicalTableName, StatisticManager.getInstance().getCacheLine(schema, logicalTableName), ec);

        } catch (Exception e) {
            StatisticUtils.logger.error("collect statistic " + schema + "." + logicalTableName + " FAIL ", e);
            return false;
        }
        return true;
    }

    public static Future<Boolean> collectStatisticConcurrent(String schema, String logicalTableName, boolean enableHll,
                                                             ExecutionContext ec, ExecutorService executorService) {
        if (executorService == null) {
            return CompletableFuture.completedFuture(collectStatistic(schema, logicalTableName, enableHll, ec));
        } else {
            return executorService.submit(() -> collectStatistic(schema, logicalTableName, enableHll, ec));
        }
    }

    /**
     * 1. collect rowcount
     * 2. sample
     * 3. persist
     * 4. sync
     * @param schema
     * @param logicalTableName
     * @return
     */
    public static boolean sampleOneTable(String schema, String logicalTableName) {
        // sample
        StatisticModuleLogUtil.logNormal(PROCESS_START, new String[] {"sample table ", schema + "," + logicalTableName});
        try {
            // check table if exists
            if (OptimizerContext.getContext(schema).getLatestSchemaManager()
                .getTableWithNull(logicalTableName) == null) {
                return false;
            }
            // don't sample oss table
            if (StatisticUtils.isFileStore(schema, logicalTableName)) {
                return false;
            }

            StatisticSubProcessUtils.collectRowCount(schema, logicalTableName, null);
            StatisticSubProcessUtils.sampleTableDdl(schema, logicalTableName, null);

            /** persist */
            StatisticSubProcessUtils.persistStatistic(schema, logicalTableName, true, null);
            /** sync other nodes */
            StatisticSubProcessUtils.syncUpdateStatistic(schema, logicalTableName, StatisticManager.getInstance().getCacheLine(schema, logicalTableName), null);

            FeatureStats.getInstance().increment(SAMPLE_TASK_SUCC);
        } catch (Exception e) {
            StatisticUtils.logger.error(e);
            OptimizerAlertUtil.statisticErrorAlert();
            FeatureStats.getInstance().increment(SAMPLE_TASK_FAIL);
            return false;
        }
        return true;
    }
}
