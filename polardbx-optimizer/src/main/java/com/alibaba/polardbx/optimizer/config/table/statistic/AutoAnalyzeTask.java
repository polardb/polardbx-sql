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

package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticCollector;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.getColumnMetas;

public class AutoAnalyzeTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger("statistics");

    private int autoAnalyzePeriodInHours;

    private long nextTime;

    private AutoAnalyzeState state;

    private String analyzingTableName;

    private String schemaName;

    private StatisticLogInfo statisticLogInfo;

    private StatisticCollector statisticCollector;

    public AutoAnalyzeTask(String schemaName, StatisticLogInfo statisticLogInfo, long delay,
                           int autoAnalyzePeriodInHours, StatisticCollector statisticCollector) {
        this.autoAnalyzePeriodInHours = autoAnalyzePeriodInHours;
        OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
        String startTime = optimizerContext.getParamManager().getString(
            ConnectionParams.BACKGROUND_STATISTIC_COLLECTION_START_TIME);
        String endTime = optimizerContext.getParamManager().getString(
            ConnectionParams.BACKGROUND_STATISTIC_COLLECTION_END_TIME);
        Timestamp nextRunningTime = new Timestamp(System.currentTimeMillis());
        long now = System.currentTimeMillis();
        GeneralUtil.shouldRunAtThatTime(now + delay, startTime, endTime, nextRunningTime);
        this.nextTime = nextRunningTime.getTime();
        if (this.nextTime > now) {
            state = AutoAnalyzeState.WAITING;
        } else {
            state = AutoAnalyzeState.RUNNING;
        }
        this.schemaName = schemaName;
        this.statisticLogInfo = statisticLogInfo;
        this.statisticCollector = statisticCollector;
    }

    public Timestamp nextRunningTime() {
        return new Timestamp(nextTime);
    }

    public boolean isRunning() {
        return state == AutoAnalyzeState.RUNNING;
    }

    public String getAnalyzingTableName() {
        return analyzingTableName;
    }

    @Override
    public void run() {
        try {
            OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
            MDC.put(MDC.MDC_KEY_APP, schemaName.toLowerCase());
            if (optimizerContext.getParamManager()
                .getBoolean(ConnectionParams.ENABLE_BACKGROUND_STATISTIC_COLLECTION)) {
                int autoAnalyzeTableLimit =
                    optimizerContext.getParamManager().getInt(ConnectionParams.AUTO_ANALYZE_ALL_COLUMN_TABLE_LIMIT);
                int autoAnalyzeTableSleepMills =
                    optimizerContext.getParamManager().getInt(ConnectionParams.AUTO_ANALYZE_TABLE_SLEEP_MILLS);

                String startTime = optimizerContext.getParamManager().getString(
                    ConnectionParams.BACKGROUND_STATISTIC_COLLECTION_START_TIME);
                String endTime = optimizerContext.getParamManager().getString(
                    ConnectionParams.BACKGROUND_STATISTIC_COLLECTION_END_TIME);

                if (!ConfigDataMode.isMasterMode()) { /** must be master mode */
                    return;
                }

                if (!TddlNode.isCurrentNodeMaster()) { /** choose a master node */
                    return;
                }
                long start = System.currentTimeMillis();
                Set<String> logicalTableSet =
                    OptimizerContext.getContext(schemaName).getStatisticManager().getTableNamesCollected();
                boolean onlyAnalyzeColumnWithIndex = true;
                if (logicalTableSet.size() <= autoAnalyzeTableLimit) {
                    onlyAnalyzeColumnWithIndex = false;
                }
                long now = System.currentTimeMillis();
                for (String logicalTableName : logicalTableSet) {
                    Timestamp nextRunningTime = new Timestamp(System.currentTimeMillis());
                    while (!GeneralUtil.shouldRunAtThatTime(now, startTime, endTime, nextRunningTime)) {
                        Thread.sleep(1 * 60 * 1000); // sleep 1 min
                        nextTime = nextRunningTime.getTime();
                        state = AutoAnalyzeState.WAITING;
                    }
                    analyzingTableName = logicalTableName;
                    state = AutoAnalyzeState.RUNNING;
                    long startPerTable = System.currentTimeMillis();
                    analyzeTable(schemaName, logicalTableName, onlyAnalyzeColumnWithIndex, statisticCollector);
                    long endPerTable = System.currentTimeMillis();
                    String msg = "auto analyze " + logicalTableName + " consuming "
                        + (endPerTable - startPerTable) / 1000.0 + " seconds";
                    logger.info(msg);
                    statisticLogInfo.add(msg);
                    if (autoAnalyzeTableSleepMills > 0) {
                        Thread.sleep(autoAnalyzeTableSleepMills);
                    }
                }
                long end = System.currentTimeMillis();
                String msg = "auto analyze " + logicalTableSet.size() + " tables statistics consuming "
                    + (end - start) / 1000.0 + " seconds";
                logger.info(msg);
                statisticLogInfo.add(msg);
            }
        } catch (Throwable e) {
            logger.error(e.getMessage());
            statisticLogInfo.add("auto analyze error " + e.getMessage());
        } finally {
            state = AutoAnalyzeState.WAITING;
            nextTime = System.currentTimeMillis() + autoAnalyzePeriodInHours * 3600 * 1000;
        }
    }

    public static void analyzeTable(String schemaName, String logicalTableName,
                                    boolean onlyAnalyzeColumnWithIndex,
                                    StatisticCollector statisticCollector) {
        List<ColumnMeta> analyzeColumnList = getColumnMetas(onlyAnalyzeColumnWithIndex, schemaName, logicalTableName);

        if (analyzeColumnList == null || analyzeColumnList.isEmpty()) {
            return;
        }
        statisticCollector.analyzeColumns(
            logicalTableName,
            analyzeColumnList,
            OptimizerContext.getContext(schemaName).getParamManager());
    }
}

enum AutoAnalyzeState {
    RUNNING,
    WAITING
}