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

package com.alibaba.polardbx.executor.corrector;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class Reporter implements CheckerCallback {

    private static final Logger logger = LoggerFactory.getLogger(Reporter.class);

    public final long earlyFailNumber;

    private final List<CheckerManager.CheckerReport> checkerReports = Collections.synchronizedList(new ArrayList<>());
    private final List<CheckerManager.CheckerReport> resultReports = new ArrayList<>();
    private final AtomicLong primaryCounter = new AtomicLong(0);
    private final AtomicLong gsiCounter = new AtomicLong(0);

    public Reporter(long earlyFailNumber) {
        this.earlyFailNumber = earlyFailNumber;
    }

    public List<CheckerManager.CheckerReport> getCheckerReports() {
        return resultReports; // Return this to prevent concurrent modify.
    }

    @Override
    public AtomicLong getPrimaryCounter() {
        return primaryCounter;
    }

    @Override
    public AtomicLong getGsiCounter() {
        return gsiCounter;
    }

    public static String pkString(Checker checker, List<Pair<ParameterContext, byte[]>> row) {
        return '(' + checker.getPrimaryKeys()
            .stream()
            .mapToInt(idx -> idx)
            .mapToObj(idx -> String.valueOf(row.get(idx).getKey().getArgs()[1]))
            .collect(Collectors.joining(",")) + ')';
    }

    public static String detailString(Checker checker, List<Pair<ParameterContext, byte[]>> primaryRow,
                                      List<Pair<ParameterContext, byte[]>> gsiRow) {
        final Map<String, Map<String, Object>> details = new HashMap<>();
        if (primaryRow != null) {
            Map<String, Object> primaryData = new HashMap<>();
            for (int i = 0; i < checker.getIndexColumns().size(); ++i) {
                primaryData.put(checker.getIndexColumns().get(i), primaryRow.get(i).getKey().getArgs()[1]);
            }
            details.put("Primary", primaryData);
        }
        if (gsiRow != null) {
            Map<String, Object> gsiData = new HashMap<>();
            for (int i = 0; i < checker.getIndexColumns().size(); ++i) {
                gsiData.put(checker.getIndexColumns().get(i), gsiRow.get(i).getKey().getArgs()[1]);
            }
            details.put("GSI", gsiData);
        }
        return JSON.toJSONString(details);
    }

    @Override
    public void start(ExecutionContext baseEc, Checker checker) {
        // Insert start mark.
        checker.getManager()
            .insertReports(ImmutableList.of(CheckerManager.CheckerReport
                .create(checker, "", "", "START", CheckerManager.CheckerReportStatus.START, "--", "--", "Reporter.")));
    }

    @Override
    public boolean batch(String logTbName, String dbIndex, String phyTable, ExecutionContext selectEc, Checker checker,
                         boolean primaryToGsi, List<List<Pair<ParameterContext, byte[]>>> baseRows,
                         List<List<Pair<ParameterContext, byte[]>>> checkRows) {

        final List<CheckerManager.CheckerReport> tmpResult = new ArrayList<>();

        // Check bad shard.
        for (List<Pair<ParameterContext, byte[]>> row : baseRows) {
            if (!checker.checkShard(dbIndex, phyTable, selectEc, primaryToGsi, row)) {
                if (checkerReports.size() + tmpResult.size() >= earlyFailNumber) {
                    break;
                }

                tmpResult.add(CheckerManager.CheckerReport.create(checker,
                    dbIndex,
                    phyTable,
                    "ERROR_SHARD",
                    CheckerManager.CheckerReportStatus.FOUND,
                    pkString(checker, row),
                    detailString(checker, primaryToGsi ? row : null, primaryToGsi ? null : row),
                    "Reporter."));
            }
        }

        final AtomicInteger errorCount = new AtomicInteger(0);
        // Check diff.
        checker.compareRows(baseRows, checkRows, primaryToGsi ? (primaryRow, indexRow) -> {
            errorCount.getAndIncrement();
            if (checkerReports.size() + tmpResult.size() >= earlyFailNumber) {
                return false;
            }

            final String type;
            final String details;
            if (null == indexRow) {
                type = "MISSING";
                details = detailString(checker, primaryRow, null);
            } else if (null == primaryRow) {
                type = "ORPHAN";
                details = detailString(checker, null, indexRow);
            } else {
                type = "CONFLICT";
                details = detailString(checker, primaryRow, indexRow);
            }
            tmpResult.add(CheckerManager.CheckerReport.create(checker,
                dbIndex,
                phyTable,
                type,
                CheckerManager.CheckerReportStatus.FOUND,
                null == primaryRow ? pkString(checker, indexRow) : pkString(checker, primaryRow),
                details,
                "Reporter.",
                // Only no lock with recheck context.
                checker.getPrimaryLock() == SqlSelect.LockMode.UNDEF && null != primaryRow ?
                    new RecheckContext(dbIndex, phyTable, true, primaryRow) : null));

            return true;
        } : (indexRow, primaryRow) -> {
            errorCount.getAndIncrement();
            if (checkerReports.size() + tmpResult.size() >= earlyFailNumber) {
                return false;
            }

            final String type;
            final String details;
            if (null == primaryRow) {
                type = "ORPHAN";
                details = detailString(checker, null, indexRow);
            } else {
                type = null;
                details = null;
            }

            if (details != null) {
                tmpResult.add(CheckerManager.CheckerReport.create(checker,
                    dbIndex,
                    phyTable,
                    type,
                    CheckerManager.CheckerReportStatus.FOUND,
                    pkString(checker, indexRow),
                    details,
                    "Reporter.",
                    // Only no lock with recheck context.
                    checker.getGsiLock() == SqlSelect.LockMode.UNDEF ? new RecheckContext(dbIndex,
                        phyTable,
                        false,
                        indexRow) : null));
            }

            return true;
        });

        // Commit if hold lock.
        if ((primaryToGsi ? checker.getPrimaryLock() : checker.getGsiLock()) != SqlSelect.LockMode.UNDEF) {
            try {
                selectEc.getTransaction().commit();
            } catch (Exception e) {
                logger.error("Close extract statement failed!", e);
                return false; // Retry this batch.
            }
            // No false positive so put it into checker reports system table directly.
            checker.getManager().insertReports(tmpResult);
        } // Or we will recheck in finish and put correct result in reports.

        // For debug.
        final String dbgInfo = selectEc.getParamManager().getString(ConnectionParams.GSI_DEBUG);
        if (!TStringUtil.isEmpty(dbgInfo) && dbgInfo.equalsIgnoreCase("recheck") && 0 == errorCount.get()) {
            if (!checker
                .recheckRow(dbIndex, phyTable, selectEc, primaryToGsi, baseRows, SqlSelect.LockMode.SHARED_LOCK)) {
                throw GeneralUtil.nestedException("Bad recheck.");
            }
        }

        // Put tmp batch into checker result list.
        checkerReports.addAll(tmpResult);
        if (checkerReports.size() >= earlyFailNumber) {
            throw GeneralUtil.nestedException("Too many conflicts");
        }

        return true;
    }

    @Override
    public void finish(ExecutionContext baseEc, Checker checker) {
        // Recheck all errors. Copy out and recheck(To prevent concurrent modify when cancel by limit exceeded).
        resultReports.clear();
        synchronized (checkerReports) {
            resultReports.addAll(checkerReports);
        }
        int[] sleepTime_ms = new int[] {0, 100, 300};
        for (int sleepTime : sleepTime_ms) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException ignore) {
            }
            int errorCount = 0;
            for (Iterator<CheckerManager.CheckerReport> it = resultReports.iterator(); it.hasNext(); ) {
                final CheckerManager.CheckerReport report = it.next();
                if (null == report.getExtraContext()) {
                    continue;
                }
                final RecheckContext recheckContext = (RecheckContext) report.getExtraContext();
                // Recheck rows one by one with shard lock.
                if (recheckContext.recheck(baseEc, checker)) {
                    // Correct? Remove it.
                    it.remove();
                } else {
                    ++errorCount;
                }
            }
            if (0 == errorCount) {
                break;
            }
        }

        // Copy back.
        synchronized (checkerReports) {
            checkerReports.clear();
            checkerReports.addAll(resultReports);
        }

        // Write all failed rechecks to reports.
        List<CheckerManager.CheckerReport> finalReports = resultReports.stream()
            .filter(report -> (report.getErrorType().equals("ERROR_SHARD") || report.getExtraContext() != null))
            .collect(Collectors.toList());
        checker.getManager().insertReports(finalReports);

        // Insert finish mark.
        final String finishDetails = "" + primaryCounter + '/' + gsiCounter + " rows checked.";
        checker.getManager()
            .insertReports(ImmutableList.of(CheckerManager.CheckerReport.create(checker,
                "",
                "",
                "SUMMARY",
                CheckerManager.CheckerReportStatus.FINISH,
                "--",
                finishDetails,
                "Reporter.")));
    }

    private static class RecheckContext {

        private String dbIndex;
        private String phyTable;
        private boolean primaryToGsi;
        private List<Pair<ParameterContext, byte[]>> row;

        public RecheckContext(String dbIndex, String phyTable, boolean primaryToGsi,
                              List<Pair<ParameterContext, byte[]>> row) {
            this.dbIndex = dbIndex;
            this.phyTable = phyTable;
            this.primaryToGsi = primaryToGsi;
            this.row = row;
        }

        public String getDbIndex() {
            return dbIndex;
        }

        public void setDbIndex(String dbIndex) {
            this.dbIndex = dbIndex;
        }

        public String getPhyTable() {
            return phyTable;
        }

        public void setPhyTable(String phyTable) {
            this.phyTable = phyTable;
        }

        public boolean isPrimaryToGsi() {
            return primaryToGsi;
        }

        public void setPrimaryToGsi(boolean primaryToGsi) {
            this.primaryToGsi = primaryToGsi;
        }

        public List<Pair<ParameterContext, byte[]>> getRow() {
            return row;
        }

        public void setRow(List<Pair<ParameterContext, byte[]>> row) {
            this.row = row;
        }

        public boolean recheck(ExecutionContext baseEc, Checker checker) {
            return checker.recheckRow(dbIndex,
                phyTable,
                baseEc,
                primaryToGsi,
                ImmutableList.of(row),
                SqlSelect.LockMode.SHARED_LOCK);
        }
    }
}
