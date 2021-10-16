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

package com.alibaba.polardbx.stats;

import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.stats.MatrixStatistics.STATISCTYPE.ADDROWS;
import static com.alibaba.polardbx.stats.MatrixStatistics.STATISCTYPE.CONNERROR;
import static com.alibaba.polardbx.stats.MatrixStatistics.STATISCTYPE.PHYSICALREAD;
import static com.alibaba.polardbx.stats.MatrixStatistics.STATISCTYPE.PHYSICALWRITE;
import static com.alibaba.polardbx.stats.MatrixStatistics.STATISCTYPE.READTIME;
import static com.alibaba.polardbx.stats.MatrixStatistics.STATISCTYPE.SQLERROR;
import static com.alibaba.polardbx.stats.MatrixStatistics.STATISCTYPE.WRITETIME;

public class MatrixStatistics {

    public static long STATS_TIME_PERIOD = 5 * 1000;
    public static long STC_TIME_PERIOD = 60 * 1000;
    public static final List<MatrixStatistics> statsNeedRecordPrevious = new ArrayList<MatrixStatistics>();
    public static final ScheduledExecutorService scheduler =
        Executors.newScheduledThreadPool(1, new NamedThreadFactory("PXC-Stats", true));
    public static final MatrixStatistics EMPTY = new MatrixStatistics(false);

    static {
        STATS_TIME_PERIOD = 5 * 1000;

        if (System.getProperty("STATS_TIME_PERIOD") != null) {
            STATS_TIME_PERIOD = Long.valueOf(System.getProperty("STATS_TIME_PERIOD"));
        }

        scheduler.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                doPreviosStats();
            }
        }, STATS_TIME_PERIOD, STATS_TIME_PERIOD, TimeUnit.MILLISECONDS);
    }

    enum STATISCTYPE {
        PHYSICALREAD, PHYSICALWRITE, SQLERROR, CONNERROR, READTIME, WRITETIME, ADDROWS
    }

    public MatrixStatistics previosStatistics = null;
    private MatrixStatistics tempStatistics = null;

    private static Map<String, AppStatistics> appStatisticsMap = new ConcurrentHashMap<>();
    private static Map<String, AppStatistics> currentAppStatisicsMap = new ConcurrentHashMap<>();

    private static long currentRound = 0L;
    public static long currentRoundTime = 0L;

    public long recordTime = System.currentTimeMillis();

    public long request = 0;

    public static AtomicLong requestAllDB = new AtomicLong(0);

    public long errorCount = 0;

    public long integrityConstraintViolationErrorCount = 0;

    public long multiDBCount = 0;

    public long tempTableCount = 0;

    public long joinMultiDBCount = 0;

    public long aggregateMultiDBCount = 0;

    public long hintCount = 0;

    public AtomicLong physicalRequest = new AtomicLong();

    public long slowRequest = 0;

    public long physicalSlowRequest = 0;

    public long netIn = 0;

    public long netOut = 0;

    public long query = 0;

    public long delete = 0;

    public long insert = 0;

    public long update = 0;

    public long replace = 0;

    public long timeCost = 0L;

    public long cclKill = 0L;

    public long cclWait = 0L;

    public long cclReschedule = 0L;

    public long cclWaitKill = 0L;

    public long cclRun = 0L;

    public long tpLoad = 0L;

    public long apLoad = 0L;

    public long local = 0L;

    public long cluster = 0L;

    public AtomicLong activeConnection = new AtomicLong(0);

    public AtomicLong connectionCount = new AtomicLong(0);

    public AtomicLong backfillRows = new AtomicLong(0);

    public AtomicLong checkedRows = new AtomicLong(0);

    private final TransactionStatistics transactionStats = new TransactionStatistics();

    public AtomicLong physicalTimeCost = new AtomicLong(0);

    public MatrixStatistics() {
        this(true);
    }

    public MatrixStatistics(boolean recordPrevious) {
        if (recordPrevious) {
            this.previosStatistics = new MatrixStatistics(false);
            this.tempStatistics = new MatrixStatistics(false);
            statsNeedRecordPrevious.add(this);
        }
    }

    public MatrixStatistics getPreviosStatistics() {
        return previosStatistics;
    }

    private static AppStatistics getAppStatistics(String dbName, String appName) {
        AppStatistics stat = appStatisticsMap.get(appName);
        if (stat == null) {
            AppStatistics newStat = new AppStatistics(dbName, appName);
            stat = appStatisticsMap.putIfAbsent(appName, newStat);
            return (stat == null) ? newStat : stat;
        }
        return stat;
    }

    private static AppStatistics getCurrentAppStatistics(String dbName, String appName) {
        AppStatistics stat = currentAppStatisicsMap.get(appName);
        if (stat == null) {
            AppStatistics newStat = new AppStatistics(dbName, appName);
            stat = currentAppStatisicsMap.putIfAbsent(appName, newStat);
            return (stat == null) ? newStat : stat;
        }
        return stat;
    }

    private static void statis(String appName, String groupName, String dbKey, long count, long sqlLength,
                               STATISCTYPE statisctype) {
        AppStatistics appStatistics = getAppStatistics("", appName);

        staticStat(groupName, dbKey, count, sqlLength, statisctype, appStatistics);

        AppStatistics lastAppStatistics = getCurrentAppStatistics("", appName);

        staticStat(groupName, dbKey, count, sqlLength, statisctype, lastAppStatistics);
    }

    private static void staticStat(String groupName, String dbKey, long count, long sqlLength,
                                   MatrixStatistics.STATISCTYPE statisctype, AppStatistics appStatistics) {
        switch (statisctype) {
        case PHYSICALREAD:
            appStatistics.addPhysicalReadRequestPerAtom(groupName, dbKey, count, sqlLength);
            break;
        case SQLERROR:
            appStatistics.addSqlErrorCount(groupName, dbKey, count);
            break;
        case CONNERROR:
            appStatistics.addConnErrorCount(groupName, dbKey, count);
            break;
        case READTIME:
            appStatistics.addReadTimeCost(groupName, dbKey, count);
            break;
        case WRITETIME:
            appStatistics.addWriteTimeCost(groupName, dbKey, count);
            break;
        case ADDROWS:
            appStatistics.addRows(groupName, dbKey, count);
            break;
        case PHYSICALWRITE:
            appStatistics.addPhysicalWriteRequestPerAtom(groupName, dbKey, count, sqlLength);
            break;
        default:

        }
    }

    public static void addPhysicalReadRequestPerAtom(String appName, String groupName, String dbKey, long count,
                                                     long sqlLength) {
        if (StringUtils.isEmpty(appName) || StringUtils.isEmpty(groupName) || StringUtils.isEmpty(dbKey)
            || count <= 0) {
            return;
        }

        statis(appName, groupName, dbKey, count, sqlLength, PHYSICALREAD);
    }

    public static void addSqlErrorCount(String appName, String groupName, String dbKey, long count) {
        if (StringUtils.isEmpty(appName) || StringUtils.isEmpty(groupName) || StringUtils.isEmpty(dbKey)
            || count <= 0) {
            return;
        }

        statis(appName, groupName, dbKey, count, 0L, SQLERROR);
    }

    public static void addConnErrorCount(String appName, String groupName, String dbKey, long count) {
        if (StringUtils.isEmpty(appName) || StringUtils.isEmpty(groupName) || StringUtils.isEmpty(dbKey)
            || count <= 0) {
            return;
        }

        statis(appName, groupName, dbKey, count, 0L, CONNERROR);
    }

    public static void addReadTimeCost(String appName, String groupName, String dbKey, long cost) {
        if (StringUtils.isEmpty(appName) || StringUtils.isEmpty(groupName) || StringUtils.isEmpty(dbKey) || cost <= 0) {
            return;
        }

        statis(appName, groupName, dbKey, cost, 0L, READTIME);
    }

    public static void addWriteTimeCost(String appName, String groupName, String dbKey, long cost) {
        if (StringUtils.isEmpty(appName) || StringUtils.isEmpty(groupName) || StringUtils.isEmpty(dbKey) || cost <= 0) {
            return;
        }

        statis(appName, groupName, dbKey, cost, 0L, WRITETIME);
    }

    public static void addRows(String appName, String groupName, String dbKey, long row) {
        if (StringUtils.isEmpty(appName) || StringUtils.isEmpty(groupName) || StringUtils.isEmpty(dbKey) || row < 0) {
            return;
        }

        statis(appName, groupName, dbKey, row, 0L, ADDROWS);
    }

    public static void addPhysicalWriteRequestPerAtom(String appName, String groupName, String dbKey, long count,
                                                      long sqlLength) {
        if (StringUtils.isEmpty(appName) || StringUtils.isEmpty(groupName) || StringUtils.isEmpty(dbKey)
            || count <= 0) {
            return;
        }

        statis(appName, groupName, dbKey, count, sqlLength, PHYSICALWRITE);
    }

    public void recordPhysicalTimeCost(long num) {
        if (num <= 0) {
            return;
        }
        physicalTimeCost.addAndGet(num);
    }

    private static void doPreviosStats() {
        int schemaSize = statsNeedRecordPrevious.size();
        for (int i = 0; i < schemaSize; i++) {
            MatrixStatistics stats = statsNeedRecordPrevious.get(i);

            assign(stats.previosStatistics, stats.tempStatistics);
            assign(stats.tempStatistics, stats);
            stats.tempStatistics.recordTime = System.currentTimeMillis();
        }

        if (System.currentTimeMillis() / STC_TIME_PERIOD != currentRound) {
            currentRound = System.currentTimeMillis() / STC_TIME_PERIOD;
        } else {
            return;
        }

        for (AppStatistics as : currentAppStatisicsMap.values()) {
            as.clear();
        }
        currentRoundTime = System.currentTimeMillis();
    }

    private static void assign(MatrixStatistics to, MatrixStatistics from) {
        to.activeConnection = new AtomicLong(from.activeConnection.get());
        to.aggregateMultiDBCount = from.aggregateMultiDBCount;
        to.connectionCount = new AtomicLong(from.connectionCount.get());
        to.delete = from.delete;
        to.errorCount = from.errorCount;
        to.hintCount = from.hintCount;
        to.insert = from.insert;
        to.integrityConstraintViolationErrorCount = from.integrityConstraintViolationErrorCount;
        to.joinMultiDBCount = from.joinMultiDBCount;
        to.multiDBCount = from.multiDBCount;
        to.netIn = from.netIn;
        to.netOut = from.netOut;
        to.physicalRequest = new AtomicLong(from.physicalRequest.get());
        to.physicalTimeCost = new AtomicLong(from.physicalTimeCost.get());
        to.query = from.query;
        to.replace = from.replace;
        to.request = from.request;
        to.tempTableCount = from.tempTableCount;
        to.timeCost = from.timeCost;
        to.update = from.update;
        to.recordTime = from.recordTime;
        to.slowRequest = from.slowRequest;
        to.physicalSlowRequest = from.physicalSlowRequest;
        to.backfillRows = new AtomicLong(from.backfillRows.get());
        to.checkedRows = new AtomicLong(from.checkedRows.get());
    }

    public static List<List<Object>> getStcInfo() {
        List<List<Object>> rlist = new ArrayList<List<Object>>();
        for (AppStatistics as : appStatisticsMap.values()) {
            as.toObjects(rlist);
        }
        return rlist;
    }

    public static List<List<Object>> getCurrentStcInfo() {
        if (currentAppStatisicsMap == null) {
            return Collections.emptyList();
        }
        List<List<Object>> rlist = new ArrayList<List<Object>>();
        for (AppStatistics as : currentAppStatisicsMap.values()) {
            as.toObjects(rlist);
        }
        return rlist;
    }

    public static void addApp(String schema, String appName) {
        if (appName == null) {
            return;
        }

        getAppStatistics(schema, appName);
        getCurrentAppStatistics(schema, appName);
    }

    public static void setApp(String schema, String appName) {
        if (appName == null || schema == null) {
            return;
        }

        AppStatistics appStatistics = appStatisticsMap.get(appName);
        if (appStatistics != null && StringUtils.isEmpty(appStatistics.getDbName())) {
            appStatistics.setDbName(schema);
        }

        AppStatistics currentAppStatistics = currentAppStatisicsMap.get(appName);
        if (currentAppStatistics != null && StringUtils.isEmpty(currentAppStatistics.getDbName())) {
            currentAppStatistics.setDbName(schema);
        }
    }

    public static void addGroup(String appName, String groupName) {
        if (appName == null || groupName == null) {
            return;
        }

        getAppStatistics("", appName).addGroup(groupName);
        getCurrentAppStatistics("", appName).addGroup(groupName);
    }

    public static void addAtom(String appName, String groupName, String atomName, String ipPort) {
        if (appName == null || groupName == null || atomName == null) {
            return;
        }

        getAppStatistics("", appName).addGroup(groupName).addAtom(atomName, ipPort);
        getCurrentAppStatistics("", appName).addGroup(groupName).addAtom(atomName, ipPort);
    }

    public static void setAtom(String appName, String groupName, String atomName, String ipPort) {
        if (appName == null || groupName == null || atomName == null || ipPort == null) {
            return;
        }

        if (appStatisticsMap.get(appName) == null) {
            addApp(null, appName);
        }
        if (appStatisticsMap.get(appName).getGroup(groupName) == null) {
            addGroup(appName, groupName);
        }
        synchronized (groupName) {
            AtomStatistics atomStatistics = appStatisticsMap.get(appName).getGroup(groupName).getAtom(atomName);
            if (atomStatistics == null) {
                appStatisticsMap.get(appName).getGroup(groupName).addAtom(atomName, ipPort);
                currentAppStatisicsMap.get(appName).getGroup(groupName).addAtom(atomName, ipPort);
            } else if (StringUtils.isEmpty(atomStatistics.getIpPort())) {
                atomStatistics.setIpPort(ipPort);
                AtomStatistics currentAtomStatistics = currentAppStatisicsMap.get(appName)
                    .getGroup(groupName)
                    .getAtom(atomName);
                if (StringUtils.isEmpty(currentAtomStatistics.getIpPort())) {
                    currentAtomStatistics.setIpPort(ipPort);
                }
            }
        }
    }

    public static void removeAtom(String appName, String groupName, String atomName) {
        if (appName == null || groupName == null || atomName == null) {
            return;
        }

        getAppStatistics("", appName).addGroup(groupName).removeAtom(atomName);
        getCurrentAppStatistics("", appName).addGroup(groupName).removeAtom(atomName);
    }

    public TransactionStatistics getTransactionStats() {
        return transactionStats;
    }

    public static void removeSchema(MatrixStatistics matrixStatistics, String appName) {
        statsNeedRecordPrevious.remove(matrixStatistics);
        appStatisticsMap.remove(appName);
        currentAppStatisicsMap.remove(appName);
    }

}
