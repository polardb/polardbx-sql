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

package com.alibaba.polardbx.executor.statistic.ndv;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableNDVSketchStatistic;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.UpdateStatisticSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.eclipse.jetty.util.StringUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_HLL;
import static com.alibaba.polardbx.executor.statistic.ndv.HyperLogLogUtil.HLL_REGISTERS;
import static com.alibaba.polardbx.executor.statistic.ndv.HyperLogLogUtil.buildReghisto;
import static com.alibaba.polardbx.executor.statistic.ndv.HyperLogLogUtil.buildSketchKey;
import static com.alibaba.polardbx.executor.statistic.ndv.HyperLogLogUtil.estimate;
import static com.alibaba.polardbx.executor.utils.ExecUtils.needSketchInterrupted;
import static com.alibaba.polardbx.gms.module.LogPattern.INTERRUPTED;
import static com.alibaba.polardbx.gms.module.LogPattern.NDV_SKETCH_NOT_READY;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESSING;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_START;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.module.LogPattern.UPDATE_NDV_FOR_EXPIRED;

public class NDVShardSketch {
    private static final Logger logger = LoggerUtil.statisticsLogger;

    public static final double MAX_DIFF_VALUE_RATIO = 0.2D;

    public static final String STATISTIC_SQL =
        "SELECT SUM(CARDINALITY) AS SUM_NDV FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME IN (%1$2s) AND COLUMN_NAME=%2$2s";

    public static final String HYPER_LOG_LOG_SQL = "SELECT HYPERLOGLOG(%1$2s) AS HLL FROM %2$2s";

    public static final String HYPER_LOG_LOG_MUL_COLUMNS_SQL = "SELECT HYPERLOGLOG(concat(%1$2s)) AS HLL FROM %2$2s";

    /**
     * schemaName:table name:columns name
     */
    private final String shardKey;

    /**
     * one shard for one physical table
     */
    private final String[] shardParts;

    /**
     * ndv value from dn statistic view, might not be accurate
     */
    private final long[] dnCardinalityArray;

    /**
     * sketch type: hyper log log only for now
     */
    private final String sketchType;

    /**
     * sketch info update time for every shard
     */
    private long[] gmtUpdate;

    /**
     * sketch info update time, use the max value in gmtUpdate array
     * this is a mem cache, -1L represents inactive state
     */
    private long lastGmtUpdate = -1L;

    /**
     * sketch info create time for every shard
     */
    private long[] gmtCreated;

    private long cardinality = -1;

    public NDVShardSketch(String shardKey, String[] shardParts, long[] dnCardinalityArray, String sketchType,
                          long[] gmtUpdate, long[] gmtCreated) {
        this.shardKey = shardKey;
        this.shardParts = shardParts;
        this.dnCardinalityArray = dnCardinalityArray;
        this.sketchType = sketchType;
        this.gmtUpdate = gmtUpdate;
        this.gmtCreated = gmtCreated;
    }

    /**
     * ndv value computed by full sketch info
     */
    /**
     * get cardinality
     *
     * @return -1 meaning invalid
     */
    public long getCardinality() {
        return cardinality;
    }

    /**
     * check the validity of the shard parts
     * table might change the topology by ddl
     */
    public boolean validityCheck() {
        String[] shardInfo = shardKey.split(":");
        String schemaName = shardInfo[0];
        String tableName = shardInfo[1];
        Map<String, Set<String>> topologyTmp;
        try {
            OptimizerContext op = OptimizerContext.getContext(schemaName);
            if (op == null) {
                return false;
            }
            topologyTmp = op.getLatestSchemaManager().getTable(tableName).getLatestTopology();
        } catch (TableNotFoundException tableNotFoundException) {
            return false;
        }

        Map<String, Set<String>> topology = Maps.newHashMap();

        // build new map to avoid concurrency update problem
        for (Map.Entry<String, Set<String>> entry : topologyTmp.entrySet()) {
            Set<String> phyTbls = Sets.newHashSet();
            entry.getValue().stream().forEach(s -> phyTbls.add(s.toLowerCase()));
            topology.put(entry.getKey().toLowerCase(), phyTbls);
        }

        // build topology map using statistic info
        Map<String, Set<String>> topologyStatistic = Maps.newHashMap();
        for (String shardPart : shardParts) {
            String[] parts = shardPart.split(":");
            String nodeName = parts[0];
            String phyTableNames = parts[1];
            if (topologyStatistic.containsKey(nodeName)) {
                topologyStatistic.get(nodeName).addAll(Arrays.asList(phyTableNames.split(",")));
            } else {
                topologyStatistic.put(nodeName, Sets.newHashSet(Arrays.asList(phyTableNames.split(","))));
            }
        }

        // check if the statistic topology contains full real topology
        for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
            Set<String> physicalTables = topologyStatistic.get(entry.getKey().toLowerCase());
            if (physicalTables == null || !physicalTables.containsAll(entry.getValue())) {
                setCardinality(-1);
                return false;
            }
        }
        return true;
    }

    public long lastModifyTime() {
        if (lastGmtUpdate == -1L) {
            lastGmtUpdate = Arrays.stream(gmtUpdate).max().getAsLong();
        }
        return lastGmtUpdate;
    }

    /**
     * update all shard parts
     */
    public boolean updateAllShardParts() throws SQLException {
        if (!InstConfUtil.getBool(ConnectionParams.ENABLE_HLL)) {
            // just return
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.STATISTICS,
                    INTERRUPTED,
                    new String[] {
                        "ndv sketch " + shardKey,
                        ENABLE_HLL + " is false"
                    },
                    LogLevel.WARNING
                );
            return false;
        }
        String[] shardInfo = shardKey.split(":");
        String schemaName = shardInfo[0];
        String tableName = shardInfo[1];
        String columnNames = shardInfo[2];

        // expired time for x-conn
        int expiredTime = InstConfUtil.getInt(ConnectionParams.STATISTIC_NDV_SKETCH_EXPIRE_TIME);
        long current = System.currentTimeMillis();
        long sketchInfoTime = 0;
        long cardinalityTime = 0;
        boolean hasUpdated = false;

        // record update shard parts and bytes for cal new cardinality(CompositeCardinality)
        List<String> updateShardParts = Lists.newLinkedList();
        Map<String, byte[]> updateBytes = Maps.newHashMap();
        for (int i = 0; i < gmtUpdate.length; i++) {
            boolean needUpdate = false;
            if (current - gmtUpdate[i] > expiredTime) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.STATISTICS,
                        UPDATE_NDV_FOR_EXPIRED,
                        new String[] {
                            shardKey, shardParts[i], new Date(current).toString(), new Date(gmtUpdate[i]).toString(),
                            expiredTime + ""
                        },
                        LogLevel.NORMAL
                    );
                needUpdate = true;
            }
            if (needUpdate) {
                long start = System.currentTimeMillis();
                long cardinalityTmp = getCurrentCardinality(shardKey, shardParts[i]);
                cardinalityTime += System.currentTimeMillis() - start;
                start = System.currentTimeMillis();
                byte[] bytes = getCurrentHll(shardKey, shardParts[i], false, null);
                if (bytes == null) {
                    // null meaning the hll request is stopped by something
                    ModuleLogInfo.getInstance()
                        .logRecord(
                            Module.STATISTICS,
                            INTERRUPTED,
                            new String[] {"ndv sketch", shardKey + "," + shardParts[i]},
                            LogLevel.NORMAL
                        );
                    continue;
                }
                updateBytes.put(shardParts[i], bytes);
                updateShardParts.add(shardParts[i]);
                dnCardinalityArray[i] = cardinalityTmp;
                gmtUpdate[i] = System.currentTimeMillis();
                SystemTableNDVSketchStatistic.SketchRow sketchRow = new SystemTableNDVSketchStatistic.SketchRow
                    (schemaName, tableName, columnNames, shardParts[i], dnCardinalityArray[i], -1,
                        "HYPER_LOG_LOG");
                sketchRow.setSketchBytes(bytes);
                PolarDbXSystemTableNDVSketchStatistic.getInstance()
                    .batchReplace(new SystemTableNDVSketchStatistic.SketchRow[] {sketchRow});

                sketchInfoTime += System.currentTimeMillis() - start;
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.STATISTICS,
                        PROCESS_END,
                        new String[] {"ndv sketch", shardKey + "," + shardParts[i]},
                        LogLevel.NORMAL
                    );
                hasUpdated = true;
            }
        }
        if (hasUpdated || cardinality < 0) {
            /**
             * update sketch bytes
             */
            int[] registers = new int[HLL_REGISTERS];
            try {
                PolarDbXSystemTableNDVSketchStatistic.getInstance()
                    .loadByTableNameAndColumnName(schemaName, tableName, columnNames, updateBytes, registers);
                /**
                 * compute new cardinality
                 */
                setCardinality(HyperLogLogUtil.reckon(buildReghisto(registers)));

                // record new cardinality
                PolarDbXSystemTableNDVSketchStatistic.getInstance()
                    .updateCompositeCardinality(schemaName, tableName, columnNames, getCardinality());
            } catch (IllegalArgumentException e) {
                if (e.getMessage().contains("sketch bytes not ready yet")) {
                    ModuleLogInfo.getInstance()
                        .logRecord(
                            Module.STATISTICS,
                            NDV_SKETCH_NOT_READY,
                            new String[] {shardKey},
                            LogLevel.NORMAL
                        );
                } else {
                    ModuleLogInfo.getInstance()
                        .logRecord(
                            Module.STATISTICS,
                            UNEXPECTED,
                            new String[] {"update ndv sketch:" + shardKey, e.getMessage()},
                            LogLevel.CRITICAL,
                            e
                        );
                }
            } catch (Exception e) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.STATISTICS,
                        UNEXPECTED,
                        new String[] {"update ndv sketch:" + shardKey, e.getMessage()},
                        LogLevel.CRITICAL,
                        e
                    );
            } finally {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.STATISTICS,
                        PROCESS_END,
                        new String[] {
                            "update ndv sketch:" + shardKey,
                            "update size:" + updateBytes.size() +
                                ", sketchInfo time:" + sketchInfoTime +
                                ", cardinality time:" + cardinalityTime +
                                ", new:" + getCardinality()
                        },
                        LogLevel.NORMAL
                    );
            }
        }
        return hasUpdated;
    }

    /**
     * @param shardKey schemaName:table name:columns name
     * @param shardPart physical node:table name;*
     */
    private static long getCurrentCardinality(String shardKey, String shardPart) {
        String[] shardKeys = shardKey.split(":");
        String schemaName = shardKeys[0];
        String columnsName = shardKeys[2];
        Map<String, Set<String>> shardPartToTopology = shardPartToTopology(shardPart);
        long sum = -1;

        for (Map.Entry<String, Set<String>> entry : shardPartToTopology.entrySet()) {
            String nodeName = entry.getKey();
            Set<String> physicalTables = entry.getValue();
            IDataSource ds = ExecutorContext.getContext(schemaName).getTopologyHandler().get(nodeName).getDataSource();

            String physicalTableNames = "";
            for (String physicalTable : physicalTables) {
                physicalTableNames += "'" + physicalTable + "',";
            }
            if (physicalTableNames.length() > 0) {
                physicalTableNames = physicalTableNames.substring(0, physicalTableNames.length() - 1);
            }

            String sql = String.format(STATISTIC_SQL, physicalTableNames, "'" + columnsName + "'");
            Connection c = null;
            Statement st = null;
            ResultSet rs = null;

            try {
                c = ds.getConnection();
                st = c.createStatement();
                rs = st.executeQuery(sql);
                while (true) {
                    if (!rs.next()) {
                        break;
                    }
                    sum += rs.getLong("SUM_NDV");
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
                if (st != null) {
                    try {
                        st.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
                if (c != null) {
                    try {
                        c.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }
        }
        return sum;
    }

    /**
     * @param shardPart physical node:table name,table name;*
     * @return map group node name -> set<table name>
     */
    public static Map<String, Set<String>> shardPartToTopology(String shardPart) {
        if (shardPart == null || shardPart.isEmpty()) {
            return null;
        }
        Map<String, Set<String>> returnMap = Maps.newHashMap();

        for (String oneNode : shardPart.split(";")) {
            String[] oneNodes = oneNode.split(":");
            String node = oneNodes[0];
            Set<String> tableNames = Sets.newHashSet(oneNodes[1].split(","));
            returnMap.put(node, tableNames);
        }

        return returnMap;
    }

    /**
     * @param topology map group node name -> set<table name>
     * @return shardPart physical node:table name,table name;*
     */
    public static String[] topologyPartToShard(Map<String, Set<String>> topology) {
        int shardNum = topology.entrySet().stream().mapToInt(e -> e.getValue().size()).sum();
        String[] shardParts = new String[shardNum];
        int count = 0;
        for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
            String node = entry.getKey();
            Set<String> physicalTables = entry.getValue();
            for (String table : physicalTables) {
                shardParts[count++] = (node + ":" + table).toLowerCase();
            }
        }

        return shardParts;
    }

    /**
     * build one ndv sketch
     */
    public static NDVShardSketch buildNDVShardSketch(String schemaName, String tableName, String columnName,
                                                     boolean isForce, ExecutionContext ec,
                                                     ThreadPoolExecutor sketchHllExecutor)
        throws SQLException {
        if (!InstConfUtil.getBool(ConnectionParams.ENABLE_HLL)) {
            // just return
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.STATISTICS,
                    INTERRUPTED,
                    new String[] {
                        "ndv sketch " + tableName + "," + columnName,
                        ENABLE_HLL + " is false"
                    },
                    LogLevel.WARNING
                );
            return null;
        }

        ModuleLogInfo.getInstance()
            .logRecord(
                Module.STATISTICS,
                PROCESS_START,
                new String[] {"ndv sketch rebuild:", tableName + "," + columnName},
                LogLevel.NORMAL
            );
        String shardKey = buildSketchKey(schemaName, tableName, columnName);

        // shard parts build
        String[] shardPart = buildShardParts(schemaName, tableName);
        if (shardPart == null) {
            return null;
        }
        long[] dnCardinalityArray = new long[shardPart.length];
        String sketchType = "HYPER_LOG_LOG";
        byte[][] sketchArray = new byte[shardPart.length][];
        long[] gmtUpdate = new long[shardPart.length];
        long[] gmtCreated = new long[shardPart.length];

        AtomicLong sketchTime = new AtomicLong(0);
        AtomicLong cardinalityTime = new AtomicLong(0);
        long cardinality = -1;

        boolean columnar = false;
        // try columnar sketch first
        cardinality = sketchByColumnar(shardKey, shardPart, dnCardinalityArray, sketchArray, gmtCreated, gmtUpdate,
            cardinalityTime, sketchTime, ec);
        if (cardinality >= 0) {
            columnar = true;
        }
        if (cardinality < 0) {
            List<Future> futures = null;
            AtomicBoolean stopped = new AtomicBoolean(false);
            if (sketchHllExecutor != null) {
                futures = new ArrayList<>(shardPart.length);
            }

            // fill cardinality and sketch bytes
            for (int i = 0; i < shardPart.length; i++) {
                if (sketchHllExecutor == null) {
                    sketchOnePart(shardKey, shardPart, dnCardinalityArray, sketchArray, gmtCreated, gmtUpdate,
                        cardinalityTime, sketchTime, isForce, ec, i, stopped);
                } else {
                    final int partIdx = i;
                    Future<?> future = sketchHllExecutor.submit(
                        () -> sketchOnePart(shardKey, shardPart, dnCardinalityArray, sketchArray, gmtCreated, gmtUpdate,
                            cardinalityTime, sketchTime, isForce, ec, partIdx, stopped));
                    futures.add(future);
                }
            }
            if (futures != null) {
                AsyncUtils.waitAll(futures);
            }
            /**
             * sketch data has null meaning it was interrupted for some reason.
             * manual analyze table to force rebuilt it or wait the update job.
             */
            if (isSketchDataReady(sketchArray)) {
                cardinality = estimate(sketchArray);
            }
            if (cardinality < 0) {
                cardinality = 0;
            }
        }

        ModuleLogInfo.getInstance()
            .logRecord(
                Module.STATISTICS,
                PROCESS_END,
                new String[] {
                    "ndv sketch rebuild:" + tableName + "," + columnName,
                    "sketch time:" + sketchTime +
                        ", cardinality time:" + cardinalityTime +
                        ", cardinality value:" + cardinality
                },
                LogLevel.NORMAL
            );

        NDVShardSketch ndvShardSketch =
            new NDVShardSketch(shardKey, shardPart, dnCardinalityArray, sketchType, gmtUpdate, gmtCreated);
        ndvShardSketch.setCardinality(cardinality);

        // persist
        PolarDbXSystemTableNDVSketchStatistic.getInstance()
            .batchReplace(ndvShardSketch.serialize(sketchArray, columnar));

        // sync other nodes
        SyncManagerHelper.syncWithDefaultDB(
            new UpdateStatisticSyncAction(
                schemaName,
                tableName,
                null),
            SyncScope.ALL);

        return ndvShardSketch;
    }

    private static long sketchByColumnar(String shardKey, String[] shardPart,
                                         long[] dnCardinalityArray, byte[][] sketchArray,
                                         long[] gmtCreated, long[] gmtUpdate,
                                         AtomicLong cardinalityTime, AtomicLong sketchTime, ExecutionContext ec) {
        String hint = genColumnarHllHint(ec);
        if (StringUtil.isEmpty(hint)) {
            return -1;
        }
        String[] shardKeys = shardKey.split(":");
        String schemaName = shardKeys[0];
        String tableName = shardKeys[1];
        String columnsName = shardKeys[2];
        TableMeta tm = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTableWithNull(tableName);
        if (tm == null) {
            return -1;
        }
        // must be a table with columnar indexes
        if (GeneralUtil.isEmpty(tm.getColumnarIndexPublished())) {
            return -1;
        }
        long cardinality = -1;
        // must visit columnar indexes
        long start = System.currentTimeMillis();
        try (Connection connection = ExecutorContext.getContext(schemaName).getInnerConnectionManager()
            .getConnection(schemaName);
            Statement stmt = connection.createStatement()) {
            String sql = hint + String.format(HYPER_LOG_LOG_SQL, columnsName, tableName);
            logger.info(sql);
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                cardinality = rs.getLong(1);
            }
            while (rs.next()) {
            }
        } catch (Exception e) {
            logger.error("Failed to get hll on columnar", e);
            return -1;
        }
        logger.info(String.format("get hll for %s.%s.%s using columnar", schemaName, tableName, columnsName));
        sketchTime.getAndAdd(System.currentTimeMillis() - start);
        for (int i = 0; i < shardPart.length; i++) {
            dnCardinalityArray[i] = cardinality;
            sketchArray[i] = null;
            gmtCreated[i] = start;
            gmtUpdate[i] = start;
        }
        return cardinality;
    }

    public static String genColumnarHllHint(ExecutionContext ec) {
        boolean isNdv = (ec == null) ?
            InstConfUtil.getBool(ConnectionParams.ENABLE_NDV_USE_COLUMNAR) :
            ec.getParamManager().getBoolean(ConnectionParams.ENABLE_NDV_USE_COLUMNAR);

        boolean isMppNdv = (ec == null) ?
            InstConfUtil.getBool(ConnectionParams.ENABLE_MPP_NDV_USE_COLUMNAR) :
            ec.getParamManager().getBoolean(ConnectionParams.ENABLE_MPP_NDV_USE_COLUMNAR);
        if (!(isNdv || isMppNdv)) {
            return null;
        }
        StringBuilder sb = new StringBuilder("/*+TDDL:cmd_extra(");
        // disable fast path
        sb.append(ConnectionProperties.ENABLE_DIRECT_PLAN).append("=false ");
        sb.append(ConnectionProperties.ENABLE_POST_PLANNER).append("=false ");
        sb.append(ConnectionProperties.ENABLE_INDEX_SELECTION).append("=false ");
        sb.append(ConnectionProperties.ENABLE_SORT_AGG).append("=false ");

        // use columnar optimizer
        sb.append(ConnectionProperties.WORKLOAD_TYPE).append("=AP ");
        sb.append(ConnectionProperties.ENABLE_COLUMNAR_OPTIMIZER).append("=true ");

        if (isMppNdv) {
            // use master mpp
            sb.append(ConnectionProperties.ENABLE_HTAP).append("=true ");
            sb.append(ConnectionProperties.ENABLE_MASTER_MPP).append("=true ");
        }
        sb.append(")*/");
        return sb.toString();
    }

    private static void sketchOnePart(String shardKey, String[] shardPart,
                                      long[] dnCardinalityArray, byte[][] sketchArray,
                                      long[] gmtCreated, long[] gmtUpdate,
                                      AtomicLong cardinalityTime, AtomicLong sketchTime,
                                      boolean isForce, ExecutionContext ec,
                                      int idx, AtomicBoolean stopped) {
        try {
            if (stopped.get()) {
                return;
            }
            long start = System.currentTimeMillis();
            dnCardinalityArray[idx] = getCurrentCardinality(shardKey, shardPart[idx]);
            long mid = System.currentTimeMillis();
            cardinalityTime.getAndAdd(mid - start);
            sketchArray[idx] = getCurrentHll(shardKey, shardPart[idx], isForce, ec);
            sketchTime.getAndAdd(System.currentTimeMillis() - mid);
            if (sketchArray[idx] == null) {
                gmtUpdate[idx] = 0L;
            } else {
                gmtUpdate[idx] = System.currentTimeMillis();
            }
            gmtCreated[idx] = System.currentTimeMillis();
        } catch (Exception e) {
            logger.error("Failed to sketch " + shardKey + " on " + shardPart[idx], e);
            stopped.compareAndSet(false, true);
            throw e;
        }
    }

    private static boolean isSketchDataReady(byte[][] sketchArray) {
        return !Arrays.stream(sketchArray).anyMatch(bytes -> bytes == null || bytes.length == 0);
    }

    /**
     * build shard parts, every phy table is one shard part.
     */
    public static String[] buildShardParts(String schemaName, String tableName) {
        OptimizerContext op = OptimizerContext.getContext(schemaName);
        if (op == null) {
            return null;
        }
        // shard parts build
        Map<String, Set<String>> topologyMap;
        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            topologyMap = op.getRuleManager().getTddlRule().getTable(tableName).getActualTopology();
        } else {
            topologyMap = op.getPartitionInfoManager().getPartitionInfo(tableName).getTopology();
        }
        return topologyPartToShard(topologyMap);
    }

    /**
     * get the hyperloglog sketch info from dn
     *
     * @param shardPart one physical table
     * @param ifForce true meaning from `analyze table`, false meaning from scheduled work
     */
    private static byte[] getCurrentHll(String shardKey, String shardPart, boolean ifForce, ExecutionContext ec) {
        String[] shardKeys = shardKey.split(":");

        String schemaName = shardKeys[0];
        String columnsName = shardKeys[2];

        // only one part for now
        Map<String, Set<String>> shardPartToTopology = shardPartToTopology(shardPart);

        byte[] hllBytes = null;
        if (shardPartToTopology.size() > 1) {
            // should not happen
            throw new IllegalArgumentException("not support multi shardpart");
        }

        if (ec != null && CrossEngineValidator.isJobInterrupted(ec)) {
            long jobId = ec.getDdlJobId();
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + jobId + "' has been cancelled");
        }

        for (Map.Entry<String, Set<String>> entry : shardPartToTopology.entrySet()) {
            String nodeName = entry.getKey();
            Set<String> physicalTables = entry.getValue();
            IDataSource ds = ExecutorContext.getContext(schemaName).getTopologyHandler().get(nodeName).getDataSource();

            if (physicalTables.size() > 1) {
                // should not happen
                throw new IllegalArgumentException("not support multi shard part");
            }

            for (String physicalTable : physicalTables) {
                if (ec != null && CrossEngineValidator.isJobInterrupted(ec)) {
                    long jobId = ec.getDdlJobId();
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                        "The job '" + jobId + "' has been cancelled");
                }

                // add time check
                if (!ifForce) {
                    Pair<Boolean, String> p = needSketchInterrupted();
                    if (p.getKey()) {
                        // just return
                        ModuleLogInfo.getInstance()
                            .logRecord(
                                Module.STATISTICS,
                                INTERRUPTED,
                                new String[] {
                                    "ndv sketch " + shardKey,
                                    p.getValue()
                                },
                                LogLevel.WARNING
                            );
                        return null;
                    }
                } else {
                    // from analyze table
                    if (!InstConfUtil.getBool(ConnectionParams.ENABLE_HLL)) {
                        // just return
                        ModuleLogInfo.getInstance()
                            .logRecord(
                                Module.STATISTICS,
                                INTERRUPTED,
                                new String[] {
                                    "ndv sketch " + shardKey,
                                    ENABLE_HLL + " is false"
                                },
                                LogLevel.WARNING
                            );
                        return null;
                    }
                }

                // check if columnsName represent one column or one index
                String sql;
                if (!columnsName.contains(",")) {
                    sql = String.format(HYPER_LOG_LOG_SQL, columnsName, physicalTable);
                } else {
                    sql = String.format(HYPER_LOG_LOG_MUL_COLUMNS_SQL, columnsName, physicalTable);
                }

                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.STATISTICS,
                        PROCESSING,
                        new String[] {
                            "ndv sketch " + shardKey + "," + nodeName,
                            sql
                        },
                        LogLevel.NORMAL
                    );

                Connection c = null;
                Statement st = null;
                ResultSet rs = null;
                try {
                    OptimizerContext op = OptimizerContext.getContext(schemaName);
                    if (op == null) {
                        return null;
                    }
                    c = ds.getConnection();
                    int queryTimeout = op.getParamManager()
                        .getInt(ConnectionParams.STATISTIC_NDV_SKETCH_QUERY_TIMEOUT);
                    if (FailPoint.isKeyEnable(FailPointKey.FP_INJECT_STATISTIC_SCHEDULE_JOB_HLL_EXCEPTION)) {
                        // inject hll exception, set timeout to 1ms
                        queryTimeout = 1;
                    }
                    Executor socketTimeoutExecutor = TGroupDirectConnection.socketTimeoutExecutor;
                    c.setNetworkTimeout(socketTimeoutExecutor, queryTimeout);
                    st = c.createStatement();
                    rs = st.executeQuery(sql);

                    while (true) {
                        if (!rs.next()) {
                            break;
                        }
                        hllBytes = rs.getBytes("HLL");
                    }
                } catch (SQLException ex) {
                    // MySQL Error = 1146 and MySQL SQLState = 42S02 indicate that the target table doesn't exist.
                    if (ex.getErrorCode() == 1146 && ex.getSQLState().equals("42S02")) {
                        StatisticManager.getInstance().getSds()
                            .removeLogicalTableList(schemaName, Lists.newArrayList(shardKeys[1]));
                        return null;
                    }
                } finally {
                    try {
                        if (rs != null) {
                            rs.close();
                        }
                        if (st != null) {
                            st.close();
                        }
                        if (c != null) {
                            c.close();
                        }
                    } catch (SQLException e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        }

        ModuleLogInfo.getInstance()
            .logRecord(
                Module.STATISTICS,
                PROCESS_END,
                new String[] {
                    "ndv sketch " + shardKey + "," + shardPart,
                    ""
                },
                LogLevel.NORMAL
            );

        return hllBytes;
    }

    public SystemTableNDVSketchStatistic.SketchRow[] serialize(byte[][] sketchBytes, boolean columnar) {
        String[] shardInfo = shardKey.split(":");
        String schemaName = shardInfo[0];
        String tableName = shardInfo[1];
        String columnNames = shardInfo[2];
        List<SystemTableNDVSketchStatistic.SketchRow> rows = Lists.newLinkedList();
        for (int i = 0; i < shardParts.length; i++) {
            byte[] sketchByte = sketchBytes[i];
            if (sketchByte == null) {
                if (!columnar) {
                    continue;
                }
                sketchByte = new byte[1];
            }
            SystemTableNDVSketchStatistic.SketchRow sketchRow =
                new SystemTableNDVSketchStatistic.SketchRow(schemaName, tableName, columnNames,
                    shardParts[i], dnCardinalityArray[i], cardinality,
                    sketchType, sketchByte, gmtCreated[i], gmtUpdate[i]);
            rows.add(sketchRow);
        }

        return rows.toArray(new SystemTableNDVSketchStatistic.SketchRow[0]);
    }

    public String getSketchType() {
        return sketchType;
    }

    public void setCardinality(long cardinality) {
        this.cardinality = cardinality;
    }
}
