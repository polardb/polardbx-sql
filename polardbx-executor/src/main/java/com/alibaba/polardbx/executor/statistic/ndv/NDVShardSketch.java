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

import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableNDVSketchStatistic;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.UpdateStatisticSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

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
import static com.alibaba.polardbx.gms.module.LogPattern.UPDATE_NDV_FOR_CHANGED;
import static com.alibaba.polardbx.gms.module.LogPattern.UPDATE_NDV_FOR_EXPIRED;

public class NDVShardSketch {
    private static final Logger logger = LoggerFactory.getLogger("STATISTICS");

    public static final double MAX_DIFF_VALUE_RATIO = 0.2D;

    public static final String STATISTIC_SQL =
        "SELECT SUM(CARDINALITY) AS SUM_NDV FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME IN (%1$2s) AND COLUMN_NAME=%2$2s";

    public static final String HYPER_LOG_LOG_SQL = "SELECT HYPERLOGLOG(%1$2s) AS HLL FROM %2$2s";

    public static final String HYPER_LOG_LOG_MUL_COLUMNS_SQL = "SELECT HYPERLOGLOG(concat(%1$2s)) AS HLL FROM %2$2s";

    /**
     * schemaName:table name:columns name
     */
    private String shardKey;

    /**
     * one shard for one physical table
     */
    private String[] shardParts;

    /**
     * ndv value from dn statistic view, might not be accurate
     */
    private long[] dnCardinalityArray;

    /**
     * sketch type: hyper log log only for now
     */
    private String sketchType;

    /**
     * sketch info update time for every shard
     */
    private long[] gmtUpdate;

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
        return Arrays.stream(gmtUpdate).max().getAsLong();
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
                byte[] bytes = getCurrentHll(shardKey, shardParts[i], false);
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
                                                     boolean isForce)
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

        long sketchTime = 0;
        long cardinalityTime = 0;

        // fill cardinality and sketch bytes
        for (int i = 0; i < shardPart.length; i++) {
            long start = System.currentTimeMillis();
            dnCardinalityArray[i] = getCurrentCardinality(shardKey, shardPart[i]);
            long mid = System.currentTimeMillis();
            cardinalityTime += mid - start;
            sketchArray[i] = getCurrentHll(shardKey, shardPart[i], isForce);
            sketchTime += System.currentTimeMillis() - mid;
            if (sketchArray[i] == null) {
                gmtUpdate[i] = 0L;
            } else {
                gmtUpdate[i] = System.currentTimeMillis();
            }
            gmtCreated[i] = System.currentTimeMillis();
        }

        long cardinality = -1;

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
        PolarDbXSystemTableNDVSketchStatistic.getInstance().batchReplace(ndvShardSketch.serialize(sketchArray));

        // sync other nodes
        SyncManagerHelper.syncWithDefaultDB(
            new UpdateStatisticSyncAction(
                schemaName,
                tableName,
                null));

        return ndvShardSketch;
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
    private static byte[] getCurrentHll(String shardKey, String shardPart, boolean ifForce) throws SQLException {
        String[] shardKeys = shardKey.split(":");

        String schemaName = shardKeys[0];
        String columnsName = shardKeys[2];

        long startTime = System.currentTimeMillis();
        // only one part for now
        Map<String, Set<String>> shardPartToTopology = shardPartToTopology(shardPart);

        byte[] hllBytes = new byte[12288];
        if (shardPartToTopology.size() > 1) {
            // should not happen
            throw new IllegalArgumentException("not support multi shardpart");
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
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (st != null) {
                        try {
                            st.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (c != null) {
                        try {
                            c.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
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

    public SystemTableNDVSketchStatistic.SketchRow[] serialize(byte[][] sketchBytes) {
        String[] shardInfo = shardKey.split(":");
        String schemaName = shardInfo[0];
        String tableName = shardInfo[1];
        String columnNames = shardInfo[2];
        List<SystemTableNDVSketchStatistic.SketchRow> rows = Lists.newLinkedList();
        for (int i = 0; i < shardParts.length; i++) {
            byte[] sketchByte = null;
            if (sketchBytes[i] == null) {
                continue;
            } else {
                sketchByte = sketchBytes[i];
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
