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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableNDVSketchStatistic;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.UpdateStatisticSyncAction;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.MetaUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import jdk.nashorn.internal.objects.Global;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.statistic.ndv.HyperLogLogUtil.buildSketchKey;
import static com.alibaba.polardbx.executor.statistic.ndv.HyperLogLogUtil.estimate;
import static com.alibaba.polardbx.optimizer.OptimizerContext.getContext;

public class NDVShardSketch {
    private static final Logger logger = LoggerFactory.getLogger("statistics");

    public static final double MAX_DIFF_VALUE_RATIO = 0.2D;

    public static final String STATISTIC_SQL =
        "SELECT SUM(CARDINALITY) AS SUM_NDV FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME IN (%1$2s) AND COLUMN_NAME=%2$2s";

    public static final String HYPER_LOG_LOG_SQL = "SELECT HYPERLOGLOG(%1$2s) AS HLL FROM %2$2s";

    public static final String HYPER_LOG_LOG_MUL_COLUMNS_SQL = "SELECT HYPERLOGLOG(concat(%1$2s)) AS HLL FROM %2$2s";

//    public static final String SAMPLE_HINT = "/*+sample_percentage(%1$2s)*/";

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
        if (!validityCheck()) {
            cardinality = -1;
            return -1;
        }
        return cardinality;
    }

    /**
     * check the validity of the shard parts
     */
    public boolean validityCheck() {
        String[] shardInfo = shardKey.split(":");
        String schemaName = shardInfo[0];
        String tableName = shardInfo[1];
        Map<String, Set<String>> topologyTmp;
        TddlRuleManager ruleManager = getContext(schemaName).getRuleManager();
        if (ruleManager == null) {
            return false;
        }
        TableRule rule = ruleManager.getTableRule(tableName);
        if (rule == null) {
            return false;
        }
        topologyTmp = rule.getActualTopology();

        Map<String, Set<String>> topology = Maps.newHashMap();

        // build new map to avoid concurrency update problem
        for (Map.Entry<String, Set<String>> entry : topologyTmp.entrySet()) {
            Set<String> phyTbls = Sets.newHashSet();
            entry.getValue().stream().forEach(s -> phyTbls.add(s.toLowerCase()));
            topology.put(entry.getKey().toLowerCase(), phyTbls);
        }

        // build topology map using statistic info
        Map<String, Set<String>> topologyStatistic = Maps.newHashMap();
        for (String shardPart : getShardParts()) {
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

    /**
     * update all shard parts
     */
    public boolean updateAllShardParts() throws SQLException {
        String[] shardInfo = shardKey.split(":");
        String schemaName = shardInfo[0];
        String tableName = shardInfo[1];
        String columnNames = shardInfo[2];

        // expired time for x-conn
        int expiredTime = OptimizerContext.getContext(schemaName).getParamManager()
            .getInt(ConnectionParams.STATISTIC_NDV_SKETCH_EXPIRE_TIME);
        long current = System.currentTimeMillis();
        long sketchInfoTime = 0;
        long cardinalityTime = 0;
        boolean hasUpdated = false;

        // record update shard parts and bytes for cal new cardinality(CompositeCardinality)
        List<String> updateShardParts = Lists.newLinkedList();
        Map<String, byte[]> updateBytes = Maps.newHashMap();
        for (int i = 0; i < getGmtUpdate().length; i++) {
            boolean needUpdate = false;
            if (current - getGmtUpdate()[i] > expiredTime) {
                StatisticUtils.logInfo(schemaName,
                    "update ndv sketch for timeout:" + shardKey + "," + Arrays.toString(getShardParts()) +
                        ",current:" + new Date(current) +
                        ",record:" + new Date(getGmtUpdate()[i]) +
                        ",expiredTime:" + expiredTime);
                needUpdate = true;
            } else {
                long currentCardinality = getCurrentCardinality(shardKey, getShardParts()[i]);
                if (currentCardinality == -1) {
                    StatisticUtils.logInfo(schemaName,
                        "update ndv sketch for currentCardinality equals -1:" + shardKey + "," + Arrays
                            .toString(getShardParts()));
                    needUpdate = true;
                } else {
                    long dValue = Math.abs(currentCardinality - getDnCardinalityArray()[i]);
                    int maxDValue = OptimizerContext.getContext(schemaName).getParamManager()
                        .getInt(ConnectionParams.STATISTIC_NDV_SKETCH_MAX_DIFFERENT_VALUE);
                    if (dValue > maxDValue || ((double) dValue / currentCardinality) > MAX_DIFF_VALUE_RATIO) {
                        StatisticUtils.logInfo(schemaName,
                            "update ndv sketch for currentCardinality changed:" + shardKey + "," + Arrays
                                .toString(getShardParts()) +
                                ", max d-value:" + maxDValue +
                                ",current d-value:" + dValue +
                                ", currentCardinality:" + currentCardinality +
                                ", d-value-ratio:" + MAX_DIFF_VALUE_RATIO
                        );
                        needUpdate = true;
                    }
                }
            }
            if (needUpdate) {
                long start = System.currentTimeMillis();
                long cardinalityTmp = getCurrentCardinality(shardKey, getShardParts()[i]);
                cardinalityTime += System.currentTimeMillis() - start;
                start = System.currentTimeMillis();
                byte[] bytes = getCurrentHll(shardKey, getShardParts()[i], false);
                if (bytes == null) {
                    // null meaning the hll request is stoped by something, like flow control
                    StatisticUtils.logInfo(schemaName, "update ndv sketch stopped by flow control");
                    continue;
                }
                updateBytes.put(getShardParts()[i], bytes);
                updateShardParts.add(shardParts[i]);
                getDnCardinalityArray()[i] = cardinalityTmp;
                getGmtUpdate()[i] = System.currentTimeMillis();
                SystemTableNDVSketchStatistic.SketchRow sketchRow = new SystemTableNDVSketchStatistic.SketchRow
                    (schemaName, tableName, columnNames, getShardParts()[i], getDnCardinalityArray()[i], -1,
                        "HYPER_LOG_LOG");
                sketchRow.setSketchBytes(bytes);
                PolarDbXSystemTableNDVSketchStatistic.getInstance()
                    .batchReplace(new SystemTableNDVSketchStatistic.SketchRow[] {sketchRow});

                sketchInfoTime += System.currentTimeMillis() - start;
                StatisticUtils.logInfo(schemaName, "update ndv sketch :" + shardKey + ", " + getShardParts()[i]);
                hasUpdated = true;
            }
        }
        if (hasUpdated) {
            /**
             * update sketch bytes
             */
            Map<String, byte[]> bytesMap = PolarDbXSystemTableNDVSketchStatistic.getInstance()
                .loadByTableNameAndColumnName(schemaName, tableName, columnNames);
            for (String shardPart : updateShardParts) {
                bytesMap.put(shardPart, updateBytes.get(shardPart));
            }

            /**
             * compute new cardinality
             */
            setCardinality(HyperLogLogUtil.estimate(bytesMap.values().toArray(new byte[0][])));

            // record new cardinality
            PolarDbXSystemTableNDVSketchStatistic.getInstance()
                .updateCompositeCardinality(schemaName, tableName, columnNames, getCardinality());

            StatisticUtils.logInfo(schemaName,
                "update ndv sketch :" + shardKey + ", sketchInfo time:" + sketchInfoTime + ", cardinality time:"
                    + cardinalityTime);
        }
        return hasUpdated;
    }

    /**
     * update stock shard parts
     */
    public boolean updateStockShardParts() throws SQLException {
        if (!validityCheck()) {
            logger.error("find invalid ndv sketch :" + shardKey + "," + Arrays.toString(getShardParts()));
            return false;
        }
        String[] shardInfo = shardKey.split(":");
        String schemaName = shardInfo[0];
        String tableName = shardInfo[1];
        String columnNames = shardInfo[2];

        // expired time for x-conn
        int expiredTime = OptimizerContext.getContext(schemaName).getParamManager()
            .getInt(ConnectionParams.STATISTIC_NDV_SKETCH_EXPIRE_TIME);
        long current = System.currentTimeMillis();
        long sketchInfoTime = 0;
        long cardinalityTime = 0;
        boolean hasUpdated = false;
        // record update shard parts and bytes for cal new cardinality(CompositeCardinality)
        List<String> updateShardParts = Lists.newLinkedList();
        Map<String, byte[]> updateBytes = Maps.newHashMap();

        for (int i = 0; i < getGmtUpdate().length; i++) {
            boolean needUpdate = false;
            if (current - getGmtUpdate()[i] > expiredTime) {
                StatisticUtils.logInfo(schemaName,
                    "update ndv sketch for timeout:" + shardKey + "," + Arrays.toString(getShardParts()) +
                        ",current:" + new Date(current) +
                        ",record:" + new Date(getGmtUpdate()[i]) +
                        ",expiredTime:" + expiredTime);
                needUpdate = true;
            } else {
                long currentCardinality = getCurrentCardinality(shardKey, getShardParts()[i]);
                if (currentCardinality != -1) {
                    long dValue = Math.abs(currentCardinality - getDnCardinalityArray()[i]);
                    int maxDValue = OptimizerContext.getContext(schemaName).getParamManager()
                        .getInt(ConnectionParams.STATISTIC_NDV_SKETCH_MAX_DIFFERENT_VALUE);
                    if (dValue > maxDValue || ((double) dValue / currentCardinality) > MAX_DIFF_VALUE_RATIO) {
                        StatisticUtils.logInfo(schemaName,
                            "update ndv sketch for currentCardinality changed:" + shardKey + "," + Arrays
                                .toString(getShardParts()) +
                                ", max d-value:" + maxDValue +
                                ",current d-value:" + dValue +
                                ", currentCardinality:" + currentCardinality +
                                ", d-value-ratio:" + MAX_DIFF_VALUE_RATIO
                        );
                        needUpdate = true;
                    }
                }
            }
            if (needUpdate) {
                long start = System.currentTimeMillis();
                long cardinalityTmp = getCurrentCardinality(shardKey, getShardParts()[i]);
                cardinalityTime += System.currentTimeMillis() - start;
                start = System.currentTimeMillis();
                byte[] bytes = getCurrentHll(shardKey, getShardParts()[i], false);
                if (bytes == null) {
                    // null meaning the hll request is stoped by something, like flow control
                    StatisticUtils.logInfo(schemaName, "update ndv sketch stopped by flow control");
                    continue;
                }
                updateBytes.put(getShardParts()[i], bytes);
                updateShardParts.add(shardParts[i]);
                getDnCardinalityArray()[i] = cardinalityTmp;
                getGmtUpdate()[i] = System.currentTimeMillis();
                SystemTableNDVSketchStatistic.SketchRow sketchRow = new SystemTableNDVSketchStatistic.SketchRow
                    (schemaName, tableName, columnNames, getShardParts()[i], getDnCardinalityArray()[i], -1,
                        "HYPER_LOG_LOG");
                sketchRow.setSketchBytes(bytes);
                PolarDbXSystemTableNDVSketchStatistic.getInstance()
                    .batchReplace(new SystemTableNDVSketchStatistic.SketchRow[] {sketchRow});

                sketchInfoTime += System.currentTimeMillis() - start;
                StatisticUtils.logInfo(schemaName, "update ndv sketch :" + shardKey + ", " + getShardParts()[i]);
                hasUpdated = true;
            }
        }
        if (hasUpdated) {
            /**
             * update sketch bytes
             */
            Map<String, byte[]> bytesMap = PolarDbXSystemTableNDVSketchStatistic.getInstance()
                .loadByTableNameAndColumnName(schemaName, tableName, columnNames);
            for (String shardPart : updateShardParts) {
                bytesMap.put(shardPart, updateBytes.get(shardPart));
            }

            /**
             * compute new cardinality
             */
            long old = getCardinality();
            setCardinality(HyperLogLogUtil.estimate(bytesMap.values().toArray(new byte[0][])));

            // record new cardinality
            PolarDbXSystemTableNDVSketchStatistic.getInstance()
                .updateCompositeCardinality(schemaName, tableName, columnNames, getCardinality());

            StatisticUtils.logInfo(schemaName,
                "update ndv sketch :" + shardKey + ", sketchInfo time:" + sketchInfoTime + ", cardinality time:"
                    + cardinalityTime + ", old value:" + old + ", new:" + getCardinality());
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
        StatisticUtils.logDebug(schemaName, "get cardinality from dn statistic start:" + shardKey + "," + shardPart);

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
            StatisticUtils.logDebug(schemaName,
                "get cardinality from dn statistic from:" + shardKey + "," + nodeName + "," + sql);

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
        StatisticUtils.logDebug(schemaName, "get cardinality from dn statistic end:" + shardKey + "," + shardPart);
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

    public static NDVShardSketch buildNDVShardSketch(String shardKey) throws SQLException {
        String[] splits = shardKey.split(":");
        String schemaName = splits[0];
        String tableName = splits[1];
        String columnName = splits[2];
        return buildNDVShardSketch(schemaName, tableName, columnName);
    }

    /**
     * build one ndv sketch
     */
    public static NDVShardSketch buildNDVShardSketch(String schemaName, String tableName, String columnName)
        throws SQLException {
        StatisticUtils.logDebug(schemaName, "build ndv sketch start:" + tableName + "," + columnName);
        String shardKey = buildSketchKey(schemaName, tableName, columnName);

        // shard parts build
        String[] shardPart = buildShardParts(schemaName, tableName);
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
            sketchArray[i] = getCurrentHll(shardKey, shardPart[i], true);
            sketchTime += System.currentTimeMillis() - mid;
            gmtUpdate[i] = System.currentTimeMillis();
            gmtCreated[i] = System.currentTimeMillis();
        }
        StatisticUtils.logInfo(schemaName,
            "build ndv sketch end:" + tableName + "," + columnName + ", sketch time:" + sketchTime
                + ", cardinality time:" + cardinalityTime);

        long cardinality = estimate(sketchArray);

        NDVShardSketch ndvShardSketch =
            new NDVShardSketch(shardKey, shardPart, dnCardinalityArray, sketchType, gmtUpdate, gmtCreated);
        ndvShardSketch.setCardinality(cardinality);

        // persist
        PolarDbXSystemTableNDVSketchStatistic.getInstance().batchReplace(ndvShardSketch.serialize(sketchArray));

        /** sync other nodes */
        SyncManagerHelper.sync(
            new UpdateStatisticSyncAction(
                schemaName,
                tableName,
                null),
            schemaName);

        return ndvShardSketch;
    }

    /**
     * build shard parts, every phy table is one shard part.
     */
    public static String[] buildShardParts(String schemaName, String tableName) {
        // shard parts build
        Map<String, Set<String>> topologyMap =
            OptimizerContext.getContext(schemaName).getRuleManager().getTddlRule().getTable(tableName)
                .getActualTopology();

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

        if (!ifForce && !FlowControl.getInstance(schemaName).acquire()) {
            // canceled by flow control
            return null;
        }

        long startTime = System.currentTimeMillis();

        StatisticUtils.logDebug(schemaName, "get hll sketch info start:" + shardKey + "," + shardPart);

        Map<String, Set<String>> shardPartToTopology = shardPartToTopology(shardPart);

        byte[] hllBytes = new byte[12288];
        if (shardPartToTopology.size() > 1) {
            throw new IllegalArgumentException("not support multi shardpart");
        }
        for (Map.Entry<String, Set<String>> entry : shardPartToTopology.entrySet()) {
            String nodeName = entry.getKey();
            Set<String> physicalTables = entry.getValue();
            IDataSource ds = ExecutorContext.getContext(schemaName).getTopologyHandler().get(nodeName).getDataSource();

            if (physicalTables.size() > 1) {
                throw new IllegalArgumentException("not support multi shard part");
            }
            for (String physicalTable : physicalTables) {
                // check if columnsName represent one column or one index
                String sql;
                if (!columnsName.contains(",")) {
                    sql = String.format(HYPER_LOG_LOG_SQL, columnsName, physicalTable);
                } else {
                    sql = String.format(HYPER_LOG_LOG_MUL_COLUMNS_SQL, columnsName, physicalTable);
                }

                StatisticUtils
                    .logDebug(schemaName, "get hll sketch info from:" + shardKey + "," + nodeName + "," + sql);
                Connection c = null;
                Statement st = null;
                ResultSet rs = null;
                try {
                    c = ds.getConnection();
                    int queryTimeout = OptimizerContext.getContext(schemaName).getParamManager()
                        .getInt(ConnectionParams.STATISTIC_NDV_SKETCH_QUERY_TIMEOUT);
                    c.setNetworkTimeout(null, queryTimeout);
                    st = c.createStatement();
                    rs = st.executeQuery(sql);

                    while (true) {
                        if (!rs.next()) {
                            break;
                        }
                        hllBytes = rs.getBytes("HLL");
                    }
                } catch (SQLException e) {
                    throw e;
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
                    FlowControl.getInstance(schemaName).feedback(System.currentTimeMillis() - startTime);
                }
            }

        }
        StatisticUtils.logDebug(schemaName, "get hll sketch info end:" + shardKey + "," + shardPart);
        return hllBytes;
    }

    private static Set<String> indexMatch(String schemaName, String tableName, String columnsName) {
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        Map<String, Set<String>> indexMap = GlobalIndexMeta.getLocalIndexColumnMap(tableMeta);

        return indexMap.get(columnsName);
    }

    public SystemTableNDVSketchStatistic.SketchRow[] serialize(byte[][] sketchBytes) {
        String[] shardInfo = shardKey.split(":");
        String schemaName = shardInfo[0];
        String tableName = shardInfo[1];
        String columnNames = shardInfo[2];
        List<SystemTableNDVSketchStatistic.SketchRow> rows = Lists.newLinkedList();
        for (int i = 0; i < shardParts.length; i++) {
            SystemTableNDVSketchStatistic.SketchRow sketchRow =
                new SystemTableNDVSketchStatistic.SketchRow(schemaName, tableName, columnNames,
                    shardParts[i], dnCardinalityArray[i], cardinality,
                    sketchType, sketchBytes[i], gmtCreated[i], gmtUpdate[i]);
            rows.add(sketchRow);
        }

        return rows.toArray(new SystemTableNDVSketchStatistic.SketchRow[0]);
    }

    public String[] getShardParts() {
        return shardParts;
    }

    public String getSketchType() {
        return sketchType;
    }

    public long[] getGmtUpdate() {
        return gmtUpdate;
    }

    public long[] getDnCardinalityArray() {
        return dnCardinalityArray;
    }

    public long[] getGmtCreated() {
        return gmtCreated;
    }

    public void setCardinality(long cardinality) {
        this.cardinality = cardinality;
    }
}
