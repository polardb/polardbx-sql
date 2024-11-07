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

package com.alibaba.polardbx.executor.balancer.splitpartition;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Math.min;

public class SplitPartitionStats {

    private int maxSamples = 200_000;

    private float sampleRate = 0.05f;

    private String schemaName;
    private String logicalTableName;

    private String partitionName;

    private PartitionInfo partitionInfo;

    private String physicalGroup;

    private String physicalTable;

    private TGroupDataSource tGroupDataSource;

    private List<ColumnMeta> partitionColumnList;

    public static SplitPartitionStats createForSplitPartition(String schemaName, String logicalTableName,
                                                              String partitionName) {
        SplitPartitionStats splitPartitionStats = new SplitPartitionStats();
        splitPartitionStats.schemaName = schemaName;
        splitPartitionStats.logicalTableName = logicalTableName;
        splitPartitionStats.partitionName = partitionName;
        return splitPartitionStats;
    }

    public void prepare() {
        partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);
        PartitionLocation partitionLocation =
            partitionInfo.getPartitionBy().getPartitionByPartName(partitionName).getLocation();
        physicalGroup = partitionLocation.getGroupKey();
        physicalTable = partitionLocation.getPhyTableName();
        tGroupDataSource = (TGroupDataSource) ExecutorContext.getContext(schemaName).getTopologyExecutor()
            .getGroupExecutor(physicalGroup).getDataSource();
        List<String> partitionColumns = partitionInfo.getPartitionColumns();
        List<ColumnMeta> fullColumnList =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName).getAllColumns();
        partitionColumnList =
            fullColumnList.stream().filter(o -> partitionColumns.contains(o.getName())).collect(Collectors.toList());
        Collections.sort(partitionColumnList,
            Comparator.comparingInt(column -> partitionColumns.indexOf(column.getName())));
    }

    private boolean checkSupportFastSample() {
        Boolean enableInnodbBtreeSampling = OptimizerContext.getContext(schemaName).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_INNODB_BTREE_SAMPLING);
        if (!enableInnodbBtreeSampling) {
            return false;
        }
        try (IConnection connection = tGroupDataSource.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("show variables like 'innodb_innodb_btree_sampling'")) {
            if (resultSet.next()) {
                String value = resultSet.getString(2);
                if ("ON".equalsIgnoreCase(value)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } catch (SQLException e) {
            return false;
        }
    }

    private String constructScanPartitionSamplingSql(String physicalTableName, List<ColumnMeta> columnMetaList,
                                                     float sampleRate, Boolean withHint) {
        String sampleSql = "SELECT %s  %s FROM `%s`";
        float cmdExtraSamplePercentage = sampleRate * 100;
        String hint = "";
        if (withHint) {
            hint = String.format("/*+sample_percentage(%s)*/ ", GeneralUtil.formatSampleRate(cmdExtraSamplePercentage));
        }
        List<String> columnMetaNames =
            columnMetaList.stream().map(o -> "`" + o.getName() + "`").collect(Collectors.toList());
        return String.format(sampleSql, hint, StringUtils.join(columnMetaNames, ","),
            physicalTableName);
    }

    private Pair<Float, Long> floorSampleRate(String physicalGroup, String physicalTable, float sampleRate) {
        String physicalDb = TableGroupLocation.getOrderedGroupList(schemaName).stream()
            .filter(o -> o.groupName.equalsIgnoreCase(physicalGroup)).collect(Collectors.toList()).get(0).phyDbName;
        String COUNT_SQL =
            "SELECT `TABLE_ROWS` from `INFORMATION_SCHEMA`.`TABLES` WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s'";
        Long tableRows = 0L;
        try (IConnection connection = (IConnection) tGroupDataSource.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(String.format(COUNT_SQL, physicalDb, physicalTable))) {
            while (resultSet.next()) {
                tableRows = resultSet.getLong("TABLE_ROWS");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        float maxSampleRate = (float) (maxSamples / (tableRows * 2.0));
        float finalSampleRate = min(maxSampleRate, sampleRate);
        return Pair.of(finalSampleRate, tableRows);
    }

    public Pair<List<List<Object>>, Long> sampleTablePartitions() throws SQLException {
        // scan sampling
        List<List<Object>> resultRows = new ArrayList<>();
        Boolean withHint = checkSupportFastSample();
        Pair<Float, Long> sampleRateResult = floorSampleRate(physicalGroup, physicalTable, sampleRate);
        float finalSampleRate = sampleRateResult.getKey();
        String sql = constructScanPartitionSamplingSql(physicalTable, partitionColumnList, finalSampleRate, withHint);
        try (IConnection connection = (IConnection) tGroupDataSource.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                if (!withHint && Math.random() > finalSampleRate) {
                    continue;
                }
                resultRows.add(new ArrayList<>());
                for (int i = 0; i < partitionColumnList.size(); i++) {
                    try {
                        Object columnValue = resultSet.getObject(partitionColumnList.get(i).getName());
                        resultRows.get(resultRows.size() - 1).add(columnValue);
                    } catch (Throwable e) {
                        continue;
                    }
                }
                GeneralUtil.checkInterrupted();
            }
        } catch (Throwable e) {
            throw e;
        }
        return Pair.of(resultRows, sampleRateResult.getValue());
    }
}
