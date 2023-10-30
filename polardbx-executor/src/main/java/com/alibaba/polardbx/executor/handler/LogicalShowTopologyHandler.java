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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowTopology;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Optional;

/**
 * @author chenmo.cm
 */
public class LogicalShowTopologyHandler extends HandlerCommon {
    public LogicalShowTopologyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowTopology showTopology = (SqlShowTopology) show.getNativeSqlNode();
        final SqlNode sqlTableName = showTopology.getTableName();

        String tableName = RelUtils.lastStringValue(sqlTableName);

        final OptimizerContext context = OptimizerContext.getContext(show.getSchemaName());
        //test the table existence
        TableMeta tableMeta = context.getLatestSchemaManager().getTable(tableName);

        final PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        if (partitionInfo != null) {
            return handlePartitionedTable(partitionInfo);
        } else {
            return handleDRDSTable(executionContext, show);
        }

    }

    private Cursor handleDRDSTable(ExecutionContext executionContext, LogicalShow show) {
        final SqlShowTopology showTopology = (SqlShowTopology) show.getNativeSqlNode();
        final SqlNode sqlTableName = showTopology.getTableName();
        final OptimizerContext context = OptimizerContext.getContext(show.getSchemaName());
        TddlRuleManager tddlRuleManager = executionContext.getSchemaManager(show.getSchemaName()).getTddlRuleManager();
        String tableName = RelUtils.lastStringValue(sqlTableName);

        Map<String, Object> calcParams = new HashMap<>();
        calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
        List<TargetDB> topology = tddlRuleManager.shard(tableName, true, true, null, null, calcParams,
            executionContext);

        ArrayResultCursor result = new ArrayResultCursor("TOPOLOGY");
        result.addColumn("ID", DataTypes.IntegerType);
        result.addColumn("GROUP_NAME", DataTypes.StringType);
        result.addColumn("TABLE_NAME", DataTypes.StringType);
        result.addColumn("PARTITION_NAME", DataTypes.StringType);
        result.addColumn("SUBPARTITION_NAME", DataTypes.StringType);
        result.addColumn("PHY_DB_NAME", DataTypes.StringType);
        result.addColumn("DN_ID", DataTypes.StringType);
        result.initMeta();

        String schemaName =
            TStringUtil.isNotEmpty(show.getSchemaName()) ? show.getSchemaName() : executionContext.getSchemaName();
        boolean isTableWithoutPrivileges = !CanAccessTable.verifyPrivileges(
            schemaName,
            tableName,
            executionContext);
        if (isTableWithoutPrivileges) {
            return result;
        }

        Map<String, GroupDetailInfoExRecord> groupDnMap = fetchGrpInfo();

        int index = 0;
        List<String> dbs = new ArrayList<String>();
        Map<String, List<String>> tbs = new HashMap<String, List<String>>(4);
        for (TargetDB db : topology) {
            List<String> oneTbs = new ArrayList<String>();
            for (String tn : db.getTableNames()) {
                oneTbs.add(tn);
            }
            String dbIndex = db.getDbIndex();
            Collections.sort(oneTbs);
            dbs.add(dbIndex);
            tbs.put(dbIndex, oneTbs);
        }

        Collections.sort(dbs);
        for (String db : dbs) {
            GroupDetailInfoExRecord grpInfo = groupDnMap.get(db);
            String dnId = "NA";
            String phyDb = "NA";
            if (grpInfo != null) {
                dnId = grpInfo.getStorageInstId();
                phyDb = grpInfo.getPhyDbName();
            }
            for (String tn : tbs.get(db)) {
                result.addRow(new Object[] {
                    index++,
                    db,
                    tn,
                    "",
                    "",
                    phyDb,
                    dnId
                });
            }
        }

        return result;
    }

    private class ShowTopologyResult {
        int id;
        String groupName;
        String tableName;
        String partitionName;
        String subpartitionName;
        String phyDbName;
        String dnId;
        String storagePoolName;

        public ShowTopologyResult(
            int id,
            String groupName,
            String tableName,
            String partitionName,
            String subpartitionName,
            String phyDbName,
            String dnId,
            String storagePoolName
        ) {
            this.id = id;
            this.groupName = groupName;
            this.tableName = tableName;
            this.partitionName = partitionName;
            this.subpartitionName = subpartitionName;
            this.phyDbName = phyDbName;
            this.dnId = dnId;
            this.storagePoolName = storagePoolName;
        }
    }

    private static class ShowTopologyResultComparator implements Comparator<ShowTopologyResult> {
        static Pattern partNamePattern = Pattern.compile("p(\\d+)");
        static Pattern subPartNamePattern = Pattern.compile("p.*sp(\\d+)");

        private Integer extractNumberFromPartName(Pattern pattern, String partName) {
            Matcher matcher = pattern.matcher(partName);
            if (matcher.find()) {
                try {
                    return Integer.parseInt(matcher.group(1));
                } catch (NumberFormatException ignore) {
                }
            }
            return null;
        }

        @Override
        public int compare(ShowTopologyResult o1, ShowTopologyResult o2) {
            // 先判断a是否为p#n形式的字符串
            Integer partNum1 = extractNumberFromPartName(partNamePattern, o1.partitionName);
            Integer partNum2 = extractNumberFromPartName(partNamePattern, o2.partitionName);
            Integer subPartNum1 = extractNumberFromPartName(subPartNamePattern, o1.subpartitionName);
            Integer subPartNum2 = extractNumberFromPartName(subPartNamePattern, o2.subpartitionName);
            if (partNum1 != null && partNum2 != null) {
                // 如果两个字符串都是p#n形式，则按#n代表的整形值进行排序
                int partNumCompare = partNum1.compareTo(partNum2);
                if (partNumCompare != 0) {
                    return partNumCompare;
                } else {
                    if (subPartNum1 != null && subPartNum2 != null) {
                        return subPartNum1.compareTo(subPartNum2);
                    } else {
                        return o1.subpartitionName.compareTo(o2.subpartitionName);
                    }
                }
            } else {
                int partNameCompare = o1.partitionName.compareTo(o2.partitionName);
                if (partNameCompare != 0) {
                    return partNameCompare;
                } else {
                    if (subPartNum1 != null && subPartNum2 != null) {
                        return subPartNum1.compareTo(subPartNum2);
                    } else {
                        return o1.subpartitionName.compareTo(o2.subpartitionName);
                    }
                }
            }
        }
    }

    private Cursor handlePartitionedTable(PartitionInfo partitionInfo) {
        ArrayResultCursor result = new ArrayResultCursor("TOPOLOGY");
        result.addColumn("ID", DataTypes.IntegerType);
        result.addColumn("GROUP_NAME", DataTypes.StringType);
        result.addColumn("TABLE_NAME", DataTypes.StringType);
        result.addColumn("PARTITION_NAME", DataTypes.StringType);
        result.addColumn("SUBPARTITION_NAME", DataTypes.StringType);
        result.addColumn("PHY_DB_NAME", DataTypes.StringType);
        result.addColumn("DN_ID", DataTypes.StringType);
        result.addColumn("STORAGE_POOL_NAME", DataTypes.StringType);

        result.initMeta();
        int index = 0;
        Map<String, List<PhysicalPartitionInfo>> physicalPartitionInfos =
            partitionInfo.getPhysicalPartitionTopology(new ArrayList<>());

        boolean useSubPart = partitionInfo.getPartitionBy().getSubPartitionBy() != null;
        List<ShowTopologyResult> showTopologyResults = new ArrayList<>();

        Map<String, GroupDetailInfoExRecord> groupDnMap = new HashMap<>();
        Map<String, String> DnStoragePooMap = StoragePoolManager.getInstance().storagePoolMap;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            List<GroupDetailInfoExRecord> completedGroupInfos =
                groupDetailInfoAccessor.getCompletedGroupInfosByInstId(InstIdUtil.getInstId());
            for (int i = 0; i < completedGroupInfos.size(); i++) {
                GroupDetailInfoExRecord grpInfo = completedGroupInfos.get(i);
                groupDnMap.put(grpInfo.getGroupName(), grpInfo);
            }
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, ex);
        }

        for (Map.Entry<String, List<PhysicalPartitionInfo>> phyPartItem : physicalPartitionInfos.entrySet()) {
            String grpGroupKey = phyPartItem.getKey();
            List<PhysicalPartitionInfo> phyPartList = phyPartItem.getValue();
            for (int i = 0; i < phyPartList.size(); i++) {
                PhysicalPartitionInfo phyPartInfo = phyPartList.get(i);
                String grpName = phyPartInfo.getGroupKey();
                GroupDetailInfoExRecord grpInfo = groupDnMap.get(grpName);
                String dnId = "NA";
                String phyDb = "NA";
                if (grpInfo != null) {
                    dnId = grpInfo.getStorageInstId();
                    phyDb = grpInfo.getPhyDbName();
                }
                String storagePoolName = Optional.ofNullable(DnStoragePooMap.get(dnId)).orElse("");
                String pName = "";
                String spName = "";
                if (useSubPart) {
                    spName = phyPartInfo.getPartName();
                    pName = phyPartInfo.getParentPartName();
                } else {
                    pName = phyPartInfo.getPartName();
                }

                ShowTopologyResult showTopologyResult = new ShowTopologyResult(
                    index++,
                    grpGroupKey,
                    phyPartList.get(i).getPhyTable(),
                    pName,
                    spName,
                    phyDb,
                    dnId,
                    storagePoolName
                );
                showTopologyResults.add(showTopologyResult);
            }
        }

        showTopologyResults.sort(new ShowTopologyResultComparator());
        int j = 0;
        for (ShowTopologyResult showTopologyResult : showTopologyResults) {
            result.addRow(
                new Object[] {
                    j++,
                    showTopologyResult.groupName,
                    showTopologyResult.tableName,
                    showTopologyResult.partitionName,
                    showTopologyResult.subpartitionName,
                    showTopologyResult.phyDbName,
                    showTopologyResult.dnId,
                    showTopologyResult.storagePoolName
                }
            );
        }
        return result;
    }

    @NotNull
    private Map<String, GroupDetailInfoExRecord> fetchGrpInfo() {
        Map<String, GroupDetailInfoExRecord> groupDnMap = new HashMap<>();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            List<GroupDetailInfoExRecord> completedGroupInfos =
                groupDetailInfoAccessor.getCompletedGroupInfosByInstId(InstIdUtil.getInstId());
            for (int i = 0; i < completedGroupInfos.size(); i++) {
                GroupDetailInfoExRecord grpInfo = completedGroupInfos.get(i);
                groupDnMap.put(grpInfo.getGroupName(), grpInfo);
            }
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, ex);
        }
        return groupDnMap;
    }
}
