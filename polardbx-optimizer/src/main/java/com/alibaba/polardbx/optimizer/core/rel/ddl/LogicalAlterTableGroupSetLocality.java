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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSetLocalityPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupSetLocality;
import org.apache.calcite.util.PrecedenceClimbingParser;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalAlterTableGroupSetLocality extends BaseDdlOperation {

    private AlterTableGroupSetLocalityPreparedData preparedData;

    public LogicalAlterTableGroupSetLocality(DDL ddl) {
        super(ddl);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    public void preparedData() {
        AlterTableGroupSetLocality alterTableGroupSetLocality = (AlterTableGroupSetLocality) relDdl;
        String tableGroupName = alterTableGroupSetLocality.getTableGroupName();
        String targetLocality = alterTableGroupSetLocality.getTargetLocality();
        LocalityDesc targetLocalityDesc = LocalityInfoUtils.parse(targetLocality);

        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        Long tableGroupId = tableGroupConfig.getTableGroupRecord().getId();
        int tgType = tableGroupConfig.getTableGroupRecord().tg_type;
        if (tgType == TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG
            || tgType == TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format(
                    "invalid alter locality action for table group! table group [%s] is default single table group or broadcast table group",
                    tableGroupName));
        }
        if (tgType == TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG) {
            if (targetLocalityDesc.getDnList().size() > 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                    String.format(
                        "invalid alter locality action for single table group! you can only set one dn as locality for single table group [%s]",
                        tableGroupName));

            }
        }

        LocalityDesc originalLocalityDesc = tableGroupConfig.getLocalityDesc();
        originalLocalityDesc = LocalityInfoUtils.parse(originalLocalityDesc.toString());

        List<String> schemaDnList =
            TableGroupLocation.getOrderedGroupList(schemaName).stream().map(group -> group.getStorageInstId())
                .collect(Collectors.toList());
        Set<String> dnList = new HashSet<>();
        if (tableGroupConfig.getTableCount() > 0) {
            PartitionInfo partitionInfo = OptimizerContext.getContext(schemaName).getPartitionInfoManager()
                .getPartitionInfo(tableGroupConfig.getTables().get(0).getTableName());
            List<ShowTopologyResult> topologyResults = getTopologyResults(partitionInfo);
            dnList = topologyResults.stream().map(o -> o.dnId).collect(Collectors.toSet());
        }

        Set<String> targetDnList = targetLocalityDesc.getDnSet();
        Set<String> fullTargetDnList = targetLocalityDesc.getFullDnSet();

        Set<String> orignialDnList = originalLocalityDesc.getDnSet();
        Boolean withRebalance;
        String rebalanceSql = "";
        // validate locality
        // generate drain node list
        // generate metadb task
        if (schemaDnList.containsAll(fullTargetDnList) && schemaDnList.containsAll(targetDnList)) {
            if (targetDnList.isEmpty() || (targetDnList.containsAll(orignialDnList) && !orignialDnList.isEmpty())
                || targetDnList.containsAll(dnList)) {
                withRebalance = false;
            } else {
                withRebalance = true;
                rebalanceSql =
                    String.format("schedule rebalance tablegroup %s policy = 'data_balance'", tableGroupName);
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("invalid locality: \"%s\", incompactible with database %s!", targetLocality, schemaName));
        }
        preparedData = new AlterTableGroupSetLocalityPreparedData();
        preparedData.setTargetLocality(targetLocality);
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
        preparedData.setWithRebalance(withRebalance);
        preparedData.setRebalanceSql(rebalanceSql);
        preparedData.setSourceSql(constructSourceSql());
    }

    public AlterTableGroupSetLocalityPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableGroupSetLocality create(DDL ddl) {
        return new LogicalAlterTableGroupSetLocality(AlterTablePartitionHelper.fixAlterTableGroupDdlIfNeed(ddl));
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        AlterTableGroupSetLocality alterTableGroupSetLocality = (AlterTableGroupSetLocality) relDdl;
        String tableGroupName = alterTableGroupSetLocality.getTableGroupName();
        return TableGroupNameUtil.isOssTg(tableGroupName);
    }

    @Override
    public boolean checkIfBindFileStorage(ExecutionContext executionContext) {
        AlterTableGroupSetLocality alterTableGroupSetLocality = (AlterTableGroupSetLocality) relDdl;
        String tableGroupName = alterTableGroupSetLocality.getTableGroupName();
        return !CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, tableGroupName);
    }

    private String constructSourceSql() {
        String stmt = "ALTER TABLEGROUP `%s` SET LOCALITY = '%s'";
        AlterTableGroupSetLocality alterTableGroupSetLocality = (AlterTableGroupSetLocality) relDdl;
        String tableGroupName = alterTableGroupSetLocality.getTableGroupName();
        String targetLocality = alterTableGroupSetLocality.getTargetLocality();
        return String.format(stmt, tableGroupName, targetLocality);
    }

    public List<ShowTopologyResult> getTopologyResults(PartitionInfo partitionInfo) {
        int index = 0;
        Map<String, List<PhysicalPartitionInfo>> physicalPartitionInfos =
            partitionInfo.getPhysicalPartitionTopology(new ArrayList<>());

        Map<String, GroupDetailInfoExRecord> groupDnMap = fetchGrpInfo();
        boolean useSubPart = partitionInfo.getPartitionBy().getSubPartitionBy() != null;
        List<ShowTopologyResult> showTopologyResults = new ArrayList<>();
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
                    dnId
                );
                showTopologyResults.add(showTopologyResult);
            }
        }
        return showTopologyResults;
    }

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

    private class ShowTopologyResult {
        int id;
        String groupName;
        String tableName;
        String partitionName;
        String subpartitionName;
        String phyDbName;
        String dnId;

        public ShowTopologyResult(
            int id,
            String groupName,
            String tableName,
            String partitionName,
            String subpartitionName,
            String phyDbName,
            String dnId
        ) {
            this.id = id;
            this.groupName = groupName;
            this.tableName = tableName;
            this.partitionName = partitionName;
            this.subpartitionName = subpartitionName;
            this.phyDbName = phyDbName;
            this.dnId = dnId;
        }
    }
}
