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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.atom.config.TAtomDsConfDO;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.ttl.TtlPartArcState;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * @author chenghui.lch
 */
public class PartitionMetaUtil {

    @Getter
    public static class PartitionMetaRecord {

        public PartitionMetaRecord() {
        }

        public Long partNum;

        public String tableSchema;
        public String tableName;
        public String indexName;
        public String primaryTable;
        public String tableType;
        public String tgName;

        public String partMethod;
        public String partCol;
        public String partColType;
        public String partExpr;
        public String partName;
        public Long partPosi;
        public String partDesc;
        public String partArcStateName;

        public String subPartMethod;
        public String subPartCol;
        public String subPartColType;
        public String subPartExpr;
        public String subPartName;
        public String subPartTempName;
        public Long subPartPosi;
        public String subPartDesc;
        public String subPartArcStateName;

        public String partComment = "";

        public String pgName;
        public String phyDbGroup;
        public String phyDb;
        public String phyTb;
        public String rwDnId;
    }

    public static List<PartitionMetaRecord> handlePartitionsMeta(PartitionInfo partInfo,
                                                                 String rawGsiName,
                                                                 String primaryTbl) {
        List<PartitionMetaRecord> result = new ArrayList<>();

        String dbName = partInfo.getTableSchema().toLowerCase(Locale.ROOT);
        String tblName = partInfo.getTableName().toLowerCase(Locale.ROOT);
        PartitionTableType partTblType = partInfo.getTableType();
        Long tgId = partInfo.getTableGroupId();
        TableGroupInfoManager tgMgr = OptimizerContext.getContext(dbName).getTableGroupInfoManager();
        TopologyHandler topology = ExecutorContext.getContext(dbName).getTopologyHandler();
        TableGroupConfig tgInfo = tgMgr.getTableGroupConfigById(tgId);
        String tblType = partTblType.getTableTypeName();
        String tgName = tgInfo.getTableGroupRecord().getTg_name();

        PartitionByDefinition partBy = partInfo.getPartitionBy();
        String partMethod = partBy.getStrategy().getStrategyExplainName();
        String partCol = org.apache.commons.lang.StringUtils.join(partBy.getPartitionColumnNameList(), ",");
        List<String> partColTypeStrList = new ArrayList<>();
        for (int i = 0; i < partBy.getPartitionFieldList().size(); i++) {
            partColTypeStrList.add(partBy.getPartitionFieldList().get(i).getField().getRelType().getFullTypeString());
        }
        String partColType = org.apache.commons.lang.StringUtils.join(partColTypeStrList, ",");

        List<String> partColExprStrList = new ArrayList<>();
        for (int i = 0; i < partBy.getPartitionExprList().size(); i++) {
            partColExprStrList.add(partBy.getPartitionExprList().get(i).toString());
        }
        String partExpr = org.apache.commons.lang.StringUtils.join(partColExprStrList, ",");

        PartitionByDefinition subPartBy = partBy.getSubPartitionBy();
        boolean useSubPart = subPartBy != null;
        boolean useSubPartTemp = useSubPart ? subPartBy.isUseSubPartTemplate() : false;
        String subPartMethod = useSubPart ? subPartBy.getStrategy().getStrategyExplainName() : null;
        String subPartCol =
            useSubPart ? org.apache.commons.lang.StringUtils.join(subPartBy.getPartitionColumnNameList(), ",") : null;
        String subPartExpr = null;
        String subPartColType = null;
        if (useSubPart) {
            List<String> subPartColTypeStrList = new ArrayList<>();
            for (int i = 0; i < subPartBy.getPartitionFieldList().size(); i++) {
                subPartColTypeStrList.add(
                    subPartBy.getPartitionFieldList().get(i).getField().getRelType().getFullTypeString());
            }
            subPartColType = org.apache.commons.lang.StringUtils.join(subPartColTypeStrList, ",");

            List<String> subPartColExprStrList = new ArrayList<>();
            for (int i = 0; i < subPartBy.getPartitionExprList().size(); i++) {
                subPartColExprStrList.add(subPartBy.getPartitionExprList().get(i).toString());
            }
            subPartExpr = StringUtils.join(subPartColExprStrList, ",");
        }

        List<PartitionSpec> subPartTempSpecs = null;
        if (useSubPartTemp) {
            subPartTempSpecs = subPartBy.getPartitions();
        }

        List<PartitionSpec> partSpecs = partBy.getPartitions();
        for (int i = 0; i < partSpecs.size(); i++) {
            PartitionSpec partSpec = partSpecs.get(i);

            String partName = partSpec.getName();
            Long partPosi = partSpec.getPosition();
            String partDesc = partSpec.getBoundSpec().toString();
            String partArcStateName =
                TtlPartArcState.getTtlPartArcStateByArcStateValue(partSpec.getArcState()).getArcStateName();
            if (useSubPart) {
                List<PartitionSpec> subpartSpecs = partSpec.getSubPartitions();
                for (int j = 0; j < subpartSpecs.size(); j++) {
                    PartitionSpec subPartSpec = subpartSpecs.get(j);
                    String subPartName = subPartSpec.getName();
                    Long subPartPosi = subPartSpec.getPosition();
                    Long phySpecPosi = subPartSpec.getPhyPartPosition();
                    String subPartDesc = subPartSpec.getBoundSpec().toString();
                    String subPartTempName = "";
                    String subPartArcStatName =
                        TtlPartArcState.getTtlPartArcStateByArcStateValue(subPartSpec.getArcState()).getArcStateName();

                    String pgName = subPartName;
                    String phyDbGroup = subPartSpec.getLocation().getGroupKey();
                    String phyTb = subPartSpec.getLocation().getPhyTableName();
                    String rwDnId = getRwDnByGroupName(topology, phyDbGroup);
                    String phyDb = getPhyDbByGroupName(topology, phyDbGroup);

                    if (useSubPartTemp) {
                        subPartTempName = subPartTempSpecs.get(j).getName();
                    }

                    PartitionMetaRecord metaRec = new PartitionMetaRecord();

                    metaRec.partNum = phySpecPosi;

                    metaRec.tableSchema = dbName;
                    metaRec.tableName = tblName;
                    metaRec.indexName = rawGsiName;
                    metaRec.primaryTable = primaryTbl;
                    metaRec.tableType = tblType;
                    metaRec.tgName = tgName;

                    metaRec.partMethod = partMethod;
                    metaRec.partCol = partCol;
                    metaRec.partColType = partColType;
                    metaRec.partExpr = partExpr;
                    metaRec.partName = partName;
                    metaRec.partPosi = partPosi;
                    metaRec.partDesc = partDesc;
                    metaRec.partArcStateName = partArcStateName;

                    metaRec.subPartMethod = subPartMethod;
                    metaRec.subPartCol = subPartCol;
                    metaRec.subPartColType = subPartColType;
                    metaRec.subPartExpr = subPartExpr;
                    metaRec.subPartName = subPartName;
                    metaRec.subPartTempName = subPartTempName;
                    metaRec.subPartPosi = subPartPosi;
                    metaRec.subPartDesc = subPartDesc;
                    metaRec.subPartArcStateName = subPartArcStatName;

                    metaRec.pgName = pgName;
                    metaRec.phyDbGroup = phyDbGroup;
                    metaRec.phyDb = phyDb;
                    metaRec.phyTb = phyTb;
                    metaRec.rwDnId = rwDnId;

                    result.add(metaRec);

                }
            } else {

                String pgName = partName;
                Long phySpecPosi = partSpec.getPhyPartPosition();
                String phyDbGroup = partSpec.getLocation().getGroupKey();
                String phyTb = partSpec.getLocation().getPhyTableName();
                String rwDnId = getRwDnByGroupName(topology, phyDbGroup);
                String phyDb = getPhyDbByGroupName(topology, phyDbGroup);

                PartitionMetaRecord metaRec = new PartitionMetaRecord();

                metaRec.partNum = phySpecPosi;

                metaRec.tableSchema = dbName;
                metaRec.tableName = tblName;
                metaRec.indexName = rawGsiName;
                metaRec.primaryTable = primaryTbl;
                metaRec.tableType = tblType;
                metaRec.tgName = tgName;

                metaRec.partMethod = partMethod;
                metaRec.partCol = partCol;
                metaRec.partColType = partColType;
                metaRec.partExpr = partExpr;
                metaRec.partName = partName;
                metaRec.partPosi = partPosi;
                metaRec.partDesc = partDesc;
                metaRec.partArcStateName = partArcStateName;

                metaRec.subPartMethod = null;
                metaRec.subPartCol = null;
                metaRec.subPartColType = null;
                metaRec.subPartExpr = null;
                metaRec.subPartName = null;
                metaRec.subPartTempName = null;
                metaRec.subPartPosi = null;
                metaRec.subPartDesc = null;
                metaRec.subPartArcStateName = null;

                metaRec.pgName = pgName;
                metaRec.phyDbGroup = phyDbGroup;
                metaRec.phyDb = phyDb;
                metaRec.phyTb = phyTb;
                metaRec.rwDnId = rwDnId;

                result.add(metaRec);
            }
        }

        return result;
    }

    protected static String getRwDnByGroupName(TopologyHandler topology, String grpName) {
        IGroupExecutor groupExecutor = topology.get(grpName);
        if (groupExecutor == null) {
            return null;
        }

        Object o = groupExecutor.getDataSource();
        if (o == null || !(o instanceof TGroupDataSource)) {
            return null;
        }
        TGroupDataSource groupDs = (TGroupDataSource) o;
        return groupDs.getMasterDNId();
    }

    public static String getPhyDbByGroupName(TopologyHandler topology, String grpName) {
        IGroupExecutor groupExecutor = topology.get(grpName);
        if (groupExecutor == null) {
            return null;
        }

        Object o = groupExecutor.getDataSource();
        if (o == null || !(o instanceof TGroupDataSource)) {
            return null;
        }
        TGroupDataSource groupDs = (TGroupDataSource) o;
        return getPhyDb(groupDs);
    }

    private static String getPhyDb(TGroupDataSource groupDataSource) {
        if (groupDataSource != null && groupDataSource.getConfigManager() != null) {
            TAtomDataSource atomDataSource = groupDataSource.getConfigManager().getDataSource(MasterSlave.MASTER_ONLY);
            if (atomDataSource != null && atomDataSource.getDsConfHandle() != null) {
                TAtomDsConfDO runTimeConf = atomDataSource.getDsConfHandle().getRunTimeConf();
                return runTimeConf != null ? runTimeConf.getDbName() : null;
            }
        }
        return null;
    }
}
