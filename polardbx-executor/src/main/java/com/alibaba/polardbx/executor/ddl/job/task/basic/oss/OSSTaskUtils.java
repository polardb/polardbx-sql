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

package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.partition.MurmurHashUtils;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.timezone.TimestampUtils;
import com.alibaba.polardbx.executor.archive.writer.OSSBackFillWriterTask;
import com.alibaba.polardbx.executor.ddl.job.builder.DropPartitionTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropPartitionTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropPartitionTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableHideTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.StoreTableLocalityTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.GmsNodeManager.GmsNode;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.utils.TableTopologyUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.utils.ITimestampOracle.BITS_LOGICAL_TIME;

public class OSSTaskUtils {
    private static final ImmutableList<String> ENDPOINTS = ImmutableList.of(
        "oss-cn-hangzhou.aliyuncs.com", "oss-cn-hangzhou-internal.aliyuncs.com",
        "oss-cn-shanghai.aliyuncs.com", "oss-cn-shanghai-internal.aliyuncs.com",
        "oss-cn-nanjing.aliyuncs.com", "oss-cn-nanjing-internal.aliyuncs.com",
        "oss-cn-qingdao.aliyuncs.com", "oss-cn-qingdao-internal.aliyuncs.com",
        "oss-cn-beijing.aliyuncs.com", "oss-cn-beijing-internal.aliyuncs.com",
        "oss-cn-zhangjiakou", "oss-cn-zhangjiakou.aliyuncs.com",
        "oss-cn-huhehaote.aliyuncs.com", "oss-cn-huhehaote-internal.aliyuncs.com",
        "oss-cn-wulanchabu.aliyuncs.com", "oss-cn-wulanchabu-internal.aliyuncs.com",
        "oss-cn-shenzhen.aliyuncs.com", "oss-cn-shenzhen-internal.aliyuncs.com",
        "oss-cn-heyuan.aliyuncs.com", "oss-cn-heyuan-internal.aliyuncs.com",
        "oss-cn-guangzhou.aliyuncs.com", "oss-cn-guangzhou-internal.aliyuncs.com",
        "oss-cn-chengdu.aliyuncs.com", "oss-cn-chengdu-internal.aliyuncs.com",
        "oss-cn-hongkong.aliyuncs.com", "oss-cn-hongkong-internal.aliyuncs.com",
        "oss-us-west-1.aliyuncs.com", "oss-us-west-1-internal.aliyuncs.com",
        "oss-us-east-1.aliyuncs.com", "oss-us-east-1-internal.aliyuncs.com",
        "oss-ap-northeast-1.aliyuncs.com", "oss-ap-northeast-1-internal.aliyuncs.com",
        "oss-ap-northeast-2.aliyuncs.com", "oss-ap-northeast-2-internal.aliyuncs.com",
        "oss-ap-southeast-1.aliyuncs.com", "oss-ap-southeast-1-internal.aliyuncs.com",
        "oss-ap-southeast-2.aliyuncs.com", "oss-ap-southeast-2-internal.aliyuncs.com",
        "oss-ap-southeast-3.aliyuncs.com", "oss-ap-southeast-3-internal.aliyuncs.com",
        "oss-ap-southeast-5.aliyuncs.com", "oss-ap-southeast-5-internal.aliyuncs.com",
        "oss-ap-southeast-6.aliyuncs.com", "oss-ap-southeast-6-internal.aliyuncs.com",
        "oss-ap-southeast-7.aliyuncs.com", "oss-ap-southeast-7-internal.aliyuncs.com",
        "oss-ap-south-1.aliyuncs.com", "oss-ap-south-1-internal.aliyuncs.com",
        "oss-eu-central-1.aliyuncs.com", "oss-eu-central-1-internal.aliyuncs.com",
        "oss-eu-west-1.aliyuncs.com", "oss-eu-west-1-internal.aliyuncs.com",
        "oss-me-east-1.aliyuncs.com", "oss-me-east-1-internal.aliyuncs.com"
        );

    public static boolean checkEndpoint(String endpoint) {
        if (endpoint == null) {
            return false;
        }
        return ENDPOINTS.contains(endpoint);
    }

    public static Pair<String, String> getSingleTopology(String sourceLogicalSchema, String sourceLogicalTable, TableMeta sourceTableMeta) {
        Pair<String, String> singlePhySchemaAndTable = null;
        boolean isSingle =
                OptimizerContext.getContext(sourceLogicalSchema).getRuleManager().isTableInSingleDb(sourceLogicalTable);
        if (isSingle) {
            String singlePhySchema, singlePhyTable;
            for (Map.Entry<String, Set<String>> e : sourceTableMeta.getLatestTopology().entrySet()) {
                singlePhySchema = e.getKey();
                for (String setVal : e.getValue()) {
                    singlePhyTable = setVal;
                    singlePhySchemaAndTable = Pair.of(singlePhySchema, singlePhyTable);
                    break;
                }
            }
        }
        return singlePhySchemaAndTable;
    }

    public static PartitionInfo getSourcePartitionInfo(ExecutionContext executionContext, String sourceLogicalSchema,
                                                       String sourceLogicalTable) {
        PartitionInfoManager partitionInfoManager =
            executionContext.getSchemaManager(sourceLogicalSchema).getTddlRuleManager().getPartitionInfoManager();
        return partitionInfoManager.getPartitionInfo(sourceLogicalTable);
    }

    public static Map<String, Set<String>> genSourcePhyTables(Map<Pair<String, String>, OSSBackFillWriterTask> tasks) {
        Map<String, Set<String>> sourcePhyTables = new HashMap<>();
        for (Pair<String, String> sourcePhySchemaAndTable : tasks.keySet()) {
            sourcePhyTables.computeIfAbsent(sourcePhySchemaAndTable.getKey(), x -> new HashSet<>());
            sourcePhyTables.get(sourcePhySchemaAndTable.getKey()).add(sourcePhySchemaAndTable.getValue());
        }
        return sourcePhyTables;
    }

    public static Pair<String, String> getSourcePhyTable(PartitionInfo loadTablePartitionInfo, String partName) {
        List<String> partitionNameList = ImmutableList.of(partName);
        PhysicalPartitionInfo loadTablePhysicalPartitionInfo =
            loadTablePartitionInfo
                .getPhysicalPartitionTopology(partitionNameList)
                .values()
                .stream()
                .findFirst().get().get(0);
        return Pair.of(loadTablePhysicalPartitionInfo.getGroupKey(), loadTablePhysicalPartitionInfo.getPhyTable());
    }

    public static List<PhysicalPartitionInfo> getFlattenedPartitionInfo(String schema, String table) {
        List<PhysicalPartitionInfo> partitionInfos = new ArrayList<>();
        OptimizerContext.getContext(schema).getLatestSchemaManager().getTable(table)
            .getPartitionInfo().getPhysicalPartitionTopology(ImmutableList.of())
            .values().forEach(partitionInfos::addAll);
        return partitionInfos;
    }

    public static List<PhysicalPartitionInfo> getFlattenedPartitionInfo(String schema, String table,
                                                                        List<String> partNames) {
        Set<String> partNamesSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        partNamesSet.addAll(partNames);
        return getFlattenedPartitionInfo(schema, table).stream().filter(e -> partNamesSet.contains(e.getPartName()))
            .collect(Collectors.toList());
    }

    public static List<PhysicalPartitionInfo> getOrderedPartitionInfo(List<PhysicalPartitionInfo> partitionInfos,
                                                                      int total, int serial) {
        partitionInfos.sort(Comparator.comparing(PhysicalPartitionInfo::getPartBitSetIdx));
        List<PhysicalPartitionInfo> result = new ArrayList<>();
        int cnt = 0;
        for (PhysicalPartitionInfo physicalPartitionInfo : partitionInfos) {
            cnt++;
            // only consider tables with the same remainder
            if (cnt % total == (serial)) {
                result.add(physicalPartitionInfo);
            }
        }
        return result;
    }

    public static List<PhysicalPartitionInfo> getOrderedPartitionInfo(String schema, String table,
                                                                      int total, int serial) {
        List<PhysicalPartitionInfo> partitionInfos = OSSTaskUtils.getFlattenedPartitionInfo(schema, table);
        return OSSTaskUtils.getOrderedPartitionInfo(partitionInfos, total, serial);
    }

    public static Optional<String> chooseRemoteNode(Long taskId) {
        List<GmsNode> remoteNodeList = GmsNodeManager.getInstance().getRemoteNodes();
        if (CollectionUtils.isEmpty(remoteNodeList)) {
            //no remote node, so choose local node
            return Optional.empty();
        }
        List<GmsNode> candidates = new ArrayList<>(remoteNodeList);
        candidates.add(null);
        int mod = (int) (MurmurHashUtils.murmurHash128WithZeroSeed(taskId) % candidates.size());
        if (mod < 0) {
            mod = mod + candidates.size();
        }
        GmsNode chosenNode = candidates.get(mod);
        if (chosenNode == null) {
            //choose local node
            return Optional.empty();
        }
        //choose remote node by taskId
        return Optional.of(chosenNode.getServerKey());
    }

    public static int getArchiveParallelism(ExecutionContext ec) {
        int parallelism = (int) ec.getParamManager()
            .getLong(ConnectionParams.FILE_STORAGE_TASK_PARALLELISM);
        return parallelism <= 0 ? 4 : parallelism;
    }

    public static int getMppParallelism(ExecutionContext executionContext, TableMeta primaryTableMeta) {
        int parallelism = OSSTaskUtils.getArchiveParallelism(executionContext);
        if (TableTopologyUtil.isShard(primaryTableMeta)) {
            return Math.min(parallelism, primaryTableMeta.getPartitionInfo().getAllPhysicalPartitionCount());
        }

        if (TableTopologyUtil.isBroadcast(primaryTableMeta)) {
            return Math.min(parallelism,
                primaryTableMeta.getPartitionInfo().getPhysicalPartitionTopology(ImmutableList.of()).size());
        }
        // single table
        return 1;
    }

    public static List<DdlTask> dropTableTasks(Engine engine, String schemaName, String logicalTableName,
                                               boolean ifExists, ExecutionContext executionContext) {
        DropTableBuilder dropTableBuilder =
            DropPartitionTableBuilder.createBuilder(schemaName, logicalTableName, true, executionContext);
        dropTableBuilder.build();
        PhysicalPlanData physicalPlanData = dropTableBuilder.genPhysicalPlanData();

        List<Long> tableGroupIds = new ArrayList<>();

        PartitionInfo partitionInfo =
                OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);
        Long tableGroupId = -1L;
        TableGroupConfig tableGroupConfig = null;
        if (partitionInfo != null) {
            tableGroupId = partitionInfo.getTableGroupId();
            tableGroupConfig = physicalPlanData.getTableGroupConfig();
        }
        List<DdlTask> tasks = new ArrayList<>();
        DropPartitionTableValidateTask validateTask =
                new DropPartitionTableValidateTask(schemaName, logicalTableName, tableGroupIds, tableGroupConfig);
        DropTableHideTableMetaTask dropTableHideTableMetaTask =
                new DropTableHideTableMetaTask(schemaName, logicalTableName);
        DropOssFilesTask dropOssFilesTask = new DropOssFilesTask(engine.name(), schemaName, logicalTableName, null);
        DropTablePhyDdlTask phyDdlTask = new DropTablePhyDdlTask(schemaName, physicalPlanData);
        DropPartitionTableRemoveMetaTask removeMetaTask =
                new DropPartitionTableRemoveMetaTask(schemaName, logicalTableName);

        DdlTask syncTableGroup = null;
        if (tableGroupId != -1) {
            //tableGroupConfig from physicalPlanData is not set tableGroup record
            tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                    .getTableGroupConfigById(tableGroupId);
            syncTableGroup =
                    new TableGroupSyncTask(schemaName, tableGroupConfig.getTableGroupRecord().getTg_name());
        }

        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);
        StoreTableLocalityTask dropLocality = new
            StoreTableLocalityTask(schemaName, logicalTableName, "", true);

        /**
         * todo chenyi
         * DropTableJobFactory中已经把元数据操作都合并到一个Task中了
         * 考虑将DropTableHideTableMetaTask、DropPartitionTableRemoveMetaTask也合并一下？
         */
        tasks.add(validateTask);
        tasks.add(dropLocality);
        tasks.add(dropTableHideTableMetaTask);
        tasks.add(dropOssFilesTask);
        tasks.add(phyDdlTask);
        tasks.add(removeMetaTask);
        if (syncTableGroup != null) {
            tasks.add(syncTableGroup);
        }
        tasks.add(tableSyncTask);
        return tasks;
    }
}
