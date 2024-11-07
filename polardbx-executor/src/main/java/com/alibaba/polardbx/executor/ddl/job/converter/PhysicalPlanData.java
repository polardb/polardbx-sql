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

package com.alibaba.polardbx.executor.ddl.job.converter;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.TddlGroupExecutor;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.lbac.LBACSecurityEntity;
import com.alibaba.polardbx.gms.tablegroup.TableGroupDetailConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Data
public class PhysicalPlanData {

    private String schemaName;

    private String logicalTableName;
    private String newLogicalTableName;

    private String indexName;

    private String defaultDbIndex;
    private String defaultPhyTableName;

    private TablesExtRecord tablesExtRecord;
    private Map<String, List<List<String>>> tableTopology;
    private Map<String, List<PhysicalPartitionInfo>> physicalPartitionTopology;

    private SqlKind kind;

    private String sqlTemplate;
    private List<Map<Integer, ParameterContext>> paramsList;

    private boolean explain;
    private boolean partitioned;
    private boolean withHint;

    private boolean ifNotExists;
    private boolean ifExists;

    private boolean temporary;

    private SequenceBean sequence;
    private String createTablePhysicalSql;

    private PartitionInfo partitionInfo;
    private TableGroupDetailConfig tableGroupConfig;

    private boolean truncatePartition;

    private LocalityDesc localityDesc;

    private AlterTablePreparedData alterTablePreparedData;

    private boolean flashbackRename = false;

    private boolean renamePhyTable = false;

    private LBACSecurityEntity tableESA;
    private List<LBACSecurityEntity> colEsaList;

    @Override
    public String toString() {
        return String.format("PhysicalPlan{table: %s, sql: %s, topology: %s",
            this.logicalTableName, this.sqlTemplate.replace("\n", ""), this.tableTopology);
    }

    public PhysicalPlanData clone() {
        PhysicalPlanData clone = new PhysicalPlanData();
        clone.schemaName = this.schemaName;
        clone.logicalTableName = this.logicalTableName;
        clone.newLogicalTableName = this.newLogicalTableName;
        clone.indexName = this.indexName;
        clone.defaultDbIndex = this.defaultDbIndex;
        clone.defaultPhyTableName = this.defaultPhyTableName;
        clone.tablesExtRecord = this.tablesExtRecord;
        clone.tableTopology = tableTopology == null ? null : new LinkedHashMap<>(this.tableTopology);
        clone.physicalPartitionTopology =
            this.physicalPartitionTopology == null ? null : new LinkedHashMap<>(this.physicalPartitionTopology);
        clone.kind = this.kind;
        clone.sqlTemplate = this.sqlTemplate;
        clone.paramsList = new ArrayList<>(this.paramsList);
        clone.explain = this.explain;
        clone.partitioned = this.partitioned;
        clone.withHint = this.withHint;
        clone.ifNotExists = this.ifNotExists;
        clone.ifExists = this.ifExists;
        clone.temporary = this.temporary;
        clone.sequence = this.sequence;
        clone.createTablePhysicalSql = this.createTablePhysicalSql;
        clone.partitionInfo = this.partitionInfo;
        clone.tableGroupConfig = this.tableGroupConfig;
        clone.truncatePartition = this.truncatePartition;
        clone.localityDesc = this.localityDesc;
        clone.alterTablePreparedData = this.alterTablePreparedData;
        clone.renamePhyTable = this.renamePhyTable;
        clone.tableESA = this.tableESA;
        clone.colEsaList = colEsaList == null ? null : new ArrayList<>(colEsaList);
        return clone;
    }

    public List<List<Map<Integer, ParameterContext>>> partitionParamsList(int count) {
        return Lists.partition(paramsList, count);
    }

    /**
     * <pre>
     *     Convert to data_struct from
     *          Map{GrpKey,List<log_name_list_with_same_phy_index}
     *              grp0->[[t1_0,t2_0,t3_0],[t1_2,t2_2,t3_2],...]
     *              grp1->[[t1_1,t2_1,t3_1],[t1_3,t2_2,t3_3],...]
     *     to
     *          SubMap1{grpKey, List[log_name_list_with_same_phy_index]}
     *              grp0->[[t1_0,t2_0,t3_0],...]
     *              grp1->[[t1_1,t2_1,t3_1],...]
     *          SubMap2{grpKey, List[log_name_list_with_same_phy_index]}
     *              grp0->[[t1_2,t2_2,t3_2],...]
     *              grp1->[[t1_3,t2_2,t3_3],...]
     *
     * </pre>
     */
    public List<Map<String, List<List<String>>>> partitionTableTopology(int parallelism) {
        /**
         * <pre>
         *     Convert to data_struct from
         *          Map{GrpKey,List<log_name_list_with_same_phy_index}
         *              grp0->[[t1_0,t2_0,t3_0],[t1_2,t2_2,t3_2],...]
         *              grp1->[[t1_1,t2_1,t3_1],[t1_3,t2_2,t3_3],...]
         *     to
         [{grp0, [t1_0,t2_0,t3_0]}, {grp0,[t1_2,t2_2,t3_2]}, {grp1, [t1_1,t2_1,t3_1]},...]
         * </pre>
         */
        List<Pair<String, List<String>>> flatTopology = new ArrayList<>();
        for (Map.Entry<String, List<List<String>>> entry : tableTopology.entrySet()) {
            for (List<String> item : entry.getValue()) {
                flatTopology.add(Pair.of(entry.getKey(), item));
            }
        }

        /**
         * <pre>
         *     Convert to data_struct from
         *      List< {grpKey, log_name_list_with_same_phy_index} >
         *          e.g.
         *              grp0, t1_0,t2_0,t3_0
         *              grp0, t1_2,t2_2,t3_2
         *              grp1, t1_3,t2_3,t3_3
         *              grp1, t1_1,t2_1,t3_1
         *     to
         *      List< {grpKey, log_name_list_with_same_phy_index} > order bb zigzag
         *              grp0, t1_0,t2_0,t3_0
         *              grp1, t1_1,t2_1,t3_1
         *              grp0, t1_2,t2_2,t3_2
         *              grp1, t1_3,t2_3,t3_3
         *
         * </pre>
         */
        List<Pair<String, List<String>>> zigzaggedFlatTopology = zigzagOrderByDnList(flatTopology);

        /**
         * <pre>
         *     Convert to data_struct from
         *      List< {grpKey, log_name_list_with_same_phy_index} >
         *          e.g.
         *              grp0, t1_0,t2_0,t3_0
         *              grp1, t1_1,t2_1,t3_1
         *              grp0, t1_2,t2_2,t3_2
         *              grp1, t1_3,t2_3,t3_3
         *     to
         *      SubList1[{grpKey, log_name_list_with_same_phy_index}],
         *          e.g.
         *              grp0, t1_0,t2_0,t3_0
         *              grp1, t1_1,t2_1,t3_1
         *      SubList2[{grpKey, log_name_list_with_same_phy_index}],s
         *          e.g.
         *              grp0, t1_2,t2_2,t3_2
         *              grp1, t1_3,t2_2,t3_3
         * </pre>
         */
        List<List<Pair<String, List<String>>>> partitionedFlatTopology =
            Lists.partition(zigzaggedFlatTopology, parallelism);

        /**
         * <pre>
         *     Convert to data_struct from
         *      SubList1[{grpKey, log_name_list_with_same_phy_index}],
         *          e.g.
         *              [grp0, t1_0,t2_0,t3_0]
         *              [grp1, t1_1,t2_1,t3_1]
         *      SubList2[{grpKey, log_name_list_with_same_phy_index}],s
         *          e.g.
         *              [grp0, t1_2,t2_2,t3_2]
         *              [grp1, t1_3,t2_2,t3_3]
         *
         *      to
         *
         *          SubMap1{grpKey, List[log_name_list_with_same_phy_index]}
         *              grp0->t1_0,t2_0,t3_0
         *              grp1->t1_1,t2_1,t3_1
         *          SubMap2{grpKey, List[log_name_list_with_same_phy_index]}
         *              grp0->t1_2,t2_2,t3_2
         *              grp1->t1_3,t2_2,t3_3
         * </pre>
         */
        List<Map<String, List<List<String>>>> result = new ArrayList<>();
        for (List<Pair<String, List<String>>> itemsInOneMap : partitionedFlatTopology) {
            Map<String, List<List<String>>> map = new HashMap<>();
            for (Pair<String, List<String>> item : itemsInOneMap) {
                map.compute(item.getKey(), (s, lists) -> {
                    if (lists == null) {
                        lists = new ArrayList<>();
                    }
                    lists.add(item.getValue());
                    return lists;
                });
            }
            result.add(map);
        }
        return result;
    }

    protected List<Pair<String, List<String>>> zigzagOrderByDnList(List<Pair<String, List<String>>> flatTopology) {

        /**
         * <pre>
         *     key: dnId,
         *     val: idx list of flatTopology, start with zero
         * </pre>
         */
        Map<String, List<Integer>> dnIdToListIdxMapping = new TreeMap<>();
        TopologyHandler topologyHandler = ExecutorContext.getContext(this.schemaName).getTopologyHandler();

        for (int i = 0; i < flatTopology.size(); i++) {
            Pair<String, List<String>> item = flatTopology.get(i);
            String grpKey = item.getKey();
            TddlGroupExecutor grpExecutor = (TddlGroupExecutor) topologyHandler.get(grpKey);
            String rwDnId = grpExecutor.getDataSource().getMasterDNId();
            List<Integer> idxList = dnIdToListIdxMapping.get(rwDnId);
            if (idxList == null) {
                idxList = new ArrayList<>();
                dnIdToListIdxMapping.put(rwDnId, idxList);
            }
            idxList.add(i);
        }

        List<Pair<String, List<String>>> newFlatTopology = new ArrayList<>();

        int curPosiOfDnList = -1;
        boolean findAnyItems = false;
        while (true) {
            ++curPosiOfDnList;
            findAnyItems = false;
            for (Map.Entry<String, List<Integer>> mapItem : dnIdToListIdxMapping.entrySet()) {
                List<Integer> idxListOfDn = mapItem.getValue();
                if (curPosiOfDnList < idxListOfDn.size()) {
                    Integer idxVal = idxListOfDn.get(curPosiOfDnList);
                    newFlatTopology.add(flatTopology.get(idxVal));
                    findAnyItems = true;
                }
            }
            if (!findAnyItems) {
                break;
            }
        }
        return newFlatTopology;
    }

    public List<String> explainInfo() {
        String explainStringTemplate = "%s( tables=\"%s\", shardCount=%d, sql=\"%s\" )";
        int shardCount = this.tableTopology.keySet().stream().mapToInt(o -> this.tableTopology.get(o).size()).sum();
        String formatSql = this.sqlTemplate.replace("?\n\t", "? ").
            replace("  ", " ").replace("?  ", "? ").replace("\t", "  ");
        String explainString = String.format(explainStringTemplate,
            this.getKind(),
            this.getLogicalTableName(),
            shardCount,
            formatSql);
        List<String> result = new ArrayList<>();
        result.add(explainString);
        return result;
    }
}
