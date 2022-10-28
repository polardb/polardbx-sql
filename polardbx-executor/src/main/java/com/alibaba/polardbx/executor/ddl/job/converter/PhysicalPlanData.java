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
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
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
import java.util.List;
import java.util.Map;

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
    private TableGroupConfig tableGroupConfig;

    private boolean truncatePartition;

    private LocalityDesc localityDesc;

    private AlterTablePreparedData alterTablePreparedData;

    private boolean flashbackRename = false;

    @Override
    public String toString() {
        return String.format("PhysicalPlan{table: %s, sql: %s, topology: %s",
            this.logicalTableName, this.sqlTemplate.replace("\n", ""), this.tableTopology);
    }

    public PhysicalPlanData clone(){
        PhysicalPlanData clone = new PhysicalPlanData();
        clone.schemaName = this.schemaName;
        clone.logicalTableName = this.logicalTableName;
        clone.newLogicalTableName = this.newLogicalTableName;
        clone.indexName = this.indexName;
        clone.defaultDbIndex = this.defaultDbIndex;
        clone.defaultPhyTableName = this.defaultPhyTableName;
        clone.tablesExtRecord = this.tablesExtRecord;
        clone.tableTopology = new HashMap<>(this.tableTopology);
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
        return clone;
    }

    public List<List<Map<Integer, ParameterContext>>> partitionParamsList(int count){
        return Lists.partition(paramsList, count);
    }

    public List<Map<String, List<List<String>>>> partitionTableTopology(int count){
        List<Pair<String, List<String>>> flatTopology = new ArrayList<>();
        for (Map.Entry<String, List<List<String>>> entry : tableTopology.entrySet()) {
            for(List<String> item: entry.getValue()){
                flatTopology.add(Pair.of(entry.getKey(), item));
            }
        }
        List<List<Pair<String, List<String>>>> partitionedFlatTopology = Lists.partition(flatTopology, count);

        List<Map<String, List<List<String>>>> result = new ArrayList<>();
        for (List<Pair<String, List<String>>> itemsInOneMap: partitionedFlatTopology){
            Map<String, List<List<String>>> map = new HashMap<>();
            for(Pair<String, List<String>> item: itemsInOneMap){
                map.compute(item.getKey(), (s, lists) -> {
                    if(lists==null){
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

}
