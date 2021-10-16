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
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import lombok.Data;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;

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

    @Override
    public String toString() {
        return String.format("PhysicalPlan{table: %s, sql: %s, topology: %s",
            this.logicalTableName, this.sqlTemplate.replace("\n", ""), this.tableTopology);
    }

}
