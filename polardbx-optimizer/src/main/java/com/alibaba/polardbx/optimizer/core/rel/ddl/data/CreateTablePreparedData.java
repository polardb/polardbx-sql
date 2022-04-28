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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.rule.TableRule;
import lombok.Data;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;

import java.util.Map;

@Data
public class CreateTablePreparedData extends DdlPreparedData {

    private TableMeta tableMeta;

    private boolean ifNotExists;
    private boolean shadow;
    private boolean autoPartition;
    private boolean broadcast;
    private boolean sharding;

    private boolean timestampColumnDefault;
    private Map<String, String> binaryColumnDefaultValues;

    private SqlNode dbPartitionBy;
    private SqlNode dbPartitions;
    private SqlNode tbPartitionBy;
    private SqlNode tbPartitions;

    private SqlNode partitioning;
    private SqlNode localPartitioning;
    private SqlNode tableGroupName;
    private Map<SqlNode, RexNode> partBoundExprInfo;

    private String tableDefinition;

    private TableRule tableRule;
    private PartitionInfo partitionInfo;
    private LocalPartitionDefinitionInfo localPartitionDefinitionInfo;

    /**
     * Create table with locality
     */
    private LocalityDesc locality;

    private String loadTableSchema;
    private String loadTableName;
    private String archiveTableName;

    /**
     * if Create gsi table
     */
    private boolean gsi;
}
