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

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.gms.lbac.LBACSecurityEntity;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.rule.TableRule;
import lombok.Data;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Data
public class CreateTablePreparedData extends DdlPreparedData {

    private TableMeta tableMeta;

    private boolean ifNotExists;
    private boolean shadow;
    private boolean autoPartition;
    private boolean broadcast;
    private boolean sharding;

    private boolean timestampColumnDefault;
    private Map<String, String> specialDefaultValues;
    private Map<String, Long> specialDefaultValueFlags;

    private SqlNode dbPartitionBy;
    private SqlNode dbPartitions;
    private SqlNode tbPartitionBy;
    private SqlNode tbPartitions;

    private SqlNode partitioning;
    private SqlNode localPartitioning;
    private SqlNode tableGroupName;
    // if true the tablegroup should no-exist or create automatically
    private boolean withImplicitTableGroup;
    //if value=true, the no-exist tablegroup will be created before create table/gsi
    private Map<String, Boolean> relatedTableGroupInfo = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private SqlNode joinGroupName;
    private Map<SqlNode, RexNode> partBoundExprInfo;

    private String tableDefinition;

    /**
     * Foreign key
     */
    private List<String> referencedTables;
    private List<ForeignKeyData> addedForeignKeys;

    private TableRule tableRule;
    private PartitionInfo partitionInfo;
    private LocalPartitionDefinitionInfo localPartitionDefinitionInfo;

    private SqlNode ttlDefinitionExpr;
    private TtlDefinitionInfo ttlDefinitionInfo;

    private String selectSql;

    /**
     * Create table with locality
     */
    private LocalityDesc locality;

    /**
     * The schemaName of ttl table
     */
    private String loadTableSchema;
    /**
     * The tableName of ttl table
     */
    private String loadTableName;

    /**
     * The schemaName of ttl-tmp table
     */
    private String archiveTmpTableSchema;

    /**
     * The tableName of ttl-tmp table
     */
    private String archiveTmpTableName;

    /**
     * The schemaName of oss table
     */
    private String archiveTableSchema;

    /**
     * The tableName of oss table
     */
    private String archiveTableName;

    /**
     * The real source schemaName of oss table, the schemaName of ttl table
     */
    private String archiveActualSourceTableSchema;
    /**
     * The real source tableName of oss table, the tableName of ttl table
     */
    private String archiveActualSourceTableName;

    /**
     * Label if curr table is a ttl-tmp table
     */
    private boolean ttlTemporary;

    /**
     * if Create gsi table
     */
    private boolean gsi;

    private String sourceSql;
    private boolean needToGetTableGroupLock;

    private boolean importTable;

    private boolean reimportTable;

    /**
     * lbac
     */
    private LBACSecurityEntity tableEAS;
    private List<LBACSecurityEntity> colEASList;

    LikeTableInfo likeTableInfo;

}
