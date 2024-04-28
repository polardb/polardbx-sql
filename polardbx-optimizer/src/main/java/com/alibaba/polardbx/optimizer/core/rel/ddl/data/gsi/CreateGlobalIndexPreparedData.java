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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi;

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DdlPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RepartitionPrepareData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.rule.TableRule;
import com.clearspring.analytics.util.Lists;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CreateGlobalIndexPreparedData extends DdlPreparedData {

    private CreateTablePreparedData indexTablePreparedData;
    private RepartitionPrepareData repartitionPrepareData;
    private String primaryTableName;
    private String indexType;
    private String indexComment;
    private boolean unique;
    private boolean clusteredIndex;
    private boolean columnarIndex;
    private boolean single;
    private boolean broadcast;
    private boolean visible = true;
    private TableRule indexTableRule;
    private SqlNode newSqlDdl;
    private TableRule primaryTableRule;
    private String primaryTableDefinition;
    private SqlIndexDefinition indexDefinition;
    private SqlCreateIndex sqlCreateIndex;
    private SqlCreateIndex originSqlCreateIndex;
    private PartitionInfo indexPartitionInfo;
    private PartitionInfo primaryPartitionInfo;
    private SqlNode partitioning;
    private SqlNode tableGroupName;
    private boolean withImplicitTableGroup;

    //if value=true, the no-exist tablegroup will be created before create table/gsi
    private Map<String, Boolean> relatedTableGroupInfo = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private SqlNode joinGroupName;
    private Map<SqlNode, RexNode> partBoundExprInfo;

    //target table name, may be primary table or previous GSI in the same create statement
    private String tableGroupAlignWithTargetTable;
    private boolean needToGetTableGroupLock = false;
    private SqlNode engineName;
    private LocalityDesc locality = new LocalityDesc();
    private List<String> oldPrimaryKeys;
    /**
     * Should skip some subtask for create table with cci
     */
    private boolean createTableWithIndex;

    /**
     * Foreign key
     */
    private List<String> referencedTables;
    private List<ForeignKeyData> addedForeignKeys;

    private boolean repartition;

    public CreateGlobalIndexPreparedData() {
    }

    /**************************************************************************/

    public CreateTablePreparedData getIndexTablePreparedData() {
        return indexTablePreparedData;
    }

    public void setIndexTablePreparedData(CreateTablePreparedData indexTablePreparedData) {
        this.indexTablePreparedData = indexTablePreparedData;
    }

    public RepartitionPrepareData getRepartitionPrepareData() {
        return repartitionPrepareData;
    }

    public void setRepartitionPrepareData(RepartitionPrepareData repartitionPrepareData) {
        this.repartitionPrepareData = repartitionPrepareData;
    }

    public String getPrimaryTableName() {
        return primaryTableName;
    }

    public void setPrimaryTableName(String primaryTableName) {
        this.primaryTableName = primaryTableName;
    }

    public TableRule getPrimaryTableRule() {
        return primaryTableRule;
    }

    public void setPrimaryTableRule(TableRule primaryTableRule) {
        this.primaryTableRule = primaryTableRule;
    }

    public String getPrimaryTableDefinition() {
        return primaryTableDefinition;
    }

    public void setPrimaryTableDefinition(String primaryTableDefinition) {
        this.primaryTableDefinition = primaryTableDefinition;
    }

    public String getIndexTableName() {
        return getTableName();
    }

    public TableRule getIndexTableRule() {
        return indexTableRule;
    }

    public void setIndexTableRule(TableRule indexTableRule) {
        this.indexTableRule = indexTableRule;
    }

    public SqlIndexDefinition getIndexDefinition() {
        return indexDefinition;
    }

    public void setIndexDefinition(SqlIndexDefinition indexDefinition) {
        this.indexDefinition = indexDefinition;
    }

    public SqlCreateIndex getSqlCreateIndex() {
        return sqlCreateIndex;
    }

    public void setSqlCreateIndex(SqlCreateIndex sqlCreateIndex) {
        this.sqlCreateIndex = sqlCreateIndex;
    }

    public SqlCreateIndex getOriginSqlCreateIndex() {
        return originSqlCreateIndex;
    }

    public void setOriginSqlCreateIndex(SqlCreateIndex originSqlCreateIndex) {
        this.originSqlCreateIndex = originSqlCreateIndex;
    }

    public SqlIdentifier getOriginIndexName() {
        return this.sqlCreateIndex.getOriginIndexName();
    }

    public List<SqlIndexColumnName> getColumns() {
        if (indexDefinition != null) {
            return indexDefinition.getColumns();
        }
        return sqlCreateIndex.getColumns();
    }

    public List<SqlIndexColumnName> getCoverings() {
        if (indexDefinition != null) {
            return indexDefinition.getCovering();
        }
        return sqlCreateIndex.getCovering();
    }

    public SqlNode getNewSqlDdl() {
        return newSqlDdl;
    }

    public void setNewSqlDdl(SqlNode newSqlDdl) {
        this.newSqlDdl = newSqlDdl;
    }

    public String getIndexComment() {
        return this.indexComment;
    }

    public void setIndexComment(final String indexComment) {
        this.indexComment = indexComment;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public boolean isSingle() {
        return this.single;
    }

    public void setSingle(final boolean single) {
        this.single = single;
    }

    public boolean isBroadcast() {
        return this.broadcast;
    }

    public void setBroadcast(final boolean broadcast) {
        this.broadcast = broadcast;
    }

    public String getComment() {
        return indexComment;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(final String indexType) {
        this.indexType = indexType;
    }

    public PartitionInfo getIndexPartitionInfo() {
        return indexPartitionInfo;
    }

    public void setIndexPartitionInfo(PartitionInfo indexPartitionInfo) {
        this.indexPartitionInfo = indexPartitionInfo;
    }

    public PartitionInfo getPrimaryPartitionInfo() {
        return primaryPartitionInfo;
    }

    public void setPrimaryPartitionInfo(PartitionInfo primaryPartitionInfo) {
        this.primaryPartitionInfo = primaryPartitionInfo;
    }

    public SqlNode getPartitioning() {
        return partitioning;
    }

    public void setPartitioning(SqlNode partitioning) {
        this.partitioning = partitioning;
    }

    public SqlNode getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(SqlNode tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    public boolean isWithImplicitTableGroup() {
        return withImplicitTableGroup;
    }

    public void setWithImplicitTableGroup(boolean withImplicitTableGroup) {
        this.withImplicitTableGroup = withImplicitTableGroup;
    }

    public Map<String, Boolean> getRelatedTableGroupInfo() {
        return relatedTableGroupInfo;
    }

    public void setRelatedTableGroupInfo(Map<String, Boolean> relatedTableGroupInfo) {
        this.relatedTableGroupInfo = relatedTableGroupInfo;
    }

    public SqlNode getJoinGroupName() {
        return joinGroupName;
    }

    public void setJoinGroupName(SqlNode joinGroupName) {
        this.joinGroupName = joinGroupName;
    }

    public SqlNode getEngineName() {
        return engineName;
    }

    public void setEngineName(SqlNode engineName) {
        this.engineName = engineName;
    }

    public Map<SqlNode, RexNode> getPartBoundExprInfo() {
        return partBoundExprInfo;
    }

    public void setPartBoundExprInfo(
        Map<SqlNode, RexNode> partBoundExprInfo) {
        this.partBoundExprInfo = partBoundExprInfo;
    }

    public List<String> getShardColumns() {
        if (indexPartitionInfo != null) {
            // Legacy code may invoke this to generate auto shard key, so force keep order here.
            return indexPartitionInfo.getPartitionColumnsNotReorder();
        } else {
            return indexTableRule.getShardColumns();
        }
    }

    public boolean isNewPartIndex() {
        return indexPartitionInfo != null;
    }

    public List<List<String>> getAllLevelPartColumns() {
        if (indexPartitionInfo != null) {
            return indexPartitionInfo.getAllLevelFullPartCols();
        } else {
            return Lists.newArrayList();
        }
    }

    public List<String> getShardColumnsNotReorder() {
        if (indexPartitionInfo != null) {
            return indexPartitionInfo.getPartitionColumnsNotReorder();
        } else {
            return indexTableRule.getShardColumns();
        }
    }

    public boolean isClusteredIndex() {
        return this.clusteredIndex;
    }

    public void setClusteredIndex(final boolean clusteredIndex) {
        this.clusteredIndex = clusteredIndex;
    }

    public boolean isColumnarIndex() {
        return this.columnarIndex;
    }

    public void setColumnarIndex(final boolean columnarIndex) {
        this.columnarIndex = columnarIndex;
    }

    public String getTableGroupAlignWithTargetTable() {
        return tableGroupAlignWithTargetTable;
    }

    public void setTableGroupAlignWithTargetTable(String tableGroupAlignWithTargetTable) {
        this.tableGroupAlignWithTargetTable = tableGroupAlignWithTargetTable;
    }

    public LocalityDesc getLocality() {
        return locality;
    }

    public void setLocality(LocalityDesc locality) {
        this.locality = locality;
    }

    public boolean isNeedToGetTableGroupLock() {
        return needToGetTableGroupLock;
    }

    public void setNeedToGetTableGroupLock(boolean needToGetTableGroupLock) {
        this.needToGetTableGroupLock = needToGetTableGroupLock;
    }

    public boolean isVisible() {
        return visible;
    }

    public void setVisible(boolean visible) {
        this.visible = visible;
    }

    public List<String> getOldPrimaryKeys() {
        return oldPrimaryKeys;
    }

    public void setOldPrimaryKeys(List<String> oldPrimaryKeys) {
        this.oldPrimaryKeys = oldPrimaryKeys;
    }

    public List<ForeignKeyData> getAddedForeignKeys() {
        return addedForeignKeys;
    }

    public void setAddedForeignKeys(List<ForeignKeyData> addedForeignKeys) {
        this.addedForeignKeys = addedForeignKeys;
    }

    public boolean isRepartition() {
        return repartition;
    }

    public void setRepartition(boolean repartition) {
        this.repartition = repartition;
    }

    public boolean isCreateTableWithIndex() {
        return createTableWithIndex;
    }

    public void setCreateTableWithIndex(boolean createTableWithIndex) {
        this.createTableWithIndex = createTableWithIndex;
    }

    public SqlIndexDefinition getOrBuildIndexDefinition() {
        if (null == indexDefinition && null == sqlCreateIndex) {
            return null;
        }
        if (null != indexDefinition) {
            return indexDefinition;
        }
        return createIndex2IndexDefinition(sqlCreateIndex);
    }

    public SqlCreateIndex getOrBuildCreateIndex() {
        if (null == indexDefinition && null == sqlCreateIndex) {
            return null;
        }
        if (null != sqlCreateIndex) {
            return sqlCreateIndex;
        }
        return indexDefinition2CreateIndex(indexDefinition, null, null, null, null);
    }

    public SqlCreateIndex getOrBuildOriginCreateIndex() {
        if (null == indexDefinition && null == originSqlCreateIndex) {
            return null;
        }
        if (null != originSqlCreateIndex) {
            return originSqlCreateIndex;
        }
        return indexDefinition2CreateIndex(indexDefinition, null, null, null, null);
    }

    public static SqlIndexDefinition createIndex2IndexDefinition(SqlCreateIndex sqlCreateIndex) {
        return new SqlIndexDefinition(
            sqlCreateIndex.getParserPosition(),
            false,
            null,
            sqlCreateIndex.getIndexResiding(),
            sqlCreateIndex.getConstraintType().name(),
            sqlCreateIndex.getIndexType(),
            sqlCreateIndex.getIndexName(),
            sqlCreateIndex.getOriginIndexName(),
            sqlCreateIndex.getOriginTableName(),
            sqlCreateIndex.getColumns(),
            sqlCreateIndex.getCovering(),
            sqlCreateIndex.getOriginCovering(),
            sqlCreateIndex.getDbPartitionBy(),
            sqlCreateIndex.getTbPartitionBy(),
            sqlCreateIndex.getTbPartitions(),
            sqlCreateIndex.getPartitioning(),
            sqlCreateIndex.getOriginPartitioning(),
            sqlCreateIndex.getClusteredKeys(),
            sqlCreateIndex.getOptions(),
            sqlCreateIndex.createClusteredIndex(),
            sqlCreateIndex.createCci(),
            sqlCreateIndex.getTableGroupName(),
            sqlCreateIndex.getEngineName(),
            sqlCreateIndex.getDictColumns(),
            sqlCreateIndex.isWithImplicitTableGroup(),
            sqlCreateIndex.isVisible()
        );
    }

    public static SqlCreateIndex indexDefinition2CreateIndex(SqlIndexDefinition sqlIndexDefinition,
                                                             SqlCreateIndex.SqlIndexConstraintType constraintType,
                                                             SqlCreateIndex.SqlIndexAlgorithmType algorithmType,
                                                             SqlCreateIndex.SqlIndexLockType lockType,
                                                             String sourceSql) {
        return new SqlCreateIndex(
            sqlIndexDefinition.getParserPosition(),
            false,
            false,
            sqlIndexDefinition.getTable(),
            sqlIndexDefinition.getTable(),
            sqlIndexDefinition.getIndexName(),
            sqlIndexDefinition.getOriginIndexName(),
            sqlIndexDefinition.getColumns(),
            constraintType,
            sqlIndexDefinition.getIndexResiding(),
            sqlIndexDefinition.getIndexType(),
            sqlIndexDefinition.getOptions(),
            algorithmType,
            lockType,
            sqlIndexDefinition.getCovering(),
            sqlIndexDefinition.getOriginCovering(),
            sqlIndexDefinition.getDbPartitionBy(),
            sqlIndexDefinition.getTbPartitionBy(),
            sqlIndexDefinition.getTbPartitions(),
            sqlIndexDefinition.getPartitioning(),
            sqlIndexDefinition.getOriginPartitioning(),
            sqlIndexDefinition.getClusteredKeys(),
            sourceSql,
            null,
            null,
            sqlIndexDefinition.isClustered(),
            sqlIndexDefinition.isColumnar(),
            sqlIndexDefinition.getTableGroupName(),
            sqlIndexDefinition.isWithImplicitTableGroup(),
            sqlIndexDefinition.getEngineName(),
            sqlIndexDefinition.getDictColumns(),
            sqlIndexDefinition.isVisible()
        );
    }
}
