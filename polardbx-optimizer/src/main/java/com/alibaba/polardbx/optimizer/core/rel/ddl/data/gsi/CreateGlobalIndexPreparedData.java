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
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;

public class CreateGlobalIndexPreparedData extends DdlPreparedData {

    public CreateGlobalIndexPreparedData() {
    }

    private CreateTablePreparedData indexTablePreparedData;
    private RepartitionPrepareData repartitionPrepareData;

    private String primaryTableName;
    private String indexType;
    private String indexComment;
    private boolean unique;
    private boolean clusteredIndex;

    private boolean single;
    private boolean broadcast;
    private boolean visible = true;

    private TableRule indexTableRule;
    private SqlNode newSqlDdl;
    private TableRule primaryTableRule;
    private String primaryTableDefinition;
    private SqlIndexDefinition indexDefinition;
    private SqlCreateIndex sqlCreateIndex;

    private PartitionInfo indexPartitionInfo;
    private PartitionInfo primaryPartitionInfo;
    private SqlNode partitioning;
    private SqlNode tableGroupName;
    private SqlNode joinGroupName;
    private Map<SqlNode, RexNode> partBoundExprInfo;
    private boolean indexAlignWithPrimaryTableGroup;
    private boolean needToGetTableGroupLock = false;

    private LocalityDesc locality = new LocalityDesc();

    private List<String> oldPrimaryKeys;

    /**
     * Foreign key
     */
    private List<String> referencedTables;
    private List<ForeignKeyData> addedForeignKeys;

    private boolean repartition;

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

    public void setIndexType(final String indexType) {
        this.indexType = indexType;
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

    public SqlNode getJoinGroupName() {
        return joinGroupName;
    }

    public void setJoinGroupName(SqlNode joinGroupName) {
        this.joinGroupName = joinGroupName;
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

    public boolean isIndexAlignWithPrimaryTableGroup() {
        return indexAlignWithPrimaryTableGroup;
    }

    public void setIndexAlignWithPrimaryTableGroup(boolean indexAlignWithPrimaryTableGroup) {
        this.indexAlignWithPrimaryTableGroup = indexAlignWithPrimaryTableGroup;
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
}
