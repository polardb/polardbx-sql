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

package com.alibaba.polardbx.optimizer.partition.common;

import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;

public class BuildPartByDefFromAstParams {
    protected String schemaName;
    protected String tableName;
    protected String tableGroupName;
    protected String joinGroupName;

    protected boolean containPartByAst;
    protected List<SqlNode> partByAstColumns;
    protected List<SqlNode> partByAstPartitions;
    protected PartitionStrategy partByStrategy;
    protected SqlNode partCntAst;

    protected Map<SqlNode, RexNode> boundExprInfo;
    protected List<ColumnMeta> pkColMetas;
    protected List<ColumnMeta> allColMetas;
    protected PartitionTableType tblType;
    protected ExecutionContext ec;
    protected LocalityDesc locality;

    protected boolean buildSubpartBy;
    protected boolean useSubPartTemplate;
    protected List<PartitionSpec> parentPartSpecs;
    protected List<SqlNode> parentPartSpecAstList;
    protected boolean containNextLevelPartSpec;

    protected boolean ttlTemporary = false;

    public BuildPartByDefFromAstParams() {
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(String tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    public String getJoinGroupName() {
        return joinGroupName;
    }

    public void setJoinGroupName(String joinGroupName) {
        this.joinGroupName = joinGroupName;
    }

    public boolean isContainPartByAst() {
        return containPartByAst;
    }

    public void setContainPartByAst(boolean containPartByAst) {
        this.containPartByAst = containPartByAst;
    }

    public List<SqlNode> getPartByAstColumns() {
        return partByAstColumns;
    }

    public void setPartByAstColumns(List<SqlNode> partByAstColumns) {
        this.partByAstColumns = partByAstColumns;
    }

    public List<SqlNode> getPartByAstPartitions() {
        return partByAstPartitions;
    }

    public void setPartByAstPartitions(List<SqlNode> partByAstPartitions) {
        this.partByAstPartitions = partByAstPartitions;
    }

    public PartitionStrategy getPartByStrategy() {
        return partByStrategy;
    }

    public void setPartByStrategy(PartitionStrategy partByStrategy) {
        this.partByStrategy = partByStrategy;
    }

    public SqlNode getPartCntAst() {
        return partCntAst;
    }

    public void setPartCntAst(SqlNode partCntAst) {
        this.partCntAst = partCntAst;
    }

    public Map<SqlNode, RexNode> getBoundExprInfo() {
        return boundExprInfo;
    }

    public void setBoundExprInfo(Map<SqlNode, RexNode> boundExprInfo) {
        this.boundExprInfo = boundExprInfo;
    }

    public List<ColumnMeta> getPkColMetas() {
        return pkColMetas;
    }

    public void setPkColMetas(List<ColumnMeta> pkColMetas) {
        this.pkColMetas = pkColMetas;
    }

    public List<ColumnMeta> getAllColMetas() {
        return allColMetas;
    }

    public void setAllColMetas(List<ColumnMeta> allColMetas) {
        this.allColMetas = allColMetas;
    }

    public PartitionTableType getTblType() {
        return tblType;
    }

    public void setTblType(PartitionTableType tblType) {
        this.tblType = tblType;
    }

    public ExecutionContext getEc() {
        return ec;
    }

    public void setEc(ExecutionContext ec) {
        this.ec = ec;
    }

    public LocalityDesc getLocality() {
        return locality;
    }

    public void setLocality(LocalityDesc locality) {
        this.locality = locality;
    }

    public boolean isBuildSubPartBy() {
        return buildSubpartBy;
    }

    public void setBuildSubPartBy(boolean buildSubpartBy) {
        this.buildSubpartBy = buildSubpartBy;
    }

    public boolean isBuildSubpartBy() {
        return buildSubpartBy;
    }

    public boolean isUseSubPartTemplate() {
        return useSubPartTemplate;
    }

    public void setUseSubPartTemplate(boolean useSubPartTemplate) {
        this.useSubPartTemplate = useSubPartTemplate;
    }

    public List<PartitionSpec> getParentPartSpecs() {
        return parentPartSpecs;
    }

    public void setParentPartSpecs(List<PartitionSpec> parentPartSpecs) {
        this.parentPartSpecs = parentPartSpecs;
    }

    public List<SqlNode> getParentPartSpecAstList() {
        return parentPartSpecAstList;
    }

    public void setParentPartSpecAstList(List<SqlNode> parentPartSpecAstList) {
        this.parentPartSpecAstList = parentPartSpecAstList;
    }

    public boolean isContainNextLevelPartSpec() {
        return containNextLevelPartSpec;
    }

    public void setContainNextLevelPartSpec(boolean containNextLevelPartSpec) {
        this.containNextLevelPartSpec = containNextLevelPartSpec;
    }

    public boolean isTtlTemporary() {
        return ttlTemporary;
    }

    public void setTtlTemporary(boolean ttlTemporary) {
        this.ttlTemporary = ttlTemporary;
    }

    public void setBuildSubpartBy(boolean buildSubpartBy) {
        this.buildSubpartBy = buildSubpartBy;
    }
}
