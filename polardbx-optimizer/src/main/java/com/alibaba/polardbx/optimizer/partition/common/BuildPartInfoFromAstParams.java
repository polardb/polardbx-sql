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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionBy;

import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class BuildPartInfoFromAstParams {
    protected String schemaName;
    protected String tableName;
    protected String tableGroupName;
    protected String joinGroupName;
    protected SqlPartitionBy sqlPartitionBy;
    protected Map<SqlNode, RexNode> boundExprInfo;
    protected List<ColumnMeta> pkColMetas;
    protected List<ColumnMeta> allColMetas;
    protected PartitionTableType tblType;
    protected ExecutionContext ec;
    protected LocalityDesc locality;

    public BuildPartInfoFromAstParams() {
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

    public SqlPartitionBy getSqlPartitionBy() {
        return sqlPartitionBy;
    }

    public void setSqlPartitionBy(SqlPartitionBy sqlPartitionBy) {
        this.sqlPartitionBy = sqlPartitionBy;
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
}
