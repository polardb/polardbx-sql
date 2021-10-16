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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.TableName;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.GenericDdl;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.MapUtils;

import java.util.List;
import java.util.Map;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */

public class PhyDdlTableOperation extends BaseTableOperation {

    private List<List<String>> tableNames;
    private Map<Integer, ParameterContext> param;
    private String logicalTableName;
    private String renameLogicalTableName;
    private boolean explain;
    private boolean shadowDbOnly;
    private boolean partitioned;
    private TableRule tableRule;
    public SequenceBean sequence;
    private boolean ifNotExists;
    private List<Pair<String, String>> logicalTablePairs;
    private boolean isHint;
    private PartitionInfo partitionInfo;

    /**
     * better not to rely on DDL.class
     */
    public PhyDdlTableOperation(DDL relNode) {
        super(relNode.getCluster(), relNode.getTraitSet(), relNode.getRowType(), null, relNode);
        if (relNode.getTableName() != null) {
            logicalTableName = Util.last(((SqlIdentifier) relNode.getTableName()).names);
        }
        this.logicalPlan = relNode;
        final SqlNode querySelNode = relNode.sqlNode;
        if (querySelNode instanceof SqlCreateTable) {
            final boolean ifNotExists = ((SqlCreateTable) querySelNode).isIfNotExists();
            this.ifNotExists = ifNotExists;
            sequence = ((SqlCreateTable) querySelNode).getAutoIncrement();
        }
    }

    public static PhyDdlTableOperation create(String schemaName,
                                              String tableName,
                                              ExecutionContext executionContext) {
        final RelOptCluster cluster =
            SqlConverter.getInstance(executionContext).createRelOptCluster(null);
        GenericDdl genericDdl = GenericDdl.create(
            cluster,
            new SqlIdentifier(schemaName, SqlParserPos.ZERO),
            new SqlIdentifier(tableName, SqlParserPos.ZERO)
        );
        return new PhyDdlTableOperation(genericDdl);
    }

    private PhyDdlTableOperation(PhyDdlTableOperation operation) {
        super(operation);
        tableNames = operation.tableNames;
        param = operation.param;
        logicalTableName = operation.logicalTableName;
        if (operation.renameLogicalTableName != null) {
            renameLogicalTableName = operation.renameLogicalTableName;
        }
        explain = operation.explain;
        shadowDbOnly = operation.shadowDbOnly;
        partitioned = operation.partitioned;
        tableRule = operation.tableRule;
        sequence = operation.sequence;
        ifNotExists = operation.ifNotExists;
        logicalTablePairs = operation.logicalTablePairs;
        isHint = operation.isHint;
        this.logicalPlan = operation.logicalPlan;
        this.partitionInfo = operation.partitionInfo;
    }

    @Override
    public List<List<String>> getTableNames() {
        return tableNames;
    }

    public void setTableNames(List<List<String>> tableNames) {
        this.tableNames = tableNames;
    }

    public void setParam(Map<Integer, ParameterContext> param) {
        this.param = param;
    }

    public String getLogicalTableName() {
        return logicalTableName;
    }

    public void setLogicalTableName(String logicalTableName) {
        this.logicalTableName = logicalTableName;
    }

    public Map<Integer, ParameterContext> getParam() {
        return param;
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           ExecutionContext executionContext) {
        return new Pair<>(dbIndex, this.param);
    }

    @Override
    public String getExplainName() {
        return "PhyTableOperation";
    }

    @Override
    protected ExplainInfo buildExplainInfo(Map<Integer, ParameterContext> params, ExecutionContext executionContext) {
        /**
         * 由于 PhyTableOperation 的物理SQL对应的叁数化参数（即this.param）
         * 全部会在
         *  PhyTableDDLViewBuilder
         *  中计算完成，所以这里就不再需要依赖逻辑SQL级别的参数化参数（即传入参数 param）进行重新计算
         */
        return new ExplainInfo(tableNames, dbIndex, this.param);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        ExplainInfo explainInfo = buildExplainInfo(((RelDrdsWriter) pw).getParams(),
            (ExecutionContext) ((RelDrdsWriter) pw).getExecutionContext());
        pw.item(RelDrdsWriter.REL_NAME, getExplainName());
        String groupAndTableName = explainInfo.groupName;
        if (explainInfo.tableNames != null && explainInfo.tableNames.size() > 0) {
            groupAndTableName += (TStringUtil.isNotBlank(explainInfo.groupName) ? "." : "")
                + TStringUtil.join(explainInfo.tableNames, ",");
            pw.itemIf("tables", groupAndTableName, groupAndTableName != null);
        } else {
            pw.itemIf("groups", groupAndTableName, groupAndTableName != null);
        }
        String sql = TStringUtil.replace(getNativeSql(), "\n", " ");
        pw.item("sql", sql);
        StringBuilder builder = new StringBuilder();
        if (MapUtils.isNotEmpty(explainInfo.params)) {
            String operator = "";
            for (Object c : explainInfo.params.values()) {
                Object v = ((ParameterContext) c).getValue();
                builder.append(operator);
                if (v instanceof TableName) {
                    builder.append(((TableName) v).getTableName());
                } else {
                    builder.append(v == null ? "NULL" : v.toString());
                }
                operator = ",";
            }
            pw.item("params", builder.toString());
        }
        // pw.done(this);
        return pw;
    }

    public boolean isShadowDbOnly() {
        return shadowDbOnly;
    }

    public boolean isExplain() {
        return explain;

    }

    public boolean isPartitioned() {
        return partitioned;
    }

    public TableRule getTableRule() {
        return tableRule;
    }

    @Override
    public String getNativeSql() {
        return sqlTemplate;
    }

    public void setExplain(boolean explain) {
        this.explain = explain;
    }

    public void setShadowDbOnly(boolean shadowDbOnly) {
        this.shadowDbOnly = shadowDbOnly;
    }

    public void setPartitioned(boolean partitioned) {
        this.partitioned = partitioned;
    }

    public void setTableRule(TableRule tableRule) {
        this.tableRule = tableRule;
    }

    public boolean isTempTable() {
        return false;
    }

    public String getNewLogicalTableName() {
        return renameLogicalTableName;
    }

    public void setRenameLogicalTableName(String renameLogicalTableName) {
        this.renameLogicalTableName = renameLogicalTableName;
    }

    public SequenceBean getSequence() {
        return sequence;
    }

    public void setSequence(SequenceBean sequence) {
        this.sequence = sequence;
    }

    public void setIfNotExists(final boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public Pair<String, String> getLogicalTablePair() {
        return new Pair<>(getLogicalTableName(), getNewLogicalTableName());
    }

    public boolean isHint() {
        return isHint;
    }

    public void setHint(boolean hint) {
        isHint = hint;
    }

    public PhyDdlTableOperation copy() {
        return new PhyDdlTableOperation(this);
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(PartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

}
