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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 构建 PhyTableOperation, 做 UNION 优化
 * <p>
 *
 * @author xiaoying 2019-11-05 14:52
 */
public class SegmentedPhyTableScanBuilder {

    protected final List<RelNode> relNodes;

    /**
     * SQL 模板,表名已经被参数化
     */

    protected final List<SqlSelect> sqlTemplates;

    /**
     * <pre>
     * key: GroupName
     * values: List of TableNames
     * </pre>
     */
    protected Map<String, List<List<String>>> targetTables;
    /**
     * SQL 参数
     */
    protected final ExecutionContext executionContext;
    protected final RelNode parent;
    protected final DbType dbType;
    protected final List<Integer> paramIndex;
    protected final RelDataType rowType;
    protected final String schemaName;
    protected UnionOptHelper unionOptHelper;
    protected List<String> tableNames;

    public SegmentedPhyTableScanBuilder(List<RelNode> relNodes, List<SqlSelect> sqlTemplates,
                                        Map<String, List<List<String>>> targetTables,
                                        ExecutionContext executionContext, RelNode parent, DbType dbType,
                                        String schemaName, List<String> tableNames) {

        this(relNodes, sqlTemplates, targetTables, executionContext, parent, dbType, parent.getRowType(), schemaName,
            tableNames);
    }

    public SegmentedPhyTableScanBuilder(List<RelNode> relNodes, List<SqlSelect> sqlTemplates,
                                        Map<String, List<List<String>>> targetTables,
                                        ExecutionContext executionContext, RelNode parent, DbType dbType,
                                        RelDataType rowType, String schemaName, List<String> tableNames) {
        this.relNodes = relNodes;
        this.sqlTemplates = sqlTemplates;
        this.targetTables = targetTables;
        this.executionContext = executionContext;
        this.parent = parent;
        this.dbType = dbType;
        this.rowType = rowType;
        this.paramIndex = PlannerUtils.getDynamicParamIndex(this.sqlTemplates.get(0));
        this.schemaName = schemaName;
        this.tableNames = tableNames;
    }

    public List<RelNode> build(Map<String, Object> extraCmd) {
        List<RelNode> resultNodes = new ArrayList<>();
        for (int i = 0; i < sqlTemplates.size(); i++) {
            ReplaceTableNameWithQuestionMarkVisitor visitor =
                new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);
            final SqlNode accept = sqlTemplates.get(i).accept(visitor);

            PhyTableScanBuilder phyTableScanbuilder = new PhyTableScanBuilder((SqlSelect) accept,
                targetTables,
                executionContext,
                relNodes.get(i),
                dbType,
                schemaName,
                tableNames);

            phyTableScanbuilder.setUnionOptHelper(unionOptHelper);
            final List<RelNode> build = phyTableScanbuilder.build(executionContext);
            resultNodes.addAll(build);
        }

        return resultNodes;
    }

    public void setUnionOptHelper(UnionOptHelper unionOptHelper) {
        this.unionOptHelper = unionOptHelper;
    }
}
