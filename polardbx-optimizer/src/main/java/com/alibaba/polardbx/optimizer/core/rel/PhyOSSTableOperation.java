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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.google.protobuf.ByteString;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;

// todo
public class PhyOSSTableOperation extends AbstractRelNode implements IPhyQueryOperation {
    private RelNode logicalPlan;
    protected String dbIndex;
    protected String schemaName;
    protected String sqlTemplate;
    protected SqlNode nativeSqlNode;
    protected DbType dbType;
    protected SqlKind kind = SqlKind.SELECT;
    protected boolean useDbIndex = false; // Use by scale out write
    private int unionSize = 1;
    protected CursorMeta cursorMeta;
    protected int affectedRows = -1;
    protected Map<Integer, ParameterContext> param;
    protected ByteString sqlDigest = null;
    protected boolean replicateRelNode = false;

    public PhyOSSTableOperation(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelDataType rowType,
        CursorMeta cursorMeta,
        RelNode logicalPlan,
        SqlNode nativeSqlNode,
        String sqlTemplate,
        DbType dbType
    ) {
        super(cluster, traitSet.replace(DrdsConvention.INSTANCE));
        this.sqlTemplate = sqlTemplate;
        this.nativeSqlNode = nativeSqlNode;
        this.dbType = dbType;
        this.logicalPlan = logicalPlan;
        this.sqlTemplate = sqlTemplate;
        this.nativeSqlNode = nativeSqlNode;
        this.rowType = rowType;
        this.cursorMeta = cursorMeta;
        if (cursorMeta == null && rowType != null) {
            this.cursorMeta = CursorMeta.build(CalciteUtils.buildColumnMeta(rowType, "TableScan"));
        }
        this.logicalPlan = logicalPlan;
        if (logicalPlan != null && logicalPlan instanceof AbstractRelNode) {
            this.schemaName = ((AbstractRelNode) logicalPlan).getSchemaName();
        }
    }

    public void setParam(Map<Integer, ParameterContext> param) {
        this.param = param;
    }

    @Override
    public String getNativeSql() {
        return this.sqlTemplate;
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           ExecutionContext executionContext) {
        /**
         * 由于 PhyTableOperation 的物理SQL对应的叁数化参数（即this.param）
         * 全部会在
         *  PhyTableScanBuilder 或 PhyTableScanBuilderForMpp 或 PhyTableModifyBuilder
         *  中计算完成，所以这里就不再需要依赖逻辑SQL级别的参数化参数（即传入参数 param）进行重新计算
         */
        return new Pair<>(dbIndex, this.param);
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           List<List<String>> phyTableNamesOutput,
                                                                           ExecutionContext executionContext) {
        throw new NotSupportException();
    }
}
