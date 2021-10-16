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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.commons.collections.MapUtils;

import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class SingleTableInsert extends SingleTableOperation {

    private Map<Integer, ParameterContext> param;

    public SingleTableInsert(RelNode logicalPlan,
                             ShardProcessor shardProcessor, String tableName, String sqlTemplate,
                             List<Integer> paramIndex, int autoIncParamIndex) {
        super(logicalPlan, shardProcessor, tableName, sqlTemplate, paramIndex, autoIncParamIndex);
    }

    public SingleTableInsert(final SingleTableInsert singleTableInsert) {
        super(singleTableInsert);
    }

    public void setParam(final Map<Integer, ParameterContext> param) {
        this.param = param;
    }

//    @Override
//    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param) {
//        return new Pair<>(dbIndex, this.param);
//    }

    public Pair<String, Map<Integer, ParameterContext>> calcDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                            ExecutionContext ec) {
        return super.getDbIndexAndParam(param, ec);
    }

    @Override
    protected String getExplainName() {
        return "PhyTableOperation";
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        ExplainInfo explainInfo = buildExplainInfo(((RelDrdsWriter) pw).getParams(),
            (ExecutionContext) ((RelDrdsWriter) pw).getExecutionContext());
        pw.item(RelDrdsWriter.REL_NAME, getExplainName());
        String groupAndTableName = explainInfo.groupName;
        if (explainInfo.tableNames != null && explainInfo.tableNames.size() > 0) {
            groupAndTableName += (TStringUtil.isNotBlank(explainInfo.groupName) ? "." : "")
                + "[" + TStringUtil.join(explainInfo.tableNames, ",") + "]";
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
        //pw.done(this);
        return pw;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        SingleTableInsert singleTableInsert = new SingleTableInsert(this);
        return singleTableInsert;
    }
}
