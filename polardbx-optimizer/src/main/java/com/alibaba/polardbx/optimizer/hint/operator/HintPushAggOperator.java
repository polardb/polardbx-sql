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

package com.alibaba.polardbx.optimizer.hint.operator;

import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.LinkedList;
import java.util.List;

/**
 * @author chenmo.cm
 */
public class HintPushAggOperator extends BaseHintOperator implements HintPushOperator {

    private final String agg;
    private final String groupBy;
    private final String having;
    private final boolean testMode;

    public HintPushAggOperator(SqlBasicCall hint, boolean testMode, ExecutionContext ec) {
        super(hint, ec);
        this.testMode = testMode;
        String tmpAgg = null;
        String tmpGroupBy = null;
        String tmpHaving = null;

        for (HintArgKey key : getArgKeys()) {
            SqlNode value = this.argMap.get(key);

            if (null == value) {
                continue;
            }

            switch (key.ordinal) {
            case 0:
                tmpAgg = RelUtils.stringValue(value);
                break;
            case 1:
                tmpGroupBy = RelUtils.stringValue(value);
                break;
            case 2:
                tmpHaving = RelUtils.stringValue(value);
            default:
                break;
            } // end of switch
        } // end of for

        this.agg = tmpAgg;
        this.groupBy = tmpGroupBy;
        this.having = tmpHaving;
    }

    @Override
    protected List<HintArgKey> getArgKeys() {
        return HintArgKey.AGG_HINT;
    }

    @Override
    public SqlNode handle(SqlSelect origin) {
        SqlSelect result = origin;

        if (null == agg && null == groupBy && null == having) {
            return result;
        }

        String sql = HintUtil.buildAggSql(agg, groupBy, having);

        final MySqlSelectQueryBlock query = parseQuery(sql);

        if (null != having) {
            if (TStringUtil.isBlank(having) && (null == groupBy || TStringUtil.isNotBlank(groupBy))) {
                // clear having
                result.setHaving(null);
            } else {
                SqlNode havingNode =
                    FastSqlConstructUtils.constructHaving(query.getGroupBy(), new ContextParameters(testMode), ec);
//                if (testMode) {
//                    havingNode = havingNode.accept(new ReplaceTableNameWithTestTableVisitor(testMode));
//                }

                if (null != havingNode) {
                    result.setHaving(havingNode);
                }
            }
        }

        if (null != groupBy) {
            if (TStringUtil.isBlank(groupBy)) {
                // clear group by
                result.setGroupBy(null);
                result.setHaving(null);
            } else {
                final SqlNodeList groupByList =
                    FastSqlConstructUtils.constructGroupBy(query.getGroupBy(), new ContextParameters(testMode), ec);
                if (null != groupByList) {
                    result.setGroupBy(groupByList);
                }
            }
        }

        if (TStringUtil.isNotBlank(agg)) {
            // append agg
            final SqlNodeList aggList =
                FastSqlConstructUtils.constructSelectList(query.getSelectList(), new ContextParameters(testMode), ec);

            List<SqlNode> sqlNodes = new LinkedList<>();
            SqlNodeList selectList = result.getSelectList();
            if (null != selectList) {
                sqlNodes.addAll(selectList.getList());
            }
            sqlNodes.addAll(aggList.getList());

            result.setSelectList(new SqlNodeList(sqlNodes, SqlParserPos.ZERO));
        }

        return result;
    }

}
