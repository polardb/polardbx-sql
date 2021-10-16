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

import java.util.List;

/**
 * @author chenmo.cm
 */
public class HintPushProjectOperator extends BaseHintOperator implements HintPushOperator {
    private final String project;
    private final boolean testMode;

    public HintPushProjectOperator(SqlBasicCall hint, boolean testMode, ExecutionContext ec) {
        super(hint, ec);
        this.testMode = testMode;
        String tmpProject = null;
        for (HintArgKey key : getArgKeys()) {
            SqlNode value = this.argMap.get(key);

            if (null == value) {
                continue;
            }

            switch (key.ordinal) {
            case 0:
                tmpProject = RelUtils.stringValue(value);
                break;
            default:
                break;
            } // end of switch
        } // end of for

        this.project = tmpProject;
    }

    @Override
    protected List<HintArgKey> getArgKeys() {
        return HintArgKey.PROJECT_HINT;
    }

    @Override
    public SqlNode handle(SqlSelect origin) {
        SqlSelect result = origin;

        if (null != this.project) {
            if (TStringUtil.isBlank(project)) {
                result.setSelectList(null);
            } else {
                final String sql = HintUtil.buildProjectSql(project);

                final MySqlSelectQueryBlock query = parseQuery(sql);

                final SqlNodeList selectList =
                    FastSqlConstructUtils
                        .constructSelectList(query.getSelectList(), new ContextParameters(testMode), ec);
                final SqlNodeList keywords = FastSqlConstructUtils.constructKeywords(query, null);

                result.setSelectList((SqlNodeList) updateParamIndex(selectList, this.paramIndexMap));
                if (null != keywords) {
                    result.setOperand(0, keywords);
                }
            }
        }

        return result;
    }
}
