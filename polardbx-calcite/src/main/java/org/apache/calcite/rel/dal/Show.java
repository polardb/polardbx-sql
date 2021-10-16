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

package org.apache.calcite.rel.dal;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import com.alibaba.polardbx.common.utils.TStringUtil;

import java.util.List;

/**
 * @author chenmo.cm
 * @date 2018/5/18 下午5:20
 */
public class Show extends Dal {

    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param traitSet
     */
    public Show(SqlShow show, RelDataType rowType, RelOptCluster cluster, RelTraitSet traitSet){
        super(show, rowType, cluster, traitSet);
    }

    public static Show create(SqlShow show, RelDataType rowType,
                              RelOptCluster cluster) {
        return new Show(show, rowType, cluster, cluster.traitSetOf(Convention.NONE));
    }

    @Override
    public SqlKind kind(){
        return ((SqlShow)this.dal).getShowKind();
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "SHOW");
        String sql = TStringUtil.replace(getAst().toSqlString(MysqlSqlDialect.DEFAULT).toString(), System.lineSeparator(), " ");
        pw.item("sql", sql);
        return pw;
    }

    @Override
    public Show copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new Show((SqlShow) dal, rowType, getCluster(), traitSet);
    }
}
