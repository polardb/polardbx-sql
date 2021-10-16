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
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDal;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import com.alibaba.polardbx.common.utils.TStringUtil;

import java.util.List;

/**
 * @author chenmo.cm
 * @date 2018/6/14 上午11:08
 */
public class Dal extends AbstractRelNode {

    protected final SqlDal dal;

    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param traitSet
     */
    public Dal(SqlDal dal, RelDataType rowType, RelOptCluster cluster, RelTraitSet traitSet){
        super(cluster, traitSet);
        this.dal = dal;
        this.rowType = rowType;
    }

    public static Dal create(SqlDal dal, RelDataType rowType, RelOptCluster cluster) {
        return new Dal(dal, rowType, cluster, cluster.traitSetOf(Convention.NONE));
    }

    public SqlKind kind() {
        return this.dal.getKind();
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "DAL");
        String sql = TStringUtil.replace(dal.toSqlString(MysqlSqlDialect.DEFAULT).toString(),
            System.lineSeparator(),
            " ");
        pw.item("sql", sql);
        return pw;
    }

    public SqlDal getAst() {
        return dal;
    }

    @Override
    public SqlNodeList getHints() {
        return dal.getHints();
    }

    @Override
    public RelNode setHints(SqlNodeList hints) {
        dal.setHints(hints);
        return this;
    }

    @Override
    public Dal copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new Dal(dal, rowType, getCluster(), traitSet);
    }
}
