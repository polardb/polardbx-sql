/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterTable extends DDL {
    /**
     * All the rex expr info collected in alter table ddl
     *
     * <pre>
     *     The rex expr info is collected by Validator.
     *     For example 1:
     *      e.g. For the alert stmt: "alter table list_tbl modify partition add values ( 1, 2+3, 4+5 ) ",
     *      then the rexExprInfos will as followed:
     *          SqlLiteral:1(key) --> RexLiteral:1(val)
     *          SqlCall:2+3(key) --> RexCall:2+3(val)
     *          SqlCall:4+5(key) --> RexCall:4+5(val)
     *    , and this info will be useful in calc the bound value of list partitions.
     *
     *    For example 2:
     *      e.g. For the alert stmt: "alter table rng_col_tbl add partition ( partition p4 values less than (1+3+4, '2020-12-12', 'NewYork') ) ENGINE = InnoDB ) ",
     *      then the rexExprInfos will as followed:
     *          SqlCall:1+3+4(key) --> RexCall:1+3+4(val)
     *          SqlLiteral:'2020-12-12'(key) --> RexLiteral:'2020-12-12'(val)
     *          SqlLiteral:'NewYork'(key) --> RexLiteral:'NewYork'(val)
     *      , and this info will be useful in calc the bound value of range columns partitions.
     *  </pre>
     */
    protected Map<SqlNode, RexNode> allRexExprInfo;

    protected AlterTable(RelOptCluster cluster, RelTraitSet traits, SqlNode sqlNode, SqlNode tableNameNode,
                         Map<SqlNode, RexNode> allRexExprInfo) {
        super(cluster, traits, null);
        this.sqlNode = sqlNode;
        this.allRexExprInfo = allRexExprInfo;
        this.setTableName(tableNameNode);
    }

    public static AlterTable create(RelOptCluster cluster, SqlNode sqlNode, SqlNode tableName,
                                    Map<SqlNode, RexNode> allRexExprInfo) {
        return new AlterTable(cluster, cluster.traitSet(), sqlNode, tableName, allRexExprInfo);
    }

    @Override
    public AlterTable copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTable(this.getCluster(), traitSet, this.sqlNode, getTableName(), allRexExprInfo);
    }

    public Map<SqlNode, RexNode> getAllRexExprInfo() {
        return allRexExprInfo;
    }
}
