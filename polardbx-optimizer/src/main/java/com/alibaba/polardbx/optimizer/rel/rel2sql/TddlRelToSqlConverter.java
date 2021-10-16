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

package com.alibaba.polardbx.optimizer.rel.rel2sql;

import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.mpp.MppExchange;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class TddlRelToSqlConverter extends RelToSqlConverter {

    /**
     * Creates a RelToSqlConverter.
     */
    public TddlRelToSqlConverter(SqlDialect dialect) {
        super(dialect);
    }

    /**
     * remove table prefix
     */
    @Override
    public Result visit(TableScan e) {
        final SqlIdentifier identifier = new SqlIdentifier(ImmutableList.of(Util.last(e.getTable().getQualifiedName())),
            null,
            SqlParserPos.ZERO,
            null,
            e.getIndexNode());
        Result result = result(identifier, ImmutableList.of(Clause.FROM), e, null);

        final List<SqlNode> selectList = new ArrayList<>();

        for (RelDataTypeField field : e.getRowType().getFieldList()) {
            addSelect(selectList, new SqlIdentifier(ImmutableList.of(field.getName()), SqlParserPos.ZERO),
                e.getRowType());
        }

        result.setExpandStar(new SqlNodeList(selectList, POS));
        return result;
    }

    public static RelToSqlConverter createInstance(DbType dbType) {
        return new TddlRelToSqlConverter(dbType.getDialect().getCalciteSqlDialect());
    }

    public static SqlNode relNodeToSqlNode(RelNode relNode, DbType dbType) {
        RelToSqlConverter relToSqlConverter = new TddlRelToSqlConverter(dbType.getDialect().getCalciteSqlDialect());
        return relToSqlConverter.visitChild(0, relNode).asStatement();
    }

    public Result visit(LogicalDynamicValues e) {
        final List<Clause> clauses = ImmutableList.of(Clause.SELECT);
        final Map<String, RelDataType> pairs = ImmutableMap.of();
        final Context context = aliasContext(pairs, false);
        final SqlNodeList selects = new SqlNodeList(POS);
        for (ImmutableList<RexNode> tuple : e.getTuples()) {
            selects.add(ANONYMOUS_ROW.createCall(exprList(context, tuple)));
        }
        SqlNode query = SqlStdOperatorTable.VALUES.createCall(selects);
        return result(query, clauses, e, null);
    }

    public Result visit(Gather e) {
        // LogicalModify will push Gather inside... WTF
        return visitChild(0, e.getInput());
    }

    public Result visit(MppExchange e) {
        if (e.isMergeSortExchange()) {
            //如果e代表归并排序，那么它的意义等同于全量排序sort
            Result x = visitChild(0, e.getInput());
            Builder builder = x.builder(e, Clause.ORDER_BY);
            List<SqlNode> orderByList = Expressions.list();
            for (RelFieldCollation field : e.getCollation().getFieldCollations()) {
                builder.addOrderItem(orderByList, field);
            }
            if (!orderByList.isEmpty()) {
                builder.setOrderBy(new SqlNodeList(orderByList, POS));
                x = builder.result();
            }
            return x;
        } else {
            return visitChild(0, e.getInput());
        }
    }
}
