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

package com.alibaba.polardbx.optimizer.core.join;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

/**
 * LookupPredicate represents batch of equi-conditions by 'IN' or 'NOT IN' expression
 * <p>
 * e.g. a IN (...),  (a, b) IN (...)
 */
public final class LookupPredicate {

    /**
     * IN or NOT IN
     */
    private final boolean not;

    public static class Column {
        /**
         * column identifier
         */
        final SqlIdentifier column;

        /**
         * target indexes of input values
         */
        final int targetIndex;

        /**
         * data types of join keys for comparing
         */
        final DataType dataType;

        Column(SqlIdentifier column, int targetIndex, DataType dataType) {
            this.column = column;
            this.targetIndex = targetIndex;
            this.dataType = dataType;
        }
    }

    private final List<Column> columns = new ArrayList<>();
    private final List<String> lvOriginNames;

    public LookupPredicate(boolean not, List<String> lvOriginNames) {
        this.not = not;
        this.lvOriginNames = lvOriginNames;
    }

    /**
     * Add a equi-predicate
     *
     * @param column identifier of column
     * @param targetIndex index of the target value when values are provided
     */
    public void addEqualPredicate(SqlIdentifier column, int targetIndex, DataType dataType) {
        columns.add(new Column(column, targetIndex, dataType));
    }

    public int size() {
        return columns.size();
    }

    public void merge(LookupPredicate other) {
        columns.addAll(other.columns);
    }

    public SqlOperator getOperator() {
        return not ? SqlStdOperatorTable.NOT_IN : SqlStdOperatorTable.IN;
    }

    public SqlIdentifier getColumn(int i) {
        return columns.get(i).column;
    }

    public int getTargetIndex(int i) {
        return columns.get(i).targetIndex;
    }

    public DataType getTargetType(int i) {
        return columns.get(i).dataType;
    }

    public DataType[] getDataTypes() {
        return columns.stream().map(c -> c.dataType).toArray(DataType[]::new);
    }

    public List<String> getLvOriginNames() {
        return lvOriginNames;
    }

    @Override
    public String toString() {
        return RelUtils.toNativeSql(explain());
    }

    public SqlNode explain() {
        SqlNode left;
        if (size() == 1) {
            left = getColumn(0);
        } else {
            SqlNode[] identifiers = this.columns.stream().map(c -> c.column).toArray(SqlNode[]::new);
            left = new SqlBasicCall(SqlStdOperatorTable.ROW, identifiers, SqlParserPos.ZERO);
        }
        SqlNode right = SqlNodeList.of(SqlLiteral.createSymbol("...", SqlParserPos.ZERO));
        return new SqlBasicCall(getOperator(), new SqlNode[] {left, right}, SqlParserPos.ZERO);
    }
}
