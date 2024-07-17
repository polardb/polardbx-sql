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

package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import java.util.HashMap;
import java.util.List;

import static org.apache.calcite.sql.SqlKind.PERCENT_RANK;

// rank不支持distinct，语义即不支持
public class PercentRank extends Rank {
    private HashMap<List<Object>, Long> rowToRank = new HashMap<>();

    public PercentRank() {
        super();
        returnType = DataTypes.DoubleType;
    }

    public PercentRank(int[] index, int filterArg) {
        super(index, filterArg);
        returnType = DataTypes.DoubleType;
    }

    @Override
    protected void conductAgg(Object value) {
        assert value instanceof Row;
        count++;
        if (!sameRank(lastRow, (Row) value)) {
            rank = count;
            lastRow = (Row) value;
            rowToRank.put(getAggregateKey(lastRow), rank);
        }
    }

    @Override
    public Object eval(Row row) {
        Long rank = rowToRank.get(getAggregateKey(row));
        if (count - 1 <= 0) {
            return 0d;
        }
        return ((double) (rank - 1)) / (count - 1);
    }

    @Override
    public SqlKind getSqlKind() {
        return PERCENT_RANK;
    }

    @Override
    public DataType getReturnType() {
        return returnType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"PERCENT_RANK"};
    }

    @Override
    public Aggregator getNew() {
        return new PercentRank(aggTargetIndexes, filterArg);
    }
}
