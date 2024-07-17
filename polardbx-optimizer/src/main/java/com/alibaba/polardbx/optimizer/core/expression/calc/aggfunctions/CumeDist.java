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

import static org.apache.calcite.sql.SqlKind.CUME_DIST;

public class CumeDist extends Rank {
    private HashMap<List<Object>, Long> rowToRank = new HashMap<>();

    public CumeDist() {
        super();
        returnType = DataTypes.DoubleType;
    }

    public CumeDist(int[] index, int filterArg) {
        super(index, filterArg);
        returnType = DataTypes.DoubleType;
    }

    @Override
    protected void conductAgg(Object value) {
        assert value instanceof Row;
        count++;
        if (aggTargetIndexes.length > 0) {
            List<Object> rankKey = getAggregateKey((Row) value);
            rowToRank.put(rankKey, count);
        }
    }

    @Override
    public Object eval(Row row) {
        if (aggTargetIndexes.length == 0) {
            return 1;
        }
        Long rankCount = rowToRank.get(getAggregateKey(row));
        if (rankCount == null) {
            throw new NullPointerException();
        }
        if (rankCount <= 0) {
            return 0d;
        }
        return ((double) rankCount) / (count);
    }

    @Override
    public SqlKind getSqlKind() {
        return CUME_DIST;
    }

    @Override
    public DataType getReturnType() {
        return returnType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CUME_DIST"};
    }

    @Override
    public Aggregator getNew() {
        return new CumeDist(aggTargetIndexes, filterArg);
    }
}
